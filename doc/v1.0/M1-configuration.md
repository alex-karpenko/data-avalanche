---
updated: 2026-09-27
domain: Configuration
tags: [v1, implementation-plan, m1, configuration, yaml, validation, v-rules, merge, reload-diff, secrets]
sources: []
---

# M1 — Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `data-avalanche-config`, which turns the YAML config file into a validated, fully
resolved `ResolvedConfig`. It reports every V-rule violation with its rule ID, YAML path and
line/column, and classifies each change between two configs for reload.

**Architecture:** Loading runs in phases (config spec §11).

- **Phase 0:** a strict YAML 1.2 parse (`serde-saphyr`) into a *raw* model. Every field is optional,
  and every scalar is a `Leaf<T>`: the source text plus its line/column, not yet coerced.
- **Phase 1:** a structural pass over the merged pipelines (`defaults` + pipeline, §8). Templates
  are unknown here, so value checks skip them.
- **Phases 2 and 3:** a resolving pass. It substitutes `${VAR}`, reads the referenced files, runs
  the same rules again and builds the typed `ResolvedConfig` with every spec default applied.

Each V-rule is a plain function over the raw model that reports into a `Ctx`. The §13 table is also
kept as data (`RULES`), and tests check that every rule has a passing and a failing case. The reload
diff flattens two resolved configs to JSON paths and classifies each changed path by its
mutability class (§1.2).

**Tech Stack:** Rust 1.98.1 (edition 2024).

| Crate | Version / features | Used for |
|---|---|---|
| `serde-saphyr` | `=1.3.0`, `deserialize` only | Strict parse: duplicate keys are errors; booleans only `true`/`false`; `Spanned` locations |
| `serde` / `serde_json` | | |
| `thiserror` 2 | | |
| `humantime` 2.4 | | Durations |
| `tracing-subscriber` 0.3 | `env-filter` | `log.level` syntax, V-017 |
| `rustls-pki-types` 1 | | PEM parsing, V-068 |
| `xxhash-rust` 0.8 | `xxh3` | Secret fingerprints |
| `tempfile` 3 | dev | |
| `rcgen` 0.14 | dev; `aws_lc_rs` + `pem`, no default features | A throwaway TLS pair generated per test run (no key material in the repository) |

**Spec:**
- `doc/specs/Configuration_Specification.md`, all of it. §13 is the rule table and §14 the example
  this plan must load.
- `doc/v1.0/00-roadmap.md`: §4 M1 scope and exit, P-3/P-4 defaults, §7 S-A/S-B/S-E (already applied).
- `doc/research/API_RBAC_and_Versioning.md` (validate endpoint, roles).
- `doc/research/Clustering_Architecture_Research.md` §5.6 (cluster anchor).
- `doc/research/Multi_Sink_Architecture_and_Delta_Lake.md` (Delta table properties).

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. `unsafe_code = "forbid"`.
  `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all --check` must pass
  before every commit.
- **Public-API conventions** (Task 1 writes them into `data-avalanche-core`'s crate docs; every
  data-avalanche library crate follows them):
  - every public type is re-exported at the crate root;
  - a type with a canonical text form implements `Display` and `FromStr`, and the two round-trip;
  - errors are `thiserror` types named after what failed, and `FromStr::Err` is that type.
  
  Config diagnostics are data (`Diagnostic`), not errors.
- `serde-saphyr` is pinned `=1.3.0` with `default-features = false, features = ["deserialize"]`.
  Parse options: `strict_booleans: true`, `DuplicateKeyPolicy::Error`. Every raw struct has
  `#[serde(deny_unknown_fields)]`.
- **Every diagnostic names its rule.** A V-rule ID (`V-001`…`V-117`), `PARSE` (YAML syntax,
  structure, unknown key, or a required field with no V-rule), or `TYPE` (a scalar that doesn't
  coerce and has no V-rule). An enum field that fails to coerce reports the enum's own V-rule, e.g.
  `sink.type: parquet` → V-083. A malformed `${` is V-063.
- **Severity alone decides the action** (§1.4, §13 intro). Any error rejects the whole config; a
  warning never blocks. Locus (`config`, `pipeline <name>`, `reload`) is only for grouping.
- **Diagnostics carry `path`** in dotted form: `pipelines.orders.sink.flush.seconds`. A key that
  contains `.`, brackets, quotes or whitespace prints as `["key"]`, e.g.
  `tables.primary_key["public.orders"]`. They carry `loc` (1-based line/column) whenever the value
  came from the file.
- **Secrets never print.** `Secret`'s `Debug` is `***`; its `Serialize` is `xxh3:<32 hex>`. No
  diagnostic message contains a secret value.
- Defaults changed by the roadmap are already in the spec (S-A, S-B):
  - `flush.seconds` **30**;
  - `replication_type` **gtid**;
  - `tokens` 128, `lease_duration` 15s, `lease_renewal_interval` 5s;
  - `flush.rows` 100000, `flush.bytes` 512MiB.
- Delta `table_properties` allow-list (V-093, Q-M1-1): `delta.dataSkippingNumIndexedCols`,
  `delta.dataSkippingStatsColumns`, `delta.deletedFileRetentionDuration`, `delta.targetFileSize`,
  `delta.tuneFileSizesForRewrites`, `delta.checkpoint.writeStatsAsJson`,
  `delta.checkpoint.writeStatsAsStruct`. Any other `delta.*` key is an error; non-`delta.` keys pass
  through.
- **No key material in the repository.** Tests that need PEM generate a self-signed pair at run
  time with `rcgen`, using the workspace's `aws-lc-rs` provider.
- `LogicalType` nesting limit `MAX_LIST_DEPTH = 8` (`list<…>` levels).
- The crate performs file I/O only in the resolving pass (Phase 2). It reads env vars only through
  the `Env` trait, and it makes no network calls.
- Code blocks are the verified content, already `rustfmt`-formatted. Diffs are `git apply`-ready;
  apply them with `git apply` or by hand.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

These are the inputs most likely to hurt someone who writes this config by hand. Each is pinned by
a test in the task named after the arrow.

1. **An unquoted `${VAR}` inside a flow collection**, e.g. `{ password: ${PW} }`. YAML reads `{` as
   a nested mapping, and the parse error says nothing about templates. The error must suggest
   quoting → Task 2 (`unquoted_template_in_flow_mapping_fails_with_a_hint`).
2. **An explicit `null` in a pipeline over a `defaults` value.**
   - For a tri-state `flush` field, `null` disables the trigger, while an absent field inherits.
   - For a list, `null` means "no list".
   
   Merging with `Option::or` on the parsed value loses the difference → Task 2
   (`distinguishes_absent_null_and_value`), Task 7 (`lists_replace_maps_merge_by_key_and_null_overrides`).
3. **A rotated `credentials_file`**: same path, new content. The config text doesn't change, but the
   reload must see a change → Task 13 (`rotated_credentials_file_is_detected_without_a_text_change`).
4. **A template in a union discriminator**, e.g. `sink.type: ${SINK_TYPE}`. Which `defaults`
   branch to drop is unknown until Phase 2. Dropping the wrong one, or failing, breaks per-environment
   files → Task 7 (`template_discriminator_keeps_only_the_pipelines_blocks`), Task 9
   (`template_in_a_union_discriminator_resolves_in_phase_2`).
5. **A secret reaching a log or the API** through `Debug`, the resolved-config JSON, a diagnostic
   message or the reload diff → Task 9 (`secrets_never_print`), Task 13 (the diff compares
   fingerprints).

## Decisions and rulings

**Asked and answered (2026-09-27).** The spec edits S-H…S-L are approved and applied to
`Configuration_Specification.md` (roadmap §7):

| # | Question | Answer |
|---|---|---|
| Q-M1-1 | What does V-093 allow? | The 7-key allow-list in Global Constraints. Other `delta.*` keys are errors; non-`delta.` keys pass through. Spec edit **S-I** |
| Q-M1-2 | Which rule does a diagnostic with no V-rule carry? | `PARSE` or `TYPE`. Spec edit **S-H** (a note in §13) |
| Q-M1-3 | What does `defaults.state.prefix` mean? | A parent prefix, applied only to explicit state locations (`state.bucket` set). No effect and no warning when state is co-located. Spec edit **S-J** (§9.6, §10) |
| Q-M1-4 | Must an operator write `mysql: {}` / `delta: {}`? | **No.** Every field of those two blocks has a default, so an absent block means all defaults. `postgres` (`publication`, `slot`) and `iceberg` (`catalog`) have required fields, so their blocks stay required. Spec edit **S-K** (V-001, V-084, §9.2, §9.4) |
| Q-M1-5 | PostgreSQL `include` without a schema? | New rule **V-117**: PostgreSQL `include`/`exclude` patterns are `schema.table`. Spec edit **S-L** |

**Rulings where the spec is silent** (each is pinned by a test; flag any you disagree with):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M1.1 | The roadmap's `Resolvable<T>` becomes `Leaf<T>`: source text + location, coerced when a pass reads it | A leaf is literal or template only relative to a pass; one type serves both passes and keeps the location for every diagnostic | Task 2, Task 6 |
| D-M1.2 | Both passes run the same rule functions; `Report` sorts and deduplicates | One rule implementation, no drift between Phase 1 and Phase 2 | Task 9 `structural_pass_stops_before_env_and_files` |
| D-M1.3 | A template in a discriminator (`source.type`, `sink.type`, catalog block choice) is supported. Phase 1 keeps every branch, and Phase 2 re-merges with the resolved value | Per-environment files are the reason templates exist | Review Focus 4 |
| D-M1.4 | A catalog `type` from `defaults` is dropped when the pipeline picks a catalog block | Otherwise `defaults: {type: glue}` + pipeline `rest: {…}` contradicts itself (V-002) through no fault of the pipeline | Task 7 `catalog_type_from_defaults_is_dropped_when_the_pipeline_picks_a_block` |
| D-M1.5 | *Superseded by Q-M1-4.* An absent `mysql` / `delta` block takes every default; `postgres` / `iceberg` stay required | Owner decision (S-K) | Task 9 `absent_mysql_and_delta_blocks_take_every_default`; Task 10 cases V-001, V-084 |
| D-M1.6 | Required fields with no V-rule are `PARSE` errors, checked after merge: `catalog.glue.name`, `catalog.hive.url`, `catalog.rest.url`, partition `column`/`transform` | A strict parse would report them as missing fields; the raw model defers that to after the merge | Task 14 cases `PARSE` |
| D-M1.7 | V-069 and V-087 have no failing input in v1. V-069's only file is the Hive keytab, and V-115 refuses Hive in Phase 1. V-087 fails only if a logical type doesn't map to a sink, and every grammar type maps to both | Recorded in the harness as `NO_FAILING_INPUT`; `material::read_file_ref` unit-tests V-069's check | Task 10 harness |
| D-M1.8 | V-043 warns whenever Hive TLS is on without `tls.ca` | A private CA can't be detected from the config | Task 12 case V-043 |
| D-M1.9 | `n`/`width` on a transform that doesn't take it is a V-107 error | The spec only states the required side; a stray parameter is almost certainly a mistake | Task 11 case V-107 |
| D-M1.10 | V-085 fires only when `sink.iceberg.strategy` is set explicitly | The default must not make a valid `changelog` config invalid | Task 12 case V-085 |
| D-M1.11 | V-026 / V-072: a value outside the hard range is one error, not an error plus a warning | One diagnostic per problem | Task 11 cases |
| D-M1.12 | *Superseded by Q-M1-5.* PostgreSQL patterns without a schema part are rejected by the new V-117 | Owner decision (S-L) | Task 12 cases V-117 |
| D-M1.13 | The validation endpoint returns only the report, never the resolved config | Config §11.1 keeps secrets out of the API; `API_RBAC` note 2's "echoes the resolved config" is superseded | Task 9 `validate_str` |
| D-M1.14 | No key material is committed. Tests generate a throwaway self-signed pair (CN `data-avalanche-test`) with the `rcgen` dev-dependency | Owner decision: a committed private key is never acceptable, even a test one | Task 8 (`material` unit tests), Task 9 (`tests/support` `cert()` / `key()`) |
| D-M1.15 | Secrets serialize as an xxh3 fingerprint | The reload diff sees a rotated secret without holding its value | Review Focus 3 |

---

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` | Workspace: new member dependency versions (serde-saphyr, humantime, tracing-subscriber, rustls-pki-types, xxhash-rust, tempfile, rcgen) |
| `crates/data-avalanche-core/src/{lib,logical,decimal}.rs` | API conventions doc, `MAX_LIST_DEPTH`, `FromStr for Decimal` |
| `crates/data-avalanche-config/Cargo.toml` | Crate manifest |
| `src/lib.rs` | Modules, root re-exports, crate docs |
| `src/leaf.rs` | `Leaf<T>` (text + `Loc`), `Tri<T>`, the `tri` deserializer |
| `src/path.rs` | `YamlPath`: the dotted/bracketed path in diagnostics |
| `src/diag.rs` | `Diagnostic`, `Severity`, `Locus`, `Report` (sorted, deduplicated, JSON), `PARSE`/`TYPE` |
| `src/parse.rs` | Strict `serde-saphyr` parse → `Diagnostic` with location and the flow-template hint |
| `src/units.rs` | Duration, `ByteSize`, `HostPort` |
| `src/coerce.rs` | `LeafType`: text → typed value, with the rule to report on failure |
| `src/enums.rs` | Every config enum, each tied to its V-rule |
| `src/raw.rs` | The parsed model: one `Raw*` struct per spec block, all fields optional |
| `src/template.rs` | `${VAR}` / `${VAR:-d}` / `$${` expansion, `Env` |
| `src/ctx.rs` | `Ctx` (pass, env, report) and `At` (locus + path) |
| `src/registry.rs` | `RULES`: the §13 master table as data |
| `src/merge.rs` | §8 merge, including the three unions |
| `src/secret.rs` | `Secret`, `fingerprint` |
| `src/material.rs` | Credential files, token files, other files, PEM (V-067…V-071) |
| `src/resolved.rs` | `ResolvedConfig` and every resolved block (§11.1) |
| `src/build.rs` | Raw + merged → resolved, applying every default |
| `src/load.rs` | `load_str` / `load_file` / `validate_str`: the phase pipeline |
| `src/rules/{mod,structure,identity,domain,cross,auth}.rs` | The config-time V-rules, grouped as in §13 |
| `src/mutability.rs` | Path pattern → R / PR-if-table-set-changes / PR / PROC |
| `src/diff.rs` | Reload diff, pipeline add/remove/rename/modify, V-061 |
| `src/external.rs` | Rules that need runtime input: cluster anchor, table set, source schema |
| `tests/parse.rs`, `tests/raw.rs`, `tests/merge.rs`, `tests/load.rs`, `tests/diff.rs`, `tests/external.rs` | Behaviour per phase |
| `tests/rules/{main,cases,structure,identity,resolution,domain,cross,auth}.rs` | Rule harness: 256 cases, each an edit of a clean base config |
| `tests/support/mod.rs` | Shared fixtures: minimal config, spec example, env, temp files, a generated TLS pair (`cert()`, `key()`) |
| `tests/fixtures/spec_example.yaml` | Verbatim copy of config spec §14 |

`src/…` and `tests/…` are relative to `crates/data-avalanche-config/`.

---

### Task 1: Core — list-depth limit, `FromStr for Decimal`, API conventions

**Files:**
- Modify: `crates/data-avalanche-core/src/decimal.rs`, `crates/data-avalanche-core/src/lib.rs`, `crates/data-avalanche-core/src/logical.rs`

**Interfaces:**
- Consumes: M0's `data_avalanche_core::{LogicalType, Decimal}`.
- Produces:
  - `data_avalanche_core::MAX_LIST_DEPTH: usize = 8`. `LogicalType::from_str` rejects deeper
    `list<…>` nesting with `LogicalTypeError`.
  - `impl FromStr for Decimal` (`Err = DecimalError`), identical to `Decimal::parse`.
  - The public-API conventions, written in the crate docs.

This settles the two M1 items recorded in roadmap §9.2: the list-depth limit and the API convention.
Config input reaches `LogicalType` through `column_type_overrides`, so unbounded nesting would be a
stack-depth hazard.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-core/src/decimal.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/decimal.rs b/crates/data-avalanche-core/src/decimal.rs
index 7690ef2..ae5a6e5 100644
--- a/crates/data-avalanche-core/src/decimal.rs
+++ b/crates/data-avalanche-core/src/decimal.rs
@@ -181,6 +191,14 @@ mod tests {
         }
     }
 
+    #[test]
+    fn from_str_is_parse() {
+        let d: Decimal = "-12.340".parse().unwrap();
+        assert_eq!(d, Decimal::parse("-12.340").unwrap());
+        assert_eq!(d.to_string(), "-12.340");
+        assert_eq!("NaN".parse::<Decimal>(), Decimal::parse("NaN"));
+    }
+
     #[test]
     fn displays_leading_zeros_for_small_values() {
         assert_eq!(Decimal::new(5, 4).to_string(), "0.0005");
```

Change `crates/data-avalanche-core/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/lib.rs b/crates/data-avalanche-core/src/lib.rs
index 0c8bbbe..3181530 100644
--- a/crates/data-avalanche-core/src/lib.rs
+++ b/crates/data-avalanche-core/src/lib.rs
@@ -1,4 +1,10 @@
 //! Domain types shared by every data-avalanche crate. Nothing in this crate performs I/O.
+//!
+//! Public-API conventions (every data-avalanche library crate follows them):
+//! - every public type is re-exported at the crate root, so callers import `data_avalanche_core::X`;
+//! - a type with a canonical text form implements `Display` and `FromStr`, and the two round-trip;
+//! - errors are `thiserror` types named after what failed (`DecimalError`, `PositionError`), and
+//!   `FromStr::Err` is that type.
 
 pub mod decimal;
 pub mod event;
@@ -9,7 +15,7 @@ pub mod value;
 
 pub use decimal::{Decimal, DecimalError, MAX_DECIMAL_PRECISION};
 pub use event::{CdcEvent, EventShapeError, Op, SourceSeq, TransactionBoundary};
-pub use logical::{LogicalType, LogicalTypeError};
+pub use logical::{LogicalType, LogicalTypeError, MAX_LIST_DEPTH};
 pub use position::{PositionError, SourcePosition};
 pub use table::{
     INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, TableRef, first_reserved_collision, is_reserved_column,
```

Change `crates/data-avalanche-core/src/logical.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/logical.rs b/crates/data-avalanche-core/src/logical.rs
index 52b1b40..7391102 100644
--- a/crates/data-avalanche-core/src/logical.rs
+++ b/crates/data-avalanche-core/src/logical.rs
@@ -216,6 +226,18 @@ mod tests {
         }
     }
 
+    #[test]
+    fn bounds_list_nesting() {
+        let nested = |n: usize| format!("{}int64{}", "list<".repeat(n), ">".repeat(n));
+        assert!(nested(MAX_LIST_DEPTH).parse::<LogicalType>().is_ok());
+        let err = nested(MAX_LIST_DEPTH + 1)
+            .parse::<LogicalType>()
+            .unwrap_err();
+        assert!(err.reason.contains("deeper than 8"), "{err}");
+        // Far past the limit: rejected without exhausting the stack.
+        assert!(nested(100_000).parse::<LogicalType>().is_err());
+    }
+
     #[test]
     fn error_names_the_input() {
         let err = "decimal(99,1)".parse::<LogicalType>().unwrap_err();
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-core`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved import `logical::MAX_LIST_DEPTH`
error[E0425]: cannot find value `MAX_LIST_DEPTH` in this scope
error[E0277]: the trait bound `decimal::Decimal: FromStr` is not satisfied
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-core/src/decimal.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/decimal.rs b/crates/data-avalanche-core/src/decimal.rs
index 7690ef2..ae5a6e5 100644
--- a/crates/data-avalanche-core/src/decimal.rs
+++ b/crates/data-avalanche-core/src/decimal.rs
@@ -1,4 +1,5 @@
 use std::fmt;
+use std::str::FromStr;
 
 use thiserror::Error;
 
@@ -137,6 +138,15 @@ impl Decimal {
     }
 }
 
+impl FromStr for Decimal {
+    type Err = DecimalError;
+
+    /// Same as [`Decimal::parse`].
+    fn from_str(s: &str) -> Result<Self, Self::Err> {
+        Self::parse(s)
+    }
+}
+
 impl fmt::Display for Decimal {
     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
         let sign = if self.unscaled < 0 { "-" } else { "" };
```

Change `crates/data-avalanche-core/src/logical.rs`:

```diff
diff --git a/crates/data-avalanche-core/src/logical.rs b/crates/data-avalanche-core/src/logical.rs
index 52b1b40..7391102 100644
--- a/crates/data-avalanche-core/src/logical.rs
+++ b/crates/data-avalanche-core/src/logical.rs
@@ -5,6 +5,10 @@ use thiserror::Error;
 
 use crate::MAX_DECIMAL_PRECISION;
 
+/// Deepest `list<…>` nesting the grammar accepts. Config input is operator-written, so the recursive
+/// parser needs a bound; real schemas nest two or three levels at most.
+pub const MAX_LIST_DEPTH: usize = 8;
+
 /// Sink-neutral logical type — the grammar of `tables.column_type_overrides` (config §9.3).
 ///
 /// This is *not* the grammar of the `data-avalanche.logical` Arrow field tag
@@ -42,14 +46,14 @@ impl FromStr for LogicalType {
     type Err = LogicalTypeError;
 
     fn from_str(s: &str) -> Result<Self, Self::Err> {
-        parse_type(s.trim()).map_err(|reason| LogicalTypeError {
+        parse_type(s.trim(), 0).map_err(|reason| LogicalTypeError {
             input: s.to_string(),
             reason,
         })
     }
 }
 
-fn parse_type(s: &str) -> Result<LogicalType, String> {
+fn parse_type(s: &str, depth: usize) -> Result<LogicalType, String> {
     let simple = match s {
         "bool" => Some(LogicalType::Bool),
         "int16" => Some(LogicalType::Int16),
@@ -96,12 +100,18 @@ fn parse_type(s: &str) -> Result<LogicalType, String> {
         return Ok(LogicalType::Decimal { precision, scale });
     }
     if let Some(args) = s.strip_prefix("list") {
+        if depth == MAX_LIST_DEPTH {
+            return Err(format!("list nesting is deeper than {MAX_LIST_DEPTH}"));
+        }
         let inner = args
             .trim()
             .strip_prefix('<')
             .and_then(|a| a.strip_suffix('>'))
             .ok_or("list needs <T>")?;
-        return Ok(LogicalType::List(Box::new(parse_type(inner.trim())?)));
+        return Ok(LogicalType::List(Box::new(parse_type(
+            inner.trim(),
+            depth + 1,
+        )?)));
     }
     Err("unknown type".to_string())
 }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-core`
Expected: PASS. unit tests (`src/lib.rs`) 76.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-core/src/decimal.rs \
  crates/data-avalanche-core/src/lib.rs \
  crates/data-avalanche-core/src/logical.rs
git commit -m "feat(core): bound list<T> nesting, FromStr for Decimal, document API conventions"
```


---

### Task 2: Crate scaffold, `Leaf<T>`, diagnostics, strict parse

**Files:**
- Create: `crates/data-avalanche-config/Cargo.toml`, `crates/data-avalanche-config/src/diag.rs`, `crates/data-avalanche-config/src/lib.rs`, `crates/data-avalanche-config/src/path.rs`, `crates/data-avalanche-config/src/leaf.rs`, `crates/data-avalanche-config/src/parse.rs`
- Modify: `Cargo.toml`
- Test: `crates/data-avalanche-config/tests/parse.rs`

**Interfaces:**
- Consumes: nothing from M1.
- Produces:
  - `Loc { line: usize, column: usize }` (1-based).
  - `Leaf<T>`: `new(text)`, `with_loc(text, loc)`, `text() -> &str`, `loc() -> Option<Loc>`. It
    deserializes from any YAML scalar and keeps its spelling.
  - `Tri<T> = Option<Option<Leaf<T>>>`, and `leaf::tri` for `#[serde(default, deserialize_with = "tri")]`.
  - `YamlPath::{root, key, index, is_root}` + `Display`.
  - `diag::{PARSE, TYPE}`, `Severity { Error, Warning }`, `Locus { Config, Pipeline(String), Reload }`.
  - `Diagnostic { locus, path: String, rule: String, severity, loc: Option<Loc>, message }`.
  - `Report { errors, warnings }` with `push` (sorted by path then rule, deduplicated), `extend`,
    `is_ok`, `has(rule)`, `iter`, and `to_json` (`{valid, errors, warnings}`).
  - `parse_document<T: DeserializeOwned>(text) -> Result<T, Box<Diagnostic>>`.

This task also closes risk **R-9**. `serde-saphyr` 1.3.0 provides everything the spec needs: strict
booleans, duplicate-key errors, `Spanned` locations and unknown-key errors with a location. Its one
trap is Review Focus 1: an unquoted `${…}` inside a flow collection. The parse test pins it, and
`parse.rs` adds the hint.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 20368dd..8352c02 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -18,6 +18,7 @@ all = { level = "warn", priority = -1 }
 
 [workspace.dependencies]
 data-avalanche-core = { path = "crates/data-avalanche-core" }
+data-avalanche-config = { path = "crates/data-avalanche-config" }
 data-avalanche-testkit = { path = "crates/data-avalanche-testkit" }
 thiserror = "2"
 serde = { version = "1", features = ["derive"] }
@@ -30,3 +31,9 @@ object_store = { version = "0.14.2", features = ["aws", "gcp", "azure"] }
 tokio-postgres = "0.7"
 mysql_async = { version = "=0.37.1", default-features = false, features = ["default-rustls", "binlog"] }
 rustls = { version = "0.23", features = ["aws_lc_rs"] }
+serde-saphyr = { version = "=1.3.0", default-features = false, features = ["deserialize"] }
+humantime = "2.4"
+tracing-subscriber = { version = "0.3", default-features = false, features = ["std", "env-filter"] }
+rustls-pki-types = { version = "1", features = ["std"] }
+xxhash-rust = { version = "0.8", features = ["xxh3"] }
+tempfile = "3"
```

Create `crates/data-avalanche-config/Cargo.toml`:

```toml
[package]
name = "data-avalanche-config"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
description = "Configuration model, strict YAML parse, env/file resolution, V-rule validation and reload diff."

[lints]
workspace = true

[dependencies]
data-avalanche-core.workspace = true
serde.workspace = true
serde_json.workspace = true
serde-saphyr.workspace = true
thiserror.workspace = true

[dev-dependencies]
```

Create `crates/data-avalanche-config/src/diag.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn d(rule: &str, severity: Severity, path: &str) -> Diagnostic {
        Diagnostic {
            locus: Locus::Config,
            path: path.into(),
            rule: rule.into(),
            severity,
            loc: None,
            message: format!("{rule} at {path}"),
        }
    }

    #[test]
    fn splits_by_severity_sorts_and_dedups() {
        let mut r = Report::default();
        r.push(d("V-007", Severity::Error, "b"));
        r.push(d("V-037", Severity::Warning, "a"));
        r.push(d("V-006", Severity::Error, "a"));
        r.push(d("V-007", Severity::Error, "b"));
        assert_eq!(
            r.errors.iter().map(|x| x.rule.as_str()).collect::<Vec<_>>(),
            ["V-006", "V-007"]
        );
        assert_eq!(r.warnings.len(), 1);
        assert!(!r.is_ok());
        assert!(r.has("V-037"));
    }

    #[test]
    fn json_shape_is_stable() {
        let mut r = Report::default();
        let mut e = d("V-055", Severity::Error, "pipelines.p.id");
        e.locus = Locus::Pipeline("p".into());
        e.loc = Some(Loc { line: 3, column: 9 });
        r.push(e);
        assert_eq!(
            r.to_json(),
            serde_json::json!({
                "valid": false,
                "errors": [{
                    "locus": {"kind": "pipeline", "pipeline": "p"},
                    "path": "pipelines.p.id",
                    "rule": "V-055",
                    "severity": "error",
                    "loc": {"line": 3, "column": 9},
                    "message": "V-055 at pipelines.p.id"
                }],
                "warnings": []
            })
        );
    }
}
```

Create `crates/data-avalanche-config/src/lib.rs`:

```rust
//! data-avalanche configuration (`doc/specs/Configuration_Specification.md`): the YAML model,
//! strict parse, `${VAR}` and file resolution, merge of `defaults`, the V-rule validators, the
//! resolved config the runtime uses, and the reload diff.

pub mod diag;
pub mod leaf;
pub mod parse;
pub mod path;

pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
pub use leaf::{Leaf, Loc, Tri};
pub use parse::parse_document;
pub use path::YamlPath;
```

Create `crates/data-avalanche-config/src/path.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prints_dotted_keys_and_quotes_names_with_dots() {
        let p = YamlPath::root()
            .key("pipelines")
            .key("orders-pg")
            .key("tables")
            .key("snapshots")
            .key("overrides")
            .key("public.orders");
        assert_eq!(
            p.to_string(),
            r#"pipelines.orders-pg.tables.snapshots.overrides["public.orders"]"#
        );
        let q = YamlPath::root()
            .key("globals")
            .key("cluster")
            .key("memberlist")
            .key("join_members")
            .index(2);
        assert_eq!(q.to_string(), "globals.cluster.memberlist.join_members[2]");
        assert_eq!(YamlPath::root().to_string(), "");
    }
}
```

Create `crates/data-avalanche-config/tests/parse.rs`:

```rust
//! Phase 0 behaviour the loader relies on (roadmap R-9: serde-saphyr strictness and locations).

use data_avalanche_config::leaf::tri;
use data_avalanche_config::{Leaf, Loc, PARSE, Tri, parse_document};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Doc {
    source: Option<Source>,
    flush: Option<Flush>,
    include: Option<Vec<Leaf<String>>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    host: Option<Leaf<String>>,
    port: Option<Leaf<u16>>,
    password: Option<Leaf<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Flush {
    #[serde(default, deserialize_with = "tri")]
    seconds: Tri<u64>,
    #[serde(default, deserialize_with = "tri")]
    rows: Tri<u64>,
}

fn err(text: &str) -> data_avalanche_config::Diagnostic {
    *parse_document::<Doc>(text).unwrap_err()
}

#[test]
fn keeps_scalar_spelling_and_location() {
    let doc: Doc =
        parse_document("source:\n  port: 5432\n  password: 0123\n  host: ${DB}-x\n").unwrap();
    let s = doc.source.unwrap();
    let port = s.port.unwrap();
    assert_eq!(port.text(), "5432");
    assert_eq!(port.loc(), Some(Loc { line: 2, column: 9 }));
    // A leading zero survives: a numeric-looking password isn't re-printed as a number.
    assert_eq!(s.password.unwrap().text(), "0123");
    assert_eq!(s.host.unwrap().text(), "${DB}-x");
}

#[test]
fn quoted_template_in_flow_mapping_is_text() {
    let doc: Doc = parse_document("source: { port: \"${PG_PORT}\" }\n").unwrap();
    assert_eq!(doc.source.unwrap().port.unwrap().text(), "${PG_PORT}");
}

#[test]
fn unquoted_template_in_flow_mapping_fails_with_a_hint() {
    let e = err("source: { port: ${PG_PORT} }\n");
    assert_eq!(e.rule, PARSE);
    assert!(e.message.contains("quote"), "{}", e.message);
}

#[test]
fn distinguishes_absent_null_and_value() {
    let doc: Doc = parse_document("flush:\n  seconds: null\n").unwrap();
    let f = doc.flush.unwrap();
    assert!(matches!(f.seconds, Some(None)), "explicit null disables");
    assert!(f.rows.is_none(), "absent means default");
    let doc: Doc = parse_document("flush:\n  rows: ~\n  seconds: 30\n").unwrap();
    let f = doc.flush.unwrap();
    assert!(matches!(f.rows, Some(None)));
    assert_eq!(f.seconds.unwrap().unwrap().text(), "30");
}

#[test]
fn unknown_key_is_rejected_with_its_location() {
    let e = err("source:\n  port: 1\n  hots: db\n");
    assert_eq!(e.rule, PARSE);
    assert_eq!(e.loc, Some(Loc { line: 3, column: 3 }));
    assert!(e.message.contains("unknown field `hots`"), "{}", e.message);
}

#[test]
fn duplicate_keys_several_documents_and_tabs_are_rejected() {
    let e = err("source:\n  port: 1\n  port: 2\n");
    assert!(e.message.contains("duplicate"), "{}", e.message);
    assert_eq!(e.loc, Some(Loc { line: 3, column: 3 }));
    let e = err("source: {port: 1}\n---\nsource: {port: 2}\n");
    assert!(e.message.contains("multiple"), "{}", e.message);
    let e = err("source:\n\tport: 1\n");
    assert!(e.message.contains("tab"), "{}", e.message);
}

#[test]
fn null_list_element_and_wrong_shape_are_parse_errors() {
    assert_eq!(err("include: [a, ~]\n").rule, PARSE);
    assert_eq!(err("source:\n  port: {a: 1}\n").rule, PARSE);
}

#[test]
fn empty_document_parses_to_nothing() {
    let doc: Doc = parse_document("").unwrap();
    assert!(doc.source.is_none() && doc.flush.is_none() && doc.include.is_none());
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0583]: file not found for module `leaf`
error[E0583]: file not found for module `parse`
error[E0432]: unresolved imports `diag::Diagnostic`, `diag::Locus`, `diag::PARSE`, `diag::Report`, `diag::Severity`, `diag::TYPE`
error[E0432]: unresolved import `path::YamlPath`
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-config/src/diag.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Diagnostics: one per violated rule, collected into a [`Report`].

use serde::Serialize;

use crate::leaf::Loc;

/// Phase 0 failure with no V-rule: YAML syntax, duplicate or unknown key, several documents, or a
/// value of the wrong shape (a map where a scalar belongs). Fatal; only the first one is reported.
pub const PARSE: &str = "PARSE";
/// A scalar that doesn't coerce to its field's type (`port: abc`, `lease_duration: 5 sec`) and
/// isn't covered by a value-domain V-rule. Reported together with every other error.
pub const TYPE: &str = "TYPE";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
}

/// Where a problem is (config §13 "Locus"). Diagnostic only: any error rejects the whole config.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(tag = "kind", content = "pipeline", rename_all = "lowercase")]
pub enum Locus {
    Config,
    /// The pipeline's human name (its key under `pipelines`).
    Pipeline(String),
    Reload,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Diagnostic {
    pub locus: Locus,
    /// YAML key path, e.g. `pipelines.orders.sink.flush`; empty for the document root.
    pub path: String,
    /// `V-nnn`, [`PARSE`] or [`TYPE`].
    pub rule: String,
    pub severity: Severity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loc: Option<Loc>,
    pub message: String,
}

impl Diagnostic {
    pub fn is_error(&self) -> bool {
        self.severity == Severity::Error
    }
}

/// Every diagnostic of one load or validation run, errors and warnings apart, each list sorted by
/// locus, path and rule, without duplicates. This is the body of the validation report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Report {
    pub errors: Vec<Diagnostic>,
    pub warnings: Vec<Diagnostic>,
}

impl Report {
    /// True when there are no errors (warnings never block, config §1.3).
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn push(&mut self, d: Diagnostic) {
        let list = match d.severity {
            Severity::Error => &mut self.errors,
            Severity::Warning => &mut self.warnings,
        };
        if let Err(i) = list.binary_search(&d) {
            list.insert(i, d);
        }
    }

    pub fn extend(&mut self, other: Report) {
        for d in other.errors.into_iter().chain(other.warnings) {
            self.push(d);
        }
    }

    /// All diagnostics, errors first.
    pub fn iter(&self) -> impl Iterator<Item = &Diagnostic> {
        self.errors.iter().chain(&self.warnings)
    }

    /// True when some diagnostic carries `rule` (any severity).
    pub fn has(&self, rule: &str) -> bool {
        self.iter().any(|d| d.rule == rule)
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "valid": self.is_ok(),
            "errors": self.errors,
            "warnings": self.warnings,
        })
    }
}

```

Create `crates/data-avalanche-config/src/leaf.rs`:

```rust
//! Raw scalar leaves: a value's text exactly as written in the YAML file, plus where it was written.

use std::fmt;
use std::marker::PhantomData;

use serde::{Deserialize, Deserializer, Serialize};
use serde_saphyr::Spanned;

/// A 1-based line/column position in the config file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Loc {
    pub line: u64,
    pub column: u64,
}

impl fmt::Display for Loc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {} column {}", self.line, self.column)
    }
}

/// A scalar as written (after YAML unquoting), with its location.
///
/// `T` is the type the scalar must coerce to. Coercion happens later, in `Ctx::value`, so a
/// template (`${VAR}`) can stay unresolved during structural validation (config §11 Phase 1), and
/// every coercion failure is reported with a rule ID instead of aborting the parse.
///
/// Any YAML scalar deserializes into a `Leaf`, whatever `T` is: `port: 5432`, `port: "5432"` and
/// `port: ${PG_PORT}` all yield text. The spelling is kept (`007` stays `007`), because
/// serde-saphyr hands a string target the scalar's original text.
pub struct Leaf<T> {
    text: String,
    loc: Option<Loc>,
    _type: PhantomData<fn() -> T>,
}

impl<T> Leaf<T> {
    /// A leaf with no file location (built in code or by tests).
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            loc: None,
            _type: PhantomData,
        }
    }

    pub fn with_loc(text: impl Into<String>, loc: Loc) -> Self {
        Self {
            text: text.into(),
            loc: Some(loc),
            _type: PhantomData,
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn loc(&self) -> Option<Loc> {
        self.loc
    }
}

// Manual impls: derives would demand `T: Clone`/`T: Debug`/`T: PartialEq`, but `T` is only a marker.
impl<T> Clone for Leaf<T> {
    fn clone(&self) -> Self {
        Self {
            text: self.text.clone(),
            loc: self.loc,
            _type: PhantomData,
        }
    }
}

impl<T> fmt::Debug for Leaf<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.loc {
            Some(loc) => write!(f, "{:?}@{}:{}", self.text, loc.line, loc.column),
            None => write!(f, "{:?}", self.text),
        }
    }
}

impl<T> PartialEq for Leaf<T> {
    /// Compares the text only; where a value was written doesn't change what it means.
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}

impl<'de, T> Deserialize<'de> for Leaf<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let spanned: Spanned<String> = Spanned::deserialize(d)?;
        let at = spanned.referenced;
        let loc = (at.line() > 0).then(|| Loc {
            line: at.line(),
            column: at.column(),
        });
        Ok(Self {
            text: spanned.value,
            loc,
            _type: PhantomData,
        })
    }
}

/// A tri-state field (config §9.4.1): absent → `None` (use the default), explicit `null` →
/// `Some(None)` (disabled), a value → `Some(Some(v))`.
pub type Tri<T> = Option<Option<Leaf<T>>>;

/// `#[serde(default, deserialize_with = "crate::leaf::tri")]` for [`Tri`] fields: serde maps an absent
/// field to `None` through `default`, so reaching this function means the key was present.
pub fn tri<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}
```

Create `crates/data-avalanche-config/src/parse.rs`:

```rust
//! Phase 0 (config §2, §11): strict YAML 1.2 parse into the raw model.

use serde::de::DeserializeOwned;
use serde_saphyr::{DuplicateKeyPolicy, SnippetMode};

use crate::diag::{Diagnostic, Locus, PARSE, Severity};
use crate::leaf::Loc;

/// Parses one YAML document into `T` (the raw model, whose structs deny unknown fields).
///
/// Options: duplicate keys are an error; only the exact words `true`/`false` are booleans (YAML 1.2,
/// no `yes`/`on`); one document only. The first failure is returned as a [`PARSE`] diagnostic with
/// its line and column (boxed: a `Diagnostic` is large and parsing is a cold path).
pub fn parse_document<T: DeserializeOwned>(text: &str) -> Result<T, Box<Diagnostic>> {
    let options = serde_saphyr::options! {
        strict_booleans: true,
        duplicate_keys: DuplicateKeyPolicy::Error,
    };
    serde_saphyr::from_str_with_options(text, options).map_err(|e| {
        let loc = e.location().filter(|l| l.line() > 0).map(|l| Loc {
            line: l.line(),
            column: l.column(),
        });
        let rendered = e.render_with_options(serde_saphyr::render_options! {
            snippets: SnippetMode::Off,
        });
        Box::new(Diagnostic {
            locus: Locus::Config,
            path: String::new(),
            rule: PARSE.into(),
            severity: Severity::Error,
            loc,
            message: flow_template_hint(text, loc, rendered),
        })
    })
}

/// An unquoted `${VAR}` inside a flow collection (`{ port: ${P} }`) breaks the YAML, because `{` and
/// `}` are flow indicators there. The parser's own message ("did not find expected ',' or '}'")
/// doesn't say why, so add a hint when the failing line has both a template and a flow collection.
fn flow_template_hint(text: &str, loc: Option<Loc>, message: String) -> String {
    let line = loc.and_then(|l| text.lines().nth(l.line.saturating_sub(1) as usize));
    let flow_with_template =
        line.is_some_and(|l| l.contains("${") && l.replace("${", "").contains(['{', '[']));
    if flow_with_template {
        format!("{message} (hint: quote ${{...}} references inside flow collections: \"${{VAR}}\")")
    } else {
        message
    }
}
```

In `crates/data-avalanche-config/src/path.rs`, insert this above the `#[cfg(test)]` module:

```rust
use std::fmt;

/// A location in the config document as a key path, e.g. `pipelines.orders.sink.flush.seconds`.
///
/// Keys that contain `.`, brackets, quotes or whitespace print quoted in brackets
/// (`tables.snapshots.overrides["public.orders"]`), so a table or pipeline name never reads as
/// several segments.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct YamlPath(Vec<Segment>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Segment {
    Key(String),
    Index(usize),
}

impl YamlPath {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn key(&self, key: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.0.push(Segment::Key(key.into()));
        next
    }

    pub fn index(&self, index: usize) -> Self {
        let mut next = self.clone();
        next.0.push(Segment::Index(index));
        next
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }
}

fn needs_quoting(key: &str) -> bool {
    key.is_empty()
        || key
            .chars()
            .any(|c| matches!(c, '.' | '[' | ']' | '"') || c.is_whitespace())
}

impl fmt::Display for YamlPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, segment) in self.0.iter().enumerate() {
            match segment {
                Segment::Key(k) if needs_quoting(k) => write!(f, "[{k:?}]")?,
                Segment::Key(k) if i == 0 => f.write_str(k)?,
                Segment::Key(k) => write!(f, ".{k}")?,
                Segment::Index(n) => write!(f, "[{n}]")?,
            }
        }
        Ok(())
    }
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 3; `tests/parse.rs` 8.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml \
  crates/data-avalanche-config/Cargo.toml \
  crates/data-avalanche-config/src/diag.rs \
  crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/src/path.rs \
  crates/data-avalanche-config/tests/parse.rs \
  crates/data-avalanche-config/src/leaf.rs \
  crates/data-avalanche-config/src/parse.rs \
  Cargo.lock
git commit -m "feat(config): crate scaffold, Leaf<T>, diagnostics and strict YAML parse"
```


---

### Task 3: Value types — durations, byte sizes, host:port, coercion, enums

**Files:**
- Create: `crates/data-avalanche-config/src/coerce.rs`, `crates/data-avalanche-config/src/enums.rs`, `crates/data-avalanche-config/src/units.rs`
- Modify: `crates/data-avalanche-config/Cargo.toml`, `crates/data-avalanche-config/src/lib.rs`

**Interfaces:**
- Consumes: Task 2 (`diag::TYPE`).
- Produces:
  - `units::{parse_duration, format_duration, ser_duration}`. A bare integer is rejected, except
    `"0"`.
  - `ByteSize(u64)` with `KIB`/`MIB`/`GIB`, `FromStr`/`Display` (KiB/MiB/GiB/KB/MB/GB or a bare
    integer).
  - `HostPort { host, port }` (a name, IPv4, or `[IPv6]`; port 1–65535).
  - `trait LeafType { const RULE: &str = TYPE; fn parse_leaf(&str) -> Result<Self, String> }` for
    `String`, `bool`, `u16`, `u32`, `u64`, `f64`, `Duration`, `ByteSize`, `HostPort`, `IpAddr`,
    `LogicalType` (RULE V-078) and `LogFilter` (RULE V-017).
  - `config_enum!`: every enum gets `ALL`, `as_str`, `FromStr`, `Display`, `Serialize` and
    `LeafType` with its V-rule. The enums are Cloud, ApiAuth, LogFormat, LockBackend, Discovery,
    SourceType, SslMode, ToastStrategy, SlotManagement, SlotOnOrphan, ReplicationType,
    Tinyint1Detection, BinlogPurgeAction, SnapshotMode, NumericOverflowAction, Transform, SinkType,
    WriteMode, IcebergStrategy, CatalogKind, HiveAuth, RestAuth, ColumnMapping, TimestampNtzAs,
    DeltaCatalogType and Role.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-config/Cargo.toml b/crates/data-avalanche-config/Cargo.toml
index 111a1ee..e365cba 100644
--- a/crates/data-avalanche-config/Cargo.toml
+++ b/crates/data-avalanche-config/Cargo.toml
@@ -15,5 +15,7 @@ serde.workspace = true
 serde_json.workspace = true
 serde-saphyr.workspace = true
 thiserror.workspace = true
+humantime.workspace = true
+tracing-subscriber.workspace = true
 
 [dev-dependencies]
```

Create `crates/data-avalanche-config/src/coerce.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn booleans_are_only_true_and_false() {
        assert!(bool::parse_leaf("true").unwrap());
        for bad in ["yes", "on", "True", "1", ""] {
            assert!(bool::parse_leaf(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn integers_are_decimal_digits_in_range() {
        assert_eq!(u16::parse_leaf("5432").unwrap(), 5432);
        assert!(
            u16::parse_leaf("99999")
                .unwrap_err()
                .contains("out of range")
        );
        for bad in ["-1", "0x10", "1e3", "+5", " 5", ""] {
            assert!(u64::parse_leaf(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn floats_must_be_finite() {
        assert_eq!(f64::parse_leaf("0.15").unwrap(), 0.15);
        for bad in ["NaN", "inf", "abc"] {
            assert!(f64::parse_leaf(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn logical_types_and_log_filters_carry_their_rules() {
        assert_eq!(<LogicalType as LeafType>::RULE, "V-078");
        assert!(LogicalType::parse_leaf("decimal(38,10)").is_ok());
        assert!(LogicalType::parse_leaf("timestamp").is_err());
        assert!(LogFilter::parse_leaf("info").is_ok());
        assert!(LogFilter::parse_leaf("info,data_avalanche_sink=debug").is_ok());
        assert!(LogFilter::parse_leaf("info,[=").is_err());
        assert!(LogFilter::parse_leaf("").is_err());
        assert_eq!(<LogFilter as LeafType>::RULE, "V-017");
    }
}
```

Create `crates/data-avalanche-config/src/enums.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_value_and_names_the_choices() {
        for m in SnapshotMode::ALL {
            assert_eq!(m.as_str().parse::<SnapshotMode>().unwrap(), *m);
        }
        assert_eq!(SslMode::VerifyFull.to_string(), "verify-full");
        let err = "gcp".parse::<Cloud>().unwrap_err();
        assert_eq!(err, "\"gcp\" is not one of: aws, google, azure");
        assert!("Aws".parse::<Cloud>().is_err(), "values are case-sensitive");
        assert_eq!(<Cloud as LeafType>::RULE, "V-015");
        assert_eq!(serde_json::to_value(RestAuth::Sigv4).unwrap(), "sigv4");
    }
}
```

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 585ebf3..5f21e0b 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -2,12 +2,17 @@
 //! strict parse, `${VAR}` and file resolution, merge of `defaults`, the V-rule validators, the
 //! resolved config the runtime uses, and the reload diff.
 
+pub mod coerce;
 pub mod diag;
+pub mod enums;
 pub mod leaf;
 pub mod parse;
 pub mod path;
+pub mod units;
 
+pub use coerce::{LeafType, LogFilter};
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use leaf::{Leaf, Loc, Tri};
 pub use parse::parse_document;
 pub use path::YamlPath;
+pub use units::{ByteSize, HostPort};
```

Create `crates/data-avalanche-config/src/units.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_need_a_unit_except_zero() {
        assert_eq!(parse_duration("200ms").unwrap(), Duration::from_millis(200));
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(
            parse_duration("30d").unwrap(),
            Duration::from_secs(30 * 86_400)
        );
        assert_eq!(parse_duration("60m").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert!(parse_duration("30").unwrap_err().contains("no unit"));
        assert!(parse_duration("5 parsecs").is_err());
        assert!(parse_duration("").is_err());
        assert_eq!(format_duration(Duration::from_secs(15)), "15s");
    }

    #[test]
    fn byte_sizes_distinguish_binary_and_decimal_units() {
        assert_eq!("512MiB".parse::<ByteSize>().unwrap().bytes(), 536_870_912);
        assert_eq!("512MB".parse::<ByteSize>().unwrap().bytes(), 512_000_000);
        assert_eq!("1GiB".parse::<ByteSize>().unwrap().bytes(), 1 << 30);
        assert_eq!("4096".parse::<ByteSize>().unwrap().bytes(), 4096);
        for bad in [
            "",
            "MiB",
            "1.5GiB",
            "512 MiB",
            "512mib",
            "-1",
            "1TiB",
            "99999999999GiB",
        ] {
            assert!(bad.parse::<ByteSize>().is_err(), "{bad:?}");
        }
        assert_eq!(ByteSize(536_870_912).to_string(), "512MiB");
        assert_eq!(ByteSize(512_000_000).to_string(), "512MB");
        assert_eq!(ByteSize(4096).to_string(), "4KiB");
        assert_eq!(ByteSize(1001).to_string(), "1001");
    }

    #[test]
    fn host_port_accepts_names_ipv4_and_bracketed_ipv6() {
        let hp: HostPort = "0.0.0.0:8080".parse().unwrap();
        assert_eq!((hp.host.as_str(), hp.port), ("0.0.0.0", 8080));
        let hp: HostPort = "cdc-headless.svc.cluster.local:7946".parse().unwrap();
        assert_eq!(hp.port, 7946);
        let hp: HostPort = "[::1]:9000".parse().unwrap();
        assert_eq!(hp.host, "::1");
        assert_eq!(hp.to_string(), "[::1]:9000");
        for bad in ["host", ":80", "h:0", "h:70000", "::1:80", "[zz]:1", "[::1]"] {
            assert!(bad.parse::<HostPort>().is_err(), "{bad:?}");
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `coerce::LeafType`, `coerce::LogFilter`
error[E0432]: unresolved imports `units::ByteSize`, `units::HostPort`
error[E0405]: cannot find trait `LeafType` in this scope
error[E0425]: cannot find type `LogicalType` in this scope
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-config/src/coerce.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Scalar coercion: the text of a [`crate::Leaf`] becomes the field's type.

use std::net::IpAddr;
use std::time::Duration;

use data_avalanche_core::LogicalType;
use serde::{Serialize, Serializer};

use crate::diag::TYPE;
use crate::units::{ByteSize, HostPort, parse_duration};

/// A type a scalar leaf can coerce to.
pub trait LeafType: Sized {
    /// The rule that reports a failed coercion: the value-domain V-rule for enums (V-015, …),
    /// [`TYPE`] for plain types.
    const RULE: &'static str = TYPE;

    fn parse_leaf(text: &str) -> Result<Self, String>;
}

impl LeafType for String {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        Ok(text.to_string())
    }
}

impl LeafType for bool {
    /// Only `true` and `false` (YAML 1.2): `yes`, `on`, `True` are not booleans.
    fn parse_leaf(text: &str) -> Result<Self, String> {
        match text {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(format!("{text:?} is not a boolean (true or false)")),
        }
    }
}

macro_rules! integer_leaf {
    ($($t:ty),+) => {$(
        impl LeafType for $t {
            /// Decimal digits only; no hex, octal or exponent forms.
            fn parse_leaf(text: &str) -> Result<Self, String> {
                if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
                    text.parse().map_err(|_| format!("{text:?} is out of range for {}", stringify!($t)))
                } else {
                    Err(format!("{text:?} is not a non-negative integer"))
                }
            }
        }
    )+};
}
integer_leaf!(u16, u32, u64);

impl LeafType for f64 {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        match text.parse::<f64>() {
            Ok(v) if v.is_finite() => Ok(v),
            _ => Err(format!("{text:?} is not a finite number")),
        }
    }
}

impl LeafType for Duration {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        parse_duration(text)
    }
}

impl LeafType for ByteSize {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        text.parse()
    }
}

impl LeafType for HostPort {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        text.parse()
    }
}

impl LeafType for IpAddr {
    fn parse_leaf(text: &str) -> Result<Self, String> {
        text.parse()
            .map_err(|_| format!("{text:?} is not an IP address"))
    }
}

impl LeafType for LogicalType {
    const RULE: &'static str = "V-078";

    fn parse_leaf(text: &str) -> Result<Self, String> {
        text.parse()
            .map_err(|e: data_avalanche_core::LogicalTypeError| e.reason)
    }
}

/// `globals.log.level`: a level (`info`) or a `tracing` EnvFilter directive list
/// (`info,data_avalanche_sink=debug`), checked with the same parser the runtime uses (V-017).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFilter(pub String);

impl LeafType for LogFilter {
    const RULE: &'static str = "V-017";

    fn parse_leaf(text: &str) -> Result<Self, String> {
        if text.trim().is_empty() {
            return Err("log level is empty".into());
        }
        tracing_subscriber::EnvFilter::builder()
            .parse(text)
            .map(|_| LogFilter(text.to_string()))
            .map_err(|e| format!("{text:?} is not a log level or EnvFilter directive: {e}"))
    }
}

impl Serialize for LogFilter {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

```

In `crates/data-avalanche-config/src/enums.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! The config's enumerations. Each enum's value-domain rule (V-nnn) reports a value outside the
//! enum; the reserved values that parse but are rejected in v1 (e.g. `lock_backend: kubernetes`)
//! are separate checks in `rules`.

use std::fmt;
use std::str::FromStr;

use serde::{Serialize, Serializer};

use crate::coerce::LeafType;

macro_rules! config_enum {
    ($(#[$meta:meta])* $name:ident, rule = $rule:literal, { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

            pub fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => $text),+
                }
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(s: &str) -> Result<Self, String> {
                match s {
                    $($text => Ok($name::$variant),)+
                    _ => Err(format!(
                        "{s:?} is not one of: {}",
                        [$($text),+].join(", ")
                    )),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl LeafType for $name {
            const RULE: &'static str = $rule;

            fn parse_leaf(text: &str) -> Result<Self, String> {
                text.parse()
            }
        }
    };
}

config_enum!(Cloud, rule = "V-015", { Aws => "aws", Google => "google", Azure => "azure" });
config_enum!(ApiAuth, rule = "V-016", { None => "none", Basic => "basic", Jwt => "jwt", Oauth2 => "oauth2" });
config_enum!(LogFormat, rule = "V-018", { Json => "json", Compact => "compact", Pretty => "pretty" });
config_enum!(
    /// `kubernetes` is reserved: it parses, and V-075 rejects it in v1.
    LockBackend, rule = "V-075", { Storage => "storage", Kubernetes => "kubernetes" }
);
config_enum!(Discovery, rule = "V-095", { Static => "static", Dns => "dns" });
config_enum!(SourceType, rule = "V-019", { Postgres => "postgres", Mysql => "mysql" });
config_enum!(SslMode, rule = "V-020", {
    Disable => "disable", Allow => "allow", Prefer => "prefer", Require => "require",
    VerifyCa => "verify-ca", VerifyFull => "verify-full",
});
config_enum!(ToastStrategy, rule = "V-080", { CarryForward => "carry_forward", ReQuery => "re_query" });
config_enum!(SlotManagement, rule = "V-102", { Managed => "managed", RequireExisting => "require_existing" });
config_enum!(SlotOnOrphan, rule = "V-102", { Halt => "halt", AdoptResnapshot => "adopt_resnapshot" });
config_enum!(ReplicationType, rule = "V-021", { Binlog => "binlog", Gtid => "gtid" });
config_enum!(Tinyint1Detection, rule = "V-081", { InformationSchema => "information_schema", None => "none" });
config_enum!(BinlogPurgeAction, rule = "V-105", { Error => "error", ReSnapshot => "re_snapshot" });
config_enum!(SnapshotMode, rule = "V-022", {
    Never => "never", Initial => "initial", Always => "always",
    DumpAlways => "dump_always", DumpInitial => "dump_initial",
});
config_enum!(NumericOverflowAction, rule = "V-082", { String => "string", Error => "error" });
config_enum!(Transform, rule = "V-107", {
    Identity => "identity", Bucket => "bucket", Truncate => "truncate",
    Year => "year", Month => "month", Day => "day", Hour => "hour",
});
config_enum!(
    /// Both values are valid config; v1 refuses an `iceberg` pipeline at start (D-V1.4), not here.
    SinkType, rule = "V-083", { Iceberg => "iceberg", Delta => "delta" }
);
config_enum!(
    /// `changelog` is reserved: V-089 rejects it in v1.
    WriteMode, rule = "V-089", { Mirror => "mirror", Changelog => "changelog" }
);
config_enum!(IcebergStrategy, rule = "V-023", { Mor => "mor", Cow => "cow" });
config_enum!(
    /// `sink.iceberg.catalog.type`, the optional discriminator; V-002 also checks it against the blocks.
    CatalogKind, rule = "V-002", { Glue => "glue", Hive => "hive", Rest => "rest" }
);
config_enum!(HiveAuth, rule = "V-025", { None => "none", Ldap => "ldap", Kerberos => "kerberos" });
config_enum!(RestAuth, rule = "V-025", { None => "none", Bearer => "bearer", Oauth2 => "oauth2", Sigv4 => "sigv4" });
config_enum!(
    /// `name` is reserved: V-092 rejects it in v1.
    ColumnMapping, rule = "V-092", { None => "none", Name => "name" }
);
config_enum!(TimestampNtzAs, rule = "V-114", {
    TimestampNtz => "timestamp_ntz", Timestamp => "timestamp", String => "string",
});
config_enum!(
    /// Only `none` is implemented in v1 (V-088).
    DeltaCatalogType, rule = "V-088", { None => "none", Glue => "glue", Unity => "unity", Hive => "hive" }
);
config_enum!(
    /// Local API roles, `viewer ⊂ operator ⊂ admin` (`globals.api.role_mapping` keys, V-099).
    Role, rule = "V-099", { Viewer => "viewer", Operator => "operator", Admin => "admin" }
);

```

In `crates/data-avalanche-config/src/units.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Value types with a text grammar of their own: durations (§4.1), byte sizes (§4.2), `host:port`.

use std::fmt;
use std::net::Ipv6Addr;
use std::str::FromStr;
use std::time::Duration;

use serde::{Serialize, Serializer};

/// Parses a `humantime` duration (`200ms`, `5s`, `1h30m`, `30d`). A bare integer is not a duration
/// (§4.1), except `0`, which `schema_evolution.rescan_interval` uses to mean "disabled".
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    if text == "0" {
        return Ok(Duration::ZERO);
    }
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("{text:?} has no unit; write e.g. \"{text}s\""));
    }
    humantime::parse_duration(text).map_err(|e| format!("{text:?} is not a duration: {e}"))
}

/// `humantime` text form of a duration (`15s`, `1h 30m`), for messages and the reload diff.
pub fn format_duration(d: Duration) -> String {
    humantime::format_duration(d).to_string()
}

/// `#[serde(serialize_with = "crate::units::ser_duration")]`: a duration serializes as its text form.
pub fn ser_duration<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&format_duration(*d))
}

/// A byte count (§4.2): `512MiB`, `256MB`, `1GiB`, or a bare integer meaning bytes. Binary
/// (`KiB`/`MiB`/`GiB`) and decimal (`KB`/`MB`/`GB`) units are both accepted and are not equal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize(pub u64);

impl ByteSize {
    pub const KIB: u64 = 1 << 10;
    pub const MIB: u64 = 1 << 20;
    pub const GIB: u64 = 1 << 30;

    pub fn bytes(self) -> u64 {
        self.0
    }
}

const UNITS: [(&str, u64); 6] = [
    ("KiB", ByteSize::KIB),
    ("MiB", ByteSize::MIB),
    ("GiB", ByteSize::GIB),
    ("KB", 1_000),
    ("MB", 1_000_000),
    ("GB", 1_000_000_000),
];

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let digits = text.bytes().take_while(u8::is_ascii_digit).count();
        let (number, unit) = text.split_at(digits);
        let bad = || {
            format!("{text:?} is not a byte size; write e.g. 512MiB, 256MB or a number of bytes")
        };
        if number.is_empty() {
            return Err(bad());
        }
        let n: u64 = number.parse().map_err(|_| bad())?;
        let factor = if unit.is_empty() {
            1
        } else {
            UNITS
                .iter()
                .find(|(u, _)| *u == unit)
                .map(|(_, f)| *f)
                .ok_or_else(bad)?
        };
        n.checked_mul(factor)
            .map(ByteSize)
            .ok_or_else(|| format!("{text:?} is too large"))
    }
}

impl fmt::Display for ByteSize {
    /// The largest unit that divides the value exactly (`512MiB`, `512MB`), else plain bytes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let units = [
            ("GiB", Self::GIB),
            ("MiB", Self::MIB),
            ("GB", 1_000_000_000),
            ("MB", 1_000_000),
            ("KiB", Self::KIB),
            ("KB", 1_000),
        ];
        for (unit, factor) in units {
            if self.0 != 0 && self.0.is_multiple_of(factor) {
                return write!(f, "{}{unit}", self.0 / factor);
            }
        }
        write!(f, "{}", self.0)
    }
}

/// `host:port` (§6.3 `api.address`, `memberlist.join_members`): an IPv4 address, a DNS name, or a
/// bracketed IPv6 address (`[::1]:8080`), then a port in 1..=65535.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl FromStr for HostPort {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let bad = |why: &str| format!("{text:?} is not host:port: {why}");
        let (host, port) = if let Some(rest) = text.strip_prefix('[') {
            let (h, p) = rest
                .split_once("]:")
                .ok_or_else(|| bad("expected [ipv6]:port"))?;
            h.parse::<Ipv6Addr>()
                .map_err(|_| bad("not an IPv6 address in brackets"))?;
            (h, p)
        } else {
            let (h, p) = text.rsplit_once(':').ok_or_else(|| bad("missing :port"))?;
            if h.contains(':') {
                return Err(bad("put an IPv6 address in brackets"));
            }
            (h, p)
        };
        if host.is_empty() {
            return Err(bad("empty host"));
        }
        let port: u16 = port.parse().map_err(|_| bad("port is not 1..=65535"))?;
        if port == 0 {
            return Err(bad("port is not 1..=65535"));
        }
        Ok(Self {
            host: host.to_string(),
            port,
        })
    }
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl Serialize for HostPort {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 11; `tests/parse.rs` 8.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/Cargo.toml \
  crates/data-avalanche-config/src/coerce.rs \
  crates/data-avalanche-config/src/enums.rs \
  crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/src/units.rs \
  Cargo.lock
git commit -m "feat(config): value types, leaf coercion and config enums"
```


---

### Task 4: Raw model — every block of §3–§10

**Files:**
- Create: `crates/data-avalanche-config/src/raw.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`
- Test: `crates/data-avalanche-config/tests/fixtures/spec_example.yaml`, `crates/data-avalanche-config/tests/raw.rs`

**Interfaces:**
- Consumes: Tasks 2–3 (`Leaf`, `Tri`, `tri`, enums, `ByteSize`, `HostPort`, `LogFilter`).
- Produces: `raw::RawConfig { config_version, globals, defaults: Option<RawPipeline>, pipelines: Option<BTreeMap<String, RawPipeline>> }`
  and one `Raw*` struct per spec block. The rules:
  - every field is `Option<Leaf<T>>`, a nested `Option<Raw…>`, a list or a map;
  - YAML `type` is the Rust field `kind`;
  - the tri-states (`flush.{seconds,rows,bytes}`, `max_transaction_batch_rows`,
    `postgres.max_wal_lag_bytes`) are `Tri<T>`;
  - every struct is `Default + Clone + Deserialize + deny_unknown_fields`.

The fixture is a verbatim copy of spec §14. When the spec example changes, copy it again.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 5f21e0b..0fd8d4b 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -8,6 +8,7 @@ pub mod enums;
 pub mod leaf;
 pub mod parse;
 pub mod path;
+pub mod raw;
 pub mod units;
 
 pub use coerce::{LeafType, LogFilter};
@@ -15,4 +16,5 @@ pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use leaf::{Leaf, Loc, Tri};
 pub use parse::parse_document;
 pub use path::YamlPath;
+pub use raw::RawConfig;
 pub use units::{ByteSize, HostPort};
```

Create `crates/data-avalanche-config/tests/fixtures/spec_example.yaml`:

```yaml
config_version: 1

globals:
  cloud: aws                                   # aws (default) | google | azure
  state:
    bucket: ${ACCOUNT_ID}-cdc-app-state
    prefix: cdc-app
  api:
    address: 0.0.0.0:8080
    auth: jwt
    jwt:
      issuer: https://idp.example.com/realms/cdc
      audience: cdc-management-api
      jwks_url: https://idp.example.com/realms/cdc/protocol/openid-connect/certs
      roles_claim: realm_access.roles
    tls:
      enabled: true
      cert: /etc/cdc/tls/api.crt
      key: ${API_TLS_KEY}
  log:
    level: info                                # rust module syntax allowed: info,rustcdc::sink=debug
    format: json
  cluster:
    tokens: 128                                # must match cluster-wide
    lease_duration: 15s                        # must match cluster-wide (anchored, V-094)
    lease_renewal_interval: 5s                 # ≤ lease_duration / 3 (V-073)
    discovery: dns
    discovery_dns_name: cdc-headless.svc.cluster.local
    memberlist:
      join_members:
        - cdc-headless.svc.cluster.local:7946
      bind_port: 7946
      advertise_port: 7946
      gossip_interval: 200ms
      probe_interval: 1s
      probe_timeout: 500ms
      push_pull_interval: 30s
      dead_node_reclaim_time: 30s
      compression_enabled: true
      tls:
        enabled: true
        cert: /etc/cdc/tls/gossip.crt
        key: ${GOSSIP_TLS_KEY}
        ca: /etc/cdc/tls/gossip-ca.crt
  tracing:
    otlp_endpoint: http://otel-collector.observability:4317
    sample_ratio: 0.05

defaults:
  source:
    type: postgres
    port: 5432
    ssl_mode: require
    timeouts: { connect: 10s, command: 30s, snapshot: 6h }
    postgres:
      auto_cleanup: true
      heartbeat: 60s
  tables:
    snapshots: { mode: never, parallelism: 1 }
  schema_evolution: { enabled: true, rescan_interval: 60m }
  sink:
    type: iceberg                              # REQUIRED somewhere (V-091); set once here
    write_mode: mirror
    flush: { seconds: 30, rows: 100000, bytes: 512MiB }
    iceberg:
      strategy: mor
      catalog:
        rest:
          url: https://catalog.example.com
          warehouse: analytics
          auth: oauth2
          oauth2:
            client_id: ${CATALOG_CLIENT_ID}      # or: credentials_file: ${CATALOG_OAUTH_CREDS_FILE} (JSON {client_id, client_secret}) — V-066
            client_secret: ${CATALOG_CLIENT_SECRET}
            token_url: https://idp.example.com/realms/cdc/protocol/openid-connect/token
            scope: catalog:read catalog:write
  state:
    prefix: pipeline-state

pipelines:
  orders-pg:
    id: orders_pg
    source:
      host: pg-orders.example.com
      db: orders
      user: cdc
      password: ${ORDERS_DB_PASSWORD}
      postgres:
        publication: orders_pub
        slot: orders_slot
    tables:
      include: [ public.* ]
      exclude: [ public.temp_* ]
      snapshots:
        mode: initial
        parallelism: 2
        overrides: { public.orders: always }
      partitioning:
        public.orders: [ { column: _ts, transform: day } ]
    sink:
      bucket: ${ACCOUNT_ID}-lake
      prefix: orders_pg
      flush: { rows: 50000 }                   # merges over defaults (seconds:30, bytes:512MiB kept)

  events-mysql:
    id: events_mysql
    source:
      type: mysql                              # drops the defaulted postgres block (union rule §8.4)
      host: mysql-events.example.com
      port: 3306
      db: events
      credentials_file: ${EVENTS_DB_CREDS_FILE}  # JSON {user, password}; mutually exclusive with user/password (V-065); rotatable in place
      ssl_mode: require
      mysql:
        replication_type: gtid
    tables:
      include: [ orders, payment_* ]           # MySQL: bare table names (db = source.db = events); a prefix would violate V-064
    schema_evolution: { enabled: true }       # rescan_interval merges from defaults (60m)
    sink:
      type: delta                              # drops the defaulted iceberg block (union rule §8.4)
      bucket: ${ACCOUNT_ID}-lake
      delta:
        checkpoint_interval: 10
        log_retention: 30d
        timestamp_ntz_as: timestamp_ntz          # default; `timestamp` keeps reader v1 (DA-MS-8)
        catalog: { type: none }                # path-based Delta tables (v1)
```

Create `crates/data-avalanche-config/tests/raw.rs`:

```rust
//! The raw model accepts the spec's complete example (§14) and rejects an unknown key in every block.

use data_avalanche_config::{PARSE, RawConfig, parse_document};

const SPEC_EXAMPLE: &str = include_str!("fixtures/spec_example.yaml");

#[test]
fn parses_the_spec_example() {
    let raw: RawConfig = parse_document(SPEC_EXAMPLE).unwrap();
    assert_eq!(raw.config_version.unwrap().text(), "1");
    let globals = raw.globals.unwrap();
    let cluster = globals.cluster.unwrap();
    assert_eq!(cluster.lease_duration.unwrap().text(), "15s");
    let tls = cluster.memberlist.unwrap().tls.unwrap();
    assert_eq!(tls.key.unwrap().text(), "${GOSSIP_TLS_KEY}");
    let defaults = raw.defaults.unwrap();
    let rest = defaults
        .sink
        .unwrap()
        .iceberg
        .unwrap()
        .catalog
        .unwrap()
        .rest
        .unwrap();
    assert_eq!(
        rest.oauth2.unwrap().scope.unwrap().text(),
        "catalog:read catalog:write"
    );
    let pipelines = raw.pipelines.unwrap();
    assert_eq!(
        pipelines.keys().collect::<Vec<_>>(),
        ["events-mysql", "orders-pg"]
    );
    let orders = &pipelines["orders-pg"];
    let flush = orders.sink.as_ref().unwrap().flush.as_ref().unwrap();
    assert!(
        flush.seconds.is_none(),
        "seconds is inherited, not disabled"
    );
    assert_eq!(
        flush.rows.as_ref().unwrap().as_ref().unwrap().text(),
        "50000"
    );
    let tables = orders.tables.as_ref().unwrap();
    let part = &tables.partitioning.as_ref().unwrap()["public.orders"][0];
    assert_eq!(part.transform.as_ref().unwrap().text(), "day");
}

/// One document per raw struct, each with an unknown key `bogus` inside that struct. A struct
/// that forgot `deny_unknown_fields` makes its case parse and fails the test.
const UNKNOWN_KEY_CASES: &[&str] = &[
    "bogus: 1",
    "globals: {bogus: 1}",
    "globals: {state: {bogus: 1}}",
    "globals: {api: {bogus: 1}}",
    "globals: {api: {jwt: {bogus: 1}}}",
    "globals: {api: {oauth2: {bogus: 1}}}",
    "globals: {api: {tls: {bogus: 1}}}",
    "globals: {log: {bogus: 1}}",
    "globals: {cluster: {bogus: 1}}",
    "globals: {cluster: {memberlist: {bogus: 1}}}",
    "globals: {cluster: {memberlist: {tls: {bogus: 1}}}}",
    "globals: {tracing: {bogus: 1}}",
    "globals: {azure: {bogus: 1}}",
    "defaults: {bogus: 1}",
    "pipelines: {p: {bogus: 1}}",
    "pipelines: {p: {source: {bogus: 1}}}",
    "pipelines: {p: {source: {timeouts: {bogus: 1}}}}",
    "pipelines: {p: {source: {postgres: {bogus: 1}}}}",
    "pipelines: {p: {source: {mysql: {bogus: 1}}}}",
    "pipelines: {p: {tables: {bogus: 1}}}",
    "pipelines: {p: {tables: {snapshots: {bogus: 1}}}}",
    "pipelines: {p: {tables: {partitioning: {t: [{bogus: 1}]}}}}",
    "pipelines: {p: {sink: {bogus: 1}}}",
    "pipelines: {p: {sink: {flush: {bogus: 1}}}}",
    "pipelines: {p: {sink: {metrics: {bogus: 1}}}}",
    "pipelines: {p: {sink: {iceberg: {bogus: 1}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {bogus: 1}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {glue: {bogus: 1}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {hive: {bogus: 1}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {hive: {kerberos: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {hive: {tls: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {rest: {bogus: 1}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {rest: {oauth2: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {rest: {sigv4: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {rest: {tls: {bogus: 1}}}}}}}",
    "pipelines: {p: {sink: {iceberg: {catalog: {retry: {bogus: 1}}}}}}",
    "pipelines: {p: {sink: {delta: {bogus: 1}}}}",
    "pipelines: {p: {sink: {delta: {catalog: {bogus: 1}}}}}",
    "pipelines: {p: {state: {bogus: 1}}}",
    "pipelines: {p: {schema_evolution: {bogus: 1}}}",
];

#[test]
fn rejects_an_unknown_key_in_every_block() {
    for case in UNKNOWN_KEY_CASES {
        let err = parse_document::<RawConfig>(case).expect_err(case);
        assert_eq!(err.rule, PARSE, "{case}");
        assert!(
            err.message.contains("unknown field `bogus`"),
            "{case}: {}",
            err.message
        );
    }
}

#[test]
fn old_draft_spellings_are_unknown_keys() {
    // §15: the draft's kebab-case `ssl-mode` and the pre-multi-sink pipeline-level `catalog`.
    for case in [
        "pipelines: {p: {source: {ssl-mode: require}}}",
        "pipelines: {p: {catalog: {glue: {name: x}}}}",
        "pipelines: {p: {sink: {strategy: mor}}}",
    ] {
        let err = parse_document::<RawConfig>(case).expect_err(case);
        assert!(
            err.message.contains("unknown field"),
            "{case}: {}",
            err.message
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0583]: file not found for module `raw`
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-config/src/raw.rs`:

```rust
//! The raw model: the YAML document as written (config §3–§9). Every field is optional so that
//! presence is checked by V-rules (with IDs) rather than by serde; every struct denies unknown
//! fields (§2); every scalar is a [`Leaf`] (text + location, coerced later). `defaults` has the
//! same shape as a pipeline (§7).

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::time::Duration;

use data_avalanche_core::LogicalType;
use serde::Deserialize;

use crate::coerce::LogFilter;
use crate::enums::*;
use crate::leaf::{Leaf, Tri, tri};
use crate::units::{ByteSize, HostPort};

type L<T> = Option<Leaf<T>>;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    pub config_version: L<u32>,
    pub globals: Option<RawGlobals>,
    pub defaults: Option<RawPipeline>,
    pub pipelines: Option<BTreeMap<String, RawPipeline>>,
}

// ---- globals (§6) ----

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawGlobals {
    pub cloud: L<Cloud>,
    pub state: Option<RawGlobalState>,
    pub api: Option<RawApi>,
    pub log: Option<RawLog>,
    pub cluster: Option<RawCluster>,
    pub tracing: Option<RawTracing>,
    pub azure: Option<RawAzure>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawGlobalState {
    pub bucket: L<String>,
    pub prefix: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawApi {
    pub address: L<HostPort>,
    pub auth: L<ApiAuth>,
    pub user: L<String>,
    pub password: L<String>,
    pub credentials_file: L<String>,
    pub jwt: Option<RawJwt>,
    pub oauth2: Option<RawApiOauth2>,
    pub role_mapping: Option<BTreeMap<String, Vec<Leaf<String>>>>,
    pub tls: Option<RawApiTls>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawJwt {
    pub issuer: L<String>,
    pub audience: L<String>,
    pub jwks_url: L<String>,
    pub roles_claim: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawApiOauth2 {
    pub issuer: L<String>,
    pub audience: L<String>,
    pub jwks_url: L<String>,
    pub discovery_url: L<String>,
    pub roles_claim: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawApiTls {
    pub enabled: L<bool>,
    pub cert: L<String>,
    pub key: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawLog {
    pub level: L<LogFilter>,
    pub format: L<LogFormat>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawCluster {
    pub tokens: L<u32>,
    pub lock_backend: L<LockBackend>,
    pub lease_duration: L<Duration>,
    pub lease_renewal_interval: L<Duration>,
    pub lease_safety_margin: L<f64>,
    pub rebalance_stabilization: L<Duration>,
    pub max_concurrent_handoffs: L<u32>,
    pub max_concurrent_takeovers: L<u32>,
    pub drain_timeout: L<Duration>,
    pub discovery: L<Discovery>,
    pub discovery_dns_name: L<String>,
    pub memberlist: Option<RawMemberlist>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawMemberlist {
    pub join_members: Option<Vec<Leaf<HostPort>>>,
    pub node_name: L<String>,
    pub advertise_addr: L<String>,
    pub advertise_port: L<u16>,
    pub bind_addr: Option<Vec<Leaf<IpAddr>>>,
    pub bind_port: L<u16>,
    pub packet_dial_timeout: L<Duration>,
    pub packet_write_timeout: L<Duration>,
    pub compression_enabled: L<bool>,
    pub gossip_interval: L<Duration>,
    pub gossip_nodes: L<u32>,
    pub probe_interval: L<Duration>,
    pub probe_timeout: L<Duration>,
    pub dead_node_reclaim_time: L<Duration>,
    pub push_pull_interval: L<Duration>,
    pub retransmit_factor: L<u32>,
    pub lifeguard_enabled: L<bool>,
    pub tls: Option<RawMemberlistTls>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawMemberlistTls {
    pub enabled: L<bool>,
    pub cert: L<String>,
    pub key: L<String>,
    pub ca: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawTracing {
    pub otlp_endpoint: L<String>,
    pub sample_ratio: L<f64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawAzure {
    pub account_name: L<String>,
}

// ---- pipelines and defaults (§7, §9) ----

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawPipeline {
    pub id: L<String>,
    pub source: Option<RawSource>,
    pub tables: Option<RawTables>,
    pub sink: Option<RawSink>,
    pub state: Option<RawPipelineState>,
    pub schema_evolution: Option<RawSchemaEvolution>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSource {
    #[serde(rename = "type")]
    pub kind: L<SourceType>,
    pub host: L<String>,
    pub port: L<u16>,
    pub db: L<String>,
    pub user: L<String>,
    pub password: L<String>,
    pub credentials_file: L<String>,
    pub ssl_mode: L<SslMode>,
    pub ssl_cert: L<String>,
    pub ssl_key: L<String>,
    pub ssl_ca: L<String>,
    pub timeouts: Option<RawTimeouts>,
    pub toast_strategy: L<ToastStrategy>,
    pub postgres: Option<RawPostgres>,
    pub mysql: Option<RawMysql>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawTimeouts {
    pub connect: L<Duration>,
    pub command: L<Duration>,
    pub snapshot: L<Duration>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawPostgres {
    pub publication: L<String>,
    pub slot: L<String>,
    pub auto_cleanup: L<bool>,
    pub heartbeat: L<Duration>,
    pub slot_management: L<SlotManagement>,
    pub slot_on_orphan: L<SlotOnOrphan>,
    #[serde(default, deserialize_with = "tri")]
    pub max_wal_lag_bytes: Tri<ByteSize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawMysql {
    pub replication_type: L<ReplicationType>,
    pub tinyint1_as_bool: L<bool>,
    pub tinyint1_detection: L<Tinyint1Detection>,
    pub binlog_purge_action: L<BinlogPurgeAction>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawTables {
    pub include: Option<Vec<Leaf<String>>>,
    pub exclude: Option<Vec<Leaf<String>>>,
    pub snapshots: Option<RawSnapshots>,
    pub column_type_overrides: Option<BTreeMap<String, Leaf<LogicalType>>>,
    pub numeric_overflow_action: L<NumericOverflowAction>,
    pub primary_key: Option<BTreeMap<String, Vec<Leaf<String>>>>,
    pub partitioning: Option<BTreeMap<String, Vec<RawPartitionField>>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSnapshots {
    pub mode: L<SnapshotMode>,
    pub parallelism: L<u32>,
    pub chunk_size: L<u64>,
    pub overrides: Option<BTreeMap<String, Leaf<SnapshotMode>>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawPartitionField {
    pub column: L<String>,
    pub transform: L<Transform>,
    pub n: L<u32>,
    pub width: L<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSink {
    #[serde(rename = "type")]
    pub kind: L<SinkType>,
    pub write_mode: L<WriteMode>,
    pub bucket: L<String>,
    pub prefix: L<String>,
    pub flush: Option<RawFlush>,
    #[serde(default, deserialize_with = "tri")]
    pub max_transaction_batch_rows: Tri<u64>,
    pub max_transaction_buffer_bytes: L<ByteSize>,
    pub buffer_capacity_rows: L<u64>,
    pub metrics: Option<RawSinkMetrics>,
    pub iceberg: Option<RawIceberg>,
    pub delta: Option<RawDelta>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawFlush {
    #[serde(default, deserialize_with = "tri")]
    pub seconds: Tri<u64>,
    #[serde(default, deserialize_with = "tri")]
    pub rows: Tri<u64>,
    #[serde(default, deserialize_with = "tri")]
    pub bytes: Tri<ByteSize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSinkMetrics {
    pub per_table: L<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawIceberg {
    pub strategy: L<IcebergStrategy>,
    pub catalog: Option<RawCatalog>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawCatalog {
    #[serde(rename = "type")]
    pub kind: L<CatalogKind>,
    pub glue: Option<RawGlue>,
    pub hive: Option<RawHive>,
    pub rest: Option<RawRest>,
    pub retry: Option<RawRetry>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawGlue {
    pub name: L<String>,
    pub database: L<String>,
    pub strip_schema: L<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawHive {
    pub url: L<String>,
    pub auth: L<HiveAuth>,
    pub user: L<String>,
    pub password: L<String>,
    pub credentials_file: L<String>,
    pub kerberos: Option<RawKerberos>,
    pub tls: Option<RawHiveTls>,
    pub database: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawKerberos {
    pub principal: L<String>,
    pub keytab: L<String>,
    pub service_name: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawHiveTls {
    pub enabled: L<bool>,
    pub ca: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRest {
    pub url: L<String>,
    pub warehouse: L<String>,
    pub prefix: L<String>,
    pub auth: L<RestAuth>,
    pub token: L<String>,
    pub token_file: L<String>,
    pub oauth2: Option<RawRestOauth2>,
    pub sigv4: Option<RawSigv4>,
    pub tls: Option<RawRestTls>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRestOauth2 {
    pub client_id: L<String>,
    pub client_secret: L<String>,
    pub credentials_file: L<String>,
    pub token_url: L<String>,
    pub scope: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSigv4 {
    pub region: L<String>,
    pub service: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRestTls {
    pub ca: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRetry {
    pub max_attempts: L<u32>,
    pub base_backoff: L<Duration>,
    pub max_backoff: L<Duration>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawDelta {
    pub column_mapping: L<ColumnMapping>,
    pub checkpoint_interval: L<u64>,
    pub log_retention: L<Duration>,
    pub timestamp_ntz_as: L<TimestampNtzAs>,
    pub table_properties: Option<BTreeMap<String, Leaf<String>>>,
    pub catalog: Option<RawDeltaCatalog>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawDeltaCatalog {
    #[serde(rename = "type")]
    pub kind: L<DeltaCatalogType>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawPipelineState {
    pub bucket: L<String>,
    pub prefix: L<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSchemaEvolution {
    pub enabled: L<bool>,
    pub rescan_interval: L<Duration>,
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 11; `tests/parse.rs` 8; `tests/raw.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/tests/fixtures/spec_example.yaml \
  crates/data-avalanche-config/tests/raw.rs \
  crates/data-avalanche-config/src/raw.rs
git commit -m "feat(config): raw config model for every spec block"
```


---

### Task 5: `${VAR}` template expansion

**Files:**
- Create: `crates/data-avalanche-config/src/template.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `trait Env { fn var(&self, name) -> Option<String> }`, implemented for `ProcessEnv`,
    `BTreeMap<String,String>` and `HashMap<String,String>`.
  - `TemplateError { Unterminated, BadName(String), NestedDefault, Unset(String) }`.
  - `Expanded { Text(String), Unresolved }`.
  - `template::expand(text, Option<&dyn Env>) -> Result<Expanded, TemplateError>`. With `None` (the
    structural pass), text that contains a template returns `Unresolved`; malformed templates still
    error.
  - `${V:-d}` falls back when V is unset **or empty** (§4.3). `$${` is a literal `${`. There is no
    recursive expansion.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 0fd8d4b..d6ea616 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -9,6 +9,7 @@ pub mod leaf;
 pub mod parse;
 pub mod path;
 pub mod raw;
+pub mod template;
 pub mod units;
 
 pub use coerce::{LeafType, LogFilter};
@@ -17,4 +18,5 @@ pub use leaf::{Leaf, Loc, Tri};
 pub use parse::parse_document;
 pub use path::YamlPath;
 pub use raw::RawConfig;
+pub use template::{Env, ProcessEnv};
 pub use units::{ByteSize, HostPort};
```

Create `crates/data-avalanche-config/src/template.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn env() -> BTreeMap<String, String> {
        [
            ("ACCOUNT_ID", "12345"),
            ("EMPTY", ""),
            ("PG_PORT", "5432"),
            ("LOOP", "${ACCOUNT_ID}"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn text(t: &str) -> String {
        match expand(t, Some(&env())).unwrap() {
            Expanded::Text(s) => s,
            Expanded::Unresolved => panic!("unresolved with an env"),
        }
    }

    #[test]
    fn substitutes_whole_and_partial_values() {
        assert_eq!(text("${PG_PORT}"), "5432");
        assert_eq!(text("${ACCOUNT_ID}-state"), "12345-state");
        assert_eq!(text("a${ACCOUNT_ID}b${PG_PORT}c"), "a12345b5432c");
        assert_eq!(text("plain"), "plain");
    }

    #[test]
    fn default_applies_when_unset_or_empty() {
        assert_eq!(text("${MISSING:-fallback}"), "fallback");
        assert_eq!(text("${EMPTY:-fallback}"), "fallback");
        assert_eq!(text("${PG_PORT:-1}"), "5432");
        assert_eq!(text("${MISSING:-}"), "");
        // Set but empty, no default: the empty string (a required field then fails its own rule).
        assert_eq!(text("${EMPTY}"), "");
    }

    #[test]
    fn escape_yields_a_literal_reference() {
        assert_eq!(text("$${ACCOUNT_ID}"), "${ACCOUNT_ID}");
        assert_eq!(text("cost: $5 and $$"), "cost: $5 and $$");
        assert_eq!(
            expand("$${X}", None).unwrap(),
            Expanded::Text("${X}".into())
        );
    }

    #[test]
    fn is_not_recursive() {
        assert_eq!(text("${LOOP}"), "${ACCOUNT_ID}");
    }

    #[test]
    fn reports_unset_and_malformed_references() {
        assert_eq!(
            expand("${NOPE}", Some(&env())).unwrap_err(),
            TemplateError::Unset("NOPE".into())
        );
        assert_eq!(
            expand("${OPEN", None).unwrap_err(),
            TemplateError::Unterminated
        );
        assert_eq!(
            expand("${1X}", None).unwrap_err(),
            TemplateError::BadName("1X".into())
        );
        assert_eq!(
            expand("${}", None).unwrap_err(),
            TemplateError::BadName("".into())
        );
        assert_eq!(
            expand("${A:-${B}}", None).unwrap_err(),
            TemplateError::NestedDefault
        );
    }

    #[test]
    fn without_env_references_stay_unresolved() {
        assert_eq!(expand("${PG_PORT}", None).unwrap(), Expanded::Unresolved);
        assert_eq!(expand("x", None).unwrap(), Expanded::Text("x".into()));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `template::Env`, `template::ProcessEnv`
error[E0425]: cannot find type `BTreeMap` in this scope
error[E0425]: cannot find function `expand` in this scope
error[E0433]: cannot find type `Expanded` in this scope
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-config/src/template.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Environment-variable substitution on string leaves (config §4.3): `${VAR}`, `${VAR:-default}`,
//! and the `$${` escape. Substitution is not recursive: a substituted value is never re-scanned.

use std::collections::{BTreeMap, HashMap};

use thiserror::Error;

/// Where `${VAR}` values come from. The process environment in production; a map in tests (so
/// tests never mutate the process environment, which is `unsafe` in edition 2024).
pub trait Env {
    fn var(&self, name: &str) -> Option<String>;
}

/// The process environment. A variable whose value isn't valid UTF-8 counts as unset.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnv;

impl Env for ProcessEnv {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

impl Env for BTreeMap<String, String> {
    fn var(&self, name: &str) -> Option<String> {
        self.get(name).cloned()
    }
}

impl Env for HashMap<String, String> {
    fn var(&self, name: &str) -> Option<String> {
        self.get(name).cloned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TemplateError {
    #[error("unterminated ${{...}} reference")]
    Unterminated,
    #[error("{0:?} is not a variable name (letters, digits and _, not starting with a digit)")]
    BadName(String),
    #[error(
        "a ${{...}} reference inside a :- default is not supported (substitution is not recursive)"
    )]
    NestedDefault,
    #[error("environment variable {0} is not set")]
    Unset(String),
}

/// The outcome of expanding one leaf's text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expanded {
    /// The final text: references substituted and `$${` unescaped.
    Text(String),
    /// The text has references and no environment was given (structural validation, §11 Phase 1).
    Unresolved,
}

/// Expands `text`. With `env: None`, only checks the syntax: returns [`Expanded::Unresolved`] if
/// there is any reference, else the unescaped text. Syntax errors are reported in both modes.
pub fn expand(text: &str, env: Option<&dyn Env>) -> Result<Expanded, TemplateError> {
    let mut out = String::with_capacity(text.len());
    let mut unresolved = false;
    let mut rest = text;
    while let Some(dollar) = rest.find('$') {
        out.push_str(&rest[..dollar]);
        let tail = &rest[dollar..];
        if let Some(after) = tail.strip_prefix("$${") {
            out.push_str("${");
            rest = after;
        } else if let Some(after) = tail.strip_prefix("${") {
            let end = after.find('}').ok_or(TemplateError::Unterminated)?;
            let inner = &after[..end];
            let (name, default) = match inner.split_once(":-") {
                Some((n, d)) => (n, Some(d)),
                None => (inner, None),
            };
            if !is_var_name(name) {
                return Err(TemplateError::BadName(name.to_string()));
            }
            if default.is_some_and(|d| d.contains("${")) {
                return Err(TemplateError::NestedDefault);
            }
            match env {
                None => unresolved = true,
                Some(env) => match (env.var(name), default) {
                    (Some(v), Some(d)) if v.is_empty() => out.push_str(d),
                    (Some(v), _) => out.push_str(&v),
                    (None, Some(d)) => out.push_str(d),
                    (None, None) => return Err(TemplateError::Unset(name.to_string())),
                },
            }
            rest = &after[end + 1..];
        } else {
            out.push('$');
            rest = &tail[1..];
        }
    }
    out.push_str(rest);
    Ok(if unresolved {
        Expanded::Unresolved
    } else {
        Expanded::Text(out)
    })
}

fn is_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 17; `tests/parse.rs` 8; `tests/raw.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/src/template.rs
git commit -m "feat(config): env-var template expansion"
```


---

### Task 6: Evaluation context and the rule registry

**Files:**
- Create: `crates/data-avalanche-config/src/ctx.rs`, `crates/data-avalanche-config/src/registry.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`

**Interfaces:**
- Consumes: Tasks 2, 3 and 5.
- Produces:
  - `At { locus, path }` with `At::config(path)`, `At::pipeline(name, path)`, `.key(k)`, `.index(i)`.
  - `Ctx::structural()`, `Ctx::resolving(&dyn Env)` and `is_resolving()`.
  - `Ctx::text(&Leaf<T>, &At) -> Option<String>`. It expands the leaf and returns `None` in the
    structural pass when the leaf is a template; a bad template reports V-063.
  - `Ctx::value::<T: LeafType>(&Leaf<T>, &At) -> Option<T>`. A coercion failure reports `T::RULE`.
  - `value_with`, `opt`, `peek` (no reporting), `error`, `warning`, `report()` and `into_report()`.
  - `registry::{RuleSeverity, RuleInput, RuleMeta, RULES, rule_meta}`. `RuleInput` is one of Config,
    Resolution, Reload, ClusterAnchor, TableSet, SourceSchema. It says which rules the file alone can
    check.

`RULES` transcribes spec §13, 112 rows (V-117 included). `text` is shortened where long; `field` and `locus` are
verbatim. When the spec's §13 changes, update this table in the same commit.

- [ ] **Step 1: Write the failing tests**

Create `crates/data-avalanche-config/src/ctx.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::*;
    use crate::enums::Cloud;

    fn at() -> At {
        At::pipeline(
            "p",
            YamlPath::root()
                .key("pipelines")
                .key("p")
                .key("source")
                .key("port"),
        )
    }

    #[test]
    fn structural_pass_leaves_templates_unknown_and_checks_literals() {
        let mut ctx = Ctx::structural();
        assert_eq!(ctx.value(&Leaf::<u16>::new("${PG_PORT}"), &at()), None);
        assert!(
            ctx.report().is_ok(),
            "a template is not an error before resolution"
        );
        assert_eq!(ctx.value(&Leaf::<u16>::new("5432"), &at()), Some(5432));
        assert_eq!(ctx.value(&Leaf::<u16>::new("abc"), &at()), None);
        let e = &ctx.report().errors[0];
        assert_eq!(
            (e.rule.as_str(), e.path.as_str()),
            ("TYPE", "pipelines.p.source.port")
        );
        assert_eq!(e.locus, Locus::Pipeline("p".into()));
    }

    #[test]
    fn resolving_pass_substitutes_then_coerces() {
        let env: BTreeMap<String, String> = [("PG_PORT".to_string(), "5432".to_string())].into();
        let mut ctx = Ctx::resolving(&env);
        assert_eq!(
            ctx.value(&Leaf::<u16>::new("${PG_PORT}"), &at()),
            Some(5432)
        );
        assert_eq!(ctx.value(&Leaf::<u16>::new("${NOPE}"), &at()), None);
        assert_eq!(ctx.report().errors[0].rule, "V-063");
        assert_eq!(
            ctx.report().errors[0].message,
            "environment variable NOPE is not set"
        );
    }

    #[test]
    fn malformed_template_is_v063_even_before_resolution() {
        let mut ctx = Ctx::structural();
        assert_eq!(ctx.text(&Leaf::<String>::new("${OPEN"), &at()), None);
        assert_eq!(ctx.report().errors[0].rule, "V-063");
    }

    #[test]
    fn enum_failures_carry_their_value_domain_rule() {
        let mut ctx = Ctx::structural();
        let at = At::config(YamlPath::root().key("globals").key("cloud"));
        assert_eq!(ctx.value(&Leaf::<Cloud>::new("gcp"), &at), None);
        assert_eq!(ctx.report().errors[0].rule, "V-015");
        assert_eq!(
            ctx.value(&Leaf::<Duration>::new("15s"), &at),
            Some(Duration::from_secs(15))
        );
    }

    #[test]
    fn peek_reports_nothing() {
        let ctx = Ctx::structural();
        assert_eq!(
            ctx.peek(&Some(Leaf::<Cloud>::new("azure"))),
            Some(Cloud::Azure)
        );
        assert_eq!(ctx.peek(&Some(Leaf::<Cloud>::new("gcp"))), None);
        assert_eq!(ctx.peek::<Cloud>(&None), None);
        assert!(ctx.report().is_ok());
    }
}
```

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index d6ea616..f853588 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -3,20 +3,24 @@
 //! resolved config the runtime uses, and the reload diff.
 
 pub mod coerce;
+pub mod ctx;
 pub mod diag;
 pub mod enums;
 pub mod leaf;
 pub mod parse;
 pub mod path;
 pub mod raw;
+pub mod registry;
 pub mod template;
 pub mod units;
 
 pub use coerce::{LeafType, LogFilter};
+pub use ctx::{At, Ctx};
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use leaf::{Leaf, Loc, Tri};
 pub use parse::parse_document;
 pub use path::YamlPath;
 pub use raw::RawConfig;
+pub use registry::{RULES, RuleInput, RuleMeta, RuleSeverity, rule_meta};
 pub use template::{Env, ProcessEnv};
 pub use units::{ByteSize, HostPort};
```

Create `crates/data-avalanche-config/src/registry.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_every_spec_rule_once_in_order() {
        assert_eq!(RULES.len(), 112);
        assert!(
            RULES.windows(2).all(|w| w[0].id < w[1].id),
            "sorted, no duplicates"
        );
        // IDs retired by the spec (§15 item 18) or never allocated in it.
        for gone in ["V-024", "V-031", "V-032", "V-039", "V-090"] {
            assert!(rule_meta(gone).is_none(), "{gone}");
        }
        assert_eq!(
            rule_meta("V-026").unwrap().severity,
            RuleSeverity::ErrorAndWarning
        );
        assert_eq!(rule_meta("V-061").unwrap().input, RuleInput::Reload);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0432]: unresolved imports `ctx::At`, `ctx::Ctx`
error[E0432]: unresolved imports `registry::RULES`, `registry::RuleInput`, `registry::RuleMeta`, `registry::RuleSeverity`, `registry::rule_meta`
error[E0425]: cannot find type `At` in this scope
error[E0433]: cannot find type `YamlPath` in this scope
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-config/src/ctx.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! The evaluation context shared by merge, rules and the resolved-config build: it turns leaves
//! into values (substituting `${VAR}` when resolving) and collects diagnostics.

use crate::coerce::LeafType;
use crate::diag::{Diagnostic, Locus, Report, Severity};
use crate::leaf::{Leaf, Loc};
use crate::path::YamlPath;
use crate::template::{Env, Expanded, expand};

/// Where a check applies: the locus (config, a named pipeline, or reload) and the YAML path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct At {
    pub locus: Locus,
    pub path: YamlPath,
}

impl At {
    /// A config-level location (`globals…`, top-level keys).
    pub fn config(path: YamlPath) -> Self {
        Self {
            locus: Locus::Config,
            path,
        }
    }

    /// Inside pipeline `name` (its key under `pipelines`); `path` is the full path from the root.
    pub fn pipeline(name: &str, path: YamlPath) -> Self {
        Self {
            locus: Locus::Pipeline(name.to_string()),
            path,
        }
    }

    pub fn key(&self, key: impl Into<String>) -> Self {
        Self {
            locus: self.locus.clone(),
            path: self.path.key(key),
        }
    }

    pub fn index(&self, index: usize) -> Self {
        Self {
            locus: self.locus.clone(),
            path: self.path.index(index),
        }
    }
}

/// One evaluation pass. `structural()` is config §11 Phase 1 (no environment, no files);
/// `resolving(env)` is Phase 2 (references substituted, files read by the build).
pub struct Ctx<'e> {
    env: Option<&'e dyn Env>,
    report: Report,
}

impl std::fmt::Debug for Ctx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ctx")
            .field("resolving", &self.env.is_some())
            .field("report", &self.report)
            .finish()
    }
}

impl<'e> Ctx<'e> {
    pub fn structural() -> Self {
        Self {
            env: None,
            report: Report::default(),
        }
    }

    pub fn resolving(env: &'e dyn Env) -> Self {
        Self {
            env: Some(env),
            report: Report::default(),
        }
    }

    pub fn is_resolving(&self) -> bool {
        self.env.is_some()
    }

    /// The leaf's final text: `${VAR}` substituted and `$${` unescaped. `None` when the text is a
    /// template and this is the structural pass, or when substitution fails (V-063, reported).
    pub fn text<T>(&mut self, leaf: &Leaf<T>, at: &At) -> Option<String> {
        match expand(leaf.text(), self.env) {
            Ok(Expanded::Text(t)) => Some(t),
            Ok(Expanded::Unresolved) => None,
            Err(e) => {
                self.error("V-063", at, leaf.loc(), e.to_string());
                None
            }
        }
    }

    /// The leaf coerced to `T`. `None` when its text is unknown (see [`Ctx::text`]) or when it
    /// doesn't coerce; the latter is reported under `T::RULE` (a value-domain V-rule or `TYPE`).
    pub fn value<T: LeafType>(&mut self, leaf: &Leaf<T>, at: &At) -> Option<T> {
        self.value_with(leaf, at, |_, reason| reason)
    }

    /// [`Ctx::value`] with a caller-built message (`text`, `reason`) → message, for rules whose
    /// message the spec fixes (V-078).
    pub fn value_with<T: LeafType>(
        &mut self,
        leaf: &Leaf<T>,
        at: &At,
        message: impl FnOnce(&str, String) -> String,
    ) -> Option<T> {
        let text = self.text(leaf, at)?;
        match T::parse_leaf(&text) {
            Ok(v) => Some(v),
            Err(reason) => {
                self.error(T::RULE, at, leaf.loc(), message(&text, reason));
                None
            }
        }
    }

    /// [`Ctx::value`] of an optional field; `None` when absent.
    pub fn opt<T: LeafType>(&mut self, leaf: &Option<Leaf<T>>, at: &At) -> Option<T> {
        leaf.as_ref().and_then(|l| self.value(l, at))
    }

    /// The leaf's value without reporting anything: for choices made before validation (the
    /// union branch in merge). `None` when absent, unknown or invalid.
    pub fn peek<T: LeafType>(&self, leaf: &Option<Leaf<T>>) -> Option<T> {
        match expand(leaf.as_ref()?.text(), self.env) {
            Ok(Expanded::Text(t)) => T::parse_leaf(&t).ok(),
            _ => None,
        }
    }

    pub fn error(&mut self, rule: &str, at: &At, loc: Option<Loc>, message: impl Into<String>) {
        self.push(rule, Severity::Error, at, loc, message.into());
    }

    pub fn warning(&mut self, rule: &str, at: &At, loc: Option<Loc>, message: impl Into<String>) {
        self.push(rule, Severity::Warning, at, loc, message.into());
    }

    fn push(&mut self, rule: &str, severity: Severity, at: &At, loc: Option<Loc>, message: String) {
        self.report.push(Diagnostic {
            locus: at.locus.clone(),
            path: at.path.to_string(),
            rule: rule.to_string(),
            severity,
            loc,
            message,
        });
    }

    pub fn report(&self) -> &Report {
        &self.report
    }

    pub fn into_report(self) -> Report {
        self.report
    }
}

```

In `crates/data-avalanche-config/src/registry.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! The master validation table (config §13) as data: every V-rule's ID, severity, locus, the
//! input it needs, the field it covers and its text. The checks themselves live in `rules`; this
//! table drives the fixture-coverage test and, later, the generated configuration reference.

/// The severity column of §13. `ErrorAndWarning` rules error outside a hard range and warn outside
/// a recommended band (e.g. V-026 `tokens`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleSeverity {
    Error,
    Warning,
    ErrorAndWarning,
}

/// What a rule needs besides the config file itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleInput {
    /// The config file (Phase 1, and again after resolution).
    Config,
    /// The environment and referenced files (Phase 2, §13.9).
    Resolution,
    /// The previously running config (reload only, V-061).
    Reload,
    /// The cluster anchor stored in `globals.state` (startup, V-062/V-076/V-094).
    ClusterAnchor,
    /// The pipeline's resolved table set (pipeline start or table-set change).
    TableSet,
    /// Source column types (schema resolution, V-079).
    SourceSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuleMeta {
    pub id: &'static str,
    pub severity: RuleSeverity,
    /// The §13 "Locus" column, verbatim.
    pub locus: &'static str,
    pub input: RuleInput,
    /// The §13 "Path / Scope" column.
    pub field: &'static str,
    /// The §13 "Rule" column (shortened where long).
    pub text: &'static str,
}

const fn rule(
    id: &'static str,
    severity: RuleSeverity,
    locus: &'static str,
    input: RuleInput,
    field: &'static str,
    text: &'static str,
) -> RuleMeta {
    RuleMeta {
        id,
        severity,
        locus,
        input,
        field,
        text,
    }
}

use RuleInput::*;
use RuleSeverity::*;

/// All V-rules of config §13, ordered by ID.
pub const RULES: &[RuleMeta] = &[
    rule(
        "V-001",
        Error,
        "pipeline",
        Config,
        r#"source"#,
        r#"Exactly one engine block (postgres xor mysql) after merge, matching type."#,
    ),
    rule(
        "V-002",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog"#,
        r#"Exactly one sub-block (glue/hive/rest); if type is set it must equal the present block."#,
    ),
    rule(
        "V-003",
        Error,
        "pipeline",
        Config,
        r#"source.{postgres,mysql}"#,
        r#"Engine block present must match type; the foreign engine block is forbidden."#,
    ),
    rule(
        "V-004",
        Error,
        "config",
        Config,
        r#"top"#,
        r#"pipelines key present (may be {})."#,
    ),
    rule(
        "V-005",
        Error,
        "config",
        Config,
        r#"top"#,
        r#"globals present."#,
    ),
    rule(
        "V-006",
        Error,
        "config",
        Config,
        r#"globals.state.bucket"#,
        r#"Present."#,
    ),
    rule(
        "V-007",
        Error,
        "pipeline",
        Config,
        r#"tables.include"#,
        r#"Present and non-empty."#,
    ),
    rule(
        "V-008",
        Error,
        "pipeline",
        Config,
        r#"source.host"#,
        r#"Resolvable (pipeline or defaults)."#,
    ),
    rule(
        "V-009",
        Error,
        "pipeline",
        Config,
        r#"source.db"#,
        r#"Resolvable."#,
    ),
    rule(
        "V-010",
        Error,
        "pipeline",
        Config,
        r#"source credentials"#,
        r#"Resolvable: either user+password, or credentials_file (exactly one — V-065)."#,
    ),
    rule(
        "V-011",
        Error,
        "pipeline",
        Config,
        r#"source.postgres.{publication,slot}"#,
        r#"Present when type=postgres."#,
    ),
    rule(
        "V-012",
        Error,
        "pipeline",
        Config,
        r#"sink.bucket"#,
        r#"Resolvable."#,
    ),
    rule(
        "V-013",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog"#,
        r#"When sink.type = iceberg: a catalog block resolvable (pipeline or defaults)."#,
    ),
    rule(
        "V-014",
        Error,
        "config",
        Config,
        r#"config_version"#,
        r#"Present and supported by this binary."#,
    ),
    rule(
        "V-015",
        Error,
        "config",
        Config,
        r#"globals.cloud"#,
        r#"∈ {aws, google, azure}."#,
    ),
    rule(
        "V-016",
        Error,
        "config",
        Config,
        r#"globals.api.auth"#,
        r#"∈ {none, basic, jwt, oauth2}."#,
    ),
    rule(
        "V-017",
        Error,
        "config",
        Config,
        r#"globals.log.level"#,
        r#"Valid level / EnvFilter syntax."#,
    ),
    rule(
        "V-018",
        Error,
        "config",
        Config,
        r#"globals.log.format"#,
        r#"∈ {json, compact, pretty}."#,
    ),
    rule(
        "V-019",
        Error,
        "pipeline",
        Config,
        r#"source.type"#,
        r#"∈ {postgres, mysql}."#,
    ),
    rule(
        "V-020",
        Error,
        "pipeline",
        Config,
        r#"source.ssl_mode"#,
        r#"∈ {disable, allow, prefer, require, verify-ca, verify-full}."#,
    ),
    rule(
        "V-021",
        Error,
        "pipeline",
        Config,
        r#"source.mysql.replication_type"#,
        r#"∈ {binlog, gtid}."#,
    ),
    rule(
        "V-022",
        Error,
        "pipeline",
        Config,
        r#"tables.snapshots.mode (and overrides values)"#,
        r#"∈ {never, initial, always, dump_always, dump_initial}."#,
    ),
    rule(
        "V-023",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.strategy"#,
        r#"∈ {mor, cow}."#,
    ),
    rule(
        "V-025",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.hive.auth / sink.iceberg.catalog.rest.auth"#,
        r#"hive ∈ {none,ldap,kerberos}; rest ∈ {none,bearer,oauth2,sigv4}."#,
    ),
    rule(
        "V-026",
        ErrorAndWarning,
        "config",
        Config,
        r#"globals.cluster.tokens"#,
        r#"1 ≤ tokens ≤ 4096. Outside 64–256 → also warning."#,
    ),
    rule(
        "V-027",
        Error,
        "pipeline",
        Config,
        r#"tables.snapshots.parallelism"#,
        r#"≥ 1."#,
    ),
    rule(
        "V-028",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.glue"#,
        r#"Requires globals.cloud = aws."#,
    ),
    rule(
        "V-029",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.rest.auth = sigv4"#,
        r#"Requires globals.cloud = aws."#,
    ),
    rule(
        "V-030",
        Error,
        "pipeline",
        Config,
        r#"source.ssl_mode = allow"#,
        r#"Invalid for MySQL."#,
    ),
    rule(
        "V-033",
        Error,
        "pipeline",
        Config,
        r#"tables.snapshots.overrides keys"#,
        r#"Must be full schema.table; globs forbidden."#,
    ),
    rule(
        "V-034",
        Warning,
        "pipeline",
        TableSet,
        r#"tables.snapshots.overrides keys"#,
        r#"Key not in the resolved include/exclude set."#,
    ),
    rule(
        "V-035",
        Error,
        "pipeline",
        Config,
        r#"sink.flush"#,
        r#"All of seconds/rows/bytes null/disabled."#,
    ),
    rule(
        "V-036",
        Warning,
        "pipeline",
        Config,
        r#"sink.flush.seconds = null"#,
        r#"With at least one other trigger set (time-based disabled)."#,
    ),
    rule(
        "V-037",
        Warning,
        "pipeline",
        Config,
        r#"sink.flush.seconds"#,
        r#"0 < seconds < 5 (write amplification)."#,
    ),
    rule(
        "V-038",
        Warning,
        "pipeline",
        Config,
        r#"tables.snapshots.parallelism"#,
        r#"> 4 (connection budget)."#,
    ),
    rule(
        "V-040",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.hive.auth = ldap"#,
        r#"Requires user+password, or credentials_file (exactly one — V-065)."#,
    ),
    rule(
        "V-041",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.hive.auth = kerberos"#,
        r#"Requires kerberos.principal + kerberos.keytab."#,
    ),
    rule(
        "V-042",
        Warning,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.hive.auth = ldap without tls.enabled"#,
        r#"Password in cleartext."#,
    ),
    rule(
        "V-043",
        Warning,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.hive.tls.enabled = true w/ private CA but no tls.ca"#,
        r#"May fail cert validation against system store."#,
    ),
    rule(
        "V-044",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.rest.auth = bearer"#,
        r#"Requires token or token_file (exactly one — V-070)."#,
    ),
    rule(
        "V-045",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.rest.auth = oauth2"#,
        r#"Requires token_url, and either oauth2.client_id+client_secret or oauth2.credentials_file (exactly one — V-066)."#,
    ),
    rule(
        "V-046",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.rest.auth = sigv4"#,
        r#"Requires sigv4.region + service."#,
    ),
    rule(
        "V-047",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.rest.auth = sigv4"#,
        r#"Requires globals.cloud = aws. (Same constraint as V-029.)"#,
    ),
    rule(
        "V-048",
        Warning,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.rest.auth = oauth2, token_url = <url>/v1/oauth/tokens"#,
        r#"Deprecated built-in endpoint (Iceberg ≥1.6, removed 2.0)."#,
    ),
    rule(
        "V-049",
        Error,
        "config",
        Config,
        r#"globals.api.auth = basic"#,
        r#"Requires user+password, or credentials_file (exactly one — V-065)."#,
    ),
    rule(
        "V-050",
        Error,
        "config",
        Config,
        r#"globals.api.auth = jwt"#,
        r#"Requires jwt.issuer + jwt.jwks_url."#,
    ),
    rule(
        "V-051",
        Error,
        "config",
        Config,
        r#"globals.api.auth = oauth2"#,
        r#"Requires oauth2.issuer + (jwks_url or discovery_url)."#,
    ),
    rule(
        "V-052",
        Warning,
        "config",
        Config,
        r#"globals.api.auth ≠ none without tls.enabled"#,
        r#"Credentials/tokens in cleartext unless TLS terminated upstream."#,
    ),
    rule(
        "V-053",
        Error,
        "config",
        Config,
        r#"globals.api.tls.enabled = true"#,
        r#"Requires cert + key."#,
    ),
    rule(
        "V-054",
        Error,
        "config",
        Config,
        r#"globals.cluster.memberlist.tls.enabled = true"#,
        r#"Requires cert + key."#,
    ),
    rule(
        "V-055",
        Error,
        "pipeline",
        Config,
        r#"pipeline.id"#,
        r#"Matches ^[a-z0-9][a-z0-9_-]{0,62}$, ≤ 63 chars."#,
    ),
    rule(
        "V-056",
        Error,
        "config",
        Config,
        r#"pipeline.id"#,
        r#"Unique across all pipelines."#,
    ),
    rule(
        "V-057",
        Error,
        "pipeline",
        Config,
        r#"pipeline name (map key)"#,
        r#"Matches ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$."#,
    ),
    rule(
        "V-058",
        Error,
        "config",
        Config,
        r#"source.postgres.slot"#,
        r#"Matches ^[a-z_][a-z0-9_]{0,62}$; unique per source host+db across pipelines."#,
    ),
    rule(
        "V-059",
        Warning,
        "pipeline",
        Config,
        r#"source.postgres.publication"#,
        r#"Valid identifier; uniqueness per source DB recommended."#,
    ),
    rule(
        "V-060",
        Error,
        "config",
        Config,
        r#"defaults"#,
        r#"Must not set per-pipeline-unique resource names (postgres.publication, postgres.slot, id)."#,
    ),
    rule(
        "V-061",
        Error,
        "reload",
        Reload,
        r#"any PROC-class field"#,
        r#"Changed during a live reload."#,
    ),
    rule(
        "V-062",
        Error,
        "config (startup)",
        ClusterAnchor,
        r#"globals.cluster.tokens"#,
        r#"Differs from the cluster-established value in globals.state."#,
    ),
    rule(
        "V-063",
        Error,
        "config",
        Resolution,
        r#"any ${VAR}"#,
        r#"Unset with no :-default."#,
    ),
    rule(
        "V-064",
        Error,
        "pipeline",
        Config,
        r#"tables.include/exclude globs and snapshots.overrides keys, when source.type = mysql"#,
        r#"Must be bare table names — no . namespace prefix (the database is fixed by source.db)."#,
    ),
    rule(
        "V-065",
        Error,
        "config / pipeline",
        Config,
        r#"globals.api, source, sink.iceberg.catalog.hive"#,
        r#"credentials_file is mutually exclusive with user/password in the same block — providing both (any of user/password alongside …"#,
    ),
    rule(
        "V-066",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.rest.oauth2"#,
        r#"oauth2.credentials_file is mutually exclusive with oauth2.client_id/client_secret — providing both is an error."#,
    ),
    rule(
        "V-067",
        Error,
        "config / pipeline",
        Resolution,
        r#"any credentials_file (incl. oauth2.credentials_file)"#,
        r#"Resolved path exists and is readable; content is non-empty, valid JSON, and contains the required non-empty fields (user+password, or …"#,
    ),
    rule(
        "V-068",
        Error,
        "config / pipeline",
        Resolution,
        r#"TLS material (cert/key/ca across globals.api, memberlist, source.ssl_*, sink.iceberg.catalog.*)"#,
        r#"Resolved value (inline / file / env) is non-empty and parses as valid PEM (and a private key where a key is expected)."#,
    ),
    rule(
        "V-069",
        Error,
        "pipeline",
        Resolution,
        r#"any other referenced file (e.g. sink.iceberg.catalog.hive.kerberos.keytab)"#,
        r#"Resolved path exists, is readable, and is non-empty."#,
    ),
    rule(
        "V-070",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.rest"#,
        r#"token_file is mutually exclusive with token — providing both is an error."#,
    ),
    rule(
        "V-071",
        Error,
        "pipeline",
        Resolution,
        r#"sink.iceberg.catalog.rest.token_file"#,
        r#"Resolved path exists and is readable; content (the raw token, trailing whitespace trimmed) is non-empty."#,
    ),
    rule(
        "V-072",
        ErrorAndWarning,
        "config",
        Config,
        r#"globals.cluster.lease_safety_margin"#,
        r#"0 ≤ margin ≤ 0.5. Outside 0.05–0.30 → also warning. Fraction of lease_duration held back as the renewal safety-deadline guard …"#,
    ),
    rule(
        "V-073",
        Error,
        "config",
        Config,
        r#"globals.cluster.lease_renewal_interval"#,
        r#"Must be ≤ lease_duration / 3 (the holder must get ≥2 renew attempts inside the renewal safety deadline). Hard error — promoted from the …"#,
    ),
    rule(
        "V-074",
        Error,
        "config",
        Config,
        r#"globals.cluster.max_concurrent_handoffs / globals.cluster.max_concurrent_takeovers"#,
        r#"≥ 1 (per-node concurrency caps; local resource knobs, not cluster-wide-consistent). Clustering doc §8.5/§20.2 (G-9, D-10)."#,
    ),
    rule(
        "V-075",
        Error,
        "config",
        Config,
        r#"globals.cluster.lock_backend"#,
        r#"∈ {storage, kubernetes}. kubernetes is reserved / not yet implemented → error in v1. Clustering doc §5.6 (D-2)."#,
    ),
    rule(
        "V-076",
        Error,
        "config (startup)",
        ClusterAnchor,
        r#"globals.cluster.lock_backend"#,
        r#"Differs from the cluster-established value in the globals.state anchor."#,
    ),
    rule(
        "V-077",
        Warning,
        "pipeline",
        Config,
        r#"source.toast_strategy = re_query with tables.snapshots.mode = always"#,
        r#"re_query on high-volume snapshots may saturate the control connection pool."#,
    ),
    rule(
        "V-078",
        Error,
        "pipeline",
        Config,
        r#"tables.column_type_overrides values"#,
        r#"Each value parses in the sink-neutral logical type grammar (§9.3); else error "invalid logical type <value> in column_type_overrides for …"#,
    ),
    rule(
        "V-079",
        Warning,
        "pipeline",
        SourceSchema,
        r#"tables.numeric_overflow_action = error with any source numeric column lacking declared precision"#,
        r#"Such a column may halt the table unexpectedly under error; consider a column_type_overrides entry. (Emitted at schema resolution.)"#,
    ),
    rule(
        "V-080",
        Error,
        "pipeline",
        Config,
        r#"source.toast_strategy"#,
        r#"∈ {carry_forward, re_query} (PostgreSQL only; ignored for MySQL). research/Type_Mapping.md."#,
    ),
    rule(
        "V-081",
        Error,
        "pipeline",
        Config,
        r#"source.mysql.tinyint1_detection"#,
        r#"∈ {information_schema, none}."#,
    ),
    rule(
        "V-082",
        Error,
        "pipeline",
        Config,
        r#"tables.numeric_overflow_action"#,
        r#"∈ {string, error}."#,
    ),
    rule(
        "V-083",
        Error,
        "pipeline",
        Config,
        r#"sink.type"#,
        r#"∈ {iceberg, delta} and compiled into this binary (Cargo feature sink-iceberg / sink-delta). v1: iceberg is a known value this version …"#,
    ),
    rule(
        "V-084",
        Error,
        "pipeline",
        Config,
        r#"sink.{iceberg,delta}"#,
        r#"Exactly the format block matching sink.type after merge; the foreign block is forbidden."#,
    ),
    rule(
        "V-085",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.strategy"#,
        r#"Only meaningful with write_mode: mirror."#,
    ),
    rule(
        "V-086",
        Error,
        "pipeline",
        Config,
        r#"tables.partitioning transforms"#,
        r#"Each transform supported by the pipeline's sink (Delta v1: identity only). Active once tables.partitioning is promoted from §16."#,
    ),
    rule(
        "V-087",
        Error,
        "pipeline",
        Config,
        r#"tables.column_type_overrides values"#,
        r#"Logical type mappable by the pipeline's sink."#,
    ),
    rule(
        "V-088",
        Error,
        "pipeline",
        Config,
        r#"sink.delta.catalog.type"#,
        r#"∈ {none, glue, unity, hive}; anything but none is reserved → error in v1 (D-MS.12)."#,
    ),
    rule(
        "V-089",
        Error,
        "pipeline",
        Config,
        r#"sink.write_mode"#,
        r#"∈ {mirror, changelog}; changelog is reserved → error in v1 (D-MS.5)."#,
    ),
    rule(
        "V-091",
        Error,
        "pipeline",
        Config,
        r#"sink.type"#,
        r#"Present after merge (pipeline or defaults). No implicit default."#,
    ),
    rule(
        "V-092",
        Error,
        "pipeline",
        Config,
        r#"sink.delta.column_mapping"#,
        r#"∈ {none, name}; name → error in v1 (delta-rs refuses ADD COLUMN under column mapping; R-1)."#,
    ),
    rule(
        "V-093",
        Error,
        "pipeline",
        Config,
        r#"sink.delta.table_properties keys"#,
        r#"Only allow-listed delta.* keys; delta.columnMapping.mode / delta.enableDeletionVectors / delta.appendOnly are managed by the sink and …"#,
    ),
    rule(
        "V-094",
        Error,
        "config (startup)",
        ClusterAnchor,
        r#"globals.cluster.lease_duration"#,
        r#"Differs from the cluster-established value in the globals.state anchor (lease_duration_ms)."#,
    ),
    rule(
        "V-095",
        Error,
        "config",
        Config,
        r#"globals.cluster.discovery"#,
        r#"∈ {static, dns}."#,
    ),
    rule(
        "V-096",
        Error,
        "config",
        Config,
        r#"globals.cluster.discovery = dns"#,
        r#"Requires discovery_dns_name."#,
    ),
    rule(
        "V-097",
        Error,
        "config",
        Config,
        r#"globals.tracing.sample_ratio"#,
        r#"∈ [0, 1]."#,
    ),
    rule(
        "V-098",
        Warning,
        "config",
        Config,
        r#"globals.tracing.sample_ratio > 0 without otlp_endpoint"#,
        r#"Sampling configured but tracing disabled."#,
    ),
    rule(
        "V-099",
        Error,
        "config",
        Config,
        r#"globals.api.role_mapping"#,
        r#"Keys ∈ {viewer, operator, admin}; values are lists of strings."#,
    ),
    rule(
        "V-100",
        Warning,
        "config",
        Config,
        r#"globals.api.role_mapping with auth ∈ {none, basic}"#,
        r#"Ignored — roles come from the open mode / single basic principal."#,
    ),
    rule(
        "V-101",
        Error,
        "config",
        Config,
        r#"globals.azure.account_name"#,
        r#"Valid storage-account name (3–24 chars, lowercase letters and digits)."#,
    ),
    rule(
        "V-102",
        Error,
        "pipeline",
        Config,
        r#"source.postgres.slot_management / slot_on_orphan"#,
        r#"slot_management ∈ {managed, require_existing}; slot_on_orphan ∈ {halt, adopt_resnapshot}."#,
    ),
    rule(
        "V-103",
        Error,
        "pipeline",
        Config,
        r#"source.postgres.slot_management = require_existing"#,
        r#"Requires auto_cleanup = false (the app must not drop a slot it did not create)."#,
    ),
    rule(
        "V-104",
        Error,
        "pipeline",
        Config,
        r#"source.postgres.max_wal_lag_bytes"#,
        r#"≥ 64 MiB when set."#,
    ),
    rule(
        "V-105",
        Error,
        "pipeline",
        Config,
        r#"source.mysql.binlog_purge_action"#,
        r#"∈ {error, re_snapshot}."#,
    ),
    rule(
        "V-106",
        ErrorAndWarning,
        "pipeline",
        Config,
        r#"tables.primary_key / tables.partitioning keys"#,
        r#"Same key form as snapshots.overrides (full schema.table / MySQL bare name, no globs — V-033/V-064); column lists non-empty. A key not in …"#,
    ),
    rule(
        "V-107",
        Error,
        "pipeline",
        Config,
        r#"tables.partitioning fields"#,
        r#"transform ∈ {identity, bucket, truncate, year, month, day, hour}; bucket requires n ≥ 1; truncate requires width ≥ 1."#,
    ),
    rule(
        "V-108",
        ErrorAndWarning,
        "pipeline",
        Config,
        r#"tables.snapshots.chunk_size"#,
        r#"≥ 1. > 1,000,000 → also warning (long chunk transactions, large memory)."#,
    ),
    rule(
        "V-109",
        ErrorAndWarning,
        "pipeline",
        Config,
        r#"sink.max_transaction_batch_rows"#,
        r#"≥ 1 when set. < 1000 → also warning (commit amplification)."#,
    ),
    rule(
        "V-110",
        Error,
        "pipeline",
        Config,
        r#"sink.max_transaction_buffer_bytes"#,
        r#"≥ 16 MiB."#,
    ),
    rule(
        "V-111",
        Error,
        "pipeline",
        Config,
        r#"sink.buffer_capacity_rows"#,
        r#"≥ 1."#,
    ),
    rule(
        "V-112",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.retry"#,
        r#"max_attempts ≥ 1; base_backoff ≤ max_backoff."#,
    ),
    rule(
        "V-113",
        Error,
        "pipeline (resolution)",
        TableSet,
        r#"sink.iceberg.catalog.glue.strip_schema = true"#,
        r#"Two resolved tables share a bare name across schemas. Evaluated when the table set is resolved (start / table-set change); refuses the …"#,
    ),
    rule(
        "V-114",
        Error,
        "pipeline",
        Config,
        r#"sink.delta.timestamp_ntz_as"#,
        r#"∈ {timestamp_ntz, timestamp, string}."#,
    ),
    rule(
        "V-115",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.type / sink.iceberg.catalog.hive"#,
        r#"hive → error: the pinned iceberg-rust HMS catalog cannot commit (update_table is FeatureUnsupported; DA-CAT-6). Lifted when a pinned …"#,
    ),
    rule(
        "V-116",
        Error,
        "pipeline",
        Config,
        r#"sink.iceberg.catalog.rest.auth"#,
        r#"sigv4 → error: the pinned iceberg-rust REST client has no SigV4 signing (DA-CAT-7; upstream #3082/#3092). none/bearer/oauth2 unaffected; …"#,
    ),
    rule(
        "V-117",
        Error,
        "pipeline",
        Config,
        r#"tables.include/exclude globs, when source.type = postgres"#,
        r#"Must be schema.table: one `.` between a non-empty schema part and a non-empty table part (globs allowed within each part)."#,
    ),
];

/// The registry entry for `id` (`"V-007"`).
pub fn rule_meta(id: &str) -> Option<&'static RuleMeta> {
    RULES.iter().find(|r| r.id == id)
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 23; `tests/parse.rs` 8; `tests/raw.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/ctx.rs \
  crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/src/registry.rs
git commit -m "feat(config): evaluation context and the §13 rule registry"
```


---

### Task 7: Merge — `defaults` + pipeline (§8)

**Files:**
- Create: `crates/data-avalanche-config/src/merge.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`
- Test: `crates/data-avalanche-config/tests/merge.rs`

**Interfaces:**
- Consumes: Tasks 2–6 (`Raw*`, `Ctx`).
- Produces:
  - `trait Merge { fn merge(base: Self, over: Self, ctx: &Ctx) -> Self }`. Scalars, tri-states and
    lists merge as `over.or(base)`; maps merge key by key.
  - `merge::merge_pipeline(defaults: Option<&RawPipeline>, pipeline: &RawPipeline, ctx: &Ctx) -> RawPipeline`.

**Unions (§8.4):**
- **Source engine:** the pipeline's `type`, or `postgres` if absent. The defaults' other engine
  block is dropped.
- **Sink format:** the pipeline's `type`, else the defaults' `type`. The defaults' other format
  block is dropped.
- **Iceberg catalog:** if the pipeline has a catalog block, the defaults' blocks and `type` are
  dropped (D-M1.4). Otherwise the pipeline inherits the defaults' catalog.

A discriminator that is a template keeps every block in the structural pass (`Ctx::peek` returns
`None`). The resolving pass re-merges with the value (D-M1.3). `state.prefix` is not inherited,
because `defaults.state.prefix` is the parent prefix (Q-M1-3).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index f853588..2831046 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -7,6 +7,7 @@ pub mod ctx;
 pub mod diag;
 pub mod enums;
 pub mod leaf;
+pub mod merge;
 pub mod parse;
 pub mod path;
 pub mod raw;
@@ -18,6 +19,7 @@ pub use coerce::{LeafType, LogFilter};
 pub use ctx::{At, Ctx};
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use leaf::{Leaf, Loc, Tri};
+pub use merge::merge_pipeline;
 pub use parse::parse_document;
 pub use path::YamlPath;
 pub use raw::RawConfig;
```

Create `crates/data-avalanche-config/tests/merge.rs`:

```rust
//! Merge & override (config §8), including the §8.5 worked example and the §14 example.

use data_avalanche_config::raw::RawPipeline;
use data_avalanche_config::{Ctx, RawConfig, merge_pipeline, parse_document};

fn effective(yaml: &str, name: &str) -> RawPipeline {
    let raw: RawConfig = parse_document(yaml).unwrap();
    let pipelines = raw.pipelines.unwrap();
    merge_pipeline(raw.defaults.as_ref(), &pipelines[name], &Ctx::structural())
}

/// §8.5, with the elided `oauth2: {...}` written out.
const WORKED: &str = r#"
defaults:
  source: { type: postgres, host: db.internal, port: 5432, ssl_mode: require }
  sink:
    type: iceberg
    write_mode: mirror
    flush: { seconds: 30, rows: 100000, bytes: 512MiB }
    iceberg:
      strategy: mor
      catalog:
        glue: { name: "123456789012", database: analytics }
pipelines:
  orders:
    id: orders
    source: { db: orders, user: cdc, password: "${PW}", postgres: { publication: p, slot: s } }
    sink:
      bucket: lake
      flush: { rows: 50000 }
      iceberg:
        catalog: { rest: { url: "https://cat.example.com", auth: oauth2, oauth2: { token_url: "https://idp/t" } } }
  clicks:
    id: clicks
    source: { db: clicks, user: cdc, password: "${PW}", postgres: { publication: pc, slot: sc } }
    sink:
      type: delta
      bucket: lake
      delta: { checkpoint_interval: 20 }
"#;

#[test]
fn worked_example_orders() {
    let p = effective(WORKED, "orders");
    let src = p.source.unwrap();
    assert_eq!(src.host.unwrap().text(), "db.internal");
    assert_eq!(src.ssl_mode.unwrap().text(), "require");
    assert_eq!(src.db.unwrap().text(), "orders");
    assert_eq!(src.postgres.unwrap().slot.unwrap().text(), "s");
    let sink = p.sink.unwrap();
    assert_eq!(sink.kind.unwrap().text(), "iceberg");
    assert_eq!(sink.write_mode.unwrap().text(), "mirror");
    let flush = sink.flush.unwrap();
    assert_eq!(flush.seconds.unwrap().unwrap().text(), "30");
    assert_eq!(flush.rows.unwrap().unwrap().text(), "50000");
    assert_eq!(flush.bytes.unwrap().unwrap().text(), "512MiB");
    let iceberg = sink.iceberg.unwrap();
    assert_eq!(iceberg.strategy.unwrap().text(), "mor");
    let catalog = iceberg.catalog.unwrap();
    assert!(
        catalog.glue.is_none(),
        "the defaulted glue branch is dropped"
    );
    assert_eq!(
        catalog.rest.unwrap().url.unwrap().text(),
        "https://cat.example.com"
    );
    assert!(sink.delta.is_none());
}

#[test]
fn worked_example_clicks_drops_the_defaulted_iceberg_block() {
    let sink = effective(WORKED, "clicks").sink.unwrap();
    assert_eq!(sink.kind.unwrap().text(), "delta");
    assert!(sink.iceberg.is_none());
    assert_eq!(
        sink.delta.unwrap().checkpoint_interval.unwrap().text(),
        "20"
    );
    assert_eq!(sink.write_mode.unwrap().text(), "mirror");
    assert_eq!(sink.flush.unwrap().rows.unwrap().unwrap().text(), "100000");
}

#[test]
fn spec_example_mysql_pipeline_drops_both_defaulted_branches() {
    let p = effective(include_str!("fixtures/spec_example.yaml"), "events-mysql");
    let src = p.source.unwrap();
    assert!(
        src.postgres.is_none(),
        "defaults.source.postgres is dropped for type: mysql"
    );
    assert_eq!(src.mysql.unwrap().replication_type.unwrap().text(), "gtid");
    assert_eq!(src.timeouts.unwrap().snapshot.unwrap().text(), "6h");
    let sink = p.sink.unwrap();
    assert!(sink.iceberg.is_none());
    assert_eq!(
        p.schema_evolution.unwrap().rescan_interval.unwrap().text(),
        "60m"
    );
}

#[test]
fn spec_example_pg_pipeline_merges_engine_block_and_inherits_catalog() {
    let p = effective(include_str!("fixtures/spec_example.yaml"), "orders-pg");
    let pg = p.source.unwrap().postgres.unwrap();
    assert_eq!(pg.slot.unwrap().text(), "orders_slot");
    assert_eq!(
        pg.heartbeat.unwrap().text(),
        "60s",
        "same-branch fields merge"
    );
    let sink = p.sink.unwrap();
    let rest = sink.iceberg.unwrap().catalog.unwrap().rest.unwrap();
    assert_eq!(rest.warehouse.unwrap().text(), "analytics");
    assert!(p.state.is_some());
    assert!(
        p.state.unwrap().prefix.is_none(),
        "defaults.state.prefix is a parent, not inherited"
    );
}

#[test]
fn lists_replace_maps_merge_by_key_and_null_overrides() {
    let yaml = r#"
defaults:
  tables:
    include: [a.x, a.y]
    snapshots: { overrides: { a.x: always, a.y: never } }
  sink: { flush: { seconds: 30 }, max_transaction_batch_rows: 5000 }
pipelines:
  p:
    tables:
      include: [b.z]
      snapshots: { overrides: { a.y: initial } }
    sink: { flush: { seconds: null }, max_transaction_batch_rows: null }
"#;
    let p = effective(yaml, "p");
    let tables = p.tables.unwrap();
    let include: Vec<_> = tables
        .include
        .unwrap()
        .iter()
        .map(|l| l.text().to_string())
        .collect();
    assert_eq!(include, ["b.z"]);
    let overrides = tables.snapshots.unwrap().overrides.unwrap();
    assert_eq!(overrides["a.x"].text(), "always");
    assert_eq!(overrides["a.y"].text(), "initial");
    let sink = p.sink.unwrap();
    assert!(
        matches!(sink.flush.unwrap().seconds, Some(None)),
        "explicit null wins over 30"
    );
    assert!(matches!(sink.max_transaction_batch_rows, Some(None)));
}

#[test]
fn catalog_type_from_defaults_is_dropped_when_the_pipeline_picks_a_block() {
    let yaml = r#"
defaults:
  sink: { type: iceberg, iceberg: { catalog: { type: glue, glue: { name: a }, retry: { max_attempts: 3 } } } }
pipelines:
  p:
    sink: { iceberg: { catalog: { rest: { url: "https://c" } } } }
  q:
    sink: { iceberg: { catalog: { retry: { max_backoff: 10s } } } }
"#;
    let c = effective(yaml, "p")
        .sink
        .unwrap()
        .iceberg
        .unwrap()
        .catalog
        .unwrap();
    assert!(c.kind.is_none() && c.glue.is_none() && c.rest.is_some());
    assert_eq!(c.retry.unwrap().max_attempts.unwrap().text(), "3");
    // A pipeline without a sub-block inherits the defaults' branch and type.
    let c = effective(yaml, "q")
        .sink
        .unwrap()
        .iceberg
        .unwrap()
        .catalog
        .unwrap();
    assert_eq!(c.kind.unwrap().text(), "glue");
    assert_eq!(c.glue.unwrap().name.unwrap().text(), "a");
    let retry = c.retry.unwrap();
    assert_eq!(retry.max_backoff.unwrap().text(), "10s");
    assert_eq!(retry.max_attempts.unwrap().text(), "3");
}

#[test]
fn template_discriminator_keeps_only_the_pipelines_blocks() {
    let yaml = r#"
defaults:
  source: { type: postgres, postgres: { heartbeat: 30s } }
pipelines:
  p:
    source: { type: "${SRC}", mysql: { replication_type: gtid } }
"#;
    let src = effective(yaml, "p").source.unwrap();
    assert!(
        src.postgres.is_none(),
        "unknown branch: nothing inherited from defaults"
    );
    assert!(src.mysql.is_some());
}

#[test]
fn pipeline_without_a_source_still_drops_the_defaults_foreign_block() {
    let yaml = r#"
defaults:
  source: { type: mysql, mysql: { replication_type: binlog }, postgres: { heartbeat: 30s } }
pipelines:
  p: { id: p }
"#;
    let src = effective(yaml, "p").source.unwrap();
    assert!(src.postgres.is_none());
    assert_eq!(
        src.mysql.unwrap().replication_type.unwrap().text(),
        "binlog"
    );
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0583]: file not found for module `merge`
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-config/src/merge.rs`:

```rust
//! Merge & override (config §8): effective pipeline = `defaults` deep-merged with the pipeline, the
//! pipeline winning. Scalars and maps merge key by key; lists are replaced; the three union
//! subtrees keep only the active branch from `defaults`.

use std::collections::BTreeMap;

use crate::ctx::Ctx;
use crate::enums::{CatalogKind, SinkType, SourceType};
use crate::leaf::Leaf;
use crate::raw::*;

/// `base` is the `defaults` value, `over` the pipeline's. The context resolves union
/// discriminators (`source.type`, …) without reporting; validation reports them later.
pub trait Merge: Sized {
    fn merge(base: Self, over: Self, ctx: &Ctx) -> Self;
}

/// A scalar: the pipeline's value if present.
impl<T> Merge for Option<Leaf<T>> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        over.or(base)
    }
}

/// A tri-state scalar (§9.4.1): an explicit `null` in the pipeline overrides a default value.
impl<T> Merge for Option<Option<Leaf<T>>> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        over.or(base)
    }
}

/// A list is replaced wholesale, never merged element-wise (§8.3).
impl<T> Merge for Option<Vec<T>> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        over.or(base)
    }
}

/// A map merges key by key; a key in the pipeline replaces the same key in `defaults` (§8.2).
impl<V> Merge for Option<BTreeMap<String, V>> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        match (base, over) {
            (Some(mut b), Some(o)) => {
                b.extend(o);
                Some(b)
            }
            (b, o) => o.or(b),
        }
    }
}

/// Field-by-field merge for a plain object.
macro_rules! merge_struct {
    ($t:ident { $($f:ident),+ $(,)? }) => {
        impl Merge for Option<$t> {
            fn merge(base: Self, over: Self, ctx: &Ctx) -> Self {
                match (base, over) {
                    (Some(b), Some(o)) => Some($t { $($f: Merge::merge(b.$f, o.$f, ctx)),+ }),
                    (b, o) => o.or(b),
                }
            }
        }
    };
}

merge_struct!(RawPipeline {
    id,
    source,
    tables,
    sink,
    state,
    schema_evolution
});
merge_struct!(RawTimeouts {
    connect,
    command,
    snapshot
});
merge_struct!(RawPostgres {
    publication,
    slot,
    auto_cleanup,
    heartbeat,
    slot_management,
    slot_on_orphan,
    max_wal_lag_bytes,
});
merge_struct!(RawMysql {
    replication_type,
    tinyint1_as_bool,
    tinyint1_detection,
    binlog_purge_action
});
merge_struct!(RawTables {
    include,
    exclude,
    snapshots,
    column_type_overrides,
    numeric_overflow_action,
    primary_key,
    partitioning,
});
merge_struct!(RawSnapshots {
    mode,
    parallelism,
    chunk_size,
    overrides
});
merge_struct!(RawFlush {
    seconds,
    rows,
    bytes
});
merge_struct!(RawSinkMetrics { per_table });
merge_struct!(RawIceberg { strategy, catalog });
merge_struct!(RawGlue {
    name,
    database,
    strip_schema
});
merge_struct!(RawHive {
    url,
    auth,
    user,
    password,
    credentials_file,
    kerberos,
    tls,
    database
});
merge_struct!(RawKerberos {
    principal,
    keytab,
    service_name
});
merge_struct!(RawHiveTls { enabled, ca });
merge_struct!(RawRest {
    url,
    warehouse,
    prefix,
    auth,
    token,
    token_file,
    oauth2,
    sigv4,
    tls
});
merge_struct!(RawRestOauth2 {
    client_id,
    client_secret,
    credentials_file,
    token_url,
    scope
});
merge_struct!(RawSigv4 { region, service });
merge_struct!(RawRestTls { ca });
merge_struct!(RawRetry {
    max_attempts,
    base_backoff,
    max_backoff
});
merge_struct!(RawDelta {
    column_mapping,
    checkpoint_interval,
    log_retention,
    timestamp_ntz_as,
    table_properties,
    catalog
});
merge_struct!(RawDeltaCatalog { kind });
merge_struct!(RawSchemaEvolution {
    enabled,
    rescan_interval
});

/// Both sides, defaulted when one is absent; `None` when both are.
fn both<T: Default>(base: Option<T>, over: Option<T>) -> Option<(T, T)> {
    match (base, over) {
        (None, None) => None,
        (b, o) => Some((b.unwrap_or_default(), o.unwrap_or_default())),
    }
}

/// Union §8.4: the `source` engine block. The active branch is `source.type` after merge (default
/// `postgres`); `defaults`' other engine block is dropped. With a template type the branch is
/// unknown in the structural pass: only the pipeline's own blocks are kept.
impl Merge for Option<RawSource> {
    fn merge(base: Self, over: Self, ctx: &Ctx) -> Self {
        let (b, o) = both(base, over)?;
        let kind = Merge::merge(b.kind, o.kind, ctx);
        let active = match &kind {
            None => Some(SourceType::Postgres),
            Some(_) => ctx.peek(&kind),
        };
        let (postgres, mysql) = match active {
            Some(SourceType::Postgres) => (Merge::merge(b.postgres, o.postgres, ctx), o.mysql),
            Some(SourceType::Mysql) => (o.postgres, Merge::merge(b.mysql, o.mysql, ctx)),
            None => (o.postgres, o.mysql),
        };
        Some(RawSource {
            kind,
            host: Merge::merge(b.host, o.host, ctx),
            port: Merge::merge(b.port, o.port, ctx),
            db: Merge::merge(b.db, o.db, ctx),
            user: Merge::merge(b.user, o.user, ctx),
            password: Merge::merge(b.password, o.password, ctx),
            credentials_file: Merge::merge(b.credentials_file, o.credentials_file, ctx),
            ssl_mode: Merge::merge(b.ssl_mode, o.ssl_mode, ctx),
            ssl_cert: Merge::merge(b.ssl_cert, o.ssl_cert, ctx),
            ssl_key: Merge::merge(b.ssl_key, o.ssl_key, ctx),
            ssl_ca: Merge::merge(b.ssl_ca, o.ssl_ca, ctx),
            timeouts: Merge::merge(b.timeouts, o.timeouts, ctx),
            toast_strategy: Merge::merge(b.toast_strategy, o.toast_strategy, ctx),
            postgres,
            mysql,
        })
    }
}

/// Union §8.4: the `sink` format block, selected by `sink.type` after merge (no default, V-091).
impl Merge for Option<RawSink> {
    fn merge(base: Self, over: Self, ctx: &Ctx) -> Self {
        let (b, o) = both(base, over)?;
        let kind = Merge::merge(b.kind, o.kind, ctx);
        let (iceberg, delta) = match ctx.peek(&kind) {
            Some(SinkType::Iceberg) => (Merge::merge(b.iceberg, o.iceberg, ctx), o.delta),
            Some(SinkType::Delta) => (o.iceberg, Merge::merge(b.delta, o.delta, ctx)),
            None => (o.iceberg, o.delta),
        };
        Some(RawSink {
            kind,
            write_mode: Merge::merge(b.write_mode, o.write_mode, ctx),
            bucket: Merge::merge(b.bucket, o.bucket, ctx),
            prefix: Merge::merge(b.prefix, o.prefix, ctx),
            flush: Merge::merge(b.flush, o.flush, ctx),
            max_transaction_batch_rows: Merge::merge(
                b.max_transaction_batch_rows,
                o.max_transaction_batch_rows,
                ctx,
            ),
            max_transaction_buffer_bytes: Merge::merge(
                b.max_transaction_buffer_bytes,
                o.max_transaction_buffer_bytes,
                ctx,
            ),
            buffer_capacity_rows: Merge::merge(b.buffer_capacity_rows, o.buffer_capacity_rows, ctx),
            metrics: Merge::merge(b.metrics, o.metrics, ctx),
            iceberg,
            delta,
        })
    }
}

fn single_block(c: &RawCatalog) -> Option<CatalogKind> {
    match (c.glue.is_some(), c.hive.is_some(), c.rest.is_some()) {
        (true, false, false) => Some(CatalogKind::Glue),
        (false, true, false) => Some(CatalogKind::Hive),
        (false, false, true) => Some(CatalogKind::Rest),
        _ => None,
    }
}

fn block_count(c: &RawCatalog) -> usize {
    [c.glue.is_some(), c.hive.is_some(), c.rest.is_some()]
        .into_iter()
        .filter(|b| *b)
        .count()
}

/// Union §8.4: `sink.iceberg.catalog`. The pipeline picks the branch by its `type`, else by its
/// single sub-block; a pipeline with no sub-block inherits `defaults`' branch. `defaults`' `type`
/// is dropped when the pipeline picks by block, so a defaulted `type: glue` never contradicts a
/// pipeline's `rest:` (§8.5).
impl Merge for Option<RawCatalog> {
    fn merge(base: Self, over: Self, ctx: &Ctx) -> Self {
        let (b, o) = both(base, over)?;
        let (kind, active) = if o.kind.is_some() {
            let active = ctx.peek(&o.kind);
            (o.kind.clone(), active)
        } else {
            match block_count(&o) {
                0 => (
                    b.kind.clone(),
                    ctx.peek(&b.kind).or_else(|| single_block(&b)),
                ),
                1 => (None, single_block(&o)),
                _ => (None, None),
            }
        };
        let pick = |k: CatalogKind| active == Some(k);
        Some(RawCatalog {
            kind,
            glue: if pick(CatalogKind::Glue) {
                Merge::merge(b.glue, o.glue, ctx)
            } else {
                o.glue
            },
            hive: if pick(CatalogKind::Hive) {
                Merge::merge(b.hive, o.hive, ctx)
            } else {
                o.hive
            },
            rest: if pick(CatalogKind::Rest) {
                Merge::merge(b.rest, o.rest, ctx)
            } else {
                o.rest
            },
            retry: Merge::merge(b.retry, o.retry, ctx),
        })
    }
}

/// `state.bucket` merges normally; `state.prefix` does not: `defaults.state.prefix` is a parent
/// prefix of every explicit state location (§9.6, §10), which the resolved build applies. Were it
/// inherited, every pipeline would share one state location.
impl Merge for Option<RawPipelineState> {
    fn merge(base: Self, over: Self, _: &Ctx) -> Self {
        let (b, o) = both(base, over)?;
        Some(RawPipelineState {
            bucket: o.bucket.or(b.bucket),
            prefix: o.prefix,
        })
    }
}

/// The effective pipeline (§8.1): `defaults` merged with `pipeline`.
pub fn merge_pipeline(
    defaults: Option<&RawPipeline>,
    pipeline: &RawPipeline,
    ctx: &Ctx,
) -> RawPipeline {
    Merge::merge(defaults.cloned(), Some(pipeline.clone()), ctx).unwrap_or_default()
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 23; `tests/merge.rs` 8; `tests/parse.rs` 8; `tests/raw.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/tests/merge.rs \
  crates/data-avalanche-config/src/merge.rs
git commit -m "feat(config): §8 merge with union subtrees"
```


---

### Task 8: Secrets, file material, resolved types

**Files:**
- Create: `crates/data-avalanche-config/src/material.rs`, `crates/data-avalanche-config/src/secret.rs`, `crates/data-avalanche-config/src/resolved.rs`
- Modify: `Cargo.toml`, `crates/data-avalanche-config/Cargo.toml`, `crates/data-avalanche-config/src/lib.rs`

**Interfaces:**
- Consumes: Tasks 2–7.
- Produces:
  - `Secret::new`, `Secret::expose() -> &str`; `Debug` prints `***`; `Serialize` = `fingerprint`.
  - `secret::fingerprint(&[u8]) -> String` (`xxh3:<32 hex>`).
  - `material::read_json_pair(path, "user", "password") -> Result<(String, Secret), String>` (V-067).
  - `read_token_file(path) -> Result<Secret, String>` (V-071; trims trailing whitespace; empty is
    an error).
  - `read_file_ref(path) -> Result<FileRef { path, fingerprint }, String>` (V-069).
  - `load_pem(value, PemKind::{Certificates, PrivateKey}) -> Result<Pem { origin: Origin::{Inline, File(PathBuf)}, text: Secret }, String>`
    (V-068). A value that starts with `-----BEGIN` is inline, otherwise a path.
  - `resolved::*`: the §11.1 runtime shapes. The two main ones:
    - `ResolvedConfig { config_version, globals, pipelines: BTreeMap<id, Pipeline> }`;
    - `Pipeline { name, id, source, tables, sink, state: StateLocation::{CoLocated, Explicit{bucket, parent_prefix, prefix}}, schema_evolution }`.
    
    `Credentials::{Inline, File}` has `user()` and `password()`. Everything derives `Serialize`;
    durations serialize as humantime text.

The PEM unit tests generate their certificate and key with `rcgen` at run time (D-M1.14). This task
adds `rcgen` as a dev-dependency.

- [ ] **Step 1: Write the failing tests**

Change `Cargo.toml`:

```diff
diff --git a/Cargo.toml b/Cargo.toml
index 8352c02..13e0288 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -37,3 +37,4 @@ tracing-subscriber = { version = "0.3", default-features = false, features = ["s
 rustls-pki-types = { version = "1", features = ["std"] }
 xxhash-rust = { version = "0.8", features = ["xxh3"] }
 tempfile = "3"
+rcgen = { version = "0.14", default-features = false, features = ["aws_lc_rs", "pem"] }
```

Change `crates/data-avalanche-config/Cargo.toml`:

```diff
diff --git a/crates/data-avalanche-config/Cargo.toml b/crates/data-avalanche-config/Cargo.toml
index e365cba..230a125 100644
--- a/crates/data-avalanche-config/Cargo.toml
+++ b/crates/data-avalanche-config/Cargo.toml
@@ -17,5 +17,9 @@ serde-saphyr.workspace = true
 thiserror.workspace = true
 humantime.workspace = true
 tracing-subscriber.workspace = true
+rustls-pki-types.workspace = true
+xxhash-rust.workspace = true
 
 [dev-dependencies]
+tempfile.workspace = true
+rcgen.workspace = true
```

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 2831046..182f345 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -7,11 +7,14 @@ pub mod ctx;
 pub mod diag;
 pub mod enums;
 pub mod leaf;
+pub mod material;
 pub mod merge;
 pub mod parse;
 pub mod path;
 pub mod raw;
 pub mod registry;
+pub mod resolved;
+pub mod secret;
 pub mod template;
 pub mod units;
 
@@ -19,10 +22,13 @@ pub use coerce::{LeafType, LogFilter};
 pub use ctx::{At, Ctx};
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use leaf::{Leaf, Loc, Tri};
+pub use material::{FileRef, Origin, Pem};
 pub use merge::merge_pipeline;
 pub use parse::parse_document;
 pub use path::YamlPath;
 pub use raw::RawConfig;
 pub use registry::{RULES, RuleInput, RuleMeta, RuleSeverity, rule_meta};
+pub use resolved::{Pipeline, ResolvedConfig};
+pub use secret::Secret;
 pub use template::{Env, ProcessEnv};
 pub use units::{ByteSize, HostPort};
```

Create `crates/data-avalanche-config/src/material.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    /// A throwaway self-signed certificate and its key, generated per run: no key material is
    /// committed.
    fn pem_pair() -> (String, String) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["data-avalanche-test".to_string()]).unwrap();
        (cert.pem(), signing_key.serialize_pem())
    }

    fn file(content: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content).unwrap();
        f
    }

    #[test]
    fn credentials_file_needs_both_fields_non_empty() {
        let ok = file(br#"{"user": "cdc", "password": "pw", "note": "ignored"}"#);
        let (user, password) = read_json_pair(ok.path(), "user", "password").unwrap();
        assert_eq!((user.as_str(), password.expose()), ("cdc", "pw"));
        for (content, why) in [
            (
                &br#"{"user": "cdc"}"#[..],
                "no non-empty string field \"password\"",
            ),
            (
                br#"{"user": "cdc", "password": ""}"#,
                "no non-empty string field \"password\"",
            ),
            (br#"{"user": 1, "password": "p"}"#, "field \"user\""),
            (b"not json", "not valid JSON"),
            (b"", "is empty"),
        ] {
            let f = file(content);
            let err = read_json_pair(f.path(), "user", "password").unwrap_err();
            assert!(err.contains(why), "{err}");
        }
        let err =
            read_json_pair(Path::new("/nonexistent/creds.json"), "user", "password").unwrap_err();
        assert!(
            err.starts_with("cannot read /nonexistent/creds.json"),
            "{err}"
        );
    }

    #[test]
    fn token_file_is_trimmed_raw_text() {
        let f = file(b"tok-123\n\n");
        assert_eq!(read_token_file(f.path()).unwrap().expose(), "tok-123");
        assert!(
            read_token_file(file(b" \n").path())
                .unwrap_err()
                .contains("only whitespace")
        );
    }

    #[test]
    fn pem_is_inline_or_a_path_and_must_hold_the_right_kind() {
        let (cert_pem, key_pem) = pem_pair();
        let inline = load_pem(&cert_pem, PemKind::Certificates).unwrap();
        assert_eq!(inline.origin, Origin::Inline);
        let key_file = file(key_pem.as_bytes());
        let key = load_pem(key_file.path().to_str().unwrap(), PemKind::PrivateKey).unwrap();
        assert_eq!(key.origin, Origin::File(key_file.path().to_path_buf()));
        assert!(
            load_pem(&key_pem, PemKind::Certificates)
                .unwrap_err()
                .contains("no certificate")
        );
        assert!(
            load_pem(&cert_pem, PemKind::PrivateKey)
                .unwrap_err()
                .contains("no private key")
        );
        assert!(
            load_pem(
                "-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n",
                PemKind::Certificates
            )
            .is_err()
        );
        assert!(
            load_pem("/nonexistent/ca.pem", PemKind::Certificates)
                .unwrap_err()
                .contains("cannot read")
        );
    }

    #[test]
    fn file_ref_fingerprints_content() {
        let a = file(b"keytab-1");
        let b = file(b"keytab-2");
        assert_ne!(
            read_file_ref(a.path()).unwrap().fingerprint,
            read_file_ref(b.path()).unwrap().fingerprint
        );
        assert!(
            read_file_ref(file(b"").path())
                .unwrap_err()
                .contains("is empty")
        );
    }
}
```

Create `crates/data-avalanche-config/src/secret.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_shows_the_value() {
        let s = Secret::new("hunter2");
        assert_eq!(format!("{s:?}"), "Secret(***)");
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("hunter2"));
        assert!(json.starts_with("\"xxh3:"));
        assert_eq!(
            json,
            serde_json::to_string(&Secret::new("hunter2")).unwrap()
        );
        assert_ne!(
            json,
            serde_json::to_string(&Secret::new("hunter3")).unwrap()
        );
        assert_eq!(s.expose(), "hunter2");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0583]: file not found for module `resolved`
error[E0432]: unresolved imports `material::FileRef`, `material::Origin`, `material::Pem`
error[E0432]: unresolved import `secret::Secret`
```

- [ ] **Step 3: Implement**

In `crates/data-avalanche-config/src/material.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Phase 2 file material (config §4.4, §4.6, §13.9): credential files, token files, PEM, keytabs.
//! Each function returns the rule's message on failure; the build reports it under the rule.

use std::fs;
use std::path::{Path, PathBuf};

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::Serialize;

use crate::secret::{Secret, fingerprint};

fn read(path: &Path) -> Result<Vec<u8>, String> {
    let bytes = fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    if bytes.is_empty() {
        return Err(format!("{} is empty", path.display()));
    }
    Ok(bytes)
}

/// A JSON object file holding two non-empty string fields (V-067): `{user, password}` or
/// `{client_id, client_secret}`. Other fields are ignored.
pub fn read_json_pair(path: &Path, first: &str, second: &str) -> Result<(String, Secret), String> {
    let bytes = read(path)?;
    let doc: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| format!("{} is not valid JSON: {e}", path.display()))?;
    let field = |name: &str| {
        doc.get(name)
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .ok_or_else(|| format!("{} has no non-empty string field {name:?}", path.display()))
    };
    Ok((field(first)?, Secret::new(field(second)?)))
}

/// A bearer-token file (V-071): the raw contents, trailing whitespace trimmed, non-empty.
pub fn read_token_file(path: &Path) -> Result<Secret, String> {
    let bytes = read(path)?;
    let text =
        String::from_utf8(bytes).map_err(|_| format!("{} is not UTF-8 text", path.display()))?;
    let token = text.trim_end();
    if token.is_empty() {
        return Err(format!("{} holds only whitespace", path.display()));
    }
    Ok(Secret::new(token))
}

/// Any other referenced file (V-069, e.g. a Kerberos keytab): exists, readable, non-empty. Only
/// the path and a content fingerprint are kept: the runtime re-reads the file when it reconnects,
/// and the fingerprint lets a reload notice that the contents changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileRef {
    pub path: PathBuf,
    pub fingerprint: String,
}

pub fn read_file_ref(path: &Path) -> Result<FileRef, String> {
    Ok(FileRef {
        path: path.to_path_buf(),
        fingerprint: fingerprint(&read(path)?),
    })
}

/// Where a PEM value came from (§4.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "path", rename_all = "lowercase")]
pub enum Origin {
    Inline,
    File(PathBuf),
}

/// Validated PEM text (V-068). The runtime builds its rustls objects from `text`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Pem {
    pub origin: Origin,
    pub text: Secret,
}

/// What a PEM value must contain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PemKind {
    /// `cert`, `ca`: at least one certificate.
    Certificates,
    /// `key`: a private key (PKCS#8, PKCS#1 or SEC1).
    PrivateKey,
}

/// A `cert`/`key`/`ca` value (§4.4): inline PEM when it starts with `-----BEGIN`, otherwise a path.
pub fn load_pem(value: &str, kind: PemKind) -> Result<Pem, String> {
    let (origin, text) = if value.starts_with("-----BEGIN") {
        (Origin::Inline, value.to_string())
    } else {
        let path = PathBuf::from(value);
        let bytes = read(&path)?;
        let text =
            String::from_utf8(bytes).map_err(|_| format!("{} is not PEM text", path.display()))?;
        (Origin::File(path), text)
    };
    let from = match &origin {
        Origin::Inline => "inline PEM".to_string(),
        Origin::File(p) => p.display().to_string(),
    };
    match kind {
        PemKind::Certificates => {
            let certs: Result<Vec<_>, _> =
                CertificateDer::pem_slice_iter(text.as_bytes()).collect();
            match certs {
                Ok(c) if !c.is_empty() => {}
                Ok(_) => return Err(format!("{from} holds no certificate")),
                Err(e) => return Err(format!("{from} is not valid PEM: {e}")),
            }
        }
        PemKind::PrivateKey => {
            PrivateKeyDer::from_pem_slice(text.as_bytes())
                .map_err(|e| format!("{from} holds no private key: {e}"))?;
        }
    }
    Ok(Pem {
        origin,
        text: Secret::new(text),
    })
}

```

Create `crates/data-avalanche-config/src/resolved.rs`:

```rust
//! The resolved config (config §11.1): every reference substituted, every file read, every default
//! applied. The only representation the runtime uses. Field names follow the spec's paths so the
//! reload diff (§12) can classify a change by its path. `Serialize` exists for that diff: secrets
//! serialize as fingerprints (see [`Secret`]).

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use data_avalanche_core::LogicalType;
use serde::{Serialize, Serializer};

use crate::coerce::LogFilter;
use crate::enums::*;
use crate::material::{FileRef, Pem};
use crate::secret::Secret;
use crate::units::{HostPort, ser_duration};

fn ser_logical_map<S: Serializer>(
    map: &BTreeMap<String, LogicalType>,
    s: S,
) -> Result<S::Ok, S::Error> {
    s.collect_map(map.iter().map(|(k, v)| (k, v.to_string())))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResolvedConfig {
    pub config_version: u32,
    pub globals: Globals,
    /// Keyed by pipeline `id` (the immutable identity, §9.1), not by the human name.
    pub pipelines: BTreeMap<String, Pipeline>,
}

// ---- globals ----

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Globals {
    pub cloud: Cloud,
    pub state: GlobalState,
    pub api: Api,
    pub log: Log,
    pub cluster: Cluster,
    pub tracing: Tracing,
    pub azure: Azure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GlobalState {
    pub bucket: String,
    /// `""` = bucket root.
    pub prefix: String,
}

/// A `user`/`password` pair given inline or through a `credentials_file` (§4.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum Credentials {
    Inline {
        user: String,
        password: Secret,
    },
    File {
        path: PathBuf,
        user: String,
        password: Secret,
    },
}

impl Credentials {
    pub fn user(&self) -> &str {
        match self {
            Self::Inline { user, .. } | Self::File { user, .. } => user,
        }
    }

    pub fn password(&self) -> &Secret {
        match self {
            Self::Inline { password, .. } | Self::File { password, .. } => password,
        }
    }
}

/// A TLS server identity (`cert` + `key`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TlsIdentity {
    pub cert: Pem,
    pub key: Pem,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Api {
    pub address: HostPort,
    pub auth: ApiAuth,
    /// Set when `auth = basic`.
    pub credentials: Option<Credentials>,
    pub jwt: Option<Jwt>,
    pub oauth2: Option<ApiOauth2>,
    /// Empty = identity mapping (§6.3).
    pub role_mapping: BTreeMap<Role, Vec<String>>,
    /// `None` = TLS disabled.
    pub tls: Option<TlsIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Jwt {
    pub issuer: String,
    pub audience: Option<String>,
    pub jwks_url: String,
    pub roles_claim: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApiOauth2 {
    pub issuer: String,
    pub audience: Option<String>,
    pub jwks_url: Option<String>,
    pub discovery_url: Option<String>,
    pub roles_claim: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Log {
    pub level: LogFilter,
    pub format: LogFormat,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Cluster {
    pub tokens: u32,
    pub lock_backend: LockBackend,
    #[serde(serialize_with = "ser_duration")]
    pub lease_duration: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub lease_renewal_interval: Duration,
    pub lease_safety_margin: f64,
    #[serde(serialize_with = "ser_duration")]
    pub rebalance_stabilization: Duration,
    pub max_concurrent_handoffs: u32,
    pub max_concurrent_takeovers: u32,
    #[serde(serialize_with = "ser_duration")]
    pub drain_timeout: Duration,
    pub discovery: Discovery,
    pub discovery_dns_name: Option<String>,
    pub memberlist: Memberlist,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Memberlist {
    pub join_members: Vec<HostPort>,
    /// `None` = the OS hostname, looked up by the runtime (§6.5.1).
    pub node_name: Option<String>,
    /// `""` = bind address or first private IP.
    pub advertise_addr: String,
    pub advertise_port: u16,
    pub bind_addr: Vec<IpAddr>,
    pub bind_port: u16,
    #[serde(serialize_with = "ser_duration")]
    pub packet_dial_timeout: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub packet_write_timeout: Duration,
    pub compression_enabled: bool,
    #[serde(serialize_with = "ser_duration")]
    pub gossip_interval: Duration,
    pub gossip_nodes: u32,
    #[serde(serialize_with = "ser_duration")]
    pub probe_interval: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub probe_timeout: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub dead_node_reclaim_time: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub push_pull_interval: Duration,
    pub retransmit_factor: u32,
    pub lifeguard_enabled: bool,
    /// `None` = TLS disabled.
    pub tls: Option<MemberlistTls>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MemberlistTls {
    pub cert: Pem,
    pub key: Pem,
    /// Presence implies mutual TLS (§6.5.2).
    pub ca: Option<Pem>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Tracing {
    /// `None` = tracing disabled.
    pub otlp_endpoint: Option<String>,
    pub sample_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Azure {
    /// `None` = from the credential chain.
    pub account_name: Option<String>,
}

// ---- pipelines ----

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Pipeline {
    /// The human name: the key under `pipelines` (`_pipeline` lineage value, metric label).
    pub name: String,
    pub id: String,
    pub source: Source,
    pub tables: Tables,
    pub sink: Sink,
    pub state: StateLocation,
    pub schema_evolution: SchemaEvolution,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Source {
    #[serde(rename = "type")]
    pub kind: SourceType,
    pub host: String,
    pub port: u16,
    pub db: String,
    pub credentials: Credentials,
    pub ssl_mode: SslMode,
    pub ssl_cert: Option<Pem>,
    pub ssl_key: Option<Pem>,
    pub ssl_ca: Option<Pem>,
    pub timeouts: Timeouts,
    pub toast_strategy: ToastStrategy,
    /// Set iff `type = postgres`.
    pub postgres: Option<Postgres>,
    /// Set iff `type = mysql`.
    pub mysql: Option<Mysql>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Timeouts {
    #[serde(serialize_with = "ser_duration")]
    pub connect: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub command: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub snapshot: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Postgres {
    pub publication: String,
    pub slot: String,
    pub auto_cleanup: bool,
    #[serde(serialize_with = "ser_duration")]
    pub heartbeat: Duration,
    pub slot_management: SlotManagement,
    pub slot_on_orphan: SlotOnOrphan,
    /// `None` = alert only (§9.2.1).
    pub max_wal_lag_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Mysql {
    pub replication_type: ReplicationType,
    pub tinyint1_as_bool: bool,
    pub tinyint1_detection: Tinyint1Detection,
    pub binlog_purge_action: BinlogPurgeAction,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Tables {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub snapshots: Snapshots,
    #[serde(serialize_with = "ser_logical_map")]
    pub column_type_overrides: BTreeMap<String, LogicalType>,
    pub numeric_overflow_action: NumericOverflowAction,
    pub primary_key: BTreeMap<String, Vec<String>>,
    pub partitioning: BTreeMap<String, Vec<PartitionField>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Snapshots {
    pub mode: SnapshotMode,
    pub parallelism: u32,
    pub chunk_size: u64,
    pub overrides: BTreeMap<String, SnapshotMode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionField {
    pub column: String,
    pub transform: PartitionTransform,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionTransform {
    Identity,
    Bucket { n: u32 },
    Truncate { width: u32 },
    Year,
    Month,
    Day,
    Hour,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Sink {
    #[serde(rename = "type")]
    pub kind: SinkType,
    pub write_mode: WriteMode,
    pub bucket: String,
    /// Defaults to the pipeline `id`.
    pub prefix: String,
    pub flush: Flush,
    /// `None` = never split (§9.4).
    pub max_transaction_batch_rows: Option<u64>,
    pub max_transaction_buffer_bytes: u64,
    pub buffer_capacity_rows: u64,
    pub metrics: SinkMetrics,
    /// Set iff `type = iceberg`.
    pub iceberg: Option<Iceberg>,
    /// Set iff `type = delta`.
    pub delta: Option<Delta>,
}

/// Flush triggers (§9.4.1); `None` = that trigger is disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Flush {
    pub seconds: Option<u64>,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SinkMetrics {
    pub per_table: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Iceberg {
    pub strategy: IcebergStrategy,
    pub catalog: Catalog,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Catalog {
    #[serde(rename = "type")]
    pub kind: CatalogKind,
    /// Exactly the block matching `type` is set.
    pub glue: Option<Glue>,
    pub hive: Option<Hive>,
    pub rest: Option<Rest>,
    pub retry: Retry,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Glue {
    pub name: String,
    /// Defaults to `source.db`.
    pub database: String,
    pub strip_schema: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Hive {
    pub url: String,
    pub auth: HiveAuth,
    /// Set when `auth = ldap`.
    pub credentials: Option<Credentials>,
    /// Set when `auth = kerberos`.
    pub kerberos: Option<Kerberos>,
    pub tls: HiveTls,
    /// Defaults to `source.db`.
    pub database: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Kerberos {
    pub principal: String,
    pub keytab: FileRef,
    pub service_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HiveTls {
    pub enabled: bool,
    pub ca: Option<Pem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Rest {
    pub url: String,
    pub warehouse: Option<String>,
    pub prefix: Option<String>,
    pub auth: RestAuth,
    /// Set when `auth = bearer`.
    pub token: Option<BearerToken>,
    /// Set when `auth = oauth2`.
    pub oauth2: Option<RestOauth2>,
    /// Set when `auth = sigv4`.
    pub sigv4: Option<Sigv4>,
    pub tls_ca: Option<Pem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum BearerToken {
    Inline { token: Secret },
    File { path: PathBuf, token: Secret },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RestOauth2 {
    pub client: OauthClient,
    pub token_url: String,
    pub scope: Option<String>,
}

/// `client_id`/`client_secret` inline or from `oauth2.credentials_file` (§4.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum OauthClient {
    Inline {
        client_id: String,
        client_secret: Secret,
    },
    File {
        path: PathBuf,
        client_id: String,
        client_secret: Secret,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Sigv4 {
    pub region: String,
    pub service: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Retry {
    pub max_attempts: u32,
    #[serde(serialize_with = "ser_duration")]
    pub base_backoff: Duration,
    #[serde(serialize_with = "ser_duration")]
    pub max_backoff: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Delta {
    pub column_mapping: ColumnMapping,
    pub checkpoint_interval: u64,
    #[serde(serialize_with = "ser_duration")]
    pub log_retention: Duration,
    pub timestamp_ntz_as: TimestampNtzAs,
    pub table_properties: BTreeMap<String, String>,
    pub catalog_type: DeltaCatalogType,
}

/// Where the pipeline's operational state lives (§9.6, §10).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "location", rename_all = "snake_case")]
pub enum StateLocation {
    /// `<sink.bucket>/<sink.prefix>/.pipeline-state/`: no `state.bucket` on the pipeline or in
    /// `defaults`. `defaults.state.prefix` has no effect here.
    CoLocated,
    /// `<bucket>/[<parent_prefix>/]<prefix>/`: `parent_prefix` is `defaults.state.prefix`,
    /// `prefix` is the pipeline's `state.prefix` or its `id`.
    Explicit {
        bucket: String,
        parent_prefix: Option<String>,
        prefix: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SchemaEvolution {
    pub enabled: bool,
    /// `0` = idle rescan disabled.
    #[serde(serialize_with = "ser_duration")]
    pub rescan_interval: Duration,
}
```

In `crates/data-avalanche-config/src/secret.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! In-memory secret material (config §11.1): never printed, never in the diff output or the
//! validation report.

use std::fmt;

use serde::{Serialize, Serializer};
use xxhash_rust::xxh3::xxh3_128;

/// A resolved secret (password, token, private key, certificate text). `Debug` prints `***`.
/// `Serialize` writes only a fingerprint, so the reload diff (§11.1) sees "changed / unchanged"
/// without the value. The fingerprint is a fast non-cryptographic hash for change detection; it
/// never leaves the process.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&fingerprint(self.0.as_bytes()))
    }
}

/// `xxh3:<32 hex digits>` of `bytes`.
pub fn fingerprint(bytes: &[u8]) -> String {
    format!("xxh3:{:032x}", xxh3_128(bytes))
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 28; `tests/merge.rs` 8; `tests/parse.rs` 8; `tests/raw.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml \
  crates/data-avalanche-config/Cargo.toml \
  crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/src/material.rs \
  crates/data-avalanche-config/src/secret.rs \
  crates/data-avalanche-config/src/resolved.rs \
  Cargo.lock
git commit -m "feat(config): secrets, credential/PEM material and resolved config types"
```


---

### Task 9: Build and load — defaults, resolution, the phase pipeline

**Files:**
- Create: `crates/data-avalanche-config/src/build.rs`, `crates/data-avalanche-config/src/load.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`
- Test: `crates/data-avalanche-config/tests/load.rs`, `crates/data-avalanche-config/tests/support/mod.rs`

**Interfaces:**
- Consumes: Tasks 2–8.
- Produces:
  - `build::merge_all(&RawConfig, &Ctx) -> Vec<(String, RawPipeline)>`.
  - `build::build(&RawConfig, &merged, &mut Ctx) -> Option<ResolvedConfig>`. It returns `None` in
    the structural pass, or when any value is missing or invalid.
  - `build::DEFAULT_*` constants.
  - `load::Loaded { config, warnings }`.
  - `load_str(text, &dyn Env) -> Result<Loaded, Report>` and `load_file(path, &dyn Env)`.
  - `validate_str(text, &dyn Env) -> Report`. It never returns the resolved config (D-M1.13).

`build` applies every default in the spec's field tables. Where a field is conditional (`api` auth
blocks, Hive/REST auth blocks), only the block the chosen mode uses is kept. An absent `mysql` or
`delta` block builds with every default (Q-M1-4). `load_str` ends with a
safety net: if `build` fails but no rule reported why, it adds an `internal:` PARSE error. The rule
harness (Task 10) fails on any `internal:` diagnostic. The net should never fire; if it does, some
rule is missing.

`tests/support/mod.rs` generates one TLS pair per test binary (`cert()`, `key()`), so no PEM file
is committed (D-M1.14).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 182f345..5037b22 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -2,11 +2,13 @@
 //! strict parse, `${VAR}` and file resolution, merge of `defaults`, the V-rule validators, the
 //! resolved config the runtime uses, and the reload diff.
 
+pub mod build;
 pub mod coerce;
 pub mod ctx;
 pub mod diag;
 pub mod enums;
 pub mod leaf;
+pub mod load;
 pub mod material;
 pub mod merge;
 pub mod parse;
@@ -22,6 +24,7 @@ pub use coerce::{LeafType, LogFilter};
 pub use ctx::{At, Ctx};
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use leaf::{Leaf, Loc, Tri};
+pub use load::{Loaded, load_file, load_str, validate_str};
 pub use material::{FileRef, Origin, Pem};
 pub use merge::merge_pipeline;
 pub use parse::parse_document;
```

Create `crates/data-avalanche-config/tests/load.rs`:

```rust
//! The load pipeline end to end: resolution, defaults, file material, and the phase boundary.

mod support;

use std::time::Duration;

use data_avalanche_config::enums::*;
use data_avalanche_config::resolved::*;
use data_avalanche_config::{Origin, load_file, load_str};
use support::{Files, MINIMAL, env, spec_example};

#[test]
fn spec_example_resolves() {
    let files = Files::new();
    let (yaml, env) = spec_example(&files);
    let loaded = load_str(&yaml, &env).unwrap_or_else(|r| panic!("{r:#?}"));
    let cfg = loaded.config;
    assert_eq!(cfg.config_version, 1);
    assert_eq!(cfg.globals.state.bucket, "123456789012-cdc-app-state");
    let api = &cfg.globals.api;
    assert_eq!(api.auth, ApiAuth::Jwt);
    assert_eq!(api.jwt.as_ref().unwrap().roles_claim, "realm_access.roles");
    let tls = api.tls.as_ref().unwrap();
    assert!(matches!(tls.cert.origin, Origin::File(_)));
    assert_eq!(tls.key.origin, Origin::Inline);
    let cluster = &cfg.globals.cluster;
    assert_eq!(cluster.discovery, Discovery::Dns);
    assert!(cluster.memberlist.tls.as_ref().unwrap().ca.is_some());
    assert_eq!(cfg.globals.tracing.sample_ratio, 0.05);
    assert_eq!(
        cfg.pipelines.keys().collect::<Vec<_>>(),
        ["events_mysql", "orders_pg"]
    );

    let orders = &cfg.pipelines["orders_pg"];
    assert_eq!(orders.name, "orders-pg");
    assert_eq!(orders.source.port, 5432);
    assert_eq!(orders.source.credentials.password().expose(), "orders-pw");
    assert_eq!(
        orders.source.postgres.as_ref().unwrap().heartbeat,
        Duration::from_secs(60)
    );
    assert_eq!(orders.sink.kind, SinkType::Iceberg);
    assert_eq!(orders.sink.bucket, "123456789012-lake");
    assert_eq!(
        orders.sink.flush,
        Flush {
            seconds: Some(30),
            rows: Some(50_000),
            bytes: Some(512 << 20)
        }
    );
    let rest = orders
        .sink
        .iceberg
        .as_ref()
        .unwrap()
        .catalog
        .rest
        .as_ref()
        .unwrap();
    assert_eq!(rest.auth, RestAuth::Oauth2);
    assert!(matches!(
        rest.oauth2.as_ref().unwrap().client,
        OauthClient::Inline { .. }
    ));
    assert_eq!(
        orders.tables.snapshots.overrides["public.orders"],
        SnapshotMode::Always
    );
    assert_eq!(
        orders.state,
        StateLocation::CoLocated,
        "defaults.state.prefix alone has no effect"
    );

    let events = &cfg.pipelines["events_mysql"];
    assert_eq!(events.source.kind, SourceType::Mysql);
    assert_eq!(events.source.port, 3306);
    assert!(events.source.postgres.is_none());
    assert!(
        matches!(&events.source.credentials, Credentials::File { user, .. } if user == "events")
    );
    assert_eq!(
        events.source.mysql.as_ref().unwrap().replication_type,
        ReplicationType::Gtid
    );
    assert_eq!(events.sink.kind, SinkType::Delta);
    assert_eq!(
        events.sink.prefix, "events_mysql",
        "sink.prefix defaults to the id"
    );
    assert_eq!(
        events.schema_evolution.rescan_interval,
        Duration::from_secs(3600)
    );
}

#[test]
fn applies_spec_defaults() {
    let cfg = load_str(MINIMAL, &env(&[("PW", "pw")])).unwrap().config;
    let g = &cfg.globals;
    assert_eq!(g.cloud, Cloud::Aws);
    assert_eq!(g.api.address.to_string(), "0.0.0.0:8080");
    assert_eq!(g.api.auth, ApiAuth::None);
    assert_eq!(g.log.level.0, "info");
    assert_eq!(g.log.format, LogFormat::Json);
    assert_eq!(g.cluster.tokens, 128);
    assert_eq!(g.cluster.lease_duration, Duration::from_secs(15));
    assert_eq!(g.cluster.lease_renewal_interval, Duration::from_secs(5));
    assert_eq!(g.cluster.lease_safety_margin, 0.15);
    assert_eq!(
        g.cluster.memberlist.bind_addr,
        ["0.0.0.0".parse::<std::net::IpAddr>().unwrap()]
    );
    assert_eq!(
        g.cluster.memberlist.gossip_interval,
        Duration::from_millis(200)
    );
    assert_eq!(
        g.tracing,
        Tracing {
            otlp_endpoint: None,
            sample_ratio: 0.0
        }
    );
    let p = &cfg.pipelines["orders"];
    assert_eq!(p.source.ssl_mode, SslMode::Require);
    assert_eq!(p.source.timeouts.snapshot, Duration::from_secs(6 * 3600));
    assert_eq!(p.source.toast_strategy, ToastStrategy::CarryForward);
    let pg = p.source.postgres.as_ref().unwrap();
    assert!(pg.auto_cleanup);
    assert_eq!(
        (pg.slot_management, pg.slot_on_orphan),
        (SlotManagement::Managed, SlotOnOrphan::Halt)
    );
    assert_eq!(pg.max_wal_lag_bytes, None);
    assert_eq!(p.tables.snapshots.mode, SnapshotMode::Never);
    assert_eq!(p.tables.snapshots.chunk_size, 10_000);
    assert_eq!(p.sink.write_mode, WriteMode::Mirror);
    assert_eq!(
        p.sink.flush,
        Flush {
            seconds: Some(30),
            rows: Some(100_000),
            bytes: Some(512 << 20)
        }
    );
    assert_eq!(p.sink.max_transaction_batch_rows, None);
    assert_eq!(p.sink.max_transaction_buffer_bytes, 1 << 30);
    assert_eq!(p.sink.buffer_capacity_rows, 200_000);
    let delta = p.sink.delta.as_ref().unwrap();
    assert_eq!(delta.checkpoint_interval, 10);
    assert_eq!(delta.log_retention, Duration::from_secs(30 * 86_400));
    assert_eq!(delta.timestamp_ntz_as, TimestampNtzAs::TimestampNtz);
    assert_eq!(delta.catalog_type, DeltaCatalogType::None);
    assert!(p.schema_evolution.enabled);
}

#[test]
fn absent_mysql_and_delta_blocks_take_every_default() {
    // Every field of `source.mysql` and `sink.delta` has a default, so the blocks may be omitted.
    let yaml = MINIMAL
        .replace(
            "host: db.example.com",
            "type: mysql\n      host: db.example.com",
        )
        .replace(
            "      postgres: { publication: orders_pub, slot: orders_slot }\n",
            "",
        )
        .replace("include: [public.orders]", "include: [orders]")
        .replace("      delta: {}\n", "");
    assert!(
        !yaml.contains("mysql:") && !yaml.contains("delta:"),
        "{yaml}"
    );
    let loaded = load_str(&yaml, &env(&[("PW", "pw")])).unwrap_or_else(|r| panic!("{r:#?}"));
    assert!(loaded.warnings.is_empty(), "{:#?}", loaded.warnings);
    let p = &loaded.config.pipelines["orders"];
    let mysql = p.source.mysql.as_ref().unwrap();
    assert_eq!(mysql.replication_type, ReplicationType::Gtid);
    assert!(mysql.tinyint1_as_bool);
    let delta = p.sink.delta.as_ref().unwrap();
    assert_eq!(delta.checkpoint_interval, 10);
    assert_eq!(delta.column_mapping, ColumnMapping::None);
}

#[test]
fn structural_pass_stops_before_env_and_files() {
    let yaml = MINIMAL
        .replace(
            "host: db.example.com",
            "host: db.example.com\n      port: abc",
        )
        .replace(
            "user: cdc\n      password: ${PW}",
            "credentials_file: /nonexistent/creds.json",
        );
    let report = load_str(&yaml, &env(&[])).unwrap_err();
    let rules: Vec<_> = report.errors.iter().map(|d| d.rule.as_str()).collect();
    assert_eq!(rules, ["TYPE"], "{report:#?}");
    assert_eq!(report.errors[0].path, "pipelines.orders.source.port");
    assert_eq!(report.errors[0].loc.unwrap().line, 10);
}

#[test]
fn unset_variable_is_v063_at_its_path() {
    let report = load_str(MINIMAL, &env(&[])).unwrap_err();
    let e = &report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        ("V-063", "pipelines.orders.source.password")
    );
    assert_eq!(e.message, "environment variable PW is not set");
}

#[test]
fn template_in_a_union_discriminator_resolves_in_phase_2() {
    let yaml = MINIMAL
        .replace(
            "host: db.example.com",
            "type: ${SRC}\n      host: db.example.com",
        )
        .replace(
            "postgres: { publication: orders_pub, slot: orders_slot }",
            "mysql: {}",
        )
        .replace("[public.orders]", "[orders]");
    let cfg = load_str(&yaml, &env(&[("PW", "pw"), ("SRC", "mysql")]))
        .unwrap()
        .config;
    assert_eq!(cfg.pipelines["orders"].source.kind, SourceType::Mysql);
    assert_eq!(cfg.pipelines["orders"].source.port, 3306);
}

#[test]
fn bad_credentials_file_is_v067() {
    let files = Files::new();
    let path = files.write("creds.json", r#"{"user": "cdc"}"#);
    let yaml = MINIMAL.replace(
        "user: cdc\n      password: ${PW}",
        &format!("credentials_file: {}", path.display()),
    );
    let report = load_str(&yaml, &env(&[])).unwrap_err();
    assert_eq!(report.errors[0].rule, "V-067");
    assert_eq!(
        report.errors[0].path,
        "pipelines.orders.source.credentials_file"
    );
}

#[test]
fn bad_pem_is_v068() {
    let files = Files::new();
    let key = files.write("key.pem", support::key());
    let yaml = MINIMAL.replace(
        "globals:\n  state: { bucket: app-state }",
        &format!(
            "globals:\n  state: {{ bucket: app-state }}\n  api:\n    tls: {{ enabled: true, cert: {k}, key: {k} }}",
            k = key.display()
        ),
    );
    let report = load_str(&yaml, &env(&[("PW", "pw")])).unwrap_err();
    let e = &report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        ("V-068", "globals.api.tls.cert")
    );
    assert!(e.message.contains("no certificate"), "{}", e.message);
}

const ICEBERG_SINK: &str = "sink:
      type: iceberg
      bucket: lake
      iceberg:
        catalog:
          ";

fn with_catalog(catalog: &str) -> String {
    MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        &format!("{ICEBERG_SINK}{catalog}"),
    )
}

#[test]
fn empty_token_file_is_v071_and_missing_keytab_is_v069() {
    let files = Files::new();
    let token = files.write("token", "\n");
    let yaml = with_catalog(&format!(
        "rest: {{ url: \"https://c\", auth: bearer, token_file: {} }}",
        token.display()
    ));
    let report = load_str(&yaml, &env(&[("PW", "pw")])).unwrap_err();
    assert_eq!(report.errors[0].rule, "V-071");

    let yaml = with_catalog(
        "hive: { url: \"thrift://hms:9083\", auth: kerberos, kerberos: { principal: cdc, keytab: /nonexistent/cdc.keytab } }",
    );
    let report = load_str(&yaml, &env(&[("PW", "pw")])).unwrap_err();
    let e = &report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        (
            "V-069",
            "pipelines.orders.sink.iceberg.catalog.hive.kerberos.keytab"
        )
    );
}

#[test]
fn explicit_state_location_takes_the_defaults_prefix_as_parent() {
    let yaml = MINIMAL.replace(
        "pipelines:",
        "defaults:\n  state: { bucket: state-bucket, prefix: parent }\npipelines:",
    );
    let cfg = load_str(&yaml, &env(&[("PW", "pw")])).unwrap().config;
    assert_eq!(
        cfg.pipelines["orders"].state,
        StateLocation::Explicit {
            bucket: "state-bucket".into(),
            parent_prefix: Some("parent".into()),
            prefix: "orders".into(),
        }
    );
}

#[test]
fn secrets_never_print() {
    let cfg = load_str(MINIMAL, &env(&[("PW", "s3cr3t-pw")]))
        .unwrap()
        .config;
    assert!(!format!("{cfg:?}").contains("s3cr3t-pw"));
    assert!(!serde_json::to_string(&cfg).unwrap().contains("s3cr3t-pw"));
}

#[test]
fn unreadable_config_file_is_a_parse_error() {
    let report = load_file(
        std::path::Path::new("/nonexistent/data-avalanche.yaml"),
        &env(&[]),
    )
    .unwrap_err();
    assert_eq!(report.errors[0].rule, "PARSE");
    assert!(
        report.errors[0]
            .message
            .starts_with("cannot read config file")
    );
}
```

Create `crates/data-avalanche-config/tests/support/mod.rs`:

```rust
//! Shared test helpers: an environment map, a temp directory for referenced files, and the spec's
//! §14 example with its file paths pointed at that directory.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::OnceLock;

pub const SPEC_EXAMPLE: &str = include_str!("../fixtures/spec_example.yaml");

/// A minimal valid config: one PostgreSQL → Delta pipeline. Needs `PW` in the environment.
pub const MINIMAL: &str = r#"
config_version: 1
globals:
  state: { bucket: app-state }
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
"#;

/// A throwaway self-signed certificate and its key, generated once per test binary: no key material
/// is committed.
fn pem_pair() -> &'static (String, String) {
    static PAIR: OnceLock<(String, String)> = OnceLock::new();
    PAIR.get_or_init(|| {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["data-avalanche-test".to_string()]).unwrap();
        (cert.pem(), signing_key.serialize_pem())
    })
}

/// A PEM certificate (self-signed, CN `data-avalanche-test`).
pub fn cert() -> &'static str {
    &pem_pair().0
}

/// The PEM (PKCS#8) private key of [`cert`].
pub fn key() -> &'static str {
    &pem_pair().1
}

pub fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// A temp directory for files a config references; removed on drop.
pub struct Files {
    dir: tempfile::TempDir,
}

impl Files {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    pub fn dir(&self) -> String {
        self.dir.path().display().to_string()
    }

    pub fn write(&self, name: &str, content: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, content).unwrap();
        path
    }
}

/// The §14 example, runnable: its `/etc/cdc/tls/` files written to `files`, and every variable it
/// references set.
pub fn spec_example(files: &Files) -> (String, BTreeMap<String, String>) {
    files.write("api.crt", cert());
    files.write("gossip.crt", cert());
    files.write("gossip-ca.crt", cert());
    let creds = files.write(
        "events-db.json",
        r#"{"user": "events", "password": "events-pw"}"#,
    );
    let yaml = SPEC_EXAMPLE.replace("/etc/cdc/tls/", &format!("{}/", files.dir()));
    let env = env(&[
        ("ACCOUNT_ID", "123456789012"),
        ("API_TLS_KEY", key()),
        ("GOSSIP_TLS_KEY", key()),
        ("CATALOG_CLIENT_ID", "cdc-client"),
        ("CATALOG_CLIENT_SECRET", "client-secret"),
        ("ORDERS_DB_PASSWORD", "orders-pw"),
        ("EVENTS_DB_CREDS_FILE", creds.to_str().unwrap()),
    ]);
    (yaml, env)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0583]: file not found for module `build`
error[E0583]: file not found for module `load`
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-config/src/build.rs`:

```rust
//! Phase 3 (config §11): the merged raw model becomes the [`ResolvedConfig`], with every default of
//! the spec's field tables applied. The build visits every leaf, so a leaf no rule reads still
//! reports a coercion failure. Presence is the rules' job: an absent required value makes the
//! build return `None` without a diagnostic of its own. Files are read only when the context is
//! resolving (§11: Phase 1 touches neither the environment nor the filesystem).

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::coerce::{LeafType, LogFilter};
use crate::ctx::{At, Ctx};
use crate::enums::*;
use crate::leaf::{Leaf, Tri};
use crate::material::{Pem, PemKind, load_pem, read_file_ref, read_json_pair, read_token_file};
use crate::path::YamlPath;
use crate::raw::*;
use crate::resolved::*;
use crate::units::{ByteSize, HostPort};

type L<T> = Option<Leaf<T>>;

/// Spec defaults (config §6–§9) that other modules also need.
pub const DEFAULT_TOKENS: u32 = 128;
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(15);
pub const DEFAULT_LEASE_RENEWAL_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_FLUSH_SECONDS: u64 = 30;
pub const DEFAULT_FLUSH_ROWS: u64 = 100_000;
pub const DEFAULT_FLUSH_BYTES: u64 = 512 * ByteSize::MIB;

/// A field that has a default: absent → `Some(default)`; present → its value (`None` if unknown
/// or invalid).
fn or<T: LeafType>(ctx: &mut Ctx, leaf: &L<T>, at: &At, key: &str, default: T) -> Option<T> {
    match leaf {
        None => Some(default),
        Some(l) => ctx.value(l, &at.key(key)),
    }
}

/// A required field: absent → `None` (its presence rule reports it).
fn req<T: LeafType>(ctx: &mut Ctx, leaf: &L<T>, at: &At, key: &str) -> Option<T> {
    leaf.as_ref().and_then(|l| ctx.value(l, &at.key(key)))
}

/// An optional field without a default: absent → `Some(None)`.
fn opt<T: LeafType>(ctx: &mut Ctx, leaf: &L<T>, at: &At, key: &str) -> Option<Option<T>> {
    match leaf {
        None => Some(None),
        Some(l) => ctx.value(l, &at.key(key)).map(Some),
    }
}

/// A tri-state field (§9.4.1): absent → `default`, `null` → disabled, value → the value.
fn tri<T: LeafType>(
    ctx: &mut Ctx,
    t: &Tri<T>,
    at: &At,
    key: &str,
    default: Option<T>,
) -> Option<Option<T>> {
    match t {
        None => Some(default),
        Some(None) => Some(None),
        Some(Some(l)) => ctx.value(l, &at.key(key)).map(Some),
    }
}

/// A list of scalars; absent → empty. Every element is visited.
fn list<T: LeafType>(
    ctx: &mut Ctx,
    v: &Option<Vec<Leaf<T>>>,
    at: &At,
    key: &str,
) -> Option<Vec<T>> {
    let at = at.key(key);
    let items: Vec<Option<T>> = v
        .iter()
        .flatten()
        .enumerate()
        .map(|(i, l)| ctx.value(l, &at.index(i)))
        .collect();
    items.into_iter().collect()
}

/// A map of scalars; absent → empty. Every value is visited.
fn map<T: LeafType>(
    ctx: &mut Ctx,
    m: &Option<BTreeMap<String, Leaf<T>>>,
    at: &At,
    key: &str,
) -> Option<BTreeMap<String, T>> {
    let at = at.key(key);
    let items: Vec<(String, Option<T>)> = m
        .iter()
        .flatten()
        .map(|(k, l)| (k.clone(), ctx.value(l, &at.key(k))))
        .collect();
    items.into_iter().map(|(k, v)| v.map(|v| (k, v))).collect()
}

/// A path to a file read in Phase 2. `None` in the structural pass (not an error) and on failure.
fn file_path(ctx: &mut Ctx, leaf: &Leaf<String>, at: &At) -> Option<PathBuf> {
    let text = ctx.text(leaf, at)?;
    ctx.is_resolving().then(|| PathBuf::from(text))
}

/// Optional PEM (`cert`/`key`/`ca`, §4.4); V-068 on invalid material.
fn pem(ctx: &mut Ctx, leaf: &L<String>, at: &At, key: &str, kind: PemKind) -> Option<Option<Pem>> {
    let Some(l) = leaf else { return Some(None) };
    let at = at.key(key);
    let text = ctx.text(l, &at)?;
    if !ctx.is_resolving() {
        return None;
    }
    match load_pem(&text, kind) {
        Ok(p) => Some(Some(p)),
        Err(e) => {
            ctx.error("V-068", &at, l.loc(), e);
            None
        }
    }
}

/// A `user`/`password` pair or a `credentials_file` (§4.6); V-067 on a bad file. `None` when
/// neither (or both) is given: V-010/V-040/V-049 and V-065 report that.
fn credentials(
    ctx: &mut Ctx,
    user: &L<String>,
    password: &L<String>,
    file: &L<String>,
    at: &At,
) -> Option<Credentials> {
    let user_v = user.as_ref().and_then(|l| ctx.text(l, &at.key("user")));
    let password_v = password
        .as_ref()
        .and_then(|l| ctx.text(l, &at.key("password")));
    match (file, user, password) {
        (Some(f), None, None) => {
            let at = at.key("credentials_file");
            let path = file_path(ctx, f, &at)?;
            match read_json_pair(&path, "user", "password") {
                Ok((user, password)) => Some(Credentials::File {
                    path,
                    user,
                    password,
                }),
                Err(e) => {
                    ctx.error("V-067", &at, f.loc(), e);
                    None
                }
            }
        }
        (None, Some(_), Some(_)) => Some(Credentials::Inline {
            user: user_v?,
            password: crate::secret::Secret::new(password_v?),
        }),
        _ => None,
    }
}

fn read_or_report<T>(
    ctx: &mut Ctx,
    rule: &str,
    at: &At,
    leaf: &Leaf<String>,
    read: impl FnOnce(&Path) -> Result<T, String>,
) -> Option<(PathBuf, T)> {
    let path = file_path(ctx, leaf, at)?;
    match read(&path) {
        Ok(v) => Some((path, v)),
        Err(e) => {
            ctx.error(rule, at, leaf.loc(), e);
            None
        }
    }
}

// ---- globals ----

fn globals(g: &RawGlobals, ctx: &mut Ctx) -> Option<Globals> {
    let at = At::config(YamlPath::root().key("globals"));
    let cloud = or(ctx, &g.cloud, &at, "cloud", Cloud::Aws);
    let state = g.state.clone().unwrap_or_default();
    let state_at = at.key("state");
    let bucket = req(ctx, &state.bucket, &state_at, "bucket");
    let prefix = or(ctx, &state.prefix, &state_at, "prefix", String::new());
    let api = api(&g.api.clone().unwrap_or_default(), &at.key("api"), ctx);
    let log = g.log.clone().unwrap_or_default();
    let log_at = at.key("log");
    let level = or(ctx, &log.level, &log_at, "level", LogFilter("info".into()));
    let format = or(ctx, &log.format, &log_at, "format", LogFormat::Json);
    let cluster = cluster(
        &g.cluster.clone().unwrap_or_default(),
        &at.key("cluster"),
        ctx,
    );
    let tracing = g.tracing.clone().unwrap_or_default();
    let tracing_at = at.key("tracing");
    let otlp_endpoint = opt(ctx, &tracing.otlp_endpoint, &tracing_at, "otlp_endpoint");
    let ratio_default = if tracing.otlp_endpoint.is_some() {
        0.05
    } else {
        0.0
    };
    let sample_ratio = or(
        ctx,
        &tracing.sample_ratio,
        &tracing_at,
        "sample_ratio",
        ratio_default,
    );
    let azure = g.azure.clone().unwrap_or_default();
    let account_name = opt(ctx, &azure.account_name, &at.key("azure"), "account_name");
    Some(Globals {
        cloud: cloud?,
        state: GlobalState {
            bucket: bucket?,
            prefix: prefix?,
        },
        api: api?,
        log: Log {
            level: level?,
            format: format?,
        },
        cluster: cluster?,
        tracing: Tracing {
            otlp_endpoint: otlp_endpoint?,
            sample_ratio: sample_ratio?,
        },
        azure: Azure {
            account_name: account_name?,
        },
    })
}

fn api(a: &RawApi, at: &At, ctx: &mut Ctx) -> Option<Api> {
    let address = or(
        ctx,
        &a.address,
        at,
        "address",
        HostPort {
            host: "0.0.0.0".into(),
            port: 8080,
        },
    );
    let auth = or(ctx, &a.auth, at, "auth", ApiAuth::None);
    let creds = credentials(ctx, &a.user, &a.password, &a.credentials_file, at);
    let jwt = a.jwt.as_ref().map(|j| {
        let at = at.key("jwt");
        let issuer = req(ctx, &j.issuer, &at, "issuer");
        let audience = opt(ctx, &j.audience, &at, "audience");
        let jwks_url = req(ctx, &j.jwks_url, &at, "jwks_url");
        let roles_claim = or(ctx, &j.roles_claim, &at, "roles_claim", "roles".to_string());
        Some(Jwt {
            issuer: issuer?,
            audience: audience?,
            jwks_url: jwks_url?,
            roles_claim: roles_claim?,
        })
    });
    let oauth2 = a.oauth2.as_ref().map(|o| {
        let at = at.key("oauth2");
        let issuer = req(ctx, &o.issuer, &at, "issuer");
        let audience = opt(ctx, &o.audience, &at, "audience");
        let jwks_url = opt(ctx, &o.jwks_url, &at, "jwks_url");
        let discovery_url = opt(ctx, &o.discovery_url, &at, "discovery_url");
        let roles_claim = or(ctx, &o.roles_claim, &at, "roles_claim", "roles".to_string());
        Some(ApiOauth2 {
            issuer: issuer?,
            audience: audience?,
            jwks_url: jwks_url?,
            discovery_url: discovery_url?,
            roles_claim: roles_claim?,
        })
    });
    let role_mapping = role_mapping(ctx, &a.role_mapping, &at.key("role_mapping"));
    let tls = a.tls.clone().unwrap_or_default();
    let tls_at = at.key("tls");
    let enabled = or(ctx, &tls.enabled, &tls_at, "enabled", false);
    let cert = pem(ctx, &tls.cert, &tls_at, "cert", PemKind::Certificates);
    let key = pem(ctx, &tls.key, &tls_at, "key", PemKind::PrivateKey);
    // Only the block the auth mode uses is kept: an unused block can't fail the load.
    let auth = auth?;
    let (credentials, jwt, oauth2) = match auth {
        ApiAuth::None => (None, None, None),
        ApiAuth::Basic => (Some(creds?), None, None),
        ApiAuth::Jwt => (None, Some(jwt??), None),
        ApiAuth::Oauth2 => (None, None, Some(oauth2??)),
    };
    let tls = match enabled? {
        true => Some(TlsIdentity {
            cert: cert??,
            key: key??,
        }),
        false => None,
    };
    Some(Api {
        address: address?,
        auth,
        credentials,
        jwt,
        oauth2,
        role_mapping: role_mapping?,
        tls,
    })
}

/// `role_mapping` keys must be roles (V-099; reported by the rules); values are IdP role strings.
fn role_mapping(
    ctx: &mut Ctx,
    m: &Option<BTreeMap<String, Vec<Leaf<String>>>>,
    at: &At,
) -> Option<BTreeMap<Role, Vec<String>>> {
    let mut out = BTreeMap::new();
    let mut ok = true;
    for (k, values) in m.iter().flatten() {
        let vals = list(ctx, &Some(values.clone()), at, k);
        match (k.parse::<Role>(), vals) {
            (Ok(role), Some(v)) => {
                out.insert(role, v);
            }
            _ => ok = false,
        }
    }
    ok.then_some(out)
}

fn cluster(c: &RawCluster, at: &At, ctx: &mut Ctx) -> Option<Cluster> {
    let tokens = or(ctx, &c.tokens, at, "tokens", DEFAULT_TOKENS);
    let lock_backend = or(
        ctx,
        &c.lock_backend,
        at,
        "lock_backend",
        LockBackend::Storage,
    );
    let lease_duration = or(
        ctx,
        &c.lease_duration,
        at,
        "lease_duration",
        DEFAULT_LEASE_DURATION,
    );
    let lease_renewal_interval = or(
        ctx,
        &c.lease_renewal_interval,
        at,
        "lease_renewal_interval",
        DEFAULT_LEASE_RENEWAL_INTERVAL,
    );
    let lease_safety_margin = or(ctx, &c.lease_safety_margin, at, "lease_safety_margin", 0.15);
    let rebalance_stabilization = or(
        ctx,
        &c.rebalance_stabilization,
        at,
        "rebalance_stabilization",
        Duration::from_secs(15),
    );
    let max_concurrent_handoffs = or(
        ctx,
        &c.max_concurrent_handoffs,
        at,
        "max_concurrent_handoffs",
        2,
    );
    let max_concurrent_takeovers = or(
        ctx,
        &c.max_concurrent_takeovers,
        at,
        "max_concurrent_takeovers",
        4,
    );
    let drain_timeout = or(
        ctx,
        &c.drain_timeout,
        at,
        "drain_timeout",
        Duration::from_secs(60),
    );
    let discovery = or(ctx, &c.discovery, at, "discovery", Discovery::Static);
    let discovery_dns_name = opt(ctx, &c.discovery_dns_name, at, "discovery_dns_name");
    let memberlist = memberlist(
        &c.memberlist.clone().unwrap_or_default(),
        &at.key("memberlist"),
        ctx,
    );
    Some(Cluster {
        tokens: tokens?,
        lock_backend: lock_backend?,
        lease_duration: lease_duration?,
        lease_renewal_interval: lease_renewal_interval?,
        lease_safety_margin: lease_safety_margin?,
        rebalance_stabilization: rebalance_stabilization?,
        max_concurrent_handoffs: max_concurrent_handoffs?,
        max_concurrent_takeovers: max_concurrent_takeovers?,
        drain_timeout: drain_timeout?,
        discovery: discovery?,
        discovery_dns_name: discovery_dns_name?,
        memberlist: memberlist?,
    })
}

fn memberlist(m: &RawMemberlist, at: &At, ctx: &mut Ctx) -> Option<Memberlist> {
    let secs = Duration::from_secs;
    let join_members = list(ctx, &m.join_members, at, "join_members");
    let node_name = opt(ctx, &m.node_name, at, "node_name");
    let advertise_addr = or(ctx, &m.advertise_addr, at, "advertise_addr", String::new());
    let advertise_port = or(ctx, &m.advertise_port, at, "advertise_port", 7946);
    let bind_addr = match &m.bind_addr {
        None => Some(vec![IpAddr::V4(Ipv4Addr::UNSPECIFIED)]),
        some => list(ctx, some, at, "bind_addr"),
    };
    let bind_port = or(ctx, &m.bind_port, at, "bind_port", 7946);
    let packet_dial_timeout = or(
        ctx,
        &m.packet_dial_timeout,
        at,
        "packet_dial_timeout",
        secs(5),
    );
    let packet_write_timeout = or(
        ctx,
        &m.packet_write_timeout,
        at,
        "packet_write_timeout",
        secs(5),
    );
    let compression_enabled = or(ctx, &m.compression_enabled, at, "compression_enabled", true);
    let gossip_interval = or(
        ctx,
        &m.gossip_interval,
        at,
        "gossip_interval",
        Duration::from_millis(200),
    );
    let gossip_nodes = or(ctx, &m.gossip_nodes, at, "gossip_nodes", 3);
    let probe_interval = or(ctx, &m.probe_interval, at, "probe_interval", secs(1));
    let probe_timeout = or(
        ctx,
        &m.probe_timeout,
        at,
        "probe_timeout",
        Duration::from_millis(500),
    );
    let dead_node_reclaim_time = or(
        ctx,
        &m.dead_node_reclaim_time,
        at,
        "dead_node_reclaim_time",
        secs(30),
    );
    let push_pull_interval = or(
        ctx,
        &m.push_pull_interval,
        at,
        "push_pull_interval",
        secs(30),
    );
    let retransmit_factor = or(ctx, &m.retransmit_factor, at, "retransmit_factor", 4);
    let lifeguard_enabled = or(ctx, &m.lifeguard_enabled, at, "lifeguard_enabled", true);
    let tls = m.tls.clone().unwrap_or_default();
    let tls_at = at.key("tls");
    let enabled = or(ctx, &tls.enabled, &tls_at, "enabled", false);
    let cert = pem(ctx, &tls.cert, &tls_at, "cert", PemKind::Certificates);
    let key = pem(ctx, &tls.key, &tls_at, "key", PemKind::PrivateKey);
    let ca = pem(ctx, &tls.ca, &tls_at, "ca", PemKind::Certificates);
    let tls = match enabled? {
        true => Some(MemberlistTls {
            cert: cert??,
            key: key??,
            ca: ca?,
        }),
        false => None,
    };
    Some(Memberlist {
        join_members: join_members?,
        node_name: node_name?,
        advertise_addr: advertise_addr?,
        advertise_port: advertise_port?,
        bind_addr: bind_addr?,
        bind_port: bind_port?,
        packet_dial_timeout: packet_dial_timeout?,
        packet_write_timeout: packet_write_timeout?,
        compression_enabled: compression_enabled?,
        gossip_interval: gossip_interval?,
        gossip_nodes: gossip_nodes?,
        probe_interval: probe_interval?,
        probe_timeout: probe_timeout?,
        dead_node_reclaim_time: dead_node_reclaim_time?,
        push_pull_interval: push_pull_interval?,
        retransmit_factor: retransmit_factor?,
        lifeguard_enabled: lifeguard_enabled?,
        tls,
    })
}

// ---- pipelines ----

fn pipeline(
    name: &str,
    p: &RawPipeline,
    parent_state_prefix: Option<&Leaf<String>>,
    ctx: &mut Ctx,
) -> Option<Pipeline> {
    let at = At::pipeline(name, YamlPath::root().key("pipelines").key(name));
    let id = req(ctx, &p.id, &at, "id");
    let source = source(
        &p.source.clone().unwrap_or_default(),
        &at.key("source"),
        ctx,
    );
    let tables = tables(
        &p.tables.clone().unwrap_or_default(),
        &at.key("tables"),
        ctx,
    );
    let db = source.as_ref().map(|s| s.db.clone());
    let sink = sink(
        &p.sink.clone().unwrap_or_default(),
        &at.key("sink"),
        id.as_deref(),
        db.as_deref(),
        ctx,
    );
    let state = p.state.clone().unwrap_or_default();
    let state_at = at.key("state");
    let state_bucket = opt(ctx, &state.bucket, &state_at, "bucket");
    let state_prefix = opt(ctx, &state.prefix, &state_at, "prefix");
    let parent_at = At::config(YamlPath::root().key("defaults").key("state").key("prefix"));
    let parent = match parent_state_prefix {
        None => Some(None),
        Some(l) => ctx
            .value(l, &parent_at)
            .map(|v| Some(v).filter(|p: &String| !p.is_empty())),
    };
    let evo = p.schema_evolution.clone().unwrap_or_default();
    let evo_at = at.key("schema_evolution");
    let enabled = or(ctx, &evo.enabled, &evo_at, "enabled", true);
    let rescan_interval = or(
        ctx,
        &evo.rescan_interval,
        &evo_at,
        "rescan_interval",
        Duration::from_secs(3600),
    );
    let id = id?;
    let state = match state_bucket? {
        None => StateLocation::CoLocated,
        Some(bucket) => StateLocation::Explicit {
            bucket,
            parent_prefix: parent?,
            prefix: state_prefix?.unwrap_or_else(|| id.clone()),
        },
    };
    Some(Pipeline {
        name: name.to_string(),
        id,
        source: source?,
        tables: tables?,
        sink: sink?,
        state,
        schema_evolution: SchemaEvolution {
            enabled: enabled?,
            rescan_interval: rescan_interval?,
        },
    })
}

fn source(s: &RawSource, at: &At, ctx: &mut Ctx) -> Option<Source> {
    let kind = or(ctx, &s.kind, at, "type", SourceType::Postgres);
    let host = req(ctx, &s.host, at, "host");
    let port = opt(ctx, &s.port, at, "port");
    let db = req(ctx, &s.db, at, "db");
    let credentials = credentials(ctx, &s.user, &s.password, &s.credentials_file, at);
    let ssl_mode = or(ctx, &s.ssl_mode, at, "ssl_mode", SslMode::Require);
    let ssl_cert = pem(ctx, &s.ssl_cert, at, "ssl_cert", PemKind::Certificates);
    let ssl_key = pem(ctx, &s.ssl_key, at, "ssl_key", PemKind::PrivateKey);
    let ssl_ca = pem(ctx, &s.ssl_ca, at, "ssl_ca", PemKind::Certificates);
    let t = s.timeouts.clone().unwrap_or_default();
    let t_at = at.key("timeouts");
    let connect = or(ctx, &t.connect, &t_at, "connect", Duration::from_secs(10));
    let command = or(ctx, &t.command, &t_at, "command", Duration::from_secs(30));
    let snapshot = or(
        ctx,
        &t.snapshot,
        &t_at,
        "snapshot",
        Duration::from_secs(6 * 3600),
    );
    let toast_strategy = or(
        ctx,
        &s.toast_strategy,
        at,
        "toast_strategy",
        ToastStrategy::CarryForward,
    );
    let postgres = s
        .postgres
        .as_ref()
        .map(|p| postgres(p, &at.key("postgres"), ctx));
    // Every `mysql` field has a default, so an absent block means all defaults (D-M1.5).
    let mysql = mysql(
        s.mysql.as_ref().unwrap_or(&RawMysql::default()),
        &at.key("mysql"),
        ctx,
    );
    let kind = kind?;
    let (postgres, mysql) = match kind {
        SourceType::Postgres => (Some(postgres??), None),
        SourceType::Mysql => (None, Some(mysql?)),
    };
    let default_port = match kind {
        SourceType::Postgres => 5432,
        SourceType::Mysql => 3306,
    };
    Some(Source {
        kind,
        host: host?,
        port: port?.unwrap_or(default_port),
        db: db?,
        credentials: credentials?,
        ssl_mode: ssl_mode?,
        ssl_cert: ssl_cert?,
        ssl_key: ssl_key?,
        ssl_ca: ssl_ca?,
        timeouts: Timeouts {
            connect: connect?,
            command: command?,
            snapshot: snapshot?,
        },
        toast_strategy: toast_strategy?,
        postgres,
        mysql,
    })
}

fn postgres(p: &RawPostgres, at: &At, ctx: &mut Ctx) -> Option<Postgres> {
    let publication = req(ctx, &p.publication, at, "publication");
    let slot = req(ctx, &p.slot, at, "slot");
    let auto_cleanup = or(ctx, &p.auto_cleanup, at, "auto_cleanup", true);
    let heartbeat = or(ctx, &p.heartbeat, at, "heartbeat", Duration::from_secs(60));
    let slot_management = or(
        ctx,
        &p.slot_management,
        at,
        "slot_management",
        SlotManagement::Managed,
    );
    let slot_on_orphan = or(
        ctx,
        &p.slot_on_orphan,
        at,
        "slot_on_orphan",
        SlotOnOrphan::Halt,
    );
    let max_wal_lag_bytes = tri(ctx, &p.max_wal_lag_bytes, at, "max_wal_lag_bytes", None);
    Some(Postgres {
        publication: publication?,
        slot: slot?,
        auto_cleanup: auto_cleanup?,
        heartbeat: heartbeat?,
        slot_management: slot_management?,
        slot_on_orphan: slot_on_orphan?,
        max_wal_lag_bytes: max_wal_lag_bytes?.map(ByteSize::bytes),
    })
}

fn mysql(m: &RawMysql, at: &At, ctx: &mut Ctx) -> Option<Mysql> {
    let replication_type = or(
        ctx,
        &m.replication_type,
        at,
        "replication_type",
        ReplicationType::Gtid,
    );
    let tinyint1_as_bool = or(ctx, &m.tinyint1_as_bool, at, "tinyint1_as_bool", true);
    let tinyint1_detection = or(
        ctx,
        &m.tinyint1_detection,
        at,
        "tinyint1_detection",
        Tinyint1Detection::InformationSchema,
    );
    let binlog_purge_action = or(
        ctx,
        &m.binlog_purge_action,
        at,
        "binlog_purge_action",
        BinlogPurgeAction::Error,
    );
    Some(Mysql {
        replication_type: replication_type?,
        tinyint1_as_bool: tinyint1_as_bool?,
        tinyint1_detection: tinyint1_detection?,
        binlog_purge_action: binlog_purge_action?,
    })
}

fn tables(t: &RawTables, at: &At, ctx: &mut Ctx) -> Option<Tables> {
    let include = list(ctx, &t.include, at, "include");
    let exclude = list(ctx, &t.exclude, at, "exclude");
    let s = t.snapshots.clone().unwrap_or_default();
    let s_at = at.key("snapshots");
    let mode = or(ctx, &s.mode, &s_at, "mode", SnapshotMode::Never);
    let parallelism = or(ctx, &s.parallelism, &s_at, "parallelism", 1);
    let chunk_size = or(ctx, &s.chunk_size, &s_at, "chunk_size", 10_000);
    let overrides = map(ctx, &s.overrides, &s_at, "overrides");
    let cto_at = at.key("column_type_overrides");
    let column_type_overrides: Option<BTreeMap<_, _>> = t
        .column_type_overrides
        .iter()
        .flatten()
        .map(|(column, l)| {
            let v = ctx.value_with(l, &cto_at.key(column), |text, _| {
                format!("invalid logical type {text} in column_type_overrides for {column}")
            });
            (column.clone(), v)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|(k, v)| v.map(|v| (k, v)))
        .collect();
    let numeric_overflow_action = or(
        ctx,
        &t.numeric_overflow_action,
        at,
        "numeric_overflow_action",
        NumericOverflowAction::String,
    );
    let pk_at = at.key("primary_key");
    let primary_key: Option<BTreeMap<_, _>> = t
        .primary_key
        .iter()
        .flatten()
        .map(|(table, cols)| (table.clone(), list(ctx, &Some(cols.clone()), &pk_at, table)))
        .collect::<Vec<_>>()
        .into_iter()
        .map(|(k, v)| v.map(|v| (k, v)))
        .collect();
    let part_at = at.key("partitioning");
    let partitioning: Option<BTreeMap<_, _>> = t
        .partitioning
        .iter()
        .flatten()
        .map(|(table, fields)| {
            let at = part_at.key(table);
            let built: Vec<Option<PartitionField>> = fields
                .iter()
                .enumerate()
                .map(|(i, f)| partition_field(f, &at.index(i), ctx))
                .collect();
            (table.clone(), built.into_iter().collect::<Option<Vec<_>>>())
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|(k, v)| v.map(|v| (k, v)))
        .collect();
    Some(Tables {
        include: include?,
        exclude: exclude?,
        snapshots: Snapshots {
            mode: mode?,
            parallelism: parallelism?,
            chunk_size: chunk_size?,
            overrides: overrides?,
        },
        column_type_overrides: column_type_overrides?,
        numeric_overflow_action: numeric_overflow_action?,
        primary_key: primary_key?,
        partitioning: partitioning?,
    })
}

fn partition_field(f: &RawPartitionField, at: &At, ctx: &mut Ctx) -> Option<PartitionField> {
    let column = req(ctx, &f.column, at, "column");
    let transform = req(ctx, &f.transform, at, "transform");
    let n = opt(ctx, &f.n, at, "n");
    let width = opt(ctx, &f.width, at, "width");
    let transform = match transform? {
        Transform::Identity => PartitionTransform::Identity,
        Transform::Bucket => PartitionTransform::Bucket { n: n?? },
        Transform::Truncate => PartitionTransform::Truncate { width: width?? },
        Transform::Year => PartitionTransform::Year,
        Transform::Month => PartitionTransform::Month,
        Transform::Day => PartitionTransform::Day,
        Transform::Hour => PartitionTransform::Hour,
    };
    Some(PartitionField {
        column: column?,
        transform,
    })
}

fn sink(s: &RawSink, at: &At, id: Option<&str>, db: Option<&str>, ctx: &mut Ctx) -> Option<Sink> {
    let kind = req(ctx, &s.kind, at, "type");
    let write_mode = or(ctx, &s.write_mode, at, "write_mode", WriteMode::Mirror);
    let bucket = req(ctx, &s.bucket, at, "bucket");
    let prefix = opt(ctx, &s.prefix, at, "prefix");
    let f = s.flush.clone().unwrap_or_default();
    let f_at = at.key("flush");
    let seconds = tri(
        ctx,
        &f.seconds,
        &f_at,
        "seconds",
        Some(DEFAULT_FLUSH_SECONDS),
    );
    let rows = tri(ctx, &f.rows, &f_at, "rows", Some(DEFAULT_FLUSH_ROWS));
    let bytes = tri(
        ctx,
        &f.bytes,
        &f_at,
        "bytes",
        Some(ByteSize(DEFAULT_FLUSH_BYTES)),
    );
    let batch_rows = tri(
        ctx,
        &s.max_transaction_batch_rows,
        at,
        "max_transaction_batch_rows",
        None,
    );
    let buffer_bytes = or(
        ctx,
        &s.max_transaction_buffer_bytes,
        at,
        "max_transaction_buffer_bytes",
        ByteSize(ByteSize::GIB),
    );
    let capacity = or(
        ctx,
        &s.buffer_capacity_rows,
        at,
        "buffer_capacity_rows",
        200_000,
    );
    let metrics = s.metrics.clone().unwrap_or_default();
    let per_table = or(
        ctx,
        &metrics.per_table,
        &at.key("metrics"),
        "per_table",
        false,
    );
    let iceberg = s
        .iceberg
        .as_ref()
        .map(|i| iceberg(i, &at.key("iceberg"), db, ctx));
    // Every `delta` field has a default, so an absent block means all defaults (D-M1.5).
    let delta = delta(
        s.delta.as_ref().unwrap_or(&RawDelta::default()),
        &at.key("delta"),
        ctx,
    );
    let kind = kind?;
    let (iceberg, delta) = match kind {
        SinkType::Iceberg => (Some(iceberg??), None),
        SinkType::Delta => (None, Some(delta?)),
    };
    Some(Sink {
        kind,
        write_mode: write_mode?,
        bucket: bucket?,
        prefix: prefix?.or_else(|| id.map(str::to_string))?,
        flush: Flush {
            seconds: seconds?,
            rows: rows?,
            bytes: bytes?.map(ByteSize::bytes),
        },
        max_transaction_batch_rows: batch_rows?,
        max_transaction_buffer_bytes: buffer_bytes?.bytes(),
        buffer_capacity_rows: capacity?,
        metrics: SinkMetrics {
            per_table: per_table?,
        },
        iceberg,
        delta,
    })
}

fn iceberg(i: &RawIceberg, at: &At, db: Option<&str>, ctx: &mut Ctx) -> Option<Iceberg> {
    let strategy = or(ctx, &i.strategy, at, "strategy", IcebergStrategy::Mor);
    let catalog = i
        .catalog
        .as_ref()
        .map(|c| catalog(c, &at.key("catalog"), db, ctx));
    Some(Iceberg {
        strategy: strategy?,
        catalog: catalog??,
    })
}

fn catalog(c: &RawCatalog, at: &At, db: Option<&str>, ctx: &mut Ctx) -> Option<Catalog> {
    let declared = opt(ctx, &c.kind, at, "type");
    let glue = c.glue.as_ref().map(|g| {
        let at = at.key("glue");
        let name = req(ctx, &g.name, &at, "name");
        let database = opt(ctx, &g.database, &at, "database");
        let strip_schema = or(ctx, &g.strip_schema, &at, "strip_schema", false);
        Some(Glue {
            name: name?,
            database: database?.or_else(|| db.map(str::to_string))?,
            strip_schema: strip_schema?,
        })
    });
    let hive = c.hive.as_ref().map(|h| hive(h, &at.key("hive"), db, ctx));
    let rest = c.rest.as_ref().map(|r| rest(r, &at.key("rest"), ctx));
    let r = c.retry.clone().unwrap_or_default();
    let r_at = at.key("retry");
    let max_attempts = or(ctx, &r.max_attempts, &r_at, "max_attempts", 5);
    let base_backoff = or(
        ctx,
        &r.base_backoff,
        &r_at,
        "base_backoff",
        Duration::from_secs(1),
    );
    let max_backoff = or(
        ctx,
        &r.max_backoff,
        &r_at,
        "max_backoff",
        Duration::from_secs(30),
    );
    let inferred = match (&glue, &hive, &rest) {
        (Some(_), None, None) => Some(CatalogKind::Glue),
        (None, Some(_), None) => Some(CatalogKind::Hive),
        (None, None, Some(_)) => Some(CatalogKind::Rest),
        _ => None,
    };
    let kind = declared?.or(inferred)?;
    let (glue, hive, rest) = match kind {
        CatalogKind::Glue => (Some(glue??), None, None),
        CatalogKind::Hive => (None, Some(hive??), None),
        CatalogKind::Rest => (None, None, Some(rest??)),
    };
    Some(Catalog {
        kind,
        glue,
        hive,
        rest,
        retry: Retry {
            max_attempts: max_attempts?,
            base_backoff: base_backoff?,
            max_backoff: max_backoff?,
        },
    })
}

fn hive(h: &RawHive, at: &At, db: Option<&str>, ctx: &mut Ctx) -> Option<Hive> {
    let url = req(ctx, &h.url, at, "url");
    let auth = or(ctx, &h.auth, at, "auth", HiveAuth::None);
    let creds = credentials(ctx, &h.user, &h.password, &h.credentials_file, at);
    let kerberos = h.kerberos.as_ref().map(|k| {
        let at = at.key("kerberos");
        let principal = req(ctx, &k.principal, &at, "principal");
        let keytab = k.keytab.as_ref().and_then(|l| {
            read_or_report(ctx, "V-069", &at.key("keytab"), l, read_file_ref).map(|(_, f)| f)
        });
        let service_name = or(
            ctx,
            &k.service_name,
            &at,
            "service_name",
            "hive".to_string(),
        );
        Some(Kerberos {
            principal: principal?,
            keytab: keytab?,
            service_name: service_name?,
        })
    });
    let tls = h.tls.clone().unwrap_or_default();
    let tls_at = at.key("tls");
    let enabled = or(ctx, &tls.enabled, &tls_at, "enabled", false);
    let ca = pem(ctx, &tls.ca, &tls_at, "ca", PemKind::Certificates);
    let database = opt(ctx, &h.database, at, "database");
    let auth = auth?;
    let (credentials, kerberos) = match auth {
        HiveAuth::None => (None, None),
        HiveAuth::Ldap => (Some(creds?), None),
        HiveAuth::Kerberos => (None, Some(kerberos??)),
    };
    Some(Hive {
        url: url?,
        auth,
        credentials,
        kerberos,
        tls: HiveTls {
            enabled: enabled?,
            ca: ca?,
        },
        database: database?.or_else(|| db.map(str::to_string))?,
    })
}

fn rest(r: &RawRest, at: &At, ctx: &mut Ctx) -> Option<Rest> {
    use crate::secret::Secret;
    let url = req(ctx, &r.url, at, "url");
    let warehouse = opt(ctx, &r.warehouse, at, "warehouse");
    let prefix = opt(ctx, &r.prefix, at, "prefix");
    let auth = or(ctx, &r.auth, at, "auth", RestAuth::None);
    let token = match (&r.token, &r.token_file) {
        (Some(t), None) => ctx.text(t, &at.key("token")).map(|t| BearerToken::Inline {
            token: Secret::new(t),
        }),
        (None, Some(f)) => read_or_report(ctx, "V-071", &at.key("token_file"), f, read_token_file)
            .map(|(path, token)| BearerToken::File { path, token }),
        _ => None,
    };
    let oauth2 = r.oauth2.as_ref().map(|o| {
        let at = at.key("oauth2");
        let id = o
            .client_id
            .as_ref()
            .and_then(|l| ctx.text(l, &at.key("client_id")));
        let secret = o
            .client_secret
            .as_ref()
            .and_then(|l| ctx.text(l, &at.key("client_secret")));
        let client = match (&o.credentials_file, &o.client_id, &o.client_secret) {
            (Some(f), None, None) => {
                read_or_report(ctx, "V-067", &at.key("credentials_file"), f, |p| {
                    read_json_pair(p, "client_id", "client_secret")
                })
                .map(|(path, (client_id, client_secret))| OauthClient::File {
                    path,
                    client_id,
                    client_secret,
                })
            }
            (None, Some(_), Some(_)) => Some(OauthClient::Inline {
                client_id: id?,
                client_secret: Secret::new(secret?),
            }),
            _ => None,
        };
        let token_url = req(ctx, &o.token_url, &at, "token_url");
        let scope = opt(ctx, &o.scope, &at, "scope");
        Some(RestOauth2 {
            client: client?,
            token_url: token_url?,
            scope: scope?,
        })
    });
    let sigv4 = r.sigv4.as_ref().map(|s| {
        let at = at.key("sigv4");
        let region = req(ctx, &s.region, &at, "region");
        let service = req(ctx, &s.service, &at, "service");
        Some(Sigv4 {
            region: region?,
            service: service?,
        })
    });
    let tls = r.tls.clone().unwrap_or_default();
    let tls_ca = pem(ctx, &tls.ca, &at.key("tls"), "ca", PemKind::Certificates);
    let auth = auth?;
    let (token, oauth2, sigv4) = match auth {
        RestAuth::None => (None, None, None),
        RestAuth::Bearer => (Some(token?), None, None),
        RestAuth::Oauth2 => (None, Some(oauth2??), None),
        RestAuth::Sigv4 => (None, None, Some(sigv4??)),
    };
    Some(Rest {
        url: url?,
        warehouse: warehouse?,
        prefix: prefix?,
        auth,
        token,
        oauth2,
        sigv4,
        tls_ca: tls_ca?,
    })
}

fn delta(d: &RawDelta, at: &At, ctx: &mut Ctx) -> Option<Delta> {
    let column_mapping = or(
        ctx,
        &d.column_mapping,
        at,
        "column_mapping",
        ColumnMapping::None,
    );
    let checkpoint_interval = or(ctx, &d.checkpoint_interval, at, "checkpoint_interval", 10);
    let log_retention = or(
        ctx,
        &d.log_retention,
        at,
        "log_retention",
        Duration::from_secs(30 * 86_400),
    );
    let timestamp_ntz_as = or(
        ctx,
        &d.timestamp_ntz_as,
        at,
        "timestamp_ntz_as",
        TimestampNtzAs::TimestampNtz,
    );
    let table_properties = map(ctx, &d.table_properties, at, "table_properties");
    let catalog = d.catalog.clone().unwrap_or_default();
    let catalog_type = or(
        ctx,
        &catalog.kind,
        &at.key("catalog"),
        "type",
        DeltaCatalogType::None,
    );
    Some(Delta {
        column_mapping: column_mapping?,
        checkpoint_interval: checkpoint_interval?,
        log_retention: log_retention?,
        timestamp_ntz_as: timestamp_ntz_as?,
        table_properties: table_properties?,
        catalog_type: catalog_type?,
    })
}

/// Every pipeline merged with `defaults` (§8), in name order.
pub fn merge_all(raw: &RawConfig, ctx: &Ctx) -> Vec<(String, RawPipeline)> {
    raw.pipelines
        .iter()
        .flatten()
        .map(|(name, p)| {
            (
                name.clone(),
                crate::merge::merge_pipeline(raw.defaults.as_ref(), p, ctx),
            )
        })
        .collect()
}

/// Builds the resolved config. Visits every leaf in both passes; returns `None` in the structural
/// pass, and in the resolving pass when anything is missing or invalid.
pub fn build(
    raw: &RawConfig,
    merged: &[(String, RawPipeline)],
    ctx: &mut Ctx,
) -> Option<ResolvedConfig> {
    let version_at = At::config(YamlPath::root());
    let config_version = req(ctx, &raw.config_version, &version_at, "config_version");
    let globals = raw.globals.as_ref().map(|g| globals(g, ctx));
    let parent = raw
        .defaults
        .as_ref()
        .and_then(|d| d.state.as_ref())
        .and_then(|s| s.prefix.as_ref());
    let pipelines: Vec<Option<Pipeline>> = merged
        .iter()
        .map(|(name, p)| pipeline(name, p, parent, ctx))
        .collect();
    raw.pipelines.as_ref()?;
    let mut by_id = BTreeMap::new();
    for p in pipelines {
        let p = p?;
        by_id.insert(p.id.clone(), p);
    }
    if !ctx.is_resolving() {
        return None;
    }
    Some(ResolvedConfig {
        config_version: config_version?,
        globals: globals??,
        pipelines: by_id,
    })
}
```

Create `crates/data-avalanche-config/src/load.rs`:

```rust
//! The load pipeline (config §11): parse → structural pass → resolving pass → resolved config.
//! The same pipeline serves startup, reload and the validation endpoint.

use std::path::Path;

use crate::build::{build, merge_all};
use crate::ctx::Ctx;
use crate::diag::{Diagnostic, Locus, PARSE, Report, Severity};
use crate::parse::parse_document;
use crate::raw::RawConfig;
use crate::resolved::ResolvedConfig;
use crate::template::Env;

/// A config that passed every phase. `warnings` never block loading (§1.3).
#[derive(Debug, Clone)]
pub struct Loaded {
    pub config: ResolvedConfig,
    pub warnings: Vec<Diagnostic>,
}

fn only(d: Diagnostic) -> Report {
    let mut r = Report::default();
    r.push(d);
    r
}

/// Loads config text. Any error rejects the whole config (§1.4) and the report lists every error
/// of the phase that failed (the first one only for a Phase 0 parse error).
pub fn load_str(text: &str, env: &dyn Env) -> Result<Loaded, Report> {
    let raw: RawConfig = parse_document(text).map_err(|d| only(*d))?;

    // Phase 1: references unresolved; no environment, no files.
    let mut ctx = Ctx::structural();
    let merged = merge_all(&raw, &ctx);
    build(&raw, &merged, &mut ctx);
    let structural = ctx.into_report();
    if !structural.is_ok() {
        return Err(structural);
    }

    // Phases 2 and 3: substitute, read files, apply defaults, check everything again with the
    // now-known values (a rule skipped on a template in Phase 1 runs here).
    let mut ctx = Ctx::resolving(env);
    let merged = merge_all(&raw, &ctx);
    let config = build(&raw, &merged, &mut ctx);
    let mut report = ctx.into_report();
    report.extend(structural);
    match config {
        Some(config) if report.is_ok() => Ok(Loaded {
            config,
            warnings: report.warnings,
        }),
        None if report.is_ok() => {
            // Every missing value must be reported by a rule; reaching here is a bug.
            report.push(Diagnostic {
                locus: Locus::Config,
                path: String::new(),
                rule: PARSE.into(),
                severity: Severity::Error,
                loc: None,
                message: "internal: the config could not be resolved but no rule reported why"
                    .into(),
            });
            Err(report)
        }
        _ => Err(report),
    }
}

/// Loads the config file at `path` (§2: the path comes from the command line).
pub fn load_file(path: &Path, env: &dyn Env) -> Result<Loaded, Report> {
    let text = std::fs::read_to_string(path).map_err(|e| {
        only(Diagnostic {
            locus: Locus::Config,
            path: String::new(),
            rule: PARSE.into(),
            severity: Severity::Error,
            loc: None,
            message: format!("cannot read config file {}: {e}", path.display()),
        })
    })?;
    load_str(&text, env)
}

/// The validation endpoint's dry run (§11): the full pipeline, returning only the report. The
/// resolved config (it holds secrets) is never part of it (§11.1).
pub fn validate_str(text: &str, env: &dyn Env) -> Report {
    match load_str(text, env) {
        Ok(loaded) => Report {
            errors: Vec::new(),
            warnings: loaded.warnings,
        },
        Err(report) => report,
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 28; `tests/load.rs` 12; `tests/merge.rs` 8; `tests/parse.rs` 8; `tests/raw.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/tests/load.rs \
  crates/data-avalanche-config/tests/support/mod.rs \
  crates/data-avalanche-config/src/build.rs \
  crates/data-avalanche-config/src/load.rs
git commit -m "feat(config): resolved-config builder and the load pipeline"
```


---

### Task 10: Rules I — structure, requiredness, identity; the rule harness

**Files:**
- Create: `crates/data-avalanche-config/src/rules/identity.rs`, `crates/data-avalanche-config/src/rules/mod.rs`, `crates/data-avalanche-config/src/rules/structure.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`, `crates/data-avalanche-config/src/load.rs`
- Test: `crates/data-avalanche-config/tests/rules/cases.rs`, `crates/data-avalanche-config/tests/rules/identity.rs`, `crates/data-avalanche-config/tests/rules/main.rs`, `crates/data-avalanche-config/tests/rules/resolution.rs`, `crates/data-avalanche-config/tests/rules/structure.rs`

**Interfaces:**
- Consumes: Tasks 2–9.
- Produces:
  - `rules::check(&RawConfig, &merged, &mut Ctx)`, called by `load_str` before `build` in both
    passes.
  - The helpers `rules::pipeline_at(name) -> At` and `rules::missing(ctx, leaf, at) -> bool`
    (absent, or known to be empty).
  - The rule harness in `tests/rules/`:
    - `case(rule, Expect::{Error, Warning, Silent}, name, || json_doc)` and `case_with(…)` for
      cases that need files or env;
    - `pg_delta()`, `mysql_delta()`, `pg_iceberg()` base configs;
    - the `Edit` trait: `.set("/json/pointer", value)`, `.del(pointer)`.

**Rules added:**
- structure and requiredness (§13.1–13.2): V-001…V-014, V-065, V-066, V-070, V-084, V-091. V-001
  and V-084 require only the `postgres` / `iceberg` block (Q-M1-4);
- identity (§13.7): V-055…V-060.

The harness also carries cases for the Phase 2 rules produced by Task 9: V-063, V-067, V-068, V-069,
V-071 and V-078. The `PENDING` list names the rules later tasks add. The harness asserts that
**every other config-time rule has a passing and a failing case**, and that no pending rule has
cases yet. Tasks 11 and 12 empty the list.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 5037b22..deb87a2 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -16,6 +16,7 @@ pub mod path;
 pub mod raw;
 pub mod registry;
 pub mod resolved;
+pub mod rules;
 pub mod secret;
 pub mod template;
 pub mod units;
```

Create `crates/data-avalanche-config/src/rules/identity.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn charsets() {
        assert!(
            is_pipeline_id("orders_pg-1")
                && !is_pipeline_id("Orders")
                && !is_pipeline_id("-x")
                && !is_pipeline_id(&"a".repeat(64))
        );
        assert!(
            is_pipeline_name("orders-pg.v2") && !is_pipeline_name(".x") && !is_pipeline_name("a b")
        );
        assert!(
            is_slot_name("_orders_slot") && !is_slot_name("orders-slot") && !is_slot_name("9x")
        );
        assert!(is_pg_identifier("Orders_Pub$1") && !is_pg_identifier("orders-pub"));
    }
}
```

Create `crates/data-avalanche-config/tests/rules/cases.rs`:

```rust
//! Case plumbing: base configs as JSON values, edits by JSON pointer, and the case type.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::support::Files;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// The rule reports an error.
    Error,
    /// The rule reports a warning.
    Warning,
    /// The rule reports nothing.
    Silent,
}

pub struct Fixture {
    pub doc: Value,
    pub env: BTreeMap<String, String>,
}

pub struct Case {
    pub rule: &'static str,
    pub expect: Expect,
    pub name: &'static str,
    pub build: Box<dyn Fn(&Files) -> Fixture>,
}

/// A case whose config needs no files and only the base environment.
pub fn case(
    rule: &'static str,
    expect: Expect,
    name: &'static str,
    doc: impl Fn() -> Value + 'static,
) -> Case {
    Case {
        rule,
        expect,
        name,
        build: Box::new(move |_| Fixture {
            doc: doc(),
            env: base_env(),
        }),
    }
}

/// A case that writes files or sets variables.
pub fn case_with(
    rule: &'static str,
    expect: Expect,
    name: &'static str,
    build: impl Fn(&Files) -> Fixture + 'static,
) -> Case {
    Case {
        rule,
        expect,
        name,
        build: Box::new(build),
    }
}

pub fn base_env() -> BTreeMap<String, String> {
    crate::support::env(&[("PW", "pw")])
}

/// PostgreSQL → Delta, pipeline `p` (id `p`).
pub fn pg_delta() -> Value {
    json!({
        "config_version": 1,
        "globals": { "state": { "bucket": "app-state" } },
        "pipelines": {
            "p": {
                "id": "p",
                "source": {
                    "host": "db.example.com", "db": "shop", "user": "cdc", "password": "${PW}",
                    "postgres": { "publication": "p_pub", "slot": "p_slot" }
                },
                "tables": { "include": ["public.orders"] },
                "sink": { "type": "delta", "bucket": "lake", "delta": {} }
            }
        }
    })
}

/// MySQL → Delta, pipeline `p`.
pub fn mysql_delta() -> Value {
    pg_delta()
        .set("/pipelines/p/source/type", json!("mysql"))
        .del("/pipelines/p/source/postgres")
        .set("/pipelines/p/source/mysql", json!({}))
        .set("/pipelines/p/tables/include", json!(["orders"]))
}

/// PostgreSQL → Iceberg with a REST catalog (no auth), pipeline `p`.
pub fn pg_iceberg() -> Value {
    pg_delta().set(
        "/pipelines/p/sink",
        json!({
            "type": "iceberg", "bucket": "lake",
            "iceberg": { "catalog": { "rest": { "url": "https://catalog.example.com" } } }
        }),
    )
}

pub fn bases() -> Vec<(&'static str, Value)> {
    vec![
        ("pg_delta", pg_delta()),
        ("mysql_delta", mysql_delta()),
        ("pg_iceberg", pg_iceberg()),
    ]
}

/// Edits by JSON pointer (`/pipelines/p/sink/type`). `set` creates missing objects on the way.
pub trait Edit: Sized {
    fn set(self, pointer: &str, value: Value) -> Self;
    fn del(self, pointer: &str) -> Self;
}

impl Edit for Value {
    fn set(mut self, pointer: &str, value: Value) -> Self {
        let mut node = &mut self;
        let keys: Vec<&str> = pointer.trim_start_matches('/').split('/').collect();
        let (last, parents) = keys.split_last().expect("non-empty pointer");
        for key in parents {
            let map = node.as_object_mut().expect("object on the path");
            node = map.entry(key.to_string()).or_insert_with(|| json!({}));
        }
        node.as_object_mut()
            .expect("object at the parent")
            .insert(last.to_string(), value);
        self
    }

    fn del(mut self, pointer: &str) -> Self {
        let (parent, key) = pointer.rsplit_once('/').expect("pointer with a parent");
        if let Some(Value::Object(map)) = self.pointer_mut(parent) {
            map.remove(key);
        }
        self
    }
}
```

Create `crates/data-avalanche-config/tests/rules/identity.rs`:

```rust
//! §13.7 identity and naming.

use serde_json::{Value, json};

use crate::cases::{Case, Edit, Expect::*, case, pg_delta};

/// The base config with a second pipeline `q` copied from `p`, then given its own id, slot and
/// publication so the two don't collide.
fn two_pipelines() -> Value {
    let doc = pg_delta();
    let q = doc.pointer("/pipelines/p").unwrap().clone();
    doc.set("/pipelines/q", q)
        .set("/pipelines/q/id", json!("q"))
        .set("/pipelines/q/source/postgres/slot", json!("q_slot"))
        .set("/pipelines/q/source/postgres/publication", json!("q_pub"))
}

pub fn cases() -> Vec<Case> {
    vec![
        case("V-055", Error, "missing id", || {
            pg_delta().del("/pipelines/p/id")
        }),
        case("V-055", Error, "upper-case id", || {
            pg_delta().set("/pipelines/p/id", json!("Orders"))
        }),
        case("V-055", Error, "64 characters", || {
            pg_delta().set("/pipelines/p/id", json!("a".repeat(64)))
        }),
        case("V-055", Silent, "63 characters", || {
            pg_delta().set("/pipelines/p/id", json!("a".repeat(63)))
        }),
        case("V-056", Error, "duplicate id", || {
            two_pipelines().set("/pipelines/q/id", json!("p"))
        }),
        case("V-056", Silent, "distinct ids", two_pipelines),
        case("V-057", Error, "name with a space", || {
            let p = pg_delta().pointer("/pipelines/p").unwrap().clone();
            pg_delta().set("/pipelines", json!({ "bad name": p }))
        }),
        case("V-057", Silent, "name with dots and dashes", || {
            let p = pg_delta().pointer("/pipelines/p").unwrap().clone();
            pg_delta().set("/pipelines", json!({ "orders-pg.v2": p }))
        }),
        case("V-058", Error, "slot with a dash", || {
            pg_delta().set("/pipelines/p/source/postgres/slot", json!("p-slot"))
        }),
        case("V-058", Error, "slot shared on one host+db", || {
            two_pipelines().set("/pipelines/q/source/postgres/slot", json!("p_slot"))
        }),
        case("V-058", Silent, "same slot name on another db", || {
            two_pipelines()
                .set("/pipelines/q/source/postgres/slot", json!("p_slot"))
                .set("/pipelines/q/source/db", json!("other"))
        }),
        case("V-059", Warning, "publication needs quoting", || {
            pg_delta().set("/pipelines/p/source/postgres/publication", json!("p-pub"))
        }),
        case("V-059", Warning, "publication shared", || {
            two_pipelines().set("/pipelines/q/source/postgres/publication", json!("p_pub"))
        }),
        case("V-059", Silent, "own publication", two_pipelines),
        case("V-060", Error, "slot in defaults", || {
            pg_delta().set("/defaults/source/postgres/slot", json!("s"))
        }),
        case("V-060", Error, "id in defaults", || {
            pg_delta().set("/defaults/id", json!("x"))
        }),
        case("V-060", Error, "snapshot overrides in defaults", || {
            pg_delta().set(
                "/defaults/tables/snapshots/overrides",
                json!({"public.a": "always"}),
            )
        }),
        case("V-060", Silent, "shared settings in defaults", || {
            pg_delta().set("/defaults/source/postgres/heartbeat", json!("30s"))
        }),
    ]
}
```

Create `crates/data-avalanche-config/tests/rules/main.rs`:

```rust
//! Every config-time V-rule, one passing and one failing case at least (M1 exit criterion). Cases
//! are small edits of valid base configs; a config is a JSON value, which is valid YAML 1.2.

#[path = "../support/mod.rs"]
mod support;

mod cases;
mod identity;
mod resolution;
mod structure;

use std::collections::BTreeSet;

use cases::{Case, Expect};
use data_avalanche_config::{RULES, RuleInput, Severity, validate_str};

fn all_cases() -> Vec<Case> {
    let mut all = Vec::new();
    all.extend(structure::cases());
    all.extend(identity::cases());
    all.extend(resolution::cases());
    all
}

/// Config-time rules whose cases are not written yet. Each rule task removes its IDs; the last one
/// leaves this empty.
const PENDING: &[&str] = &[
    "V-015", "V-016", "V-017", "V-018", "V-019", "V-020", "V-021", "V-022", "V-023", "V-025",
    "V-026", "V-027", "V-028", "V-029", "V-030", "V-033", "V-035", "V-036", "V-037", "V-038",
    "V-040", "V-041", "V-042", "V-043", "V-044", "V-045", "V-046", "V-047", "V-048", "V-049",
    "V-050", "V-051", "V-052", "V-053", "V-054", "V-064", "V-072", "V-073", "V-074", "V-075",
    "V-077", "V-080", "V-081", "V-082", "V-083", "V-085", "V-086", "V-087", "V-088", "V-089",
    "V-092", "V-093", "V-095", "V-096", "V-097", "V-098", "V-099", "V-100", "V-101", "V-102",
    "V-103", "V-104", "V-105", "V-106", "V-107", "V-108", "V-109", "V-110", "V-111", "V-112",
    "V-114", "V-115", "V-116", "V-117",
];

/// Rules with no failing input in v1. V-087: every logical type of the grammar maps to both sinks
/// (Delta turns `time`/`uuid` into strings), so nothing can fail it until a sink loses a type.
const NO_FAILING_INPUT: &[&str] = &["V-087"];

#[test]
fn base_configs_are_clean() {
    for (name, doc) in cases::bases() {
        let report = validate_str(&doc.to_string(), &cases::base_env());
        assert!(
            report.errors.is_empty() && report.warnings.is_empty(),
            "{name}: {report:#?}"
        );
    }
}

#[test]
fn rule_cases() {
    let mut failures = Vec::new();
    for case in all_cases() {
        let files = support::Files::new();
        let fixture = (case.build)(&files);
        let report = validate_str(&fixture.doc.to_string(), &fixture.env);
        let hits: Vec<_> = report.iter().filter(|d| d.rule == case.rule).collect();
        let ok = match case.expect {
            Expect::Error => hits.iter().any(|d| d.severity == Severity::Error),
            Expect::Warning => hits.iter().any(|d| d.severity == Severity::Warning),
            Expect::Silent => hits.is_empty(),
        };
        let internal = report.iter().any(|d| d.message.starts_with("internal:"));
        if !ok || internal {
            failures.push(format!(
                "{} {:?} ({}): got {:#?}",
                case.rule,
                case.expect,
                case.name,
                report
                    .iter()
                    .map(|d| format!("{} {:?} {} {}", d.rule, d.severity, d.path, d.message))
                    .collect::<Vec<_>>()
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "{} failing case(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn every_config_rule_has_a_passing_and_a_failing_case() {
    let cases = all_cases();
    let with = |id: &str, pass: bool| {
        cases
            .iter()
            .any(|c| c.rule == id && (c.expect == Expect::Silent) == pass)
    };
    let pending: BTreeSet<&str> = PENDING.iter().copied().collect();
    let mut missing = Vec::new();
    for rule in RULES {
        if !matches!(rule.input, RuleInput::Config | RuleInput::Resolution)
            || pending.contains(rule.id)
        {
            continue;
        }
        if !with(rule.id, true) {
            missing.push(format!("{}: no passing case", rule.id));
        }
        if !with(rule.id, false) && !NO_FAILING_INPUT.contains(&rule.id) {
            missing.push(format!("{}: no failing case", rule.id));
        }
    }
    for id in &pending {
        assert!(!with(id, false), "{id} has cases; remove it from PENDING");
    }
    assert!(missing.is_empty(), "{}", missing.join("\n"));
}
```

Create `crates/data-avalanche-config/tests/rules/resolution.rs`:

```rust
//! §13.9 resolution and materialization (Phase 2): references, credential and token files, PEM,
//! other files, and the logical-type grammar.

use serde_json::json;

use crate::cases::{
    Case, Edit, Expect::*, Fixture, base_env, case, case_with, pg_delta, pg_iceberg,
};
use crate::support::{cert, key};

pub fn cases() -> Vec<Case> {
    vec![
        case("V-063", Error, "unset variable", || {
            pg_delta().set("/pipelines/p/source/password", json!("${NOT_SET}"))
        }),
        case("V-063", Error, "malformed reference", || {
            pg_delta().set("/pipelines/p/source/host", json!("db-${OPEN"))
        }),
        case("V-063", Silent, "default for an unset variable", || {
            pg_delta().set("/pipelines/p/source/host", json!("${NOT_SET:-db}"))
        }),
        case_with(
            "V-067",
            Error,
            "credentials file without password",
            |files| {
                let path = files.write("creds.json", r#"{"user": "cdc"}"#);
                Fixture {
                    doc: pg_delta()
                        .del("/pipelines/p/source/user")
                        .del("/pipelines/p/source/password")
                        .set("/pipelines/p/source/credentials_file", json!(path)),
                    env: base_env(),
                }
            },
        ),
        case_with(
            "V-067",
            Silent,
            "valid credentials file via a variable",
            |files| {
                let path = files.write("creds.json", r#"{"user": "cdc", "password": "pw"}"#);
                let mut env = base_env();
                env.insert("CREDS".into(), path.display().to_string());
                Fixture {
                    doc: pg_delta()
                        .del("/pipelines/p/source/user")
                        .del("/pipelines/p/source/password")
                        .set("/pipelines/p/source/credentials_file", json!("${CREDS}")),
                    env,
                }
            },
        ),
        case_with("V-068", Error, "key given as cert", |files| {
            let key = files.write("key.pem", key());
            Fixture {
                doc: pg_delta().set("/pipelines/p/source/ssl_cert", json!(key)),
                env: base_env(),
            }
        }),
        case("V-068", Silent, "inline certificate", || {
            pg_delta().set("/pipelines/p/source/ssl_ca", json!(cert()))
        }),
        case_with("V-069", Error, "empty keytab", |files| {
            let keytab = files.write("cdc.keytab", "");
            Fixture {
                doc: pg_iceberg().set(
                    "/pipelines/p/sink/iceberg/catalog",
                    json!({"hive": {"url": "thrift://hms:9083", "auth": "kerberos", "kerberos": {"principal": "cdc", "keytab": keytab}}}),
                ),
                env: base_env(),
            }
        }),
        case_with("V-069", Silent, "readable keytab", |files| {
            let keytab = files.write("cdc.keytab", "\u{5}\u{2}binary");
            Fixture {
                doc: pg_iceberg().set(
                    "/pipelines/p/sink/iceberg/catalog",
                    json!({"hive": {"url": "thrift://hms:9083", "auth": "kerberos", "kerberos": {"principal": "cdc", "keytab": keytab}}}),
                ),
                env: base_env(),
            }
        }),
        case_with("V-071", Error, "whitespace-only token file", |files| {
            let token = files.write("token", " \n");
            Fixture {
                doc: pg_iceberg()
                    .set(
                        "/pipelines/p/sink/iceberg/catalog/rest/auth",
                        json!("bearer"),
                    )
                    .set(
                        "/pipelines/p/sink/iceberg/catalog/rest/token_file",
                        json!(token),
                    ),
                env: base_env(),
            }
        }),
        case_with(
            "V-071",
            Silent,
            "token file with a trailing newline",
            |files| {
                let token = files.write("token", "tok\n");
                Fixture {
                    doc: pg_iceberg()
                        .set(
                            "/pipelines/p/sink/iceberg/catalog/rest/auth",
                            json!("bearer"),
                        )
                        .set(
                            "/pipelines/p/sink/iceberg/catalog/rest/token_file",
                            json!(token),
                        ),
                    env: base_env(),
                }
            },
        ),
        case("V-078", Error, "bare timestamp", || {
            pg_delta().set(
                "/pipelines/p/tables/column_type_overrides",
                json!({"public.orders.ts": "timestamp"}),
            )
        }),
        case("V-078", Error, "decimal beyond 38 digits", || {
            pg_delta().set(
                "/pipelines/p/tables/column_type_overrides",
                json!({"public.orders.amount": "decimal(40,2)"}),
            )
        }),
        case("V-078", Silent, "grammar types", || {
            pg_delta().set(
                "/pipelines/p/tables/column_type_overrides",
                json!({"public.orders.amount": "decimal(38,10)", "public.orders.tags": "list<string>", "public.orders.at": "timestamp_ntz"}),
            )
        }),
    ]
}
```

Create `crates/data-avalanche-config/tests/rules/structure.rs`:

```rust
//! §13.1 structure and mutual exclusion, §13.2 requiredness.

use serde_json::json;

use crate::cases::{Case, Edit, Expect::*, case, mysql_delta, pg_delta, pg_iceberg};

pub fn cases() -> Vec<Case> {
    vec![
        // V-001: the engine block matching source.type
        case("V-001", Error, "postgres without a postgres block", || {
            pg_delta().del("/pipelines/p/source/postgres")
        }),
        case(
            "V-001",
            Silent,
            "mysql block omitted: every field has a default",
            || mysql_delta().del("/pipelines/p/source/mysql"),
        ),
        case(
            "V-001",
            Silent,
            "engine block inherited from defaults",
            || {
                // The merged block lacks publication/slot (V-011), but it exists.
                pg_delta()
                    .set("/defaults/source/postgres", json!({"heartbeat": "30s"}))
                    .del("/pipelines/p/source/postgres")
            },
        ),
        // V-002: exactly one catalog sub-block, matching `type`
        case("V-002", Error, "no sub-block", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog",
                json!({"retry": {"max_attempts": 3}}),
            )
        }),
        case("V-002", Error, "two sub-blocks", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/glue",
                json!({"name": "a"}),
            )
        }),
        case("V-002", Error, "type contradicts block", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/type", json!("glue"))
        }),
        case("V-002", Silent, "type matches block", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/type", json!("rest"))
        }),
        // V-003: foreign engine block
        case("V-003", Error, "mysql block on a postgres source", || {
            pg_delta().set("/pipelines/p/source/mysql", json!({}))
        }),
        case(
            "V-003",
            Silent,
            "defaults' foreign block is dropped",
            || {
                pg_delta().set(
                    "/defaults/source/mysql",
                    json!({"replication_type": "gtid"}),
                )
            },
        ),
        // V-004, V-005
        case("V-004", Error, "no pipelines key", || {
            pg_delta().del("/pipelines")
        }),
        case("V-004", Silent, "empty pipelines map", || {
            pg_delta().set("/pipelines", json!({}))
        }),
        case("V-005", Error, "no globals", || pg_delta().del("/globals")),
        case("V-005", Silent, "globals present", pg_delta),
        // V-006
        case("V-006", Error, "no state bucket", || {
            pg_delta().set("/globals/state", json!({"prefix": "x"}))
        }),
        case("V-006", Error, "empty state bucket", || {
            pg_delta().set("/globals/state/bucket", json!(""))
        }),
        case("V-006", Silent, "state bucket set", pg_delta),
        // V-007
        case("V-007", Error, "no include", || {
            pg_delta().del("/pipelines/p/tables/include")
        }),
        case("V-007", Error, "empty include", || {
            pg_delta().set("/pipelines/p/tables/include", json!([]))
        }),
        case("V-007", Silent, "include from defaults", || {
            pg_delta()
                .set("/defaults/tables/include", json!(["public.*"]))
                .del("/pipelines/p/tables/include")
        }),
        // V-008, V-009, V-010
        case("V-008", Error, "no host", || {
            pg_delta().del("/pipelines/p/source/host")
        }),
        case("V-008", Silent, "host from defaults", || {
            pg_delta()
                .set("/defaults/source/host", json!("db"))
                .del("/pipelines/p/source/host")
        }),
        case("V-009", Error, "no db", || {
            pg_delta().del("/pipelines/p/source/db")
        }),
        case("V-009", Silent, "db set", pg_delta),
        case("V-010", Error, "user without password", || {
            pg_delta().del("/pipelines/p/source/password")
        }),
        case("V-010", Error, "password resolves empty", || {
            pg_delta().set("/pipelines/p/source/password", json!("${EMPTY:-}"))
        }),
        case("V-010", Silent, "credentials_file instead", || {
            pg_delta()
                .del("/pipelines/p/source/user")
                .del("/pipelines/p/source/password")
                .set(
                    "/pipelines/p/source/credentials_file",
                    json!("${NO_SUCH_FILE:-/x}"),
                )
        }),
        // V-011
        case("V-011", Error, "no slot", || {
            pg_delta().del("/pipelines/p/source/postgres/slot")
        }),
        case("V-011", Silent, "publication and slot", pg_delta),
        // V-012, V-013, V-091
        case("V-012", Error, "no sink bucket", || {
            pg_delta().del("/pipelines/p/sink/bucket")
        }),
        case("V-012", Silent, "bucket from defaults", || {
            pg_delta()
                .set("/defaults/sink/bucket", json!("lake"))
                .del("/pipelines/p/sink/bucket")
        }),
        case("V-013", Error, "iceberg without catalog", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg", json!({"strategy": "mor"}))
        }),
        case("V-013", Silent, "catalog from defaults", || {
            pg_iceberg()
                .set(
                    "/defaults/sink/iceberg/catalog",
                    json!({"glue": {"name": "123"}}),
                )
                .set("/pipelines/p/sink/iceberg", json!({}))
        }),
        case("V-091", Error, "no sink type", || {
            pg_delta().del("/pipelines/p/sink/type")
        }),
        case("V-091", Silent, "type from defaults", || {
            pg_delta()
                .set("/defaults/sink/type", json!("delta"))
                .del("/pipelines/p/sink/type")
        }),
        // V-014
        case("V-014", Error, "missing", || {
            pg_delta().del("/config_version")
        }),
        case("V-014", Error, "unsupported version", || {
            pg_delta().set("/config_version", json!(2))
        }),
        case("V-014", Silent, "version 1", pg_delta),
        // V-084
        case(
            "V-084",
            Silent,
            "delta block omitted: every field has a default",
            || pg_delta().del("/pipelines/p/sink/delta"),
        ),
        case("V-084", Error, "iceberg without iceberg block", || {
            pg_iceberg().del("/pipelines/p/sink/iceberg")
        }),
        case("V-084", Error, "iceberg block on a delta sink", || {
            pg_delta().set("/pipelines/p/sink/iceberg", json!({}))
        }),
        case(
            "V-084",
            Silent,
            "defaults' iceberg dropped for delta",
            || pg_delta().set("/defaults/sink/iceberg/strategy", json!("cow")),
        ),
        // V-065, V-066, V-070
        case("V-065", Error, "source credentials_file with user", || {
            pg_delta()
                .del("/pipelines/p/source/password")
                .set("/pipelines/p/source/credentials_file", json!("/x"))
        }),
        case("V-065", Error, "api credentials_file with password", || {
            pg_delta().set(
                "/globals/api",
                json!({"auth": "basic", "password": "x", "credentials_file": "/x"}),
            )
        }),
        case("V-065", Silent, "user and password only", pg_delta),
        case(
            "V-066",
            Error,
            "oauth2 credentials_file with client_id",
            || {
                pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/rest/oauth2",
                json!({"client_id": "a", "credentials_file": "/x", "token_url": "https://idp/t"}),
            )
            },
        ),
        case("V-066", Silent, "no oauth2 block", pg_iceberg),
        case("V-070", Error, "token and token_file", || {
            pg_iceberg()
                .set("/pipelines/p/sink/iceberg/catalog/rest/token", json!("t"))
                .set(
                    "/pipelines/p/sink/iceberg/catalog/rest/token_file",
                    json!("/x"),
                )
        }),
        case("V-070", Silent, "token only", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/rest/token", json!("t"))
        }),
    ]
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0583]: file not found for module `rules`
```

- [ ] **Step 3: Implement**

Change `crates/data-avalanche-config/src/load.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/load.rs b/crates/data-avalanche-config/src/load.rs
index 6f34dc5..6475305 100644
--- a/crates/data-avalanche-config/src/load.rs
+++ b/crates/data-avalanche-config/src/load.rs
@@ -9,6 +9,7 @@ use crate::diag::{Diagnostic, Locus, PARSE, Report, Severity};
 use crate::parse::parse_document;
 use crate::raw::RawConfig;
 use crate::resolved::ResolvedConfig;
+use crate::rules;
 use crate::template::Env;
 
 /// A config that passed every phase. `warnings` never block loading (§1.3).
@@ -32,6 +33,7 @@ pub fn load_str(text: &str, env: &dyn Env) -> Result<Loaded, Report> {
     // Phase 1: references unresolved; no environment, no files.
     let mut ctx = Ctx::structural();
     let merged = merge_all(&raw, &ctx);
+    rules::check(&raw, &merged, &mut ctx);
     build(&raw, &merged, &mut ctx);
     let structural = ctx.into_report();
     if !structural.is_ok() {
@@ -42,6 +44,7 @@ pub fn load_str(text: &str, env: &dyn Env) -> Result<Loaded, Report> {
     // now-known values (a rule skipped on a template in Phase 1 runs here).
     let mut ctx = Ctx::resolving(env);
     let merged = merge_all(&raw, &ctx);
+    rules::check(&raw, &merged, &mut ctx);
     let config = build(&raw, &merged, &mut ctx);
     let mut report = ctx.into_report();
     report.extend(structural);
```

In `crates/data-avalanche-config/src/rules/identity.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! §13.7 identity and naming.

use std::collections::BTreeMap;

use super::pipeline_at;
use crate::ctx::{At, Ctx};
use crate::enums::SourceType;
use crate::path::YamlPath;
use crate::raw::{RawConfig, RawPipeline};

/// `^[a-z0-9][a-z0-9_-]{0,62}$` (V-055).
fn is_pipeline_id(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'_' | b'-'))
}

/// `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$` (V-057).
fn is_pipeline_name(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=128).contains(&b.len())
        && b[0].is_ascii_alphanumeric()
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
}

/// `^[a-z_][a-z0-9_]{0,62}$` (V-058, the PostgreSQL slot-name rule).
fn is_slot_name(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && (b[0].is_ascii_lowercase() || b[0] == b'_')
        && b.iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

/// An unquoted PostgreSQL identifier, `^[A-Za-z_][A-Za-z0-9_$]{0,62}$` (V-059).
fn is_pg_identifier(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=63).contains(&b.len())
        && (b[0].is_ascii_alphabetic() || b[0] == b'_')
        && b.iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'$'))
}

pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    // V-060: per-pipeline unique names can't be defaulted.
    if let Some(d) = &raw.defaults {
        let at = At::config(YamlPath::root().key("defaults"));
        let pg = d.source.as_ref().and_then(|s| s.postgres.as_ref());
        let forbidden = [
            (
                "id",
                d.id.as_ref().and_then(|l| l.loc()),
                at.key("id"),
                d.id.is_some(),
            ),
            (
                "source.postgres.publication",
                pg.and_then(|p| p.publication.as_ref())
                    .and_then(|l| l.loc()),
                at.key("source").key("postgres").key("publication"),
                pg.is_some_and(|p| p.publication.is_some()),
            ),
            (
                "source.postgres.slot",
                pg.and_then(|p| p.slot.as_ref()).and_then(|l| l.loc()),
                at.key("source").key("postgres").key("slot"),
                pg.is_some_and(|p| p.slot.is_some()),
            ),
            (
                "tables.snapshots.overrides",
                None,
                at.key("tables").key("snapshots").key("overrides"),
                d.tables
                    .as_ref()
                    .and_then(|t| t.snapshots.as_ref())
                    .is_some_and(|s| s.overrides.is_some()),
            ),
        ];
        for (field, loc, field_at, set) in forbidden {
            if set {
                ctx.error(
                    "V-060",
                    &field_at,
                    loc,
                    format!("{field} is per pipeline and must not be set in defaults"),
                );
            }
        }
    }

    let mut ids: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    let mut slots: BTreeMap<(String, String, String), Vec<&str>> = BTreeMap::new();
    let mut publications: BTreeMap<(String, String, String), Vec<&str>> = BTreeMap::new();
    for (name, p) in merged {
        let at = pipeline_at(name);
        // V-057
        if !is_pipeline_name(name) {
            ctx.error(
                "V-057",
                &at,
                None,
                format!("pipeline name {name:?} must match ^[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}$"),
            );
        }
        // V-055
        match &p.id {
            None => ctx.error("V-055", &at.key("id"), None, "pipeline id is required"),
            Some(l) => {
                if let Some(id) = ctx.text(l, &at.key("id")) {
                    if is_pipeline_id(&id) {
                        ids.entry(id).or_default().push(name);
                    } else {
                        ctx.error(
                            "V-055",
                            &at.key("id"),
                            l.loc(),
                            format!("pipeline id {id:?} must match ^[a-z0-9][a-z0-9_-]{{0,62}}$"),
                        );
                    }
                }
            }
        }
        // V-058, V-059
        let Some(src) = &p.source else { continue };
        let is_pg = src.kind.is_none() || ctx.peek(&src.kind) == Some(SourceType::Postgres);
        let Some(pg) = src.postgres.as_ref().filter(|_| is_pg) else {
            continue;
        };
        let pg_at = at.key("source").key("postgres");
        let host = src
            .host
            .as_ref()
            .and_then(|l| ctx.text(l, &at.key("source").key("host")));
        let db = src
            .db
            .as_ref()
            .and_then(|l| ctx.text(l, &at.key("source").key("db")));
        if let Some(l) = &pg.slot
            && let Some(slot) = ctx.text(l, &pg_at.key("slot"))
        {
            if !is_slot_name(&slot) {
                ctx.error(
                    "V-058",
                    &pg_at.key("slot"),
                    l.loc(),
                    format!("slot {slot:?} must match ^[a-z_][a-z0-9_]{{0,62}}$"),
                );
            } else if let (Some(h), Some(d)) = (&host, &db) {
                slots
                    .entry((h.clone(), d.clone(), slot))
                    .or_default()
                    .push(name);
            }
        }
        if let Some(l) = &pg.publication
            && let Some(publication) = ctx.text(l, &pg_at.key("publication"))
        {
            if !is_pg_identifier(&publication) {
                ctx.warning("V-059", &pg_at.key("publication"), l.loc(), format!("publication {publication:?} is not a plain PostgreSQL identifier; it will need quoting"));
            } else if let (Some(h), Some(d)) = (&host, &db) {
                publications
                    .entry((h.clone(), d.clone(), publication))
                    .or_default()
                    .push(name);
            }
        }
    }
    // V-056
    for (id, names) in ids.iter().filter(|(_, n)| n.len() > 1) {
        for name in names {
            ctx.error(
                "V-056",
                &At::config(pipeline_at(name).path.key("id")),
                None,
                format!("id {id:?} is used by pipelines {}", names.join(", ")),
            );
        }
    }
    // V-058 (uniqueness per source host+db)
    for ((host, db, slot), names) in slots.iter().filter(|(_, n)| n.len() > 1) {
        for name in names {
            ctx.error(
                "V-058",
                &At::config(
                    pipeline_at(name)
                        .path
                        .key("source")
                        .key("postgres")
                        .key("slot"),
                ),
                None,
                format!(
                    "slot {slot:?} on {host}/{db} is used by pipelines {}",
                    names.join(", ")
                ),
            );
        }
    }
    // V-059 (uniqueness recommended)
    for ((host, db, publication), names) in publications.iter().filter(|(_, n)| n.len() > 1) {
        for name in names {
            ctx.warning(
                "V-059",
                &pipeline_at(name)
                    .key("source")
                    .key("postgres")
                    .key("publication"),
                None,
                format!(
                    "publication {publication:?} on {host}/{db} is shared by pipelines {}",
                    names.join(", ")
                ),
            );
        }
    }
}

```

Create `crates/data-avalanche-config/src/rules/mod.rs`:

```rust
//! The config-time V-rules of §13 (the master table is `crate::registry::RULES`). Every check runs
//! twice: in the structural pass, where a template's value is unknown and value checks skip it,
//! and in the resolving pass, where every value is known. Diagnostics are deduplicated.

use crate::ctx::{At, Ctx};
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::{RawConfig, RawPipeline};

mod identity;
mod structure;

/// Runs every config-time rule over the parsed document and its merged pipelines.
pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    structure::check(raw, merged, ctx);
    identity::check(raw, merged, ctx);
}

/// `pipelines.<name>` as a pipeline-locus location.
pub(crate) fn pipeline_at(name: &str) -> At {
    At::pipeline(name, YamlPath::root().key("pipelines").key(name))
}

/// True when a required string is absent or known to be empty (§11 Phase 2: "any required value
/// that resolves to empty is an error"). A template not yet resolved counts as present.
pub(crate) fn missing(ctx: &mut Ctx, leaf: &Option<Leaf<String>>, at: &At) -> bool {
    match leaf {
        None => true,
        Some(l) => ctx.text(l, at).is_some_and(|t| t.trim().is_empty()),
    }
}
```

Create `crates/data-avalanche-config/src/rules/structure.rs`:

```rust
//! §13.1 structure and mutual exclusion, §13.2 requiredness.

use super::{missing, pipeline_at};
use crate::ctx::{At, Ctx};
use crate::enums::{CatalogKind, SinkType, SourceType};
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::*;

pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    let root = At::config(YamlPath::root());
    // V-014
    match &raw.config_version {
        None => ctx.error(
            "V-014",
            &root.key("config_version"),
            None,
            "config_version is required (current: 1)",
        ),
        Some(l) => {
            if let Some(v) = ctx.value(l, &root.key("config_version"))
                && v != 1
            {
                ctx.error(
                    "V-014",
                    &root.key("config_version"),
                    l.loc(),
                    format!("config_version {v} is not supported by this binary, which reads version 1; migrate the file to version 1"),
                );
            }
        }
    }
    // V-005, V-006
    match &raw.globals {
        None => ctx.error("V-005", &root.key("globals"), None, "globals is required"),
        Some(g) => {
            let at = root.key("globals").key("state").key("bucket");
            let bucket = g.state.as_ref().and_then(|s| s.bucket.clone());
            if missing(ctx, &bucket, &at) {
                ctx.error(
                    "V-006",
                    &at,
                    bucket.and_then(|b| b.loc()),
                    "globals.state.bucket is required",
                );
            }
            if let Some(api) = &g.api {
                credentials_exclusive(
                    ctx,
                    &root.key("globals").key("api"),
                    &api.user,
                    &api.password,
                    &api.credentials_file,
                );
            }
        }
    }
    // V-004
    if raw.pipelines.is_none() {
        ctx.error(
            "V-004",
            &root.key("pipelines"),
            None,
            "pipelines is required (use {} for a deployment with no pipelines)",
        );
    }
    for (name, p) in merged {
        pipeline(name, p, ctx);
    }
}

/// V-065: `credentials_file` excludes `user`/`password` in the same block.
fn credentials_exclusive(
    ctx: &mut Ctx,
    at: &At,
    user: &Option<Leaf<String>>,
    password: &Option<Leaf<String>>,
    file: &Option<Leaf<String>>,
) {
    if let Some(f) = file
        && (user.is_some() || password.is_some())
    {
        ctx.error(
            "V-065",
            &at.key("credentials_file"),
            f.loc(),
            "credentials_file and user/password are mutually exclusive: give one",
        );
    }
}

fn pipeline(name: &str, p: &RawPipeline, ctx: &mut Ctx) {
    let at = pipeline_at(name);
    let default = RawSource::default();
    let src = p.source.as_ref().unwrap_or(&default);
    source(src, &at.key("source"), ctx);
    // V-007
    let include_at = at.key("tables").key("include");
    let include = p.tables.as_ref().and_then(|t| t.include.as_ref());
    if include.is_none_or(|v| v.is_empty()) {
        let loc = include.and_then(|v| v.first()).and_then(|l| l.loc());
        ctx.error(
            "V-007",
            &include_at,
            loc,
            "tables.include is required and must not be empty",
        );
    }
    let default = RawSink::default();
    sink(p.sink.as_ref().unwrap_or(&default), &at.key("sink"), ctx);
}

fn source(s: &RawSource, at: &At, ctx: &mut Ctx) {
    // V-008, V-009
    if missing(ctx, &s.host, &at.key("host")) {
        ctx.error(
            "V-008",
            &at.key("host"),
            s.host.as_ref().and_then(|l| l.loc()),
            "source.host is required (on the pipeline or in defaults)",
        );
    }
    if missing(ctx, &s.db, &at.key("db")) {
        ctx.error(
            "V-009",
            &at.key("db"),
            s.db.as_ref().and_then(|l| l.loc()),
            "source.db is required (on the pipeline or in defaults)",
        );
    }
    // V-065, V-010
    credentials_exclusive(ctx, at, &s.user, &s.password, &s.credentials_file);
    if s.credentials_file.is_none() {
        for (key, leaf) in [("user", &s.user), ("password", &s.password)] {
            if missing(ctx, leaf, &at.key(key)) {
                ctx.error(
                    "V-010",
                    &at.key(key),
                    leaf.as_ref().and_then(|l| l.loc()),
                    format!("source.{key} is required unless credentials_file is given"),
                );
            }
        }
    }
    // V-001, V-003, V-011
    let kind = if s.kind.is_none() {
        Some(SourceType::Postgres)
    } else {
        ctx.peek(&s.kind)
    };
    let Some(kind) = kind else { return };
    let (own, foreign, own_present, foreign_present) = match kind {
        SourceType::Postgres => ("postgres", "mysql", s.postgres.is_some(), s.mysql.is_some()),
        SourceType::Mysql => ("mysql", "postgres", s.mysql.is_some(), s.postgres.is_some()),
    };
    // Every `mysql` field has a default, so only a missing `postgres` block is an error (D-M1.5).
    if !own_present && kind == SourceType::Postgres {
        ctx.error(
            "V-001",
            &at.key(own),
            None,
            "source.type is postgres but there is no source.postgres block (publication and slot are required)",
        );
    }
    if foreign_present {
        ctx.error(
            "V-003",
            &at.key(foreign),
            None,
            format!("source.{foreign} is not allowed when source.type is {own}"),
        );
    }
    if let (SourceType::Postgres, Some(pg)) = (kind, &s.postgres) {
        let pg_at = at.key("postgres");
        for (key, leaf) in [("publication", &pg.publication), ("slot", &pg.slot)] {
            if missing(ctx, leaf, &pg_at.key(key)) {
                ctx.error(
                    "V-011",
                    &pg_at.key(key),
                    leaf.as_ref().and_then(|l| l.loc()),
                    format!("source.postgres.{key} is required"),
                );
            }
        }
    }
}

fn sink(s: &RawSink, at: &At, ctx: &mut Ctx) {
    // V-091, V-012
    if s.kind.is_none() {
        ctx.error(
            "V-091",
            &at.key("type"),
            None,
            "sink.type is required (iceberg or delta; there is no default)",
        );
    }
    if missing(ctx, &s.bucket, &at.key("bucket")) {
        ctx.error(
            "V-012",
            &at.key("bucket"),
            s.bucket.as_ref().and_then(|l| l.loc()),
            "sink.bucket is required (on the pipeline or in defaults)",
        );
    }
    // V-084
    let Some(kind) = ctx.peek(&s.kind) else {
        return;
    };
    let (own, foreign, own_present, foreign_present) = match kind {
        SinkType::Iceberg => ("iceberg", "delta", s.iceberg.is_some(), s.delta.is_some()),
        SinkType::Delta => ("delta", "iceberg", s.delta.is_some(), s.iceberg.is_some()),
    };
    // Every `delta` field has a default, so only a missing `iceberg` block is an error (D-M1.5).
    if !own_present && kind == SinkType::Iceberg {
        ctx.error(
            "V-084",
            &at.key(own),
            None,
            "sink.type is iceberg but there is no sink.iceberg block (catalog is required)",
        );
    }
    if foreign_present {
        ctx.error(
            "V-084",
            &at.key(foreign),
            None,
            format!("sink.{foreign} is not allowed when sink.type is {own}"),
        );
    }
    // V-013, V-002 and the catalog's exclusivity rules
    if let (SinkType::Iceberg, Some(iceberg)) = (kind, &s.iceberg) {
        let c_at = at.key("iceberg").key("catalog");
        match &iceberg.catalog {
            None => ctx.error(
                "V-013",
                &c_at,
                None,
                "sink.iceberg.catalog is required for an Iceberg sink",
            ),
            Some(c) => catalog(c, &c_at, ctx),
        }
    }
}

fn catalog(c: &RawCatalog, at: &At, ctx: &mut Ctx) {
    let present: Vec<CatalogKind> = [
        (CatalogKind::Glue, c.glue.is_some()),
        (CatalogKind::Hive, c.hive.is_some()),
        (CatalogKind::Rest, c.rest.is_some()),
    ]
    .into_iter()
    .filter_map(|(k, p)| p.then_some(k))
    .collect();
    match present.as_slice() {
        [] => ctx.error(
            "V-002",
            at,
            None,
            "sink.iceberg.catalog needs exactly one of glue, hive, rest",
        ),
        [one] => {
            if let Some(declared) = ctx.peek(&c.kind)
                && declared != *one
            {
                ctx.error(
                    "V-002",
                    &at.key("type"),
                    c.kind.as_ref().and_then(|l| l.loc()),
                    format!("catalog type is {declared} but the block present is {one}"),
                );
            }
        }
        many => {
            let names: Vec<_> = many.iter().map(|k| k.as_str()).collect();
            ctx.error(
                "V-002",
                at,
                None,
                format!(
                    "sink.iceberg.catalog has {} blocks; give exactly one",
                    names.join(" and ")
                ),
            );
        }
    }
    if let Some(h) = &c.hive {
        credentials_exclusive(
            ctx,
            &at.key("hive"),
            &h.user,
            &h.password,
            &h.credentials_file,
        );
    }
    if let Some(r) = &c.rest {
        let r_at = at.key("rest");
        if let (Some(f), Some(_)) = (&r.token_file, &r.token) {
            ctx.error(
                "V-070",
                &r_at.key("token_file"),
                f.loc(),
                "token_file and token are mutually exclusive: give one",
            );
        }
        if let Some(o) = &r.oauth2
            && let Some(f) = &o.credentials_file
            && (o.client_id.is_some() || o.client_secret.is_some())
        {
            ctx.error("V-066", &r_at.key("oauth2").key("credentials_file"), f.loc(), "oauth2.credentials_file and client_id/client_secret are mutually exclusive: give one");
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 29; `tests/load.rs` 12; `tests/merge.rs` 8; `tests/parse.rs` 8; `tests/raw.rs` 3; `tests/rules/main.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/src/rules/identity.rs \
  crates/data-avalanche-config/tests/rules/cases.rs \
  crates/data-avalanche-config/tests/rules/identity.rs \
  crates/data-avalanche-config/tests/rules/main.rs \
  crates/data-avalanche-config/tests/rules/resolution.rs \
  crates/data-avalanche-config/tests/rules/structure.rs \
  crates/data-avalanche-config/src/load.rs \
  crates/data-avalanche-config/src/rules/mod.rs \
  crates/data-avalanche-config/src/rules/structure.rs
git commit -m "feat(config): structure and identity rules, rule-case harness"
```


---

### Task 11: Rules II — value domain (§13.3)

**Files:**
- Create: `crates/data-avalanche-config/src/rules/domain.rs`
- Modify: `crates/data-avalanche-config/tests/load.rs`, `crates/data-avalanche-config/tests/rules/main.rs`, `crates/data-avalanche-config/tests/rules/resolution.rs`, `crates/data-avalanche-config/src/rules/mod.rs`
- Test: `crates/data-avalanche-config/tests/rules/domain.rs`

**Interfaces:**
- Consumes: Task 10 (harness, `rules::check`).
- Produces: `rules::domain::check`, covering the value-domain rules with ranges and conditions:
  - V-026, V-027, V-072, V-074, V-075, V-088, V-089, V-092, V-097, V-101, V-104;
  - V-107…V-112, V-115, V-116.
  
  Harness cases for them and for the enum rules that coercion already reports:
  - V-015…V-023, V-025;
  - V-080…V-083, V-095, V-102, V-105, V-114.

V-115 rejects `sink.type: iceberg` in v1 (the Iceberg sink is v2). The Iceberg blocks still parse
and validate, so a v2 config is checked today. The harness drops these IDs from `PENDING`.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/tests/load.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/load.rs b/crates/data-avalanche-config/tests/load.rs
index 114f789..2ffbb01 100644
--- a/crates/data-avalanche-config/tests/load.rs
+++ b/crates/data-avalanche-config/tests/load.rs
@@ -285,7 +285,7 @@ fn with_catalog(catalog: &str) -> String {
 }
 
 #[test]
-fn empty_token_file_is_v071_and_missing_keytab_is_v069() {
+fn empty_token_file_is_v071() {
     let files = Files::new();
     let token = files.write("token", "\n");
     let yaml = with_catalog(&format!(
@@ -293,18 +293,12 @@ fn empty_token_file_is_v071_and_missing_keytab_is_v069() {
         token.display()
     ));
     let report = load_str(&yaml, &env(&[("PW", "pw")])).unwrap_err();
-    assert_eq!(report.errors[0].rule, "V-071");
-
-    let yaml = with_catalog(
-        "hive: { url: \"thrift://hms:9083\", auth: kerberos, kerberos: { principal: cdc, keytab: /nonexistent/cdc.keytab } }",
-    );
-    let report = load_str(&yaml, &env(&[("PW", "pw")])).unwrap_err();
     let e = &report.errors[0];
     assert_eq!(
         (e.rule.as_str(), e.path.as_str()),
         (
-            "V-069",
-            "pipelines.orders.sink.iceberg.catalog.hive.kerberos.keytab"
+            "V-071",
+            "pipelines.orders.sink.iceberg.catalog.rest.token_file"
         )
     );
 }
```

Create `crates/data-avalanche-config/tests/rules/domain.rs`:

```rust
//! §13.3 value domain: enumerations (checked by coercion), ranges, reserved values, v1 rejections.

use serde_json::{Value, json};

use crate::cases::{Case, Edit, Expect::*, case, mysql_delta, pg_delta, pg_iceberg};

fn catalog(value: Value) -> Value {
    pg_iceberg().set("/pipelines/p/sink/iceberg/catalog", value)
}

pub fn cases() -> Vec<Case> {
    vec![
        // enumerations
        case("V-015", Error, "cloud gcp", || {
            pg_delta().set("/globals/cloud", json!("gcp"))
        }),
        case("V-015", Silent, "cloud google", || {
            pg_delta().set("/globals/cloud", json!("google"))
        }),
        case("V-016", Error, "api auth ldap", || {
            pg_delta().set("/globals/api/auth", json!("ldap"))
        }),
        case("V-016", Silent, "api auth none", || {
            pg_delta().set("/globals/api/auth", json!("none"))
        }),
        case("V-017", Error, "broken filter", || {
            pg_delta().set("/globals/log/level", json!("info,[="))
        }),
        case("V-017", Silent, "per-module filter", || {
            pg_delta().set(
                "/globals/log/level",
                json!("info,data_avalanche_config=debug"),
            )
        }),
        case("V-018", Error, "format xml", || {
            pg_delta().set("/globals/log/format", json!("xml"))
        }),
        case("V-018", Silent, "format pretty", || {
            pg_delta().set("/globals/log/format", json!("pretty"))
        }),
        case("V-019", Error, "source oracle", || {
            pg_delta().set("/pipelines/p/source/type", json!("oracle"))
        }),
        case("V-019", Silent, "source postgres", || {
            pg_delta().set("/pipelines/p/source/type", json!("postgres"))
        }),
        case("V-020", Error, "ssl_mode strict", || {
            pg_delta().set("/pipelines/p/source/ssl_mode", json!("strict"))
        }),
        case("V-020", Silent, "ssl_mode verify-full", || {
            pg_delta().set("/pipelines/p/source/ssl_mode", json!("verify-full"))
        }),
        case("V-021", Error, "replication_type position", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/replication_type",
                json!("position"),
            )
        }),
        case("V-021", Silent, "replication_type binlog", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/replication_type",
                json!("binlog"),
            )
        }),
        case("V-022", Error, "mode sometimes", || {
            pg_delta().set("/pipelines/p/tables/snapshots/mode", json!("sometimes"))
        }),
        case("V-022", Error, "override value", || {
            pg_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"public.orders": "sometimes"}),
            )
        }),
        case("V-022", Silent, "mode dump_initial", || {
            pg_delta().set("/pipelines/p/tables/snapshots/mode", json!("dump_initial"))
        }),
        case("V-023", Error, "strategy append", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/strategy", json!("append"))
        }),
        case("V-023", Silent, "strategy cow", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/strategy", json!("cow"))
        }),
        case("V-025", Error, "rest auth basic", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/rest/auth",
                json!("basic"),
            )
        }),
        case("V-025", Silent, "rest auth none", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/rest/auth", json!("none"))
        }),
        case("V-080", Error, "toast refetch", || {
            pg_delta().set("/pipelines/p/source/toast_strategy", json!("refetch"))
        }),
        case("V-080", Silent, "toast re_query", || {
            pg_delta().set("/pipelines/p/source/toast_strategy", json!("re_query"))
        }),
        case("V-081", Error, "detection catalog", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/tinyint1_detection",
                json!("catalog"),
            )
        }),
        case("V-081", Silent, "detection none", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/tinyint1_detection",
                json!("none"),
            )
        }),
        case("V-082", Error, "overflow truncate", || {
            pg_delta().set(
                "/pipelines/p/tables/numeric_overflow_action",
                json!("truncate"),
            )
        }),
        case("V-082", Silent, "overflow error", || {
            pg_delta().set(
                "/pipelines/p/tables/numeric_overflow_action",
                json!("error"),
            )
        }),
        case("V-083", Error, "sink hudi", || {
            pg_delta().set("/pipelines/p/sink/type", json!("hudi"))
        }),
        case(
            "V-083",
            Silent,
            "sink iceberg is valid config in v1",
            pg_iceberg,
        ),
        case("V-095", Error, "discovery consul", || {
            pg_delta().set("/globals/cluster/discovery", json!("consul"))
        }),
        case("V-095", Silent, "discovery static", || {
            pg_delta().set("/globals/cluster/discovery", json!("static"))
        }),
        case("V-102", Error, "slot_management external", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/slot_management",
                json!("external"),
            )
        }),
        case("V-102", Error, "slot_on_orphan adopt", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/slot_on_orphan",
                json!("adopt"),
            )
        }),
        case("V-102", Silent, "adopt_resnapshot", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/slot_on_orphan",
                json!("adopt_resnapshot"),
            )
        }),
        case("V-105", Error, "purge ignore", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/binlog_purge_action",
                json!("ignore"),
            )
        }),
        case("V-105", Silent, "purge re_snapshot", || {
            mysql_delta().set(
                "/pipelines/p/source/mysql/binlog_purge_action",
                json!("re_snapshot"),
            )
        }),
        case("V-114", Error, "ntz as date", || {
            pg_delta().set("/pipelines/p/sink/delta/timestamp_ntz_as", json!("date"))
        }),
        case("V-114", Silent, "ntz as string", || {
            pg_delta().set("/pipelines/p/sink/delta/timestamp_ntz_as", json!("string"))
        }),
        // ranges
        case("V-026", Error, "tokens 0", || {
            pg_delta().set("/globals/cluster/tokens", json!(0))
        }),
        case("V-026", Error, "tokens 4097", || {
            pg_delta().set("/globals/cluster/tokens", json!(4097))
        }),
        case("V-026", Warning, "tokens 32", || {
            pg_delta().set("/globals/cluster/tokens", json!(32))
        }),
        case("V-026", Silent, "tokens 256", || {
            pg_delta().set("/globals/cluster/tokens", json!(256))
        }),
        case("V-027", Error, "parallelism 0", || {
            pg_delta().set("/pipelines/p/tables/snapshots/parallelism", json!(0))
        }),
        case("V-027", Silent, "parallelism 1", || {
            pg_delta().set("/pipelines/p/tables/snapshots/parallelism", json!(1))
        }),
        case("V-072", Error, "margin 0.6", || {
            pg_delta().set("/globals/cluster/lease_safety_margin", json!(0.6))
        }),
        case("V-072", Warning, "margin 0.4", || {
            pg_delta().set("/globals/cluster/lease_safety_margin", json!(0.4))
        }),
        case("V-072", Silent, "margin 0.15", || {
            pg_delta().set("/globals/cluster/lease_safety_margin", json!(0.15))
        }),
        case("V-074", Error, "handoffs 0", || {
            pg_delta().set("/globals/cluster/max_concurrent_handoffs", json!(0))
        }),
        case("V-074", Error, "takeovers 0", || {
            pg_delta().set("/globals/cluster/max_concurrent_takeovers", json!(0))
        }),
        case("V-074", Silent, "takeovers 1", || {
            pg_delta().set("/globals/cluster/max_concurrent_takeovers", json!(1))
        }),
        case("V-097", Error, "ratio 1.5", || {
            pg_delta().set(
                "/globals/tracing",
                json!({"otlp_endpoint": "http://otel:4317", "sample_ratio": 1.5}),
            )
        }),
        case("V-097", Silent, "ratio 1", || {
            pg_delta().set(
                "/globals/tracing",
                json!({"otlp_endpoint": "http://otel:4317", "sample_ratio": 1}),
            )
        }),
        case("V-101", Error, "account with a dash", || {
            pg_delta().set("/globals/azure/account_name", json!("my-account"))
        }),
        case("V-101", Error, "account too short", || {
            pg_delta().set("/globals/azure/account_name", json!("ab"))
        }),
        case("V-101", Silent, "account name", || {
            pg_delta().set("/globals/azure/account_name", json!("lakeaccount01"))
        }),
        case("V-104", Error, "lag 32MiB", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/max_wal_lag_bytes",
                json!("32MiB"),
            )
        }),
        case("V-104", Silent, "lag 64MiB", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/max_wal_lag_bytes",
                json!("64MiB"),
            )
        }),
        case("V-104", Silent, "lag null", || {
            pg_delta().set(
                "/pipelines/p/source/postgres/max_wal_lag_bytes",
                json!(null),
            )
        }),
        case("V-107", Error, "transform monthly", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "_ts", "transform": "monthly"}]}),
            )
        }),
        case("V-107", Error, "bucket without n", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "id", "transform": "bucket"}]}),
            )
        }),
        case("V-107", Error, "width on identity", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "id", "transform": "identity", "width": 4}]}),
            )
        }),
        case("V-107", Silent, "bucket 16 and truncate 4", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "id", "transform": "bucket", "n": 16}, {"column": "sku", "transform": "truncate", "width": 4}]}),
            )
        }),
        case("V-108", Error, "chunk 0", || {
            pg_delta().set("/pipelines/p/tables/snapshots/chunk_size", json!(0))
        }),
        case("V-108", Warning, "chunk 2000000", || {
            pg_delta().set("/pipelines/p/tables/snapshots/chunk_size", json!(2_000_000))
        }),
        case("V-108", Silent, "chunk 1000000", || {
            pg_delta().set("/pipelines/p/tables/snapshots/chunk_size", json!(1_000_000))
        }),
        case("V-109", Error, "batch rows 0", || {
            pg_delta().set("/pipelines/p/sink/max_transaction_batch_rows", json!(0))
        }),
        case("V-109", Warning, "batch rows 500", || {
            pg_delta().set("/pipelines/p/sink/max_transaction_batch_rows", json!(500))
        }),
        case("V-109", Silent, "batch rows 1000", || {
            pg_delta().set("/pipelines/p/sink/max_transaction_batch_rows", json!(1000))
        }),
        case("V-110", Error, "buffer 8MiB", || {
            pg_delta().set(
                "/pipelines/p/sink/max_transaction_buffer_bytes",
                json!("8MiB"),
            )
        }),
        case("V-110", Silent, "buffer 16MiB", || {
            pg_delta().set(
                "/pipelines/p/sink/max_transaction_buffer_bytes",
                json!("16MiB"),
            )
        }),
        case("V-111", Error, "capacity 0", || {
            pg_delta().set("/pipelines/p/sink/buffer_capacity_rows", json!(0))
        }),
        case("V-111", Silent, "capacity 1", || {
            pg_delta().set("/pipelines/p/sink/buffer_capacity_rows", json!(1))
        }),
        case("V-112", Error, "no attempts", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/retry/max_attempts",
                json!(0),
            )
        }),
        case("V-112", Error, "base above default max", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/retry/base_backoff",
                json!("1m"),
            )
        }),
        case("V-112", Silent, "base equals max", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/retry",
                json!({"base_backoff": "5s", "max_backoff": "5s"}),
            )
        }),
        // reserved values and v1 rejections
        case("V-075", Error, "kubernetes lock", || {
            pg_delta().set("/globals/cluster/lock_backend", json!("kubernetes"))
        }),
        case("V-075", Silent, "storage lock", || {
            pg_delta().set("/globals/cluster/lock_backend", json!("storage"))
        }),
        case("V-088", Error, "unity catalog", || {
            pg_delta().set("/pipelines/p/sink/delta/catalog/type", json!("unity"))
        }),
        case("V-088", Silent, "no catalog", || {
            pg_delta().set("/pipelines/p/sink/delta/catalog/type", json!("none"))
        }),
        case("V-089", Error, "changelog", || {
            pg_delta().set("/pipelines/p/sink/write_mode", json!("changelog"))
        }),
        case("V-089", Silent, "mirror", || {
            pg_delta().set("/pipelines/p/sink/write_mode", json!("mirror"))
        }),
        case("V-092", Error, "column mapping name", || {
            pg_delta().set("/pipelines/p/sink/delta/column_mapping", json!("name"))
        }),
        case("V-092", Silent, "column mapping none", || {
            pg_delta().set("/pipelines/p/sink/delta/column_mapping", json!("none"))
        }),
        case("V-115", Error, "hive block", || {
            catalog(json!({"hive": {"url": "thrift://hms:9083"}}))
        }),
        case("V-115", Silent, "glue block", || {
            catalog(json!({"glue": {"name": "123456789012"}}))
        }),
        case("V-116", Error, "rest sigv4", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/rest/auth",
                json!("sigv4"),
            )
        }),
        case("V-116", Silent, "rest bearer", || {
            pg_iceberg()
                .set(
                    "/pipelines/p/sink/iceberg/catalog/rest/auth",
                    json!("bearer"),
                )
                .set("/pipelines/p/sink/iceberg/catalog/rest/token", json!("t"))
        }),
    ]
}
```

Change `crates/data-avalanche-config/tests/rules/main.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/main.rs b/crates/data-avalanche-config/tests/rules/main.rs
index e879cb5..4b0ec5a 100644
--- a/crates/data-avalanche-config/tests/rules/main.rs
+++ b/crates/data-avalanche-config/tests/rules/main.rs
@@ -5,6 +5,7 @@
 mod support;
 
 mod cases;
+mod domain;
 mod identity;
 mod resolution;
 mod structure;
@@ -17,6 +18,7 @@ use data_avalanche_config::{RULES, RuleInput, Severity, validate_str};
 fn all_cases() -> Vec<Case> {
     let mut all = Vec::new();
     all.extend(structure::cases());
+    all.extend(domain::cases());
     all.extend(identity::cases());
     all.extend(resolution::cases());
     all
@@ -25,19 +27,19 @@ fn all_cases() -> Vec<Case> {
 /// Config-time rules whose cases are not written yet. Each rule task removes its IDs; the last one
 /// leaves this empty.
 const PENDING: &[&str] = &[
-    "V-015", "V-016", "V-017", "V-018", "V-019", "V-020", "V-021", "V-022", "V-023", "V-025",
-    "V-026", "V-027", "V-028", "V-029", "V-030", "V-033", "V-035", "V-036", "V-037", "V-038",
-    "V-040", "V-041", "V-042", "V-043", "V-044", "V-045", "V-046", "V-047", "V-048", "V-049",
-    "V-050", "V-051", "V-052", "V-053", "V-054", "V-064", "V-072", "V-073", "V-074", "V-075",
-    "V-077", "V-080", "V-081", "V-082", "V-083", "V-085", "V-086", "V-087", "V-088", "V-089",
-    "V-092", "V-093", "V-095", "V-096", "V-097", "V-098", "V-099", "V-100", "V-101", "V-102",
-    "V-103", "V-104", "V-105", "V-106", "V-107", "V-108", "V-109", "V-110", "V-111", "V-112",
-    "V-114", "V-115", "V-116", "V-117",
+    "V-028", "V-029", "V-030", "V-033", "V-035", "V-036", "V-037", "V-038", "V-040", "V-041",
+    "V-042", "V-043", "V-044", "V-045", "V-046", "V-047", "V-048", "V-049", "V-050", "V-051",
+    "V-052", "V-053", "V-054", "V-064", "V-073", "V-077", "V-085", "V-086", "V-087", "V-093",
+    "V-096", "V-098", "V-099", "V-100", "V-103", "V-106", "V-117",
 ];
 
-/// Rules with no failing input in v1. V-087: every logical type of the grammar maps to both sinks
-/// (Delta turns `time`/`uuid` into strings), so nothing can fail it until a sink loses a type.
-const NO_FAILING_INPUT: &[&str] = &["V-087"];
+/// Rules with no failing input through `validate_str` in v1:
+/// - V-069: the only "other referenced file" is `catalog.hive.kerberos.keytab`, and V-115 rejects
+///   every Hive catalog in Phase 1, so Phase 2 never reads it. `material::read_file_ref` has unit
+///   tests for the check itself.
+/// - V-087: every logical type of the grammar maps to both sinks (Delta turns `time`/`uuid` into
+///   strings), so nothing can fail it until a sink loses a type.
+const NO_FAILING_INPUT: &[&str] = &["V-069", "V-087"];
 
 #[test]
 fn base_configs_are_clean() {
```

Change `crates/data-avalanche-config/tests/rules/resolution.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/resolution.rs b/crates/data-avalanche-config/tests/rules/resolution.rs
index bf7e4ac..a0f0879 100644
--- a/crates/data-avalanche-config/tests/rules/resolution.rs
+++ b/crates/data-avalanche-config/tests/rules/resolution.rs
@@ -61,16 +61,6 @@ pub fn cases() -> Vec<Case> {
         case("V-068", Silent, "inline certificate", || {
             pg_delta().set("/pipelines/p/source/ssl_ca", json!(cert()))
         }),
-        case_with("V-069", Error, "empty keytab", |files| {
-            let keytab = files.write("cdc.keytab", "");
-            Fixture {
-                doc: pg_iceberg().set(
-                    "/pipelines/p/sink/iceberg/catalog",
-                    json!({"hive": {"url": "thrift://hms:9083", "auth": "kerberos", "kerberos": {"principal": "cdc", "keytab": keytab}}}),
-                ),
-                env: base_env(),
-            }
-        }),
         case_with("V-069", Silent, "readable keytab", |files| {
             let keytab = files.write("cdc.keytab", "\u{5}\u{2}binary");
             Fixture {
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compiles, then FAILS in `rule_cases`. It reports 28 failing case(s), each with an empty `got [...]` because the rules don't exist yet. The first ones:

```text
V-026 Error (tokens 0): got []
V-026 Error (tokens 4097): got []
V-026 Warning (tokens 32): got []
V-027 Error (parallelism 0): got []
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-config/src/rules/domain.rs`:

```rust
//! §13.3 value domain. Enumerations are checked by coercion (each enum carries its V-rule, see
//! `crate::enums`); this module checks ranges, reserved values and the v1 rejections.

use std::time::Duration;

use super::pipeline_at;
use crate::coerce::LeafType;
use crate::ctx::{At, Ctx};
use crate::enums::*;
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::*;
use crate::units::ByteSize;

/// The value of `leaf` at `at`, with its location for the message.
fn val<T: LeafType>(
    ctx: &mut Ctx,
    leaf: &Option<Leaf<T>>,
    at: &At,
) -> Option<(T, Option<crate::leaf::Loc>)> {
    let l = leaf.as_ref()?;
    ctx.value(l, at).map(|v| (v, l.loc()))
}

fn at_least<T: LeafType + PartialOrd + std::fmt::Display>(
    ctx: &mut Ctx,
    rule: &str,
    leaf: &Option<Leaf<T>>,
    at: &At,
    min: T,
    field: &str,
) {
    if let Some((v, loc)) = val(ctx, leaf, at)
        && v < min
    {
        ctx.error(
            rule,
            at,
            loc,
            format!("{field} must be at least {min}, got {v}"),
        );
    }
}

pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    if let Some(g) = &raw.globals {
        globals(g, ctx);
    }
    for (name, p) in merged {
        pipeline(name, p, ctx);
    }
}

fn globals(g: &RawGlobals, ctx: &mut Ctx) {
    let root = At::config(YamlPath::root().key("globals"));
    if let Some(c) = &g.cluster {
        let at = root.key("cluster");
        // V-026
        if let Some((tokens, loc)) = val(ctx, &c.tokens, &at.key("tokens")) {
            if !(1..=4096).contains(&tokens) {
                ctx.error(
                    "V-026",
                    &at.key("tokens"),
                    loc,
                    format!("tokens must be 1..=4096, got {tokens}"),
                );
            } else if !(64..=256).contains(&tokens) {
                ctx.warning(
                    "V-026",
                    &at.key("tokens"),
                    loc,
                    format!("tokens {tokens} is outside the recommended 64..=256"),
                );
            }
        }
        // V-072
        if let Some((m, loc)) = val(ctx, &c.lease_safety_margin, &at.key("lease_safety_margin")) {
            if !(0.0..=0.5).contains(&m) {
                ctx.error(
                    "V-072",
                    &at.key("lease_safety_margin"),
                    loc,
                    format!("lease_safety_margin must be 0..=0.5, got {m}"),
                );
            } else if !(0.05..=0.30).contains(&m) {
                ctx.warning(
                    "V-072",
                    &at.key("lease_safety_margin"),
                    loc,
                    format!("lease_safety_margin {m} is outside the recommended 0.05..=0.30"),
                );
            }
        }
        // V-074
        at_least(
            ctx,
            "V-074",
            &c.max_concurrent_handoffs,
            &at.key("max_concurrent_handoffs"),
            1,
            "max_concurrent_handoffs",
        );
        at_least(
            ctx,
            "V-074",
            &c.max_concurrent_takeovers,
            &at.key("max_concurrent_takeovers"),
            1,
            "max_concurrent_takeovers",
        );
        // V-075
        if let Some((LockBackend::Kubernetes, loc)) =
            val(ctx, &c.lock_backend, &at.key("lock_backend"))
        {
            ctx.error(
                "V-075",
                &at.key("lock_backend"),
                loc,
                "lock_backend kubernetes is reserved and not implemented in v1; use storage",
            );
        }
    }
    // V-097
    if let Some(t) = &g.tracing
        && let Some((r, loc)) = val(
            ctx,
            &t.sample_ratio,
            &root.key("tracing").key("sample_ratio"),
        )
        && !(0.0..=1.0).contains(&r)
    {
        ctx.error(
            "V-097",
            &root.key("tracing").key("sample_ratio"),
            loc,
            format!("sample_ratio must be 0..=1, got {r}"),
        );
    }
    // V-101
    if let Some(a) = &g.azure
        && let Some((name, loc)) = val(ctx, &a.account_name, &root.key("azure").key("account_name"))
    {
        let ok = (3..=24).contains(&name.len())
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
        if !ok {
            ctx.error(
                "V-101",
                &root.key("azure").key("account_name"),
                loc,
                format!(
                    "{name:?} is not a storage account name (3-24 lower-case letters and digits)"
                ),
            );
        }
    }
}

fn pipeline(name: &str, p: &RawPipeline, ctx: &mut Ctx) {
    let at = pipeline_at(name);
    if let Some(pg) = p.source.as_ref().and_then(|s| s.postgres.as_ref()) {
        // V-104
        let lag_at = at.key("source").key("postgres").key("max_wal_lag_bytes");
        if let Some(Some(l)) = &pg.max_wal_lag_bytes
            && let Some(v) = ctx.value(l, &lag_at)
            && v.bytes() < 64 * ByteSize::MIB
        {
            ctx.error(
                "V-104",
                &lag_at,
                l.loc(),
                format!("max_wal_lag_bytes must be at least 64MiB when set, got {v}"),
            );
        }
    }
    if let Some(t) = &p.tables {
        let t_at = at.key("tables");
        if let Some(s) = &t.snapshots {
            let s_at = t_at.key("snapshots");
            // V-027, V-108
            at_least(
                ctx,
                "V-027",
                &s.parallelism,
                &s_at.key("parallelism"),
                1,
                "snapshots.parallelism",
            );
            if let Some((n, loc)) = val(ctx, &s.chunk_size, &s_at.key("chunk_size")) {
                if n < 1 {
                    ctx.error(
                        "V-108",
                        &s_at.key("chunk_size"),
                        loc,
                        "snapshots.chunk_size must be at least 1",
                    );
                } else if n > 1_000_000 {
                    ctx.warning("V-108", &s_at.key("chunk_size"), loc, format!("snapshots.chunk_size {n} is above 1000000: long chunk transactions and large memory"));
                }
            }
        }
        // V-107
        for (table, fields) in t.partitioning.iter().flatten() {
            for (i, f) in fields.iter().enumerate() {
                partition_field(f, &t_at.key("partitioning").key(table).index(i), ctx);
            }
        }
    }
    if let Some(s) = &p.sink {
        sink(s, &at.key("sink"), ctx);
    }
}

fn partition_field(f: &RawPartitionField, at: &At, ctx: &mut Ctx) {
    let Some((transform, loc)) = val(ctx, &f.transform, &at.key("transform")) else {
        return;
    };
    let n = val(ctx, &f.n, &at.key("n"));
    let width = val(ctx, &f.width, &at.key("width"));
    match transform {
        Transform::Bucket => match n {
            Some((v, _)) if v >= 1 => {}
            _ => ctx.error("V-107", &at.key("n"), loc, "transform bucket needs n >= 1"),
        },
        Transform::Truncate => match width {
            Some((v, _)) if v >= 1 => {}
            _ => ctx.error(
                "V-107",
                &at.key("width"),
                loc,
                "transform truncate needs width >= 1",
            ),
        },
        _ => {}
    }
    if transform != Transform::Bucket && f.n.is_some() {
        ctx.error(
            "V-107",
            &at.key("n"),
            loc,
            format!("n applies only to transform bucket, not {transform}"),
        );
    }
    if transform != Transform::Truncate && f.width.is_some() {
        ctx.error(
            "V-107",
            &at.key("width"),
            loc,
            format!("width applies only to transform truncate, not {transform}"),
        );
    }
}

fn sink(s: &RawSink, at: &At, ctx: &mut Ctx) {
    // V-089
    if let Some((WriteMode::Changelog, loc)) = val(ctx, &s.write_mode, &at.key("write_mode")) {
        ctx.error(
            "V-089",
            &at.key("write_mode"),
            loc,
            "write_mode changelog is reserved and not available in v1; use mirror",
        );
    }
    // V-109
    let rows_at = at.key("max_transaction_batch_rows");
    if let Some(Some(l)) = &s.max_transaction_batch_rows
        && let Some(v) = ctx.value(l, &rows_at)
    {
        if v < 1 {
            ctx.error(
                "V-109",
                &rows_at,
                l.loc(),
                "max_transaction_batch_rows must be at least 1 when set",
            );
        } else if v < 1000 {
            ctx.warning(
                "V-109",
                &rows_at,
                l.loc(),
                format!("max_transaction_batch_rows {v} is below 1000: many small commits"),
            );
        }
    }
    // V-110, V-111
    if let Some((v, loc)) = val(
        ctx,
        &s.max_transaction_buffer_bytes,
        &at.key("max_transaction_buffer_bytes"),
    ) && v.bytes() < 16 * ByteSize::MIB
    {
        ctx.error(
            "V-110",
            &at.key("max_transaction_buffer_bytes"),
            loc,
            format!("max_transaction_buffer_bytes must be at least 16MiB, got {v}"),
        );
    }
    at_least(
        ctx,
        "V-111",
        &s.buffer_capacity_rows,
        &at.key("buffer_capacity_rows"),
        1,
        "buffer_capacity_rows",
    );
    if let Some(d) = &s.delta {
        let d_at = at.key("delta");
        // V-092, V-088
        if let Some((ColumnMapping::Name, loc)) =
            val(ctx, &d.column_mapping, &d_at.key("column_mapping"))
        {
            ctx.error("V-092", &d_at.key("column_mapping"), loc, "column_mapping name is rejected in v1: delta-rs refuses ADD COLUMN on column-mapped tables");
        }
        let cat_at = d_at.key("catalog").key("type");
        if let Some(c) = &d.catalog
            && let Some((kind, loc)) = val(ctx, &c.kind, &cat_at)
            && kind != DeltaCatalogType::None
        {
            ctx.error(
                "V-088",
                &cat_at,
                loc,
                format!(
                    "delta catalog type {kind} is reserved; v1 writes path-based tables (none)"
                ),
            );
        }
    }
    if let Some(c) = s.iceberg.as_ref().and_then(|i| i.catalog.as_ref()) {
        catalog(c, &at.key("iceberg").key("catalog"), ctx);
    }
}

fn catalog(c: &RawCatalog, at: &At, ctx: &mut Ctx) {
    // V-112
    if let Some(r) = &c.retry {
        let r_at = at.key("retry");
        at_least(
            ctx,
            "V-112",
            &r.max_attempts,
            &r_at.key("max_attempts"),
            1,
            "retry.max_attempts",
        );
        let base = r
            .base_backoff
            .as_ref()
            .map_or(Some(Duration::from_secs(1)), |l| {
                ctx.value(l, &r_at.key("base_backoff"))
            });
        let max = r
            .max_backoff
            .as_ref()
            .map_or(Some(Duration::from_secs(30)), |l| {
                ctx.value(l, &r_at.key("max_backoff"))
            });
        if let (Some(b), Some(m)) = (base, max)
            && b > m
        {
            ctx.error(
                "V-112",
                &r_at,
                None,
                format!(
                    "retry.base_backoff {} is above max_backoff {}",
                    crate::units::format_duration(b),
                    crate::units::format_duration(m)
                ),
            );
        }
    }
    // V-115
    let hive_declared = ctx.peek(&c.kind) == Some(CatalogKind::Hive);
    if let Some(h) = &c.hive {
        ctx.error("V-115", &at.key("hive"), h.url.as_ref().and_then(|l| l.loc()), "the Hive catalog is rejected: the pinned iceberg-rust HMS catalog cannot commit (DA-CAT-6)");
    } else if hive_declared {
        ctx.error("V-115", &at.key("type"), c.kind.as_ref().and_then(|l| l.loc()), "the Hive catalog is rejected: the pinned iceberg-rust HMS catalog cannot commit (DA-CAT-6)");
    }
    // V-116
    if let Some(r) = &c.rest
        && let Some((RestAuth::Sigv4, loc)) = val(ctx, &r.auth, &at.key("rest").key("auth"))
    {
        ctx.error("V-116", &at.key("rest").key("auth"), loc, "rest auth sigv4 is rejected: the pinned iceberg-rust REST client has no SigV4 signing (DA-CAT-7); on AWS use catalog.glue");
    }
}
```

Change `crates/data-avalanche-config/src/rules/mod.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/mod.rs b/crates/data-avalanche-config/src/rules/mod.rs
index 98e1100..7ed5bda 100644
--- a/crates/data-avalanche-config/src/rules/mod.rs
+++ b/crates/data-avalanche-config/src/rules/mod.rs
@@ -7,6 +7,7 @@ use crate::leaf::Leaf;
 use crate::path::YamlPath;
 use crate::raw::{RawConfig, RawPipeline};
 
+mod domain;
 mod identity;
 mod structure;
 
@@ -14,6 +15,7 @@ mod structure;
 pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
     structure::check(raw, merged, ctx);
     identity::check(raw, merged, ctx);
+    domain::check(raw, merged, ctx);
 }
 
 /// `pipelines.<name>` as a pipeline-locus location.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 29; `tests/load.rs` 12; `tests/merge.rs` 8; `tests/parse.rs` 8; `tests/raw.rs` 3; `tests/rules/main.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/tests/load.rs \
  crates/data-avalanche-config/tests/rules/domain.rs \
  crates/data-avalanche-config/tests/rules/main.rs \
  crates/data-avalanche-config/tests/rules/resolution.rs \
  crates/data-avalanche-config/src/rules/domain.rs \
  crates/data-avalanche-config/src/rules/mod.rs
git commit -m "feat(config): value-domain rules"
```


---

### Task 12: Rules III — cross-field, catalog auth, API security

**Files:**
- Create: `crates/data-avalanche-config/src/rules/auth.rs`, `crates/data-avalanche-config/src/rules/cross.rs`
- Modify: `crates/data-avalanche-config/tests/load.rs`, `crates/data-avalanche-config/tests/rules/main.rs`, `crates/data-avalanche-config/src/rules/mod.rs`
- Test: `crates/data-avalanche-config/tests/rules/auth.rs`, `crates/data-avalanche-config/tests/rules/cross.rs`

**Interfaces:**
- Consumes: Tasks 10–11.
- Produces:
  - `rules::cross::{check, DELTA_PROPERTY_ALLOW_LIST, sink_maps, sink_partitions}` for
    V-028…V-038, V-064, V-073, V-077, V-085…V-087, V-093, V-096, V-098, V-103, V-117, and the
    error half of V-106. V-064 (MySQL) and V-117 (PostgreSQL) share one check of the pattern form.
  - `rules::auth::check` for V-040…V-054, V-099, V-100.
  - `PENDING` is gone. From here on, **every config-time rule has cases** (except D-M1.7).

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/tests/load.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/load.rs b/crates/data-avalanche-config/tests/load.rs
index 2ffbb01..ef4c268 100644
--- a/crates/data-avalanche-config/tests/load.rs
+++ b/crates/data-avalanche-config/tests/load.rs
@@ -14,6 +14,7 @@ fn spec_example_resolves() {
     let files = Files::new();
     let (yaml, env) = spec_example(&files);
     let loaded = load_str(&yaml, &env).unwrap_or_else(|r| panic!("{r:#?}"));
+    assert!(loaded.warnings.is_empty(), "{:#?}", loaded.warnings);
     let cfg = loaded.config;
     assert_eq!(cfg.config_version, 1);
     assert_eq!(cfg.globals.state.bucket, "123456789012-cdc-app-state");
```

Create `crates/data-avalanche-config/tests/rules/auth.rs`:

```rust
//! §13.5 catalog authentication and §13.6 management-API security.

use serde_json::{Value, json};

use crate::cases::{Case, Edit, Expect::*, case, pg_delta, pg_iceberg};
use crate::support::{cert, key};

fn hive(block: Value) -> Value {
    pg_iceberg().set(
        "/pipelines/p/sink/iceberg/catalog",
        json!({ "hive": block }),
    )
}

fn rest(extra: Value) -> Value {
    let mut block = json!({"url": "https://catalog.example.com"});
    block
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/rest", block)
}

pub fn cases() -> Vec<Case> {
    vec![
        case("V-040", Error, "ldap without credentials", || {
            hive(json!({"url": "thrift://hms:9083", "auth": "ldap"}))
        }),
        case("V-040", Silent, "ldap with credentials_file", || {
            hive(json!({"url": "thrift://hms:9083", "auth": "ldap", "credentials_file": "/x"}))
        }),
        case("V-041", Error, "kerberos without keytab", || {
            hive(
                json!({"url": "thrift://hms:9083", "auth": "kerberos", "kerberos": {"principal": "cdc"}}),
            )
        }),
        case("V-041", Silent, "kerberos complete", || {
            hive(
                json!({"url": "thrift://hms:9083", "auth": "kerberos", "kerberos": {"principal": "cdc", "keytab": "/k"}}),
            )
        }),
        case("V-042", Warning, "ldap in cleartext", || {
            hive(json!({"url": "thrift://hms:9083", "auth": "ldap", "user": "u", "password": "p"}))
        }),
        case("V-042", Silent, "ldap over tls", || {
            hive(
                json!({"url": "thrift://hms:9083", "auth": "ldap", "user": "u", "password": "p", "tls": {"enabled": true, "ca": cert()}}),
            )
        }),
        case("V-043", Warning, "tls without ca", || {
            hive(json!({"url": "thrift://hms:9083", "tls": {"enabled": true}}))
        }),
        case("V-043", Silent, "tls with ca", || {
            hive(json!({"url": "thrift://hms:9083", "tls": {"enabled": true, "ca": cert()}}))
        }),
        case("V-044", Error, "bearer without token", || {
            rest(json!({"auth": "bearer"}))
        }),
        case("V-044", Silent, "bearer with token_file", || {
            rest(json!({"auth": "bearer", "token_file": "/t"}))
        }),
        case("V-045", Error, "oauth2 without token_url", || {
            rest(json!({"auth": "oauth2", "oauth2": {"client_id": "a", "client_secret": "b"}}))
        }),
        case("V-045", Error, "oauth2 without client", || {
            rest(json!({"auth": "oauth2", "oauth2": {"token_url": "https://idp/t"}}))
        }),
        case("V-045", Silent, "oauth2 complete", || {
            rest(
                json!({"auth": "oauth2", "oauth2": {"client_id": "a", "client_secret": "b", "token_url": "https://idp/t"}}),
            )
        }),
        case("V-046", Error, "sigv4 without region", || {
            rest(json!({"auth": "sigv4", "sigv4": {"service": "glue"}}))
        }),
        case("V-046", Silent, "sigv4 complete", || {
            rest(json!({"auth": "sigv4", "sigv4": {"region": "eu-west-1", "service": "glue"}}))
        }),
        case("V-047", Error, "sigv4 on google", || {
            rest(json!({"auth": "sigv4", "sigv4": {"region": "eu-west-1", "service": "glue"}}))
                .set("/globals/cloud", json!("google"))
        }),
        case("V-047", Silent, "sigv4 on aws", || {
            rest(json!({"auth": "sigv4", "sigv4": {"region": "eu-west-1", "service": "glue"}}))
        }),
        case("V-048", Warning, "built-in token endpoint", || {
            rest(
                json!({"auth": "oauth2", "oauth2": {"client_id": "a", "client_secret": "b", "token_url": "https://catalog.example.com/v1/oauth/tokens"}}),
            )
        }),
        case("V-048", Silent, "external IdP", || {
            rest(
                json!({"auth": "oauth2", "oauth2": {"client_id": "a", "client_secret": "b", "token_url": "https://idp.example.com/token"}}),
            )
        }),
        case("V-049", Error, "basic without password", || {
            pg_delta().set("/globals/api", json!({"auth": "basic", "user": "admin"}))
        }),
        case("V-049", Silent, "basic with credentials_file", || {
            pg_delta().set(
                "/globals/api",
                json!({"auth": "basic", "credentials_file": "${NONE:-/x}"}),
            )
        }),
        case("V-050", Error, "jwt without jwks_url", || {
            pg_delta().set(
                "/globals/api",
                json!({"auth": "jwt", "jwt": {"issuer": "https://idp"}}),
            )
        }),
        case("V-050", Silent, "jwt complete", || {
            pg_delta().set("/globals/api", json!({"auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"}}))
        }),
        case("V-051", Error, "oauth2 without key source", || {
            pg_delta().set(
                "/globals/api",
                json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp"}}),
            )
        }),
        case("V-051", Silent, "oauth2 with discovery", || {
            pg_delta().set("/globals/api", json!({"auth": "oauth2", "oauth2": {"issuer": "https://idp", "discovery_url": "https://idp/.well-known/openid-configuration"}}))
        }),
        case("V-052", Warning, "jwt without tls", || {
            pg_delta().set("/globals/api", json!({"auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"}}))
        }),
        case("V-052", Silent, "jwt over tls", || {
            pg_delta().set("/globals/api", json!({
                "auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"},
                "tls": {"enabled": true, "cert": cert(), "key": key()}
            }))
        }),
        case("V-053", Error, "api tls without key", || {
            pg_delta().set("/globals/api/tls", json!({"enabled": true, "cert": cert()}))
        }),
        case("V-053", Silent, "api tls complete", || {
            pg_delta().set(
                "/globals/api/tls",
                json!({"enabled": true, "cert": cert(), "key": key()}),
            )
        }),
        case("V-054", Error, "gossip tls without cert", || {
            pg_delta().set(
                "/globals/cluster/memberlist/tls",
                json!({"enabled": true, "key": key()}),
            )
        }),
        case("V-054", Silent, "gossip tls disabled", || {
            pg_delta().set("/globals/cluster/memberlist/tls", json!({"enabled": false}))
        }),
        case("V-099", Error, "unknown role", || {
            pg_delta().set("/globals/api", json!({
                "auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"},
                "role_mapping": {"superuser": ["cdc-admins"]}
            }))
        }),
        case("V-099", Silent, "mapped roles", || {
            pg_delta().set("/globals/api", json!({
                "auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"},
                "role_mapping": {"admin": ["cdc-admins"], "viewer": ["everyone"]}
            }))
        }),
        case("V-100", Warning, "mapping with basic auth", || {
            pg_delta().set("/globals/api", json!({"auth": "basic", "user": "a", "password": "b", "role_mapping": {"admin": ["x"]}}))
        }),
        case("V-100", Silent, "mapping with jwt", || {
            pg_delta().set("/globals/api", json!({
                "auth": "jwt", "jwt": {"issuer": "https://idp", "jwks_url": "https://idp/certs"},
                "role_mapping": {"admin": ["x"]}
            }))
        }),
    ]
}
```

Create `crates/data-avalanche-config/tests/rules/cross.rs`:

```rust
//! §13.4 cross-field and conditional rules.

use serde_json::json;

use crate::cases::{Case, Edit, Expect::*, case, mysql_delta, pg_delta, pg_iceberg};

pub fn cases() -> Vec<Case> {
    vec![
        case("V-028", Error, "glue on google", || {
            pg_iceberg().set("/globals/cloud", json!("google")).set(
                "/pipelines/p/sink/iceberg/catalog",
                json!({"glue": {"name": "123"}}),
            )
        }),
        case("V-028", Silent, "glue on aws", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog",
                json!({"glue": {"name": "123"}}),
            )
        }),
        case("V-029", Error, "sigv4 on azure", || {
            pg_iceberg().set("/globals/cloud", json!("azure")).set(
                "/pipelines/p/sink/iceberg/catalog/rest/auth",
                json!("sigv4"),
            )
        }),
        case("V-029", Silent, "sigv4 on aws", || {
            pg_iceberg().set(
                "/pipelines/p/sink/iceberg/catalog/rest/auth",
                json!("sigv4"),
            )
        }),
        case("V-030", Error, "ssl allow on mysql", || {
            mysql_delta().set("/pipelines/p/source/ssl_mode", json!("allow"))
        }),
        case("V-030", Silent, "ssl allow on postgres", || {
            pg_delta().set("/pipelines/p/source/ssl_mode", json!("allow"))
        }),
        case("V-073", Error, "renewal 6s of 15s", || {
            pg_delta().set("/globals/cluster/lease_renewal_interval", json!("6s"))
        }),
        case(
            "V-073",
            Error,
            "duration shortened below 3x the default renewal",
            || pg_delta().set("/globals/cluster/lease_duration", json!("12s")),
        ),
        case("V-073", Silent, "renewal exactly a third", || {
            pg_delta()
                .set("/globals/cluster/lease_duration", json!("30s"))
                .set("/globals/cluster/lease_renewal_interval", json!("10s"))
        }),
        case("V-033", Error, "glob key", || {
            pg_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"public.*": "always"}),
            )
        }),
        case("V-033", Error, "key without schema", || {
            pg_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"orders": "always"}),
            )
        }),
        case("V-033", Silent, "schema.table key", || {
            pg_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"public.orders": "always"}),
            )
        }),
        case("V-035", Error, "all triggers null", || {
            pg_delta().set(
                "/pipelines/p/sink/flush",
                json!({"seconds": null, "rows": null, "bytes": null}),
            )
        }),
        case("V-035", Silent, "rows only", || {
            pg_delta().set(
                "/pipelines/p/sink/flush",
                json!({"seconds": null, "bytes": null}),
            )
        }),
        case("V-036", Warning, "time trigger off", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(null))
        }),
        case("V-036", Silent, "time trigger on", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(60))
        }),
        case("V-037", Warning, "seconds 2", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(2))
        }),
        case("V-037", Silent, "seconds 0 = every transaction", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(0))
        }),
        case("V-037", Silent, "seconds 5", || {
            pg_delta().set("/pipelines/p/sink/flush/seconds", json!(5))
        }),
        case("V-038", Warning, "parallelism 5", || {
            pg_delta().set("/pipelines/p/tables/snapshots/parallelism", json!(5))
        }),
        case("V-038", Silent, "parallelism 4", || {
            pg_delta().set("/pipelines/p/tables/snapshots/parallelism", json!(4))
        }),
        case("V-064", Error, "mysql include with db prefix", || {
            mysql_delta().set("/pipelines/p/tables/include", json!(["events.orders"]))
        }),
        case("V-064", Error, "mysql override with prefix", || {
            mysql_delta().set(
                "/pipelines/p/tables/snapshots/overrides",
                json!({"events.orders": "always"}),
            )
        }),
        case("V-064", Silent, "mysql bare names", || {
            mysql_delta()
                .set(
                    "/pipelines/p/tables/include",
                    json!(["orders", "payment_*"]),
                )
                .set(
                    "/pipelines/p/tables/snapshots/overrides",
                    json!({"orders": "always"}),
                )
        }),
        case("V-077", Warning, "re_query with always", || {
            pg_delta()
                .set("/pipelines/p/source/toast_strategy", json!("re_query"))
                .set("/pipelines/p/tables/snapshots/mode", json!("always"))
        }),
        case("V-077", Silent, "carry_forward with always", || {
            pg_delta().set("/pipelines/p/tables/snapshots/mode", json!("always"))
        }),
        case("V-085", Error, "strategy with changelog", || {
            pg_iceberg()
                .set("/pipelines/p/sink/write_mode", json!("changelog"))
                .set("/pipelines/p/sink/iceberg/strategy", json!("mor"))
        }),
        case("V-085", Silent, "strategy with mirror", || {
            pg_iceberg().set("/pipelines/p/sink/iceberg/strategy", json!("cow"))
        }),
        case("V-086", Error, "day transform on delta", || {
            pg_delta().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "_ts", "transform": "day"}]}),
            )
        }),
        case("V-086", Silent, "identity on delta", || {
            pg_delta().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "region", "transform": "identity"}]}),
            )
        }),
        case("V-086", Silent, "day on iceberg", || {
            pg_iceberg().set(
                "/pipelines/p/tables/partitioning",
                json!({"public.orders": [{"column": "_ts", "transform": "day"}]}),
            )
        }),
        case("V-087", Silent, "time and uuid map to delta", || {
            pg_delta().set(
                "/pipelines/p/tables/column_type_overrides",
                json!({"public.orders.t": "time", "public.orders.u": "list<uuid>"}),
            )
        }),
        case("V-093", Error, "managed property", || {
            pg_delta().set(
                "/pipelines/p/sink/delta/table_properties",
                json!({"delta.appendOnly": "true"}),
            )
        }),
        case("V-093", Error, "fence retention property", || {
            pg_delta().set(
                "/pipelines/p/sink/delta/table_properties",
                json!({"delta.setTransactionRetentionDuration": "interval 1 day"}),
            )
        }),
        case("V-093", Silent, "allow-listed and user keys", || {
            pg_delta().set(
                "/pipelines/p/sink/delta/table_properties",
                json!({"delta.targetFileSize": 134217728, "owner": "team-x"}),
            )
        }),
        case("V-096", Error, "dns without name", || {
            pg_delta().set("/globals/cluster/discovery", json!("dns"))
        }),
        case("V-096", Silent, "dns with name", || {
            pg_delta()
                .set("/globals/cluster/discovery", json!("dns"))
                .set("/globals/cluster/discovery_dns_name", json!("cdc-headless"))
        }),
        case("V-098", Warning, "ratio without endpoint", || {
            pg_delta().set("/globals/tracing/sample_ratio", json!(0.1))
        }),
        case("V-098", Silent, "ratio 0 without endpoint", || {
            pg_delta().set("/globals/tracing/sample_ratio", json!(0))
        }),
        case(
            "V-103",
            Error,
            "require_existing with default cleanup",
            || {
                pg_delta().set(
                    "/pipelines/p/source/postgres/slot_management",
                    json!("require_existing"),
                )
            },
        ),
        case("V-103", Silent, "require_existing without cleanup", || {
            pg_delta()
                .set(
                    "/pipelines/p/source/postgres/slot_management",
                    json!("require_existing"),
                )
                .set("/pipelines/p/source/postgres/auto_cleanup", json!(false))
        }),
        case("V-106", Error, "glob primary_key key", || {
            pg_delta().set(
                "/pipelines/p/tables/primary_key",
                json!({"public.*": ["id"]}),
            )
        }),
        case("V-106", Error, "empty column list", || {
            pg_delta().set(
                "/pipelines/p/tables/primary_key",
                json!({"public.orders": []}),
            )
        }),
        case("V-106", Error, "mysql partitioning key with prefix", || {
            mysql_delta().set(
                "/pipelines/p/tables/partitioning",
                json!({"events.orders": [{"column": "region", "transform": "identity"}]}),
            )
        }),
        case("V-106", Silent, "override identifier", || {
            pg_delta().set(
                "/pipelines/p/tables/primary_key",
                json!({"public.orders": ["tenant_id", "order_id"]}),
            )
        }),
        // V-117: PostgreSQL patterns are schema.table
        case("V-117", Error, "pg include without a schema", || {
            pg_delta().set("/pipelines/p/tables/include", json!(["orders"]))
        }),
        case("V-117", Error, "pg exclude with three parts", || {
            pg_delta().set("/pipelines/p/tables/exclude", json!(["public.orders.x"]))
        }),
        case("V-117", Error, "pg include with an empty schema", || {
            pg_delta().set("/pipelines/p/tables/include", json!([".orders"]))
        }),
        case("V-117", Silent, "pg globs in either part", || {
            pg_delta().set(
                "/pipelines/p/tables/include",
                json!(["public.*", "*.orders"]),
            )
        }),
    ]
}
```

Change `crates/data-avalanche-config/tests/rules/main.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/main.rs b/crates/data-avalanche-config/tests/rules/main.rs
index 4b0ec5a..7c2142e 100644
--- a/crates/data-avalanche-config/tests/rules/main.rs
+++ b/crates/data-avalanche-config/tests/rules/main.rs
@@ -4,14 +4,14 @@
 #[path = "../support/mod.rs"]
 mod support;
 
+mod auth;
 mod cases;
+mod cross;
 mod domain;
 mod identity;
 mod resolution;
 mod structure;
 
-use std::collections::BTreeSet;
-
 use cases::{Case, Expect};
 use data_avalanche_config::{RULES, RuleInput, Severity, validate_str};
 
@@ -19,20 +19,13 @@ fn all_cases() -> Vec<Case> {
     let mut all = Vec::new();
     all.extend(structure::cases());
     all.extend(domain::cases());
+    all.extend(cross::cases());
+    all.extend(auth::cases());
     all.extend(identity::cases());
     all.extend(resolution::cases());
     all
 }
 
-/// Config-time rules whose cases are not written yet. Each rule task removes its IDs; the last one
-/// leaves this empty.
-const PENDING: &[&str] = &[
-    "V-028", "V-029", "V-030", "V-033", "V-035", "V-036", "V-037", "V-038", "V-040", "V-041",
-    "V-042", "V-043", "V-044", "V-045", "V-046", "V-047", "V-048", "V-049", "V-050", "V-051",
-    "V-052", "V-053", "V-054", "V-064", "V-073", "V-077", "V-085", "V-086", "V-087", "V-093",
-    "V-096", "V-098", "V-099", "V-100", "V-103", "V-106", "V-117",
-];
-
 /// Rules with no failing input through `validate_str` in v1:
 /// - V-069: the only "other referenced file" is `catalog.hive.kerberos.keytab`, and V-115 rejects
 ///   every Hive catalog in Phase 1, so Phase 2 never reads it. `material::read_file_ref` has unit
@@ -95,12 +88,9 @@ fn every_config_rule_has_a_passing_and_a_failing_case() {
             .iter()
             .any(|c| c.rule == id && (c.expect == Expect::Silent) == pass)
     };
-    let pending: BTreeSet<&str> = PENDING.iter().copied().collect();
     let mut missing = Vec::new();
     for rule in RULES {
-        if !matches!(rule.input, RuleInput::Config | RuleInput::Resolution)
-            || pending.contains(rule.id)
-        {
+        if !matches!(rule.input, RuleInput::Config | RuleInput::Resolution) {
             continue;
         }
         if !with(rule.id, true) {
@@ -110,8 +100,5 @@ fn every_config_rule_has_a_passing_and_a_failing_case() {
             missing.push(format!("{}: no failing case", rule.id));
         }
     }
-    for id in &pending {
-        assert!(!with(id, false), "{id} has cases; remove it from PENDING");
-    }
     assert!(missing.is_empty(), "{}", missing.join("\n"));
 }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compiles, then FAILS in `rule_cases`. It reports 45 failing case(s), each with an empty `got [...]` because the rules don't exist yet. The first ones:

```text
V-028 Error (glue on google): got []
V-029 Error (sigv4 on azure): got [
V-030 Error (ssl allow on mysql): got []
V-073 Error (renewal 6s of 15s): got []
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-config/src/rules/auth.rs`:

```rust
//! §13.5 catalog authentication (the nine rules of `Catalog_Auth_Research.md`) and §13.6
//! management-API security.

use super::{missing, pipeline_at};
use crate::ctx::{At, Ctx};
use crate::enums::*;
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::*;

type L = Option<Leaf<String>>;

/// `user`+`password` or a `credentials_file` (V-010-style requirement for V-040/V-049).
fn require_credentials(
    ctx: &mut Ctx,
    rule: &str,
    at: &At,
    user: &L,
    password: &L,
    file: &L,
    what: &str,
) {
    if file.is_some() {
        return;
    }
    for (key, leaf) in [("user", user), ("password", password)] {
        if missing(ctx, leaf, &at.key(key)) {
            ctx.error(
                rule,
                &at.key(key),
                leaf.as_ref().and_then(|l| l.loc()),
                format!("{what} needs user and password, or credentials_file"),
            );
        }
    }
}

fn require(ctx: &mut Ctx, rule: &str, at: &At, key: &str, leaf: &L, what: &str) {
    if missing(ctx, leaf, &at.key(key)) {
        ctx.error(
            rule,
            &at.key(key),
            leaf.as_ref().and_then(|l| l.loc()),
            format!("{what} needs {key}"),
        );
    }
}

pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    let default = RawGlobals::default();
    let g = raw.globals.as_ref().unwrap_or(&default);
    let cloud = if g.cloud.is_none() {
        Some(Cloud::Aws)
    } else {
        ctx.peek(&g.cloud)
    };
    if let Some(api) = &g.api {
        management_api(
            api,
            &At::config(YamlPath::root().key("globals").key("api")),
            ctx,
        );
    }
    if let Some(tls) = g
        .cluster
        .as_ref()
        .and_then(|c| c.memberlist.as_ref())
        .and_then(|m| m.tls.as_ref())
        && ctx.peek(&tls.enabled) == Some(true)
    {
        // V-054
        let at = At::config(
            YamlPath::root()
                .key("globals")
                .key("cluster")
                .key("memberlist")
                .key("tls"),
        );
        require(ctx, "V-054", &at, "cert", &tls.cert, "memberlist TLS");
        require(ctx, "V-054", &at, "key", &tls.key, "memberlist TLS");
    }
    for (name, p) in merged {
        let is_iceberg = p
            .sink
            .as_ref()
            .is_some_and(|s| ctx.peek(&s.kind) == Some(SinkType::Iceberg));
        let Some(c) = p
            .sink
            .as_ref()
            .and_then(|s| s.iceberg.as_ref())
            .and_then(|i| i.catalog.as_ref())
        else {
            continue;
        };
        if !is_iceberg {
            continue;
        }
        let at = pipeline_at(name).key("sink").key("iceberg").key("catalog");
        if let Some(h) = &c.hive {
            hive(h, &at.key("hive"), ctx);
        }
        if let Some(r) = &c.rest {
            rest(r, &at.key("rest"), cloud, ctx);
        }
    }
}

fn hive(h: &RawHive, at: &At, ctx: &mut Ctx) {
    let tls_enabled = h
        .tls
        .as_ref()
        .is_some_and(|t| ctx.peek(&t.enabled) == Some(true));
    match ctx.peek(&h.auth) {
        Some(HiveAuth::Ldap) => {
            // V-040, V-042
            require_credentials(
                ctx,
                "V-040",
                at,
                &h.user,
                &h.password,
                &h.credentials_file,
                "hive auth ldap",
            );
            if !tls_enabled {
                ctx.warning(
                    "V-042",
                    &at.key("auth"),
                    h.auth.as_ref().and_then(|l| l.loc()),
                    "hive auth ldap without tls.enabled sends the password in cleartext",
                );
            }
        }
        Some(HiveAuth::Kerberos) => {
            // V-041
            let k = h.kerberos.clone().unwrap_or_default();
            let k_at = at.key("kerberos");
            require(
                ctx,
                "V-041",
                &k_at,
                "principal",
                &k.principal,
                "hive auth kerberos",
            );
            require(
                ctx,
                "V-041",
                &k_at,
                "keytab",
                &k.keytab,
                "hive auth kerberos",
            );
        }
        _ => {}
    }
    // V-043: a private CA can't be detected from the config, so warn whenever TLS has no CA.
    if tls_enabled && h.tls.as_ref().is_some_and(|t| t.ca.is_none()) {
        ctx.warning("V-043", &at.key("tls"), None, "hive tls without tls.ca: if the HMS certificate is signed by a private CA, set tls.ca or validation fails against the system store");
    }
}

fn rest(r: &RawRest, at: &At, cloud: Option<Cloud>, ctx: &mut Ctx) {
    match ctx.peek(&r.auth) {
        Some(RestAuth::Bearer) => {
            // V-044
            if r.token_file.is_none() && missing(ctx, &r.token, &at.key("token")) {
                ctx.error(
                    "V-044",
                    &at.key("token"),
                    r.auth.as_ref().and_then(|l| l.loc()),
                    "rest auth bearer needs token or token_file",
                );
            }
        }
        Some(RestAuth::Oauth2) => {
            // V-045, V-048
            let o = r.oauth2.clone().unwrap_or_default();
            let o_at = at.key("oauth2");
            require(
                ctx,
                "V-045",
                &o_at,
                "token_url",
                &o.token_url,
                "rest auth oauth2",
            );
            if o.credentials_file.is_none() {
                require(
                    ctx,
                    "V-045",
                    &o_at,
                    "client_id",
                    &o.client_id,
                    "rest auth oauth2 without credentials_file",
                );
                require(
                    ctx,
                    "V-045",
                    &o_at,
                    "client_secret",
                    &o.client_secret,
                    "rest auth oauth2 without credentials_file",
                );
            }
            let url = r.url.as_ref().and_then(|l| ctx.text(l, &at.key("url")));
            let token_url = o
                .token_url
                .as_ref()
                .and_then(|l| ctx.text(l, &o_at.key("token_url")));
            if let (Some(url), Some(token_url)) = (url, token_url)
                && token_url.trim_end_matches('/')
                    == format!("{}/v1/oauth/tokens", url.trim_end_matches('/'))
            {
                ctx.warning("V-048", &o_at.key("token_url"), o.token_url.as_ref().and_then(|l| l.loc()), "token_url is the catalog's built-in /v1/oauth/tokens endpoint, deprecated in Iceberg 1.6 and removed in 2.0; point it at the identity provider");
            }
        }
        Some(RestAuth::Sigv4) => {
            // V-046, V-047
            let s = r.sigv4.clone().unwrap_or_default();
            let s_at = at.key("sigv4");
            require(ctx, "V-046", &s_at, "region", &s.region, "rest auth sigv4");
            require(
                ctx,
                "V-046",
                &s_at,
                "service",
                &s.service,
                "rest auth sigv4",
            );
            if cloud.is_some_and(|c| c != Cloud::Aws) {
                ctx.error(
                    "V-047",
                    &at.key("auth"),
                    r.auth.as_ref().and_then(|l| l.loc()),
                    "rest auth sigv4 requires globals.cloud aws",
                );
            }
        }
        _ => {}
    }
}

fn management_api(a: &RawApi, at: &At, ctx: &mut Ctx) {
    let auth = if a.auth.is_none() {
        Some(ApiAuth::None)
    } else {
        ctx.peek(&a.auth)
    };
    let tls_enabled = a
        .tls
        .as_ref()
        .is_some_and(|t| ctx.peek(&t.enabled) == Some(true));
    match auth {
        // V-049
        Some(ApiAuth::Basic) => require_credentials(
            ctx,
            "V-049",
            at,
            &a.user,
            &a.password,
            &a.credentials_file,
            "api auth basic",
        ),
        // V-050
        Some(ApiAuth::Jwt) => {
            let j = a.jwt.clone().unwrap_or_default();
            require(
                ctx,
                "V-050",
                &at.key("jwt"),
                "issuer",
                &j.issuer,
                "api auth jwt",
            );
            require(
                ctx,
                "V-050",
                &at.key("jwt"),
                "jwks_url",
                &j.jwks_url,
                "api auth jwt",
            );
        }
        // V-051
        Some(ApiAuth::Oauth2) => {
            let o = a.oauth2.clone().unwrap_or_default();
            let o_at = at.key("oauth2");
            require(ctx, "V-051", &o_at, "issuer", &o.issuer, "api auth oauth2");
            if missing(ctx, &o.jwks_url, &o_at.key("jwks_url"))
                && missing(ctx, &o.discovery_url, &o_at.key("discovery_url"))
            {
                ctx.error(
                    "V-051",
                    &o_at,
                    None,
                    "api auth oauth2 needs jwks_url or discovery_url",
                );
            }
        }
        _ => {}
    }
    // V-052
    if auth.is_some_and(|a| a != ApiAuth::None) && !tls_enabled {
        ctx.warning("V-052", &at.key("auth"), a.auth.as_ref().and_then(|l| l.loc()), "api auth without tls.enabled sends credentials in cleartext unless TLS is terminated upstream");
    }
    // V-099, V-100
    if let Some(m) = &a.role_mapping {
        for key in m.keys() {
            if key.parse::<Role>().is_err() {
                ctx.error(
                    "V-099",
                    &at.key("role_mapping").key(key),
                    None,
                    format!("role_mapping key {key:?} is not a role (viewer, operator, admin)"),
                );
            }
        }
        if matches!(auth, Some(ApiAuth::None | ApiAuth::Basic)) {
            ctx.warning(
                "V-100",
                &at.key("role_mapping"),
                None,
                "role_mapping is ignored unless api auth is jwt or oauth2",
            );
        }
    }
    // V-053
    if let Some(tls) = &a.tls
        && tls_enabled
    {
        require(ctx, "V-053", &at.key("tls"), "cert", &tls.cert, "api TLS");
        require(ctx, "V-053", &at.key("tls"), "key", &tls.key, "api TLS");
    }
}
```

Create `crates/data-avalanche-config/src/rules/cross.rs`:

```rust
//! §13.4 cross-field and conditional rules.

use data_avalanche_core::LogicalType;

use super::{missing, pipeline_at};
use crate::build::{DEFAULT_LEASE_DURATION, DEFAULT_LEASE_RENEWAL_INTERVAL};
use crate::ctx::{At, Ctx};
use crate::enums::*;
use crate::leaf::Leaf;
use crate::path::YamlPath;
use crate::raw::*;
use crate::units::format_duration;

/// `delta.*` table properties a pipeline may pass through (V-093; decided at M1 planning,
/// 2026-09-27). Tuning and statistics only; everything that changes the protocol, the write path,
/// the `txn` fence or the sink's own fields is excluded. Keys without the `delta.` prefix are user
/// metadata and pass through.
pub const DELTA_PROPERTY_ALLOW_LIST: [&str; 7] = [
    "delta.dataSkippingNumIndexedCols",
    "delta.dataSkippingStatsColumns",
    "delta.deletedFileRetentionDuration",
    "delta.targetFileSize",
    "delta.tuneFileSizesForRewrites",
    "delta.checkpoint.writeStatsAsJson",
    "delta.checkpoint.writeStatsAsStruct",
];

/// Whether `sink` can map `t` (V-087). In v1 both sinks map every type of the grammar: Delta has no
/// `uuid` or `time` and stores them as strings (Type_Mapping Hop 3b), Iceberg stores `json` as a
/// string (Hop 3a).
pub fn sink_maps(sink: SinkType, t: &LogicalType) -> bool {
    match (sink, t) {
        (_, LogicalType::List(inner)) => sink_maps(sink, inner),
        (SinkType::Delta | SinkType::Iceberg, _) => true,
    }
}

/// Whether `sink` supports partition transform `t` (V-086; Delta v1: `identity` only).
pub fn sink_partitions(sink: SinkType, t: Transform) -> bool {
    match sink {
        SinkType::Iceberg => true,
        SinkType::Delta => t == Transform::Identity,
    }
}

fn has_glob(s: &str) -> bool {
    s.contains(['*', '?', '['])
}

pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
    let default = RawGlobals::default();
    let g = raw.globals.as_ref().unwrap_or(&default);
    let cloud = if g.cloud.is_none() {
        Some(Cloud::Aws)
    } else {
        ctx.peek(&g.cloud)
    };
    globals(g, ctx);
    for (name, p) in merged {
        pipeline(name, p, cloud, ctx);
    }
}

fn globals(g: &RawGlobals, ctx: &mut Ctx) {
    let root = At::config(YamlPath::root().key("globals"));
    if let Some(c) = &g.cluster {
        let at = root.key("cluster");
        // V-073
        let duration = c
            .lease_duration
            .as_ref()
            .map_or(Some(DEFAULT_LEASE_DURATION), |l| {
                ctx.value(l, &at.key("lease_duration"))
            });
        let renewal = c
            .lease_renewal_interval
            .as_ref()
            .map_or(Some(DEFAULT_LEASE_RENEWAL_INTERVAL), |l| {
                ctx.value(l, &at.key("lease_renewal_interval"))
            });
        if let (Some(d), Some(r)) = (duration, renewal)
            && r > d / 3
        {
            ctx.error("V-073", &at.key("lease_renewal_interval"), c.lease_renewal_interval.as_ref().and_then(|l| l.loc()), format!(
                "lease_renewal_interval {} exceeds lease_duration / 3 ({}): the holder needs at least two renewals inside the safety deadline",
                format_duration(r),
                format_duration(d / 3)
            ));
        }
        // V-096
        if ctx.peek(&c.discovery) == Some(Discovery::Dns)
            && missing(ctx, &c.discovery_dns_name, &at.key("discovery_dns_name"))
        {
            ctx.error(
                "V-096",
                &at.key("discovery_dns_name"),
                c.discovery.as_ref().and_then(|l| l.loc()),
                "discovery dns needs discovery_dns_name",
            );
        }
    }
    // V-098
    if let Some(t) = &g.tracing
        && t.otlp_endpoint.is_none()
        && let Some(l) = &t.sample_ratio
        && ctx
            .value(l, &root.key("tracing").key("sample_ratio"))
            .is_some_and(|r| r > 0.0)
    {
        ctx.warning(
            "V-098",
            &root.key("tracing").key("sample_ratio"),
            l.loc(),
            "sample_ratio > 0 has no effect: tracing is disabled without otlp_endpoint",
        );
    }
}

/// A per-table key (`snapshots.overrides`, `primary_key`, `partitioning`): no globs (V-033); PG
/// `schema.table` (V-033), MySQL a bare name (V-064). `rule` overrides the ID for V-106.
/// `schema.table`: one `.` between a non-empty schema part and a non-empty table part.
fn is_schema_table(s: &str) -> bool {
    matches!(s.split_once('.'), Some((schema, table)) if !schema.is_empty() && !table.is_empty() && !table.contains('.'))
}

fn table_key(ctx: &mut Ctx, key: &str, at: &At, mysql: bool, rule: Option<&str>) {
    if has_glob(key) {
        ctx.error(
            rule.unwrap_or("V-033"),
            at,
            None,
            format!("{key:?}: globs are not allowed in a per-table key"),
        );
    } else if mysql && key.contains('.') {
        ctx.error(
            rule.unwrap_or("V-064"),
            at,
            None,
            format!("{key:?}: a MySQL table key is a bare table name (the database is source.db)"),
        );
    } else if !mysql && !is_schema_table(key) {
        ctx.error(
            rule.unwrap_or("V-033"),
            at,
            None,
            format!("{key:?}: a PostgreSQL table key is schema.table"),
        );
    }
}

fn pipeline(name: &str, p: &RawPipeline, cloud: Option<Cloud>, ctx: &mut Ctx) {
    let at = pipeline_at(name);
    let default_source = RawSource::default();
    let src = p.source.as_ref().unwrap_or(&default_source);
    let kind = if src.kind.is_none() {
        Some(SourceType::Postgres)
    } else {
        ctx.peek(&src.kind)
    };
    let s_at = at.key("source");
    // V-030
    if kind == Some(SourceType::Mysql) && ctx.peek(&src.ssl_mode) == Some(SslMode::Allow) {
        ctx.error(
            "V-030",
            &s_at.key("ssl_mode"),
            src.ssl_mode.as_ref().and_then(|l| l.loc()),
            "ssl_mode allow is not valid for MySQL",
        );
    }
    // V-103
    if let Some(pg) = &src.postgres
        && ctx.peek(&pg.slot_management) == Some(SlotManagement::RequireExisting)
        && ctx.peek(&pg.auto_cleanup) != Some(false)
    {
        ctx.error("V-103", &s_at.key("postgres").key("auto_cleanup"), pg.slot_management.as_ref().and_then(|l| l.loc()), "slot_management require_existing needs auto_cleanup: false (the slot belongs to the DBA)");
    }
    let sink_kind = p.sink.as_ref().and_then(|s| ctx.peek(&s.kind));
    if let Some(t) = &p.tables {
        tables(t, &at, kind, sink_kind, src, ctx);
    }
    if let Some(s) = &p.sink {
        sink(s, &at.key("sink"), sink_kind, cloud, ctx);
    }
}

fn tables(
    t: &RawTables,
    p_at: &At,
    kind: Option<SourceType>,
    sink_kind: Option<SinkType>,
    src: &RawSource,
    ctx: &mut Ctx,
) {
    let at = &p_at.key("tables");
    let mysql = kind == Some(SourceType::Mysql);
    // V-064, V-117: the per-engine pattern form
    if kind.is_some() {
        for key in ["include", "exclude"] {
            let list = if key == "include" {
                &t.include
            } else {
                &t.exclude
            };
            for (i, l) in list.iter().flatten().enumerate() {
                let Some(g) = ctx.text(l, &at.key(key).index(i)) else {
                    continue;
                };
                if mysql && g.contains('.') {
                    ctx.error(
                        "V-064",
                        &at.key(key).index(i),
                        l.loc(),
                        format!(
                            "{g:?}: MySQL table patterns are bare names (the database is source.db)"
                        ),
                    );
                } else if !mysql && !is_schema_table(&g) {
                    ctx.error(
                        "V-117",
                        &at.key(key).index(i),
                        l.loc(),
                        format!("{g:?}: PostgreSQL table patterns are schema.table (e.g. public.* or public.orders)"),
                    );
                }
            }
        }
    }
    // V-033, V-064 (override keys); V-038, V-077
    if let Some(s) = &t.snapshots {
        let s_at = at.key("snapshots");
        if kind.is_some() {
            for key in s.overrides.iter().flatten().map(|(k, _)| k) {
                table_key(ctx, key, &s_at.key("overrides").key(key), mysql, None);
            }
        }
        if let Some(l) = &s.parallelism
            && ctx
                .value(l, &s_at.key("parallelism"))
                .is_some_and(|n| n > 4)
        {
            ctx.warning("V-038", &s_at.key("parallelism"), l.loc(), "snapshots.parallelism > 4: each PostgreSQL snapshot job uses at least 3 connections");
        }
        if ctx.peek(&src.toast_strategy) == Some(ToastStrategy::ReQuery)
            && ctx.peek(&s.mode) == Some(SnapshotMode::Always)
        {
            ctx.warning("V-077", &p_at.key("source").key("toast_strategy"), src.toast_strategy.as_ref().and_then(|l| l.loc()), "toast_strategy re_query with snapshots.mode always may saturate the control connection pool");
        }
    }
    // V-106
    if kind.is_some() {
        for (key, cols) in t.primary_key.iter().flatten() {
            let k_at = at.key("primary_key").key(key);
            table_key(ctx, key, &k_at, mysql, Some("V-106"));
            if cols.is_empty() {
                ctx.error("V-106", &k_at, None, "primary_key column list is empty");
            }
        }
        for (key, fields) in t.partitioning.iter().flatten() {
            let k_at = at.key("partitioning").key(key);
            table_key(ctx, key, &k_at, mysql, Some("V-106"));
            if fields.is_empty() {
                ctx.error("V-106", &k_at, None, "partitioning field list is empty");
            }
        }
    }
    let Some(sink_kind) = sink_kind else { return };
    // V-086
    for (key, fields) in t.partitioning.iter().flatten() {
        for (i, f) in fields.iter().enumerate() {
            if let Some(tr) = ctx.peek(&f.transform)
                && !sink_partitions(sink_kind, tr)
            {
                ctx.error("V-086", &at.key("partitioning").key(key).index(i).key("transform"), f.transform.as_ref().and_then(|l| l.loc()), format!("the {sink_kind} sink does not support partition transform {tr} (v1 Delta: identity only)"));
            }
        }
    }
    // V-087
    for (column, l) in t.column_type_overrides.iter().flatten() {
        if let Some(lt) = ctx.peek::<LogicalType>(&Some(l.clone()))
            && !sink_maps(sink_kind, &lt)
        {
            ctx.error(
                "V-087",
                &at.key("column_type_overrides").key(column),
                l.loc(),
                format!("the {sink_kind} sink cannot map logical type {lt}"),
            );
        }
    }
}

fn sink(s: &RawSink, at: &At, kind: Option<SinkType>, cloud: Option<Cloud>, ctx: &mut Ctx) {
    // V-035, V-036, V-037
    if let Some(f) = &s.flush {
        let f_at = at.key("flush");
        let disabled = |t: &Option<Option<Leaf<u64>>>| matches!(t, Some(None));
        let bytes_disabled = matches!(f.bytes, Some(None));
        if disabled(&f.seconds) && disabled(&f.rows) && bytes_disabled {
            ctx.error(
                "V-035",
                &f_at,
                None,
                "all flush triggers are null: nothing would ever flush",
            );
        } else if disabled(&f.seconds) {
            ctx.warning("V-036", &f_at.key("seconds"), None, "time-based flushing is disabled: a quiet source may hold data unflushed for a long time");
        }
        if let Some(Some(l)) = &f.seconds
            && ctx
                .value(l, &f_at.key("seconds"))
                .is_some_and(|v| (1..5).contains(&v))
        {
            ctx.warning(
                "V-037",
                &f_at.key("seconds"),
                l.loc(),
                "flush.seconds below 5 causes write amplification",
            );
        }
    }
    // V-085
    if kind == Some(SinkType::Iceberg)
        && ctx.peek(&s.write_mode) == Some(WriteMode::Changelog)
        && let Some(l) = s.iceberg.as_ref().and_then(|i| i.strategy.as_ref())
    {
        ctx.error(
            "V-085",
            &at.key("iceberg").key("strategy"),
            l.loc(),
            "sink.iceberg.strategy applies only to write_mode mirror",
        );
    }
    // V-093
    if let Some(d) = &s.delta {
        for (key, l) in d.table_properties.iter().flatten() {
            if key.starts_with("delta.") && !DELTA_PROPERTY_ALLOW_LIST.contains(&key.as_str()) {
                ctx.error(
                    "V-093",
                    &at.key("delta").key("table_properties").key(key),
                    l.loc(),
                    format!(
                        "table property {key} is not allowed; allowed delta.* keys: {}",
                        DELTA_PROPERTY_ALLOW_LIST.join(", ")
                    ),
                );
            }
        }
    }
    // V-028, V-029 (and V-047 in `auth`)
    if let Some(c) = s.iceberg.as_ref().and_then(|i| i.catalog.as_ref())
        && cloud.is_some_and(|c| c != Cloud::Aws)
    {
        let c_at = at.key("iceberg").key("catalog");
        if c.glue.is_some() {
            ctx.error(
                "V-028",
                &c_at.key("glue"),
                None,
                "the Glue catalog requires globals.cloud aws",
            );
        }
        if let Some(r) = &c.rest
            && ctx.peek(&r.auth) == Some(RestAuth::Sigv4)
        {
            ctx.error(
                "V-029",
                &c_at.key("rest").key("auth"),
                r.auth.as_ref().and_then(|l| l.loc()),
                "rest auth sigv4 requires globals.cloud aws",
            );
        }
    }
}
```

Change `crates/data-avalanche-config/src/rules/mod.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/mod.rs b/crates/data-avalanche-config/src/rules/mod.rs
index 7ed5bda..cfa4fa2 100644
--- a/crates/data-avalanche-config/src/rules/mod.rs
+++ b/crates/data-avalanche-config/src/rules/mod.rs
@@ -7,6 +7,8 @@ use crate::leaf::Leaf;
 use crate::path::YamlPath;
 use crate::raw::{RawConfig, RawPipeline};
 
+mod auth;
+pub mod cross;
 mod domain;
 mod identity;
 mod structure;
@@ -16,6 +18,8 @@ pub fn check(raw: &RawConfig, merged: &[(String, RawPipeline)], ctx: &mut Ctx) {
     structure::check(raw, merged, ctx);
     identity::check(raw, merged, ctx);
     domain::check(raw, merged, ctx);
+    cross::check(raw, merged, ctx);
+    auth::check(raw, merged, ctx);
 }
 
 /// `pipelines.<name>` as a pipeline-locus location.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 29; `tests/load.rs` 12; `tests/merge.rs` 8; `tests/parse.rs` 8; `tests/raw.rs` 3; `tests/rules/main.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/tests/load.rs \
  crates/data-avalanche-config/tests/rules/auth.rs \
  crates/data-avalanche-config/tests/rules/cross.rs \
  crates/data-avalanche-config/tests/rules/main.rs \
  crates/data-avalanche-config/src/rules/auth.rs \
  crates/data-avalanche-config/src/rules/cross.rs \
  crates/data-avalanche-config/src/rules/mod.rs
git commit -m "feat(config): cross-field, catalog-auth and API-security rules"
```


---

### Task 13: Mutability classes and the reload diff (§12, V-061)

**Files:**
- Create: `crates/data-avalanche-config/src/mutability.rs`, `crates/data-avalanche-config/src/diff.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`
- Test: `crates/data-avalanche-config/tests/diff.rs`

**Interfaces:**
- Consumes: Tasks 8–9 (`ResolvedConfig`, `Pipeline`, `Report`).
- Produces:
  - `Mutability::{R, PrIfTableSetChanges, Pr, Proc}`, ordered.
  - `mutability::CLASSES` (the path-pattern table) and `classify(&[&str]) -> Option<Mutability>`
    (longest match wins).
  - `FieldChange { path, class }`.
  - `PipelineChange::{Added(id), Removed(id), Modified { id, name, renamed_from, restart, fields }}`.
  - `ConfigDiff { globals, pipelines, report }` with `is_empty()` and `is_rejected()`.
  - `diff(old, new, table_set_changed: &dyn Fn(&Pipeline, &Pipeline) -> Option<bool>) -> ConfigDiff`.
  - `diff::unclassified_paths(&ResolvedConfig) -> Vec<String>`.

**How the diff works:**
- Pipelines are matched by `id`. A name change with the same `id` is a rename; a changed `id` is
  remove + add (§9.1).
- A PROC change adds V-061, which rejects the reload.
- `tables.include`/`exclude` changes are PR only when the resolved table set changes. The runtime
  answers through the callback; `None` (unknown) counts as changed.
- A test asserts that every leaf path of the resolved config has a class. A new field without a
  class fails the build.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index deb87a2..fa1fb41 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -6,11 +6,13 @@ pub mod build;
 pub mod coerce;
 pub mod ctx;
 pub mod diag;
+pub mod diff;
 pub mod enums;
 pub mod leaf;
 pub mod load;
 pub mod material;
 pub mod merge;
+pub mod mutability;
 pub mod parse;
 pub mod path;
 pub mod raw;
@@ -24,10 +26,12 @@ pub mod units;
 pub use coerce::{LeafType, LogFilter};
 pub use ctx::{At, Ctx};
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
+pub use diff::{ConfigDiff, FieldChange, PipelineChange, diff};
 pub use leaf::{Leaf, Loc, Tri};
 pub use load::{Loaded, load_file, load_str, validate_str};
 pub use material::{FileRef, Origin, Pem};
 pub use merge::merge_pipeline;
+pub use mutability::Mutability;
 pub use parse::parse_document;
 pub use path::YamlPath;
 pub use raw::RawConfig;
```

Create `crates/data-avalanche-config/src/mutability.rs` with only its test module. The implementation goes above it in Step 3:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn longest_pattern_wins() {
        assert_eq!(classify(&["globals", "cluster", "tokens"]), Some(Proc));
        assert_eq!(classify(&["globals", "cluster", "drain_timeout"]), Some(R));
        assert_eq!(
            classify(&["globals", "cluster", "memberlist", "tls", "cert", "text"]),
            Some(Proc)
        );
        assert_eq!(classify(&["pipelines", "p", "source", "host"]), Some(Pr));
        assert_eq!(
            classify(&["pipelines", "p", "source", "postgres", "heartbeat"]),
            Some(R)
        );
        assert_eq!(
            classify(&["pipelines", "p", "tables", "include"]),
            Some(PrIfTableSetChanges)
        );
        assert_eq!(
            classify(&["pipelines", "p", "sink", "flush", "rows"]),
            Some(R)
        );
        assert_eq!(classify(&["nowhere"]), None);
        assert!(Proc > Pr && Pr > PrIfTableSetChanges && PrIfTableSetChanges > R);
    }
}
```

Create `crates/data-avalanche-config/tests/diff.rs`:

```rust
//! The reload diff (config §12) and the mutability classes (§1.2).

mod support;

use data_avalanche_config::diff::unclassified_paths;
use data_avalanche_config::{Mutability, PipelineChange, ResolvedConfig, diff, load_str};
use support::{Files, MINIMAL, cert, env, key, spec_example};

fn load(yaml: &str) -> ResolvedConfig {
    load_str(yaml, &env(&[("PW", "pw")]))
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config
}

fn unknown(
    _: &data_avalanche_config::Pipeline,
    _: &data_avalanche_config::Pipeline,
) -> Option<bool> {
    None
}

fn modified(
    d: &data_avalanche_config::ConfigDiff,
) -> (bool, Vec<(String, Mutability)>, Option<String>) {
    match d.pipelines.as_slice() {
        [
            PipelineChange::Modified {
                restart,
                fields,
                renamed_from,
                ..
            },
        ] => (
            *restart,
            fields.iter().map(|f| (f.path.clone(), f.class)).collect(),
            renamed_from.clone(),
        ),
        other => panic!("expected one modified pipeline, got {other:#?}"),
    }
}

#[test]
fn identical_configs_have_no_changes() {
    let d = diff(&load(MINIMAL), &load(MINIMAL), &unknown);
    assert!(d.is_empty() && !d.is_rejected());
}

#[test]
fn r_change_applies_in_place() {
    let new = MINIMAL.replace("delta: {}", "delta: {}\n      flush: { rows: 5000 }");
    let (restart, fields, _) = modified(&diff(&load(MINIMAL), &load(&new), &unknown));
    assert!(!restart);
    assert_eq!(
        fields,
        [(
            "pipelines.orders.sink.flush.rows".to_string(),
            Mutability::R
        )]
    );
}

#[test]
fn pr_change_restarts_the_pipeline() {
    let new = MINIMAL.replace("db.example.com", "db2.example.com");
    let (restart, fields, _) = modified(&diff(&load(MINIMAL), &load(&new), &unknown));
    assert!(restart);
    assert_eq!(
        fields,
        [("pipelines.orders.source.host".to_string(), Mutability::Pr)]
    );
}

#[test]
fn proc_change_rejects_the_reload_with_v061() {
    let new = MINIMAL.replace(
        "state: { bucket: app-state }",
        "state: { bucket: app-state }\n  cluster: { tokens: 64 }",
    );
    let d = diff(&load(MINIMAL), &load(&new), &unknown);
    assert!(d.is_rejected());
    let e = &d.report.errors[0];
    assert_eq!(
        (e.rule.as_str(), e.path.as_str()),
        ("V-061", "globals.cluster.tokens")
    );
    assert_eq!(d.globals[0].class, Mutability::Proc);
}

#[test]
fn log_level_is_hot() {
    let new = MINIMAL.replace(
        "state: { bucket: app-state }",
        "state: { bucket: app-state }\n  log: { level: debug }",
    );
    let d = diff(&load(MINIMAL), &load(&new), &unknown);
    assert!(!d.is_rejected());
    assert_eq!(d.globals[0].path, "globals.log.level");
    assert_eq!(d.globals[0].class, Mutability::R);
}

#[test]
fn rotated_credentials_file_is_detected_without_a_text_change() {
    let files = Files::new();
    let path = files.write("creds.json", r#"{"user": "cdc", "password": "old"}"#);
    let yaml = MINIMAL.replace(
        "user: cdc\n      password: ${PW}",
        &format!("credentials_file: {}", path.display()),
    );
    let old = load(&yaml);
    files.write("creds.json", r#"{"user": "cdc", "password": "new"}"#);
    let new = load(&yaml);
    let d = diff(&old, &new, &unknown);
    let (restart, fields, _) = modified(&d);
    assert!(restart, "source credentials are PR");
    assert_eq!(fields[0].0, "pipelines.orders.source.credentials.password");
    let json = serde_json::to_string(&d).unwrap();
    assert!(
        !json.contains("old") && !json.contains("new\""),
        "values never appear: {json}"
    );
}

#[test]
fn rename_keeps_the_pipeline() {
    let new = MINIMAL.replace("  orders:\n    id: orders", "  orders-v2:\n    id: orders");
    let (restart, fields, renamed_from) = modified(&diff(&load(MINIMAL), &load(&new), &unknown));
    assert!(!restart);
    assert_eq!(renamed_from.as_deref(), Some("orders"));
    assert_eq!(
        fields,
        [("pipelines.orders.name".to_string(), Mutability::R)]
    );
}

#[test]
fn id_change_is_remove_plus_add() {
    let new = MINIMAL.replace("id: orders", "id: orders2");
    let d = diff(&load(MINIMAL), &load(&new), &unknown);
    assert!(matches!(&d.pipelines[..], [
        PipelineChange::Removed { id: a, .. },
        PipelineChange::Added { id: b, .. },
    ] if a == "orders" && b == "orders2"));
}

#[test]
fn include_change_restarts_only_when_the_table_set_changes() {
    let new = MINIMAL.replace("[public.orders]", "[public.orders, public.order_*]");
    let (old, new) = (load(MINIMAL), load(&new));
    let (restart, fields, _) = modified(&diff(&old, &new, &|_, _| Some(false)));
    assert!(!restart);
    assert_eq!(fields[0].1, Mutability::R);
    assert!(modified(&diff(&old, &new, &|_, _| Some(true))).0);
    assert!(
        modified(&diff(&old, &new, &unknown)).0,
        "unknown counts as changed"
    );
}

/// Every leaf of these configs is covered by a mutability pattern.
#[test]
fn every_resolved_path_has_a_class() {
    let files = Files::new();
    let (spec, spec_env) = spec_example(&files);
    let token = files.write("token", "tok\n");
    let (pem_cert, pem_key) = (cert(), key());
    let rich = MINIMAL
        .replace(
            "globals:\n  state: { bucket: app-state }",
            &format!(
                "globals:\n  cloud: azure\n  state: {{ bucket: app-state, prefix: app }}\n  azure: {{ account_name: lakeacct }}\n  api:\n    auth: basic\n    user: admin\n    password: x\n    tls: {{ enabled: true, cert: {c:?}, key: {k:?} }}\n  cluster:\n    memberlist:\n      join_members: [\"a:7946\"]\n      tls: {{ enabled: true, cert: {c:?}, key: {k:?}, ca: {c:?} }}\n  tracing: {{ otlp_endpoint: \"http://otel:4317\" }}",
                c = pem_cert,
                k = pem_key
            ),
        )
        .replace(
            "postgres: { publication: orders_pub, slot: orders_slot }",
            &format!("ssl_ca: {pem_cert:?}\n      postgres: {{ publication: orders_pub, slot: orders_slot, max_wal_lag_bytes: 1GiB }}"),
        )
        .replace(
            "include: [public.orders]",
            "include: [public.orders]\n      exclude: [public.tmp_*]\n      snapshots: { mode: initial, overrides: { public.orders: always } }\n      column_type_overrides: { public.orders.amount: \"decimal(38,10)\" }\n      primary_key: { public.orders: [id] }\n      partitioning: { public.orders: [ { column: region, transform: identity } ] }",
        )
        .replace(
            "delta: {}",
            "delta: { table_properties: { delta.targetFileSize: \"134217728\" } }\n      max_transaction_batch_rows: 50000\n    state: { bucket: state }",
        );
    let bearer = MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        &format!("sink:\n      type: iceberg\n      bucket: lake\n      iceberg:\n        catalog:\n          rest: {{ url: \"https://c\", auth: bearer, token_file: {}, tls: {{ ca: {pem_cert:?} }} }}", token.display()),
    );
    let glue = MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        "sink:\n      type: iceberg\n      bucket: lake\n      iceberg:\n        catalog:\n          glue: { name: \"123\" }\n          retry: { max_attempts: 3 }",
    );
    let configs = [
        load_str(&spec, &spec_env).unwrap().config,
        load(&rich),
        load(&bearer),
        load(&glue),
    ];
    for cfg in &configs {
        assert_eq!(unclassified_paths(cfg), Vec::<String>::new());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0583]: file not found for module `diff`
error[E0432]: unresolved import `mutability::Mutability`
error[E0425]: cannot find value `Proc` in this scope
error[E0425]: cannot find value `R` in this scope
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-config/src/diff.rs`:

```rust
//! The reload diff (config §12): the new resolved config against the running one, per global field
//! and per pipeline (keyed by `id`). Computed on resolved values (§11.1), so a changed
//! `credentials_file` or certificate is detected even when the config text is unchanged. The output
//! names fields and classes only, never values.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::diag::{Diagnostic, Locus, Report, Severity};
use crate::mutability::{Mutability, classify};
use crate::resolved::{Pipeline, ResolvedConfig};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldChange {
    /// The changed path (`globals.log.level`, `pipelines.orders.sink.flush.rows`).
    pub path: String,
    /// `R`, `Pr` or `Proc`, after the table-set check.
    pub class: Mutability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum PipelineChange {
    /// A new `id` (§12.3): start it.
    Added { id: String, name: String },
    /// An `id` gone (§12.3): stop, drain, clean up per `auto_cleanup`. An `id` change is a
    /// removal plus an addition.
    Removed { id: String, name: String },
    /// Same `id`, different fields. `restart` = some field is PR (§12.4); otherwise every change
    /// applies in place. `renamed_from` = the map key changed (a pure rename, §12.3).
    Modified {
        id: String,
        name: String,
        renamed_from: Option<String>,
        restart: bool,
        fields: Vec<FieldChange>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ConfigDiff {
    pub globals: Vec<FieldChange>,
    pub pipelines: Vec<PipelineChange>,
    /// V-061 errors: a PROC field changed. Any error rejects the reload (§11).
    pub report: Report,
}

impl ConfigDiff {
    pub fn is_empty(&self) -> bool {
        self.globals.is_empty() && self.pipelines.is_empty()
    }

    /// True when the reload must be rejected and the running config kept.
    pub fn is_rejected(&self) -> bool {
        !self.report.is_ok()
    }
}

/// Flattens a serialized value into `path → leaf`. Objects (structs and maps) recurse; arrays are
/// leaves, since a list changes as a whole (§8.3).
fn flatten(prefix: &[String], v: &Value, out: &mut BTreeMap<Vec<String>, Value>) {
    match v {
        Value::Object(map) => {
            for (k, v) in map {
                let mut path = prefix.to_vec();
                path.push(k.clone());
                flatten(&path, v, out);
            }
        }
        leaf => {
            out.insert(prefix.to_vec(), leaf.clone());
        }
    }
}

fn flat<T: Serialize>(prefix: &[&str], v: &T) -> BTreeMap<Vec<String>, Value> {
    let mut out = BTreeMap::new();
    let value = serde_json::to_value(v).expect("the resolved config serializes");
    flatten(
        &prefix.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        &value,
        &mut out,
    );
    out
}

/// Paths whose value differs, appears or disappears.
fn changed_paths(
    old: &BTreeMap<Vec<String>, Value>,
    new: &BTreeMap<Vec<String>, Value>,
) -> Vec<Vec<String>> {
    let mut paths: Vec<Vec<String>> = old
        .iter()
        .filter(|(k, v)| new.get(*k) != Some(v))
        .map(|(k, _)| k.clone())
        .chain(new.keys().filter(|k| !old.contains_key(*k)).cloned())
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

/// The class of a changed path; an uncovered path counts as `Pr` (a restart is always safe).
fn class_of(path: &[String]) -> Mutability {
    let segments: Vec<&str> = path.iter().map(String::as_str).collect();
    classify(&segments).unwrap_or(Mutability::Pr)
}

/// Diffs `new` against `old`. `table_set_changed(old, new)` answers whether a pipeline's resolved
/// table set changed (it needs the live source); `None` means "unknown", which counts as changed.
pub fn diff(
    old: &ResolvedConfig,
    new: &ResolvedConfig,
    table_set_changed: &dyn Fn(&Pipeline, &Pipeline) -> Option<bool>,
) -> ConfigDiff {
    let mut out = ConfigDiff::default();
    let mut old_g = flat(&["globals"], &old.globals);
    let mut new_g = flat(&["globals"], &new.globals);
    old_g.insert(vec!["config_version".into()], old.config_version.into());
    new_g.insert(vec!["config_version".into()], new.config_version.into());
    for path in changed_paths(&old_g, &new_g) {
        let class = class_of(&path);
        let text = path.join(".");
        if class == Mutability::Proc {
            out.report.push(Diagnostic {
                locus: Locus::Reload,
                path: text.clone(),
                rule: "V-061".into(),
                severity: Severity::Error,
                loc: None,
                message: format!(
                    "{text} changed: it needs a process restart, so the reload is rejected"
                ),
            });
        }
        out.globals.push(FieldChange { path: text, class });
    }

    for (id, o) in &old.pipelines {
        if !new.pipelines.contains_key(id) {
            out.pipelines.push(PipelineChange::Removed {
                id: id.clone(),
                name: o.name.clone(),
            });
        }
    }
    for (id, n) in &new.pipelines {
        let Some(o) = old.pipelines.get(id) else {
            out.pipelines.push(PipelineChange::Added {
                id: id.clone(),
                name: n.name.clone(),
            });
            continue;
        };
        let paths = changed_paths(&flat(&["pipelines", id], o), &flat(&["pipelines", id], n));
        if paths.is_empty() {
            continue;
        }
        let mut table_set: Option<bool> = None;
        let fields: Vec<FieldChange> = paths
            .iter()
            .map(|path| {
                let class = match class_of(path) {
                    Mutability::PrIfTableSetChanges => {
                        let changed = *table_set
                            .get_or_insert_with(|| table_set_changed(o, n).unwrap_or(true));
                        if changed {
                            Mutability::Pr
                        } else {
                            Mutability::R
                        }
                    }
                    c => c,
                };
                FieldChange {
                    path: path.join("."),
                    class,
                }
            })
            .collect();
        let restart = fields.iter().any(|f| f.class >= Mutability::Pr);
        out.pipelines.push(PipelineChange::Modified {
            id: id.clone(),
            name: n.name.clone(),
            renamed_from: (o.name != n.name).then(|| o.name.clone()),
            restart,
            fields,
        });
    }
    out
}

/// Every leaf path of `cfg` that no mutability pattern covers (for tests: must be empty).
pub fn unclassified_paths(cfg: &ResolvedConfig) -> Vec<String> {
    let mut all = flat(&["globals"], &cfg.globals);
    for (id, p) in &cfg.pipelines {
        all.extend(flat(&["pipelines", id], p));
    }
    all.keys()
        .filter(|p| classify(&p.iter().map(String::as_str).collect::<Vec<_>>()).is_none())
        .map(|p| p.join("."))
        .collect()
}
```

In `crates/data-avalanche-config/src/mutability.rs`, insert this above the `#[cfg(test)]` module:

```rust
//! Mutability classes (config §1.2) of the resolved config's paths: what a change to a field
//! triggers on reload.

use serde::Serialize;

/// Ordered by blast radius.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mutability {
    /// Applied in place, no interruption.
    R,
    /// `tables.include`/`exclude`: `Pr` if the resolved table set changes, else `R` (§1.2).
    PrIfTableSetChanges,
    /// The pipeline restarts (§12.4).
    Pr,
    /// Needs a process restart: the reload is rejected (V-061).
    Proc,
}

use Mutability::*;

/// Path patterns over the resolved config's serialized form (`*` = one segment: a pipeline id).
/// The longest matching pattern wins, so a block's class can be refined field by field.
/// Source: the Mutability columns of config §3, §6, §9.
pub const CLASSES: &[(&str, Mutability)] = &[
    ("config_version", Proc),
    ("globals.cloud", Proc),
    ("globals.state", Proc),
    ("globals.api.address", Proc),
    ("globals.api.auth", R),
    ("globals.api.credentials", R),
    ("globals.api.jwt", R),
    ("globals.api.oauth2", R),
    ("globals.api.role_mapping", R),
    ("globals.api.tls", Proc),
    ("globals.log", R),
    ("globals.cluster", Proc),
    ("globals.cluster.rebalance_stabilization", R),
    ("globals.cluster.max_concurrent_handoffs", R),
    ("globals.cluster.max_concurrent_takeovers", R),
    ("globals.cluster.drain_timeout", R),
    ("globals.tracing", R),
    ("globals.azure", Proc),
    ("pipelines.*.name", R),
    ("pipelines.*.id", Pr),
    ("pipelines.*.source", Pr),
    ("pipelines.*.source.postgres.auto_cleanup", R),
    ("pipelines.*.source.postgres.heartbeat", R),
    ("pipelines.*.source.postgres.slot_on_orphan", R),
    ("pipelines.*.source.postgres.max_wal_lag_bytes", R),
    ("pipelines.*.source.mysql.binlog_purge_action", R),
    ("pipelines.*.tables", Pr),
    ("pipelines.*.tables.include", PrIfTableSetChanges),
    ("pipelines.*.tables.exclude", PrIfTableSetChanges),
    ("pipelines.*.tables.snapshots.parallelism", R),
    ("pipelines.*.tables.snapshots.chunk_size", R),
    ("pipelines.*.sink", Pr),
    ("pipelines.*.sink.flush", R),
    ("pipelines.*.sink.max_transaction_batch_rows", R),
    ("pipelines.*.sink.max_transaction_buffer_bytes", R),
    ("pipelines.*.sink.buffer_capacity_rows", R),
    ("pipelines.*.sink.metrics", R),
    ("pipelines.*.sink.iceberg.catalog.retry", R),
    ("pipelines.*.sink.iceberg.catalog.hive.credentials", R),
    ("pipelines.*.sink.iceberg.catalog.hive.kerberos", R),
    ("pipelines.*.sink.iceberg.catalog.rest.token", R),
    ("pipelines.*.sink.iceberg.catalog.rest.oauth2", R),
    ("pipelines.*.sink.iceberg.catalog.rest.sigv4", R),
    ("pipelines.*.state", Pr),
    ("pipelines.*.schema_evolution", R),
];

fn matches(pattern: &str, path: &[&str]) -> Option<usize> {
    let pat: Vec<&str> = pattern.split('.').collect();
    (pat.len() <= path.len() && pat.iter().zip(path).all(|(p, s)| *p == "*" || p == s))
        .then_some(pat.len())
}

/// The class of a resolved-config path given as segments (`["pipelines", "orders", "sink",
/// "flush", "rows"]`), or `None` when no pattern covers it.
pub fn classify(path: &[&str]) -> Option<Mutability> {
    CLASSES
        .iter()
        .filter_map(|(pattern, class)| matches(pattern, path).map(|len| (len, *class)))
        .max_by_key(|(len, _)| *len)
        .map(|(_, class)| class)
}

```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 30; `tests/diff.rs` 10; `tests/load.rs` 12; `tests/merge.rs` 8; `tests/parse.rs` 8; `tests/raw.rs` 3; `tests/rules/main.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/src/mutability.rs \
  crates/data-avalanche-config/tests/diff.rs \
  crates/data-avalanche-config/src/diff.rs
git commit -m "feat(config): mutability classes and reload diff"
```


---

### Task 14: Rules that need runtime input; required fields without a V-rule

**Files:**
- Create: `crates/data-avalanche-config/src/external.rs`
- Modify: `crates/data-avalanche-config/src/lib.rs`, `crates/data-avalanche-config/tests/rules/structure.rs`, `crates/data-avalanche-config/src/rules/structure.rs`
- Test: `crates/data-avalanche-config/tests/external.rs`

**Interfaces:**
- Consumes: Tasks 8–13.
- Produces:
  - `ClusterAnchor { tokens: u32, lock_backend: LockBackend, lease_duration: Duration }`.
  - `check_cluster_anchor(&Globals, &ClusterAnchor) -> Report`: V-062, V-076, V-094 (M2/M8 call it
    at startup with the anchor from `globals.state`).
  - `check_table_set(&Pipeline, &[TableRef]) -> Report`: V-034 and the V-106 warning half, and the
    V-113 error (M5/M7 call it when the table set resolves).
  - `check_unbounded_numeric(&Pipeline, &[(TableRef, String)]) -> Report`: V-079, a warning (M5/M7
    call it at schema resolution).
  - The PARSE requiredness checks of D-M1.6.

The coverage test asserts two things:
- the rules checked here (plus V-061 in `tests/diff.rs`) are exactly the `RULES` entries whose input
  is not `Config`/`Resolution`;
- with Task 10's harness, every rule in §13 has a test.

- [ ] **Step 1: Write the failing tests**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index fa1fb41..3fc34d9 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -8,6 +8,7 @@ pub mod ctx;
 pub mod diag;
 pub mod diff;
 pub mod enums;
+pub mod external;
 pub mod leaf;
 pub mod load;
 pub mod material;
@@ -27,6 +28,7 @@ pub use coerce::{LeafType, LogFilter};
 pub use ctx::{At, Ctx};
 pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
 pub use diff::{ConfigDiff, FieldChange, PipelineChange, diff};
+pub use external::{ClusterAnchor, check_cluster_anchor, check_table_set, check_unbounded_numeric};
 pub use leaf::{Leaf, Loc, Tri};
 pub use load::{Loaded, load_file, load_str, validate_str};
 pub use material::{FileRef, Origin, Pem};
```

Create `crates/data-avalanche-config/tests/external.rs`:

```rust
//! Rules that need input besides the config file: the cluster anchor, the resolved table set,
//! source column types (and V-061, which `tests/diff.rs` covers).

mod support;

use std::time::Duration;

use data_avalanche_config::enums::LockBackend;
use data_avalanche_config::{
    ClusterAnchor, RULES, ResolvedConfig, RuleInput, check_cluster_anchor, check_table_set,
    check_unbounded_numeric, load_str,
};
use data_avalanche_core::TableRef;
use support::{MINIMAL, env};

fn load(yaml: &str) -> ResolvedConfig {
    load_str(yaml, &env(&[("PW", "pw")]))
        .unwrap_or_else(|r| panic!("{r:#?}"))
        .config
}

fn anchor() -> ClusterAnchor {
    ClusterAnchor {
        tokens: 128,
        lock_backend: LockBackend::Storage,
        lease_duration: Duration::from_secs(15),
    }
}

fn rules(r: &data_avalanche_config::Report) -> Vec<&str> {
    r.iter().map(|d| d.rule.as_str()).collect()
}

#[test]
fn anchor_rules_v062_v076_v094() {
    let cfg = load(MINIMAL);
    assert!(check_cluster_anchor(&cfg.globals, &anchor()).is_ok());
    let other = ClusterAnchor {
        tokens: 64,
        lock_backend: LockBackend::Kubernetes,
        lease_duration: Duration::from_secs(30),
    };
    let report = check_cluster_anchor(&cfg.globals, &other);
    // Diagnostics come sorted by path.
    assert_eq!(rules(&report), ["V-094", "V-076", "V-062"]);
    assert!(
        report
            .errors
            .iter()
            .all(|d| d.path.starts_with("globals.cluster."))
    );
}

#[test]
fn table_set_rules_v034_v106() {
    let yaml = MINIMAL.replace(
        "include: [public.orders]",
        "include: [public.*]\n      snapshots: { overrides: { public.orders: always, public.gone: always } }\n      primary_key: { public.gone: [id] }",
    );
    let cfg = load(&yaml);
    let p = &cfg.pipelines["orders"];
    let report = check_table_set(p, &[TableRef::new("public", "orders")]);
    assert_eq!(rules(&report), ["V-106", "V-034"]);
    assert!(report.errors.is_empty(), "both are warnings");
    let report = check_table_set(
        p,
        &[
            TableRef::new("public", "orders"),
            TableRef::new("public", "gone"),
        ],
    );
    assert!(report.iter().next().is_none());
}

#[test]
fn mysql_keys_resolve_in_source_db() {
    let yaml = MINIMAL
        .replace(
            "host: db.example.com",
            "type: mysql\n      host: db.example.com",
        )
        .replace(
            "postgres: { publication: orders_pub, slot: orders_slot }",
            "mysql: {}",
        )
        .replace(
            "include: [public.orders]",
            "include: [orders]\n      snapshots: { overrides: { orders: always } }",
        );
    let cfg = load(&yaml);
    let report = check_table_set(&cfg.pipelines["orders"], &[TableRef::new("shop", "orders")]);
    assert!(report.iter().next().is_none(), "{report:#?}");
}

#[test]
fn glue_strip_schema_collision_v113() {
    let yaml = MINIMAL.replace(
        "sink:\n      type: delta\n      bucket: lake\n      delta: {}",
        "sink:\n      type: iceberg\n      bucket: lake\n      iceberg:\n        catalog:\n          glue: { name: \"123\", strip_schema: true }",
    );
    let cfg = load(&yaml);
    let p = &cfg.pipelines["orders"];
    let report = check_table_set(
        p,
        &[
            TableRef::new("public", "orders"),
            TableRef::new("archive", "orders"),
        ],
    );
    assert_eq!(rules(&report), ["V-113"]);
    assert!(
        report.errors[0]
            .message
            .contains("archive.orders, public.orders")
    );
    assert!(
        check_table_set(
            p,
            &[
                TableRef::new("public", "orders"),
                TableRef::new("archive", "old")
            ]
        )
        .is_ok()
    );
}

#[test]
fn unbounded_numeric_v079() {
    let unbounded = [(TableRef::new("public", "orders"), "amount".to_string())];
    let cfg = load(MINIMAL);
    assert!(
        check_unbounded_numeric(&cfg.pipelines["orders"], &unbounded)
            .iter()
            .next()
            .is_none(),
        "default action string"
    );
    let cfg = load(&MINIMAL.replace(
        "include: [public.orders]",
        "include: [public.orders]\n      numeric_overflow_action: error",
    ));
    let report = check_unbounded_numeric(&cfg.pipelines["orders"], &unbounded);
    assert_eq!(rules(&report), ["V-079"]);
    assert!(report.warnings[0].message.contains("public.orders.amount"));
    assert!(
        check_unbounded_numeric(&cfg.pipelines["orders"], &[])
            .iter()
            .next()
            .is_none()
    );
}

/// Every rule that needs input besides the file has a test in this file or in `tests/diff.rs`.
#[test]
fn every_external_rule_is_covered() {
    const COVERED: [&str; 7] = [
        "V-034", "V-061", "V-062", "V-076", "V-079", "V-094", "V-113",
    ];
    let external: Vec<&str> = RULES
        .iter()
        .filter(|r| !matches!(r.input, RuleInput::Config | RuleInput::Resolution))
        .map(|r| r.id)
        .collect();
    assert_eq!(external, COVERED);
}
```

Change `crates/data-avalanche-config/tests/rules/structure.rs`:

```diff
diff --git a/crates/data-avalanche-config/tests/rules/structure.rs b/crates/data-avalanche-config/tests/rules/structure.rs
index d889e34..85f44d9 100644
--- a/crates/data-avalanche-config/tests/rules/structure.rs
+++ b/crates/data-avalanche-config/tests/rules/structure.rs
@@ -2,6 +2,8 @@
 
 use serde_json::json;
 
+use data_avalanche_config::diag::PARSE;
+
 use crate::cases::{Case, Edit, Expect::*, case, mysql_delta, pg_delta, pg_iceberg};
 
 pub fn cases() -> Vec<Case> {
@@ -214,5 +216,35 @@ pub fn cases() -> Vec<Case> {
         case("V-070", Silent, "token only", || {
             pg_iceberg().set("/pipelines/p/sink/iceberg/catalog/rest/token", json!("t"))
         }),
+        // Required fields with no V-rule are PARSE errors, checked after the merge.
+        case(PARSE, Error, "rest without url", || {
+            pg_iceberg().del("/pipelines/p/sink/iceberg/catalog/rest/url")
+        }),
+        case(PARSE, Silent, "rest url from defaults", || {
+            pg_iceberg()
+                .set(
+                    "/defaults/sink/iceberg/catalog/rest/url",
+                    json!("https://c.example.com"),
+                )
+                .del("/pipelines/p/sink/iceberg/catalog/rest/url")
+        }),
+        case(PARSE, Error, "glue without name", || {
+            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog", json!({"glue": {}}))
+        }),
+        case(PARSE, Error, "hive without url", || {
+            pg_iceberg().set("/pipelines/p/sink/iceberg/catalog", json!({"hive": {}}))
+        }),
+        case(PARSE, Error, "partition field without transform", || {
+            pg_delta().set(
+                "/pipelines/p/tables/partitioning",
+                json!({"public.orders": [{"column": "region"}]}),
+            )
+        }),
+        case(PARSE, Error, "partition field without column", || {
+            pg_delta().set(
+                "/pipelines/p/tables/partitioning",
+                json!({"public.orders": [{"transform": "identity"}]}),
+            )
+        }),
     ]
 }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p data-avalanche-config`
Expected: compilation FAILS. The first errors:

```text
error[E0583]: file not found for module `external`
```

- [ ] **Step 3: Implement**

Create `crates/data-avalanche-config/src/external.rs`:

```rust
//! Rules that need more than the config file. The runtime calls these with the extra input:
//! - the cluster anchor from `globals.state` at startup (V-062, V-076, V-094; storage in M2);
//! - a pipeline's resolved table set at start or on a table-set change (V-034, the warning half of
//!   V-106, V-113; sources in M5/M7);
//! - source column types at schema resolution (V-079).
//!
//! V-061 (a PROC field changed on reload) is part of [`crate::diff()`].

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use data_avalanche_core::TableRef;

use crate::ctx::{At, Ctx};
use crate::diag::Report;
use crate::enums::{LockBackend, NumericOverflowAction, SourceType};
use crate::path::YamlPath;
use crate::resolved::{Globals, Pipeline};
use crate::units::format_duration;

/// The values the first node wrote to the `globals.state` cluster anchor (Clustering §5.6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterAnchor {
    pub tokens: u32,
    pub lock_backend: LockBackend,
    pub lease_duration: Duration,
}

/// V-062, V-076, V-094: this node's cluster-wide settings must equal the anchor's. An error means
/// the node refuses to start.
pub fn check_cluster_anchor(globals: &Globals, anchor: &ClusterAnchor) -> Report {
    let mut ctx = Ctx::structural();
    let at = At::config(YamlPath::root().key("globals").key("cluster"));
    let c = &globals.cluster;
    if c.tokens != anchor.tokens {
        ctx.error("V-062", &at.key("tokens"), None, format!("tokens is {} but the cluster was established with {}; every node must use the same value", c.tokens, anchor.tokens));
    }
    if c.lock_backend != anchor.lock_backend {
        ctx.error("V-076", &at.key("lock_backend"), None, format!("lock_backend is {} but the cluster uses {}; a mixed-backend cluster has two arbiters", c.lock_backend, anchor.lock_backend));
    }
    if c.lease_duration != anchor.lease_duration {
        ctx.error(
            "V-094",
            &at.key("lease_duration"),
            None,
            format!(
                "lease_duration is {} but the cluster was established with {}",
                format_duration(c.lease_duration),
                format_duration(anchor.lease_duration)
            ),
        );
    }
    ctx.into_report()
}

/// A per-table config key as a table: PG `schema.table`; MySQL `name` in `source.db` (§10).
fn key_table(p: &Pipeline, key: &str) -> Option<TableRef> {
    match p.source.kind {
        SourceType::Postgres => key.split_once('.').map(|(s, t)| TableRef::new(s, t)),
        SourceType::Mysql => Some(TableRef::new(p.source.db.clone(), key)),
    }
}

fn pipeline_at(p: &Pipeline) -> At {
    At::pipeline(&p.name, YamlPath::root().key("pipelines").key(&p.name))
}

/// V-034 and the warning half of V-106 (per-table keys naming no resolved table), and V-113 (Glue
/// `strip_schema: true` with two tables of the same name). An error refuses the pipeline.
pub fn check_table_set(p: &Pipeline, tables: &[TableRef]) -> Report {
    let mut ctx = Ctx::structural();
    let at = pipeline_at(p).key("tables");
    let set: BTreeSet<&TableRef> = tables.iter().collect();
    let unknown = |key: &str| key_table(p, key).is_none_or(|t| !set.contains(&t));
    for key in p.tables.snapshots.overrides.keys().filter(|k| unknown(k)) {
        ctx.warning(
            "V-034",
            &at.key("snapshots").key("overrides").key(key),
            None,
            format!("{key} is not in the pipeline's resolved table set"),
        );
    }
    for (field, keys) in [
        (
            "primary_key",
            p.tables.primary_key.keys().collect::<Vec<_>>(),
        ),
        ("partitioning", p.tables.partitioning.keys().collect()),
    ] {
        for key in keys.into_iter().filter(|k| unknown(k)) {
            ctx.warning(
                "V-106",
                &at.key(field).key(key),
                None,
                format!("{key} is not in the pipeline's resolved table set"),
            );
        }
    }
    let strip = p
        .sink
        .iceberg
        .as_ref()
        .and_then(|i| i.catalog.glue.as_ref())
        .is_some_and(|g| g.strip_schema);
    if strip {
        let mut by_name: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        for t in tables {
            by_name.entry(&t.name).or_default().insert(t.to_string());
        }
        for (name, list) in by_name.iter().filter(|(_, l)| l.len() > 1) {
            ctx.error("V-113", &pipeline_at(p).key("sink").key("iceberg").key("catalog").key("glue").key("strip_schema"), None, format!(
                "strip_schema: true maps {} to the one Glue table {name}; set strip_schema: false or exclude all but one",
                list.iter().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
    }
    ctx.into_report()
}

/// V-079: with `numeric_overflow_action: error`, source `numeric` columns without a declared
/// precision may halt their table. `columns` are those columns, found at schema resolution.
pub fn check_unbounded_numeric(p: &Pipeline, columns: &[(TableRef, String)]) -> Report {
    let mut ctx = Ctx::structural();
    if p.tables.numeric_overflow_action == NumericOverflowAction::Error && !columns.is_empty() {
        let list: Vec<String> = columns.iter().map(|(t, c)| format!("{t}.{c}")).collect();
        ctx.warning("V-079", &pipeline_at(p).key("tables").key("numeric_overflow_action"), None, format!(
            "numeric_overflow_action error with unconstrained numeric columns {}: a value beyond 38 digits halts the table; consider a column_type_overrides entry",
            list.join(", ")
        ));
    }
    ctx.into_report()
}
```

Change `crates/data-avalanche-config/src/rules/structure.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/rules/structure.rs b/crates/data-avalanche-config/src/rules/structure.rs
index cba9b68..8e2c2de 100644
--- a/crates/data-avalanche-config/src/rules/structure.rs
+++ b/crates/data-avalanche-config/src/rules/structure.rs
@@ -2,6 +2,7 @@
 
 use super::{missing, pipeline_at};
 use crate::ctx::{At, Ctx};
+use crate::diag::PARSE;
 use crate::enums::{CatalogKind, SinkType, SourceType};
 use crate::leaf::Leaf;
 use crate::path::YamlPath;
@@ -108,6 +109,48 @@ fn pipeline(name: &str, p: &RawPipeline, ctx: &mut Ctx) {
     }
     let default = RawSink::default();
     sink(p.sink.as_ref().unwrap_or(&default), &at.key("sink"), ctx);
+    required_fields(p, &at, ctx);
+}
+
+/// Required fields that §13 gives no V-rule: a strict parse would report them as missing, but the
+/// raw model keeps every field optional so that layers can merge, so they are checked after the
+/// merge and reported as `PARSE`.
+fn required_fields(p: &RawPipeline, at: &At, ctx: &mut Ctx) {
+    fn require<T>(ctx: &mut Ctx, at: &At, key: &str, leaf: &Option<Leaf<T>>) {
+        if leaf.is_none() {
+            ctx.error(
+                PARSE,
+                &at.key(key),
+                None,
+                format!("missing required field `{key}`"),
+            );
+        }
+    }
+    let catalog = p
+        .sink
+        .as_ref()
+        .and_then(|s| s.iceberg.as_ref())
+        .and_then(|i| i.catalog.as_ref());
+    if let Some(c) = catalog {
+        let c_at = at.key("sink").key("iceberg").key("catalog");
+        if let Some(g) = &c.glue {
+            require(ctx, &c_at.key("glue"), "name", &g.name);
+        }
+        if let Some(h) = &c.hive {
+            require(ctx, &c_at.key("hive"), "url", &h.url);
+        }
+        if let Some(r) = &c.rest {
+            require(ctx, &c_at.key("rest"), "url", &r.url);
+        }
+    }
+    let partitioning = p.tables.as_ref().and_then(|t| t.partitioning.as_ref());
+    for (table, fields) in partitioning.into_iter().flatten() {
+        let t_at = at.key("tables").key("partitioning").key(table);
+        for (i, f) in fields.iter().enumerate() {
+            require(ctx, &t_at.index(i), "column", &f.column);
+            require(ctx, &t_at.index(i), "transform", &f.transform);
+        }
+    }
 }
 
 fn source(s: &RawSource, at: &At, ctx: &mut Ctx) {
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p data-avalanche-config`
Expected: PASS. unit tests (`src/lib.rs`) 30; `tests/diff.rs` 10; `tests/external.rs` 6; `tests/load.rs` 12; `tests/merge.rs` 8; `tests/parse.rs` 8; `tests/raw.rs` 3; `tests/rules/main.rs` 3.

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings, no output.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/lib.rs \
  crates/data-avalanche-config/tests/external.rs \
  crates/data-avalanche-config/tests/rules/structure.rs \
  crates/data-avalanche-config/src/external.rs \
  crates/data-avalanche-config/src/rules/structure.rs
git commit -m "feat(config): runtime-input rules and requiredness without V-rules"
```


---

### Task 15: Crate documentation and M1 exit checks

**Files:**
- Modify: `crates/data-avalanche-config/src/lib.rs`

**Interfaces:**
- Consumes: everything above.
- Produces: the crate docs, which say how each later milestone uses the crate.

- [ ] **Step 1: Write the crate docs**

Change `crates/data-avalanche-config/src/lib.rs`:

```diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 3fc34d9..3f34bc7 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -1,6 +1,22 @@
 //! data-avalanche configuration (`doc/specs/Configuration_Specification.md`): the YAML model,
 //! strict parse, `${VAR}` and file resolution, merge of `defaults`, the V-rule validators, the
 //! resolved config the runtime uses, and the reload diff.
+//!
+//! How the runtime uses it:
+//!
+//! - **Startup and reload:** [`load_file`] (or [`load_str`]) returns a [`ResolvedConfig`] plus
+//!   warnings, or a [`Report`] of every error. Any error rejects the whole config (§1.4).
+//! - **Validation endpoint:** [`validate_str`] runs the same phases and returns only the report;
+//!   it never exposes the resolved config (§11.1). [`Report::to_json`] is the response body.
+//! - **Reload:** [`diff()`] compares the running and the new config, classifying each changed
+//!   field by [`Mutability`]; a changed restart-only field rejects the reload (V-061).
+//! - **Rules that need more than the file:** [`check_cluster_anchor`] at startup,
+//!   [`check_table_set`] when a pipeline's tables resolve, [`check_unbounded_numeric`] at schema
+//!   resolution.
+//!
+//! Every diagnostic carries its V-rule ID ([`RULES`] is the §13 master table), or [`PARSE`] /
+//! [`TYPE`] for YAML and value-type errors. [`Secret`] values never print: `Debug` shows `***` and
+//! `Serialize` a fingerprint.
 
 pub mod build;
 pub mod coerce;
```

- [ ] **Step 2: Build the docs with warnings as errors**

Run: `RUSTDOCFLAGS="-D warnings" cargo doc -p data-avalanche-config --no-deps`
Expected: finishes with no warnings. Every intra-doc link resolves.

- [ ] **Step 3: Run the M1 exit checks**

Run: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all --check`
Expected: PASS. Config crate: unit tests (`src/lib.rs`) 30; `tests/diff.rs` 10; `tests/external.rs` 6; `tests/load.rs` 12; `tests/merge.rs` 8; `tests/parse.rs` 8; `tests/raw.rs` 3; `tests/rules/main.rs` 3. Core: 76. No clippy warnings, no fmt output.

- [ ] **Step 4: Commit**

```bash
git add crates/data-avalanche-config/src/lib.rs
git commit -m "docs(config): crate overview for runtime users"
```


---

## M1 exit checklist

The roadmap's M1 exit criteria, and where each is met:

- [ ] **Every V-rule has at least one passing and one failing fixture.**
  - Config-time rules: `tests/rules/main.rs` `every_config_rule_has_a_passing_and_a_failing_case`
    (V-069 and V-087 are recorded exceptions, D-M1.7).
  - Rules that need runtime input: `tests/external.rs` `every_external_rule_is_covered`.
  - V-061: `tests/diff.rs`.
- [ ] **The §14 example parses and resolves**, with zero warnings: `tests/load.rs` `spec_example_resolves`.
- [ ] **Diff classification is tested for each mutability class and for conditional PR**:
  - `tests/diff.rs` covers R, PR, PROC, `log.level`, include with and without a table-set change,
    rename, and id change;
  - `every_resolved_path_has_a_class` covers the whole table.
- [ ] **Error messages carry the YAML path and, when available, line/column**: `tests/parse.rs`,
  `tests/load.rs`, and the harness output format.
- [ ] `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo fmt --all --check` and `RUSTDOCFLAGS="-D warnings" cargo doc -p data-avalanche-config --no-deps`
  are clean.
- [ ] Roadmap §3 gains the landed M1 interface (`load_file`, `ResolvedConfig`, `diff`, the external
  checks), per the doc/v1.0 README.
- [ ] The implementation matches the spec as amended by S-H…S-L (applied 2026-09-27).
