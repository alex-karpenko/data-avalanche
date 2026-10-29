---
updated: 2026-10-29
domain: Architecture
tags: [v1, implementation-plan, m11b, documentation, starlight, astro, github-pages, xtask, config-reference, openapi, runbooks]
resolves: []
---

# M11b — Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The second half of roadmap M11 (Q-M11-A): the documentation site, published to GitHub
Pages from `main` (Q-M11-2).
- the configuration reference, the validation rules and the management API reference, generated
  from the code by `cargo xtask docs` and checked against it by a test;
- the hand-written pages: the config guide (sources, storage, clusters, the sink, examples), the
  architecture at an operator's depth, the deployment (the image, the chart, observability), and
  the runbooks;
- the exit: the site builds with its link validation (a missing page or anchor fails it), and
  every config example of the docs validates.

**Architecture:**

- **The config model describes its keys** (Task 1, `data-avalanche-config`): `walk_fields!`,
  which already lists every raw struct's fields, also generates a `Keys` impl, so
  `config_keys()` yields every key of the file (147) with its shape (a value, a list, a map…),
  the `LeafType`'s type phrase and value rule. `key_mutability(path)` gives its reload class from
  `mutability::classify`. `FIELD_DOCS`, a hand-written table, gives each key its default and its
  meaning; tests tie it to the model (one row per key, in the model's order; every class known;
  every default written as a value equal to the one the resolved config applies).
- **`cargo xtask docs`** (Task 2, a new `xtask` crate and a cargo alias): renders
  `reference/configuration.md` (a table per block: type, default, reload class, rules, meaning),
  `reference/validation-rules.md` (every V-rule with an anchor, its severity and the key it
  names) and `reference/api.md` (every `/v1` route of the OpenAPI document by tag), and copies
  `openapi.json` beside the pages. `--check` exits 1 naming a stale page; the workspace test
  `the_generated_pages_are_current` does the same in CI.
- **The site** (Task 3, `docs/`): Starlight 0.42.7 on Astro 7.4.4 (Q-M11b-1), its links validator
  failing the build on a missing page, a missing anchor or a relative link, Mermaid diagrams,
  Pagefind search. Every page is plain Markdown (`.md`), except two runbooks with per-engine tabs
  (`.mdx`). `docs.yml` builds the site on a pull request that touches it, and builds and deploys
  it on a push to `main` through `actions/deploy-pages` (Q-M11b-4). The workspace test
  `every_config_example_in_the_docs_validates` loads every whole-file YAML example of the pages
  (a block starting `config_version:`) with `validate_str`. The chart gains `minReadySeconds`
  (30), which M11a left to these pages.
- **The gap tracker** (Task 4) records the doc obligations the pages close.

**Tech Stack:** Rust 1.98.1; Node 24 (npm 11), `@astrojs/starlight` 0.42.7, `astro` 7.4.4,
`starlight-links-validator` 0.27.0, `astro-mermaid` 2.2.0, `mermaid` 12.2.0, `sharp` 0.35.5
(exact pins, `package-lock.json` committed); `actions/setup-node` v7,
`actions/upload-pages-artifact` v5, `actions/deploy-pages` v5; `rcgen` (the workspace's) for the
examples' throwaway certificates.

**Spec:**
- `doc/specs/System-Overview.md`: the v1 scope, the snapshot modes, the clustering requirements
  ("a restart does not apply config-file changes"), the API endpoint table and the observability
  catalogue.
- `doc/specs/Configuration_Specification.md`: §2–§4 (the file, `${VAR}`, values, secrets), §6
  `globals`, §7–§8 (defaults and merging), §9 (pipelines), §10 (paths), §11 (validation, the
  start-time preconditions), §12 (reload), §13 (the V-rules).
- `doc/specs/Data-Flow.md`: the phases, the sink contract, the error paths, the lineage columns.
- `doc/specs/Schema_Change_Detection_and_Evolution.md`: §3 (detection), §7.2 (the DDL table).
- `doc/research/Documentation_Site_Generator.md` (the tool, Q-M11b-1),
  `Clustering_Architecture_Research.md` (§5, §8, §9, §19), `Postgres_Slot_and_Publication_Lifecycle.md`,
  `Operational_and_Scope_Gaps_Batch.md` (G-22, G-26, G-27, G-37),
  `Multi_Sink_Architecture_and_Delta_Lake.md` (D-MS.15, R-5), `Type_Mapping.md` (Hop 3b),
  `Delivery_Guarantee_and_Position_Guard.md`.
- `doc/v1.0/00-roadmap.md`: P-6, §2 (the layout), §4 M11b, the R-5 and R-12 rows.

## Global Constraints

- Toolchain `1.98.1`, edition `2024`. Before every commit these pass:
  - `cargo fmt --all --check`;
  - `cargo clippy --workspace --all-targets -- -D warnings`;
  - `cargo clippy -p data-avalanche --features e2e-hooks --all-targets -- -D warnings`;
  - `cargo test --workspace`;
  - from Task 3 on, `npm ci && npm run build` in `docs/`, ending "All internal links are valid".
- **The binary doesn't change.** The config crate gains public items (`config_keys`,
  `key_mutability`, `FIELD_DOCS` and their types) that nothing at run time calls; the chart gains
  one value. Nothing else outside `xtask/`, `docs/` and the workflow changes.
- **Generated files are never edited by hand:** `docs/src/content/docs/reference/*.md` and
  `docs/public/openapi.json` are `cargo xtask docs`'s output. A change to the config model, its
  rules or the API changes them; the test names the stale ones.
- **Links between pages are absolute under the site's base**, `/data-avalanche/…`, with a
  trailing slash: the validator refuses a relative link (its default, kept).
- **A whole-file config example starts with `config_version:`** and must validate. Examples
  carry no credential and no file path: secrets are `${VAR}`. The test's environment gives a
  `${…_CERT}`, `${…_KEY}` or `${…_CA}` a throwaway PEM made at run time, a `${…_CREDENTIALS}` a
  credentials file under `CARGO_TARGET_TMPDIR`, a `${…_IP}` an address, any other a stand-in.
- **Pins:** the npm packages by exact version, the lock committed
  (`sha256 58b60a26…8771f`, 6529 lines, from `npm install` on 2026-10-29); Node from
  `docs/.nvmrc` (`24`); every action by its major, as the existing workflows do.
- **Plain `cargo test` stays hermetic:** the `xtask` tests read the repository's files and write
  one file under `CARGO_TARGET_TMPDIR`. The site's build needs Node and the network for `npm ci`;
  it runs in `docs.yml`, not in `ci.yml`.
- **Nothing is published before the merge.** `docs.yml` deploys only from `main`; Oleksii sets
  the repository's Pages source to "GitHub Actions" before the first deploy.
- **The pages state what was measured.** A claim that rests on a test or a run says so where it
  matters (the privileges' provenance, the DuckDB limit, the walk-through), and a page never
  promises behaviour the code doesn't have.
- Code blocks are the verified content, already formatted. Every block is a `git apply`-ready
  diff, one per file; a step's diffs apply in the order given, after the earlier steps'.
  `Cargo.lock`, `docs/package-lock.json` and the generated files are produced by the commands
  the steps name, not shown.
- One commit per task, with the message given. Every commit ends with the session's attribution
  trailer, if one is configured.

## Review Focus

The conditions most likely to hurt a reader of the docs, each pinned by a test or a check in the
task after the arrow.

1. **An example that no longer loads.** A key renamed, a new required key, a rule tightened: a
   reader copies an example and the node refuses it → Task 3
   (`every_config_example_in_the_docs_validates`, with the file and line of the block).
2. **The reference drifting from the code.** A key added to the model without a row, a default
   changed in code, a new route or rule: the reference says something the node doesn't do →
   Task 1 (`every_key_of_the_model_has_one_row_in_its_order`,
   `every_default_stated_as_a_value_is_the_resolved_one`), Task 2
   (`the_generated_pages_are_current`, the same under a workspace build's `serde_json` features).
3. **A link to a heading that moved.** A runbook links to an anchor that a rename removed → Task 3
   (the links validator fails the build; `docs.yml` runs it on every PR touching `docs/`).
4. **The walk-through with a real role.** A reader's role doesn't own the tables, or a DBA owns
   the slot: the pipeline stops in `error` and the docs must say why and what to set → Task 3
   (the getting-started and privileges pages, from the planning run: "must be owner of table …",
   the orphan slot and `slot_on_orphan: adopt_resnapshot`; see Verification).
5. **A rolling update that outruns its pipelines.** The next pod rolls before the replaced one's
   pipelines are back, moving some twice → Task 3
   (`a_rollout_waits_for_the_pipelines_to_come_back`).

## Decisions and rulings

**Asked and answered** (2026-10-29). Q-M11b-1 followed a research spike Oleksii asked for
(`doc/research/Documentation_Site_Generator.md`); Q-M11b-2…6 are each the "Recommended" answer.
Oleksii approved the plan 2026-10-29, with the chart's `minReadySeconds` (D-M11b.9); F-4's spec
patch waits for his word.

| # | Question | Answer |
|---|---|---|
| Q-M11b-1 | Which site generator? (P-6 named MkDocs; mkdocs-material reaches end of life 2027-05-05) | **Starlight**, pinned, with `starlight-links-validator`: the one tool of six that met every need with no custom code, and the best UI of them. Runner-up mdBook (no link check, no tabs) |
| Q-M11b-2 | Where does the configuration reference come from? | **A field table in the config crate** (path, default, meaning), tied to the raw model by tests; `xtask` renders it with the types, the V-rules and the reload classes |
| Q-M11b-3 | Where does the API reference come from? | **Markdown rendered by `xtask`** from the OpenAPI document, and the document itself beside the pages, with a staleness test |
| Q-M11b-4 | How is the site published? | **`actions/deploy-pages` from `docs.yml`**; Oleksii sets the Pages source to "GitHub Actions" |
| Q-M11b-5 | How much of the pages does the plan carry? | **Their full text**, each page written and checked in a scratch tree: the build with its link validation, every example validated |
| Q-M11b-6 | How deep is the architecture section? | **An operator's depth**: six pages with diagrams, linking to the specs for the rest |

**Rulings** (planning, 2026-10-29):

| # | Ruling | Why | Pinned by |
|---|---|---|---|
| D-M11b.1 | **The site's toolchain** is exact: Starlight 0.42.7, Astro 7.4.4, the links validator 0.27.0, `astro-mermaid` 2.2.0, `mermaid` 12.2.0, `sharp` 0.35.5, the lock committed, Node 24. `site` is `https://alex-karpenko.github.io`, `base` `/data-avalanche` | Starlight is pre-1.0 and its config has changed between minors (0.39 dropped labelled `autogenerate`, which the spike's config used) | Task 3's build |
| D-M11b.2 | **Every link is absolute under the base**, the validator's defaults kept (relative links and invalid hashes are errors); `/data-avalanche/openapi.json`, a file of `public/`, is excluded from it | The validator resolves absolute links against the base; a relative one is its documented error | Task 3's build; a broken link, a broken anchor and a relative link each failed it (exit 1) |
| D-M11b.3 | **`config_keys()` is generated from the raw model** by `walk_fields!` (a `Keys` impl per struct; `Option<T>` describes `T::default()`), paths written with `*` for a map's key and `pipelines.*` for a pipeline, `.*[]` for a partition list. `defaults` isn't listed: it takes a pipeline's keys | A key list kept by hand would drift; the macro already walks every field for the V-rules | Task 1 (`every_key_of_the_model_has_one_row_in_its_order`) |
| D-M11b.4 | **`FIELD_DOCS` states a default three ways**: none, a value (checked against the resolved config of a minimal file with a PostgreSQL, a MySQL, a REST and a Glue pipeline, under `auth: none`, `jwt` and `oauth2`), or a note for one that depends on context (`port`, `server_id`, `sink.prefix`, `node_name`, `sample_ratio`, `state.*`) or that no valid file can resolve (the Hive `auth`, `kerberos.service_name`, `tls.enabled`, V-115; `globals.api.tls.enabled`, which resolves as `tls: null`). Iceberg rows start "v2:" | A value the test can't check is stated as a note, never as a value | Task 1 (`every_default_stated_as_a_value_is_the_resolved_one`, a mutation of `lease_duration` caught) |
| D-M11b.5 | **A rule is placed by its "Path / Scope" text**: its leading dotted path, each `a / b` alternative and `{x,y}` branch, a pipeline's field under `pipelines.*`. A path that is a key puts the rule on that key's row, a block's on the block, anything else only on the rules page. The enum rule of a key's type (`LeafType::RULE`) is always on its row | The registry carries no key per rule; the text is the spec's §13 column | Task 2 (`a_rule_names_its_key_its_block_or_neither`) |
| D-M11b.6 | **The generated pages are plain `.md`**, their frontmatter strings quoted (`{:?}`), `editUrl: false`, and a comment saying they are generated. A table cell escapes `\|` everywhere and `<`, `>`, `&` outside code spans | `.md` needs no MDX escaping; a description with a colon broke YAML (measured) | Task 2 (the pages build in Task 3) |
| D-M11b.7 | **The API page sorts what it reads**, and `openapi.json` is utoipa's own pretty JSON | A workspace build turns on `serde_json`'s `preserve_order` (DataFusion does), which reorders a `serde_json::Value`: the staleness test failed under `cargo test --workspace` and passed under `-p xtask` until then (measured). The file is now byte-identical to the API crate's `openapi.json` | Task 2 (`the_generated_pages_are_current` under both builds) |
| D-M11b.8 | **`docs.yml`** builds on a pull request touching `docs/**` or itself, and on a push to `main` uploads the artifact and deploys it (`pages: write`, `id-token: write` on the deploy job only, the `github-pages` environment). The generated pages' staleness and the examples are `ci.yml`'s, through the workspace tests | `ci.yml` runs on pull requests only and already runs the workspace tests; the deploy needs `main` | Task 3 |
| D-M11b.9 | **The chart's `minReadySeconds: 30`**, above `rebalance_stabilization` (15 s): a rolling update moves to the next pod once the replaced pod's pipelines are back. A chart change, the one outside `docs/` and `xtask/` | M11a left it to these pages; documenting a setting the chart can't set would leave the reader nothing to do | Task 3 (`a_rollout_waits_for_the_pipelines_to_come_back`) |
| D-M11b.10 | **The hand-written pages are one task.** They link to each other in every direction (a runbook to the architecture, the architecture to the runbooks), so any split would leave a build failing its link check in between | The task's reviewer still reads them by section | Task 3 |
| D-M11b.11 | **Operator-facing facts come from the code where the specs are silent**: the halt codes (`HaltReason`), the actual states (`ActualState`), the slot decision table (`slot.rs`), the exit codes, the chart's values, the object-store variables the E2E tests use | The docs describe the binary as built | Task 3 |

**Findings raised by the planning** (not fixed by M11b; the tracker records them when the plan is
committed, §49):

| # | Finding | Proposed |
|---|---|---|
| F-1 | **A process-restart key written as a literal can't change once the cluster has a generation**: a reload refuses it (V-061, `diff.rs`), and a restarted node adopts the stored generation (`startup.rs`). Only a `${VAR}` value (each node resolves it from its environment) or a file's contents (re-read at start) changes, by a rolling restart. Stuck: `lease_renewal_interval`, `lease_safety_margin`, `discovery`, `discovery_dns_name`, `join_members`, the memberlist timers, `globals.api.tls.enabled`, `globals.azure.account_name` (`tokens`, `lock_backend`, `lease_duration` are pinned by design) | New gap DA-OPS-12 (🟢). The docs give the workaround (write them as `${VAR:-default}`; otherwise a new `globals.state` with every node stopped, untested) |
| F-2 | **`BINLOG_SOURCE_CHANGED` without GTIDs has no in-place recovery**: its message says "switch to gtid or re-snapshot", but a pipeline in `error` can't take a snapshot job (`409` unless running and held), and `same_server` doesn't follow `binlog_purge_action` | New gap DA-SRC-29 (🟢). The docs: switch to `gtid` with `binlog_purge_action: re_snapshot`, or replace the pipeline (a new `id` and prefix) |
| F-3 | **Two operator-facing texts are wrong**: the orphan-slot refusal says `slot.on_orphan: adopt_resnapshot` (the key is `source.postgres.slot_on_orphan`; seen in the planning run), and the CLI's `about` says "from PostgreSQL into Delta Lake" (MySQL and MariaDB missing) | New gap DA-OPS-13 (🟢): two one-line fixes, for M12 or a fix PR |
| F-4 | **The config spec's §14 example sets `otlp_endpoint` to `:4317`**, the OTLP/gRPC port, while §6.6 and Q-M10-1 say OTLP/HTTP on `:4318` | A spec patch, which needs Oleksii's approval; the pages use `:4318` |
| F-5 | **The research doc's 390 px captures were clipped by headless Chrome**, whose layout viewport is at least 500 px whatever the window (measured: `innerWidth` 500 at `--window-size=390`). Its "the wide table runs past the edge in every tool" is that artifact | A `research:` correction to `Documentation_Site_Generator.md` §4, committed with the plan. At 500 px the M11b site keeps its width and each wide table scrolls in its own box (`custom.css`) |

## Verification (planning, 2026-10-29)

Every block below was replayed as six stage commits on `e6654f0` in a scratch worktree; the tree
after the last one equals the one the pages were written in, file for file.

- **The quality gates** on the final stage: `cargo fmt --all --check`, both clippy runs and
  `cargo test --workspace` green: 1348 tests in 60 binaries, none failed.
- **Each test's failure before its implementation**, recorded in the steps: Task 1's tests don't
  compile without the table; Task 2's finds the four pages stale; Task 3's examples test doesn't
  compile without `config_examples`, and the chart test panics on `no "minReadySeconds: "`.
- **Mutations**: `lease_duration` stated as `20s` fails Task 1's defaults test; an invalid slot
  name in an example fails Task 3's with `configuration/examples.md:118: V-058 …`; a link to a
  missing page, to a missing anchor and a relative link each fail the site's build (exit 1).
- **The site**: `npm ci` from the lock and `npm run build`: 29 pages, "All internal links are
  valid", 7 config examples. Captured with headless Chrome at 1440 px (light and dark) and 500 px.
- **The walk-through**, run on test-infra's `postgres-18` and RustFS with the debug binary and a
  role with `LOGIN REPLICATION`, `CREATE` on the database and `SELECT` on the tables, which it
  doesn't own:
  - `managed` (the default): `actual_state: error`, "must be owner of table order_lines
    (SQLSTATE 42501)";
  - a publication and a slot made by the owner, `slot_management: require_existing`: refused as
    an orphan at the first start ("…exists but the pipeline has no state…");
  - `slot_on_orphan: adopt_resnapshot` added to the file and the node restarted: it ran the
    stored generation 1 and warned "the cluster runs config generation 1 from globals.state, not
    this node's file"; `POST /v1/config/validate` listed the change (class `r`),
    `POST /v1/config/reload` published generation 2, the pipeline stayed in `error`, and
    `POST /v1/pipelines/shop/restart` brought it to `running` (`pg:2/5874D8A0`);
  - an insert, an update and a delete: `rows_read_total` 4 snapshot rows and one of each op,
    `rows_written_total` 7, the lag 0, no halted table. The slot, publication, database and role
    were dropped after.

## File map

| File | Task | What |
|---|---|---|
| `crates/data-avalanche-config/src/keys.rs` | 1 | `Key`, `KeyShape`, `config_keys`, `key_mutability`; the crate-private `Keys` trait, its leaf impls, `child`, `resolved_path` |
| `crates/data-avalanche-config/src/walk.rs` | 1 | `walk_fields!` also generates `Keys` for each struct and its `Option` |
| `crates/data-avalanche-config/src/field_docs.rs` | 1 | `DocDefault`, `FieldDoc`, `FIELD_DOCS` (147 rows), its four tests |
| `crates/data-avalanche-config/src/lib.rs` | 1 | the two modules and their re-exports |
| `Cargo.toml`, `.cargo/config.toml` | 2 | `xtask` a member; the `cargo xtask` alias |
| `xtask/Cargo.toml`, `xtask/src/{lib,main,config,rules,api}.rs` | 2 | the generators and the command |
| `xtask/tests/docs.rs` | 2, 3 | the staleness test; the examples test |
| `docs/src/content/docs/reference/*.md`, `docs/public/openapi.json` | 2 | generated |
| `xtask/src/examples.rs` | 3 | `ConfigExample`, `config_examples` |
| `docs/{package.json,package-lock.json,astro.config.mjs,.nvmrc,.gitignore}`, `docs/src/content.config.ts`, `docs/src/styles/custom.css` | 3 | the site |
| `docs/src/content/docs/**` (25 hand-written pages) | 3 | the pages |
| `.github/workflows/docs.yml` | 3 | build on PR, deploy from `main` |
| `deploy/helm/data-avalanche/{values.yaml,templates/statefulset.yaml,README.md}`, `crates/data-avalanche/tests/packaging.rs` | 3 | `minReadySeconds` and its test |
| `doc/analysis/CDC_Design_Gap_Analysis_v1.md` | 4 | the doc obligations closed |

**On approval**, before Task 1, these planning commits go on the branch:
- `infra: M11b plan — …`: this file, and the index row in `doc/v1.0/README.md`.
- `infra: roadmap — M11b on Starlight`: P-6 and §4 M11b say "an Astro Starlight site" for
  "MkDocs"; the exit says "the site builds with its link validation" for `mkdocs build --strict`;
  §2's `docs/` line says "Starlight site (`astro.config.mjs`, `package.json`, the pages under
  `src/content/docs/`)"; §9 gains a D-IMPL row for D-M11b.1…11 and §9.1 the rows Q-M11b-1…6.
- `gap: v1 §49 — M11b planning`: the plan, and the new gaps DA-OPS-12, DA-SRC-29, DA-OPS-13
  (F-1…F-3).
- `research: documentation site generator — the 390 px captures`: F-5's correction.
F-4 waits for Oleksii's word on the spec.

---

### Task 1: The config keys and their documentation

**Files:**
- Create: `crates/data-avalanche-config/src/keys.rs`, `crates/data-avalanche-config/src/field_docs.rs`
- Modify: `crates/data-avalanche-config/src/walk.rs`, `crates/data-avalanche-config/src/lib.rs`

**Interfaces:**
- Consumes: `walk_fields!` and `walk_key!` (`walk.rs`), `LeafType::{EXPECTED, RULE}`,
  `mutability::classify`, `RawPipeline::default()`, `load_str`, `ResolvedConfig: Serialize`.
- Produces (re-exported at the crate root, used by Task 2):
  - `pub enum KeyShape { Scalar, Nullable, List, NullableList, Map, MapOfLists }`;
  - `pub struct Key { pub path: String, pub shape: KeyShape, pub expects: &'static str, pub rule: &'static str }`;
  - `pub fn config_keys() -> Vec<Key>`: `config_version`, the `globals` keys, then a pipeline's
    under `pipelines.*`, in the model's order;
  - `pub fn key_mutability(path: &str) -> Option<Mutability>`;
  - `pub enum DocDefault { None, Value(&'static str), Note(&'static str) }`,
    `pub struct FieldDoc { pub path: &'static str, pub default: DocDefault, pub doc: &'static str }`,
    `pub const FIELD_DOCS: &[FieldDoc]`.

- [ ] **Step 1: Write the failing tests**

`field_docs.rs` starts as its module doc and its tests, and `lib.rs` declares it:

`crates/data-avalanche-config/src/field_docs.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
new file mode 100644
index 0000000..72bd081
--- /dev/null
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -0,0 +1,167 @@
+//! The configuration reference's prose (M11b): one line per key of [`crate::config_keys`], with
+//! its default. The key's type, value rule and mutability come from the model itself; this table
+//! carries what the model can't — what the key means — and is tied to the model by tests: every
+//! key has exactly one row, in the model's order, and every default stated as a value is the one
+//! the resolved config applies.
+
+#[cfg(test)]
+mod tests {
+    use std::collections::BTreeMap;
+    use std::str::FromStr;
+    use std::time::Duration;
+
+    use serde_json::Value as Json;
+
+    use super::*;
+    use crate::{ByteSize, FingerprintKey, config_keys, key_mutability, load_str};
+
+    #[test]
+    fn every_key_of_the_model_has_one_row_in_its_order() {
+        let keys: Vec<String> = config_keys().into_iter().map(|k| k.path).collect();
+        let rows: Vec<&str> = FIELD_DOCS.iter().map(|d| d.path).collect();
+        assert_eq!(rows, keys);
+    }
+
+    #[test]
+    fn every_key_has_a_mutability_class() {
+        for k in config_keys() {
+            assert!(key_mutability(&k.path).is_some(), "{}", k.path);
+        }
+    }
+
+    /// A PostgreSQL, a MySQL and two Iceberg pipelines (REST and Glue: valid in the file, refused
+    /// at start), each with every defaulted key left out; `{auth}` is the API's block, so the JWT
+    /// and OAuth2 blocks each resolve once.
+    const MINIMAL: &str = r#"
+config_version: 1
+globals:
+  state: { bucket: state }
+  api: {auth}
+pipelines:
+  pg:
+    id: pg
+    source: { host: h, db: d, user: u, password: p, postgres: { publication: pb, slot: sl } }
+    tables: { include: ["public.*"] }
+    sink: { type: delta, bucket: lake }
+  my:
+    id: my
+    source: { type: mysql, host: h, db: d, user: u, password: p }
+    tables: { include: ["*"] }
+    sink: { type: delta, bucket: lake }
+  rest:
+    id: rest
+    source: { host: h, db: d, user: u, password: p, postgres: { publication: pr, slot: sr } }
+    tables: { include: ["public.*"] }
+    sink: { type: iceberg, bucket: lake, iceberg: { catalog: { rest: { url: "https://c" } } } }
+  glue:
+    id: glue
+    source: { host: h, db: d, user: u, password: p, postgres: { publication: pg2, slot: sg } }
+    tables: { include: ["public.*"] }
+    sink: { type: iceberg, bucket: lake, iceberg: { catalog: { glue: { name: "1" } } } }
+"#;
+
+    fn at<'a>(root: &'a Json, path: &str) -> Option<&'a Json> {
+        path.split('.').try_fold(root, |v, seg| v.get(seg))
+    }
+
+    /// The resolved value at a key's path: the one global value, or the value in each pipeline
+    /// that leaves the key out of the file (`written` is the file as JSON).
+    fn resolved_values<'a>(resolved: &'a Json, written: &Json, path: &str) -> Vec<&'a Json> {
+        match path.strip_prefix("pipelines.*.") {
+            Some(rest) => resolved["pipelines"]
+                .as_object()
+                .unwrap()
+                .iter()
+                .filter(|(id, _)| at(&written["pipelines"][id.as_str()], rest).is_none())
+                .filter_map(|(_, p)| at(p, rest))
+                .collect(),
+            None => at(resolved, path)
+                .filter(|v| !v.is_null())
+                .into_iter()
+                .collect(),
+        }
+    }
+
+    /// Whether a default as the reference spells it is the resolved value, compared by the key's
+    /// type: `60s` is `1m`, `512MiB` is `536870912`.
+    fn same(expects: &str, written: &str, resolved: &Json) -> bool {
+        match (expects, resolved) {
+            (_, Json::Null) => written == "null",
+            ("a duration", Json::String(s)) => {
+                humantime::parse_duration(written).ok() == humantime::parse_duration(s).ok()
+            }
+            ("a byte size", Json::Number(n)) => {
+                ByteSize::from_str(written).ok().map(|b| b.bytes()) == n.as_u64()
+            }
+            (_, Json::Number(n)) => written.parse::<f64>().ok() == n.as_f64(),
+            (_, Json::Bool(b)) => written == b.to_string(),
+            (_, Json::String(s)) => written == s,
+            (_, Json::Array(_)) => {
+                let list: Vec<String> = written
+                    .trim_matches(['[', ']'])
+                    .split(',')
+                    .map(str::trim)
+                    .filter(|s| !s.is_empty())
+                    .map(String::from)
+                    .collect();
+                serde_json::to_value(list).unwrap() == *resolved
+            }
+            _ => false,
+        }
+    }
+
+    #[test]
+    fn every_default_stated_as_a_value_is_the_resolved_one() {
+        let env: BTreeMap<String, String> = BTreeMap::new();
+        // The first document that resolves a key is the one it is checked in: the API's own
+        // default auth is the plain one's.
+        let resolved: Vec<(Json, Json)> = [
+            "{}",
+            "{ auth: jwt, jwt: { issuer: i, jwks_url: \"https://k\" } }",
+            "{ auth: oauth2, oauth2: { issuer: i, jwks_url: \"https://k\" } }",
+        ]
+        .iter()
+        .map(|auth| {
+            let text = MINIMAL.replace("{auth}", auth);
+            let loaded = load_str(&text, &env, &FingerprintKey::from_bytes([7; 32])).unwrap();
+            let written: Json = serde_saphyr::from_str(&text).unwrap();
+            (serde_json::to_value(&loaded.config).unwrap(), written)
+        })
+        .collect();
+        let keys: BTreeMap<String, &'static str> = config_keys()
+            .into_iter()
+            .map(|k| (k.path, k.expects))
+            .collect();
+        let mut wrong = Vec::new();
+        for doc in FIELD_DOCS {
+            let DocDefault::Value(written) = doc.default else {
+                continue;
+            };
+            let path = crate::keys::resolved_path(doc.path);
+            let values = resolved
+                .iter()
+                .map(|(r, written)| resolved_values(r, written, path))
+                .find(|v| !v.is_empty())
+                .unwrap_or_default();
+            if values.is_empty() {
+                wrong.push(format!("{}: not in the resolved config", doc.path));
+            }
+            for v in values {
+                if !same(keys[doc.path], written, v) {
+                    wrong.push(format!("{}: {written}, resolved {v}", doc.path));
+                }
+            }
+        }
+        assert!(wrong.is_empty(), "{wrong:#?}");
+    }
+
+    #[test]
+    fn durations_compare_by_value() {
+        assert!(same("a duration", "60s", &Json::String("1m".into())));
+        assert!(!same("a duration", "60s", &Json::String("30s".into())));
+        assert_eq!(
+            humantime::parse_duration("30d").unwrap(),
+            Duration::from_secs(30 * 86_400)
+        );
+    }
+}
````

`crates/data-avalanche-config/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 0aeabee..9d27786 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -72,6 +72,7 @@ mod diag;
 mod diff;
 mod enums;
 mod external;
+mod field_docs;
 mod leaf;
 mod load;
 mod material;
````


- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p data-avalanche-config --lib field_docs`
Expected: FAIL to compile: `cannot find value FIELD_DOCS`, `unresolved imports crate::config_keys,
crate::key_mutability`, `cannot find keys in crate`, `cannot find type DocDefault`.

- [ ] **Step 3: Implement the keys, the table and the macro's `Keys` impls**

`crates/data-avalanche-config/src/field_docs.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/field_docs.rs b/crates/data-avalanche-config/src/field_docs.rs
index 72bd081..c4fe615 100644
--- a/crates/data-avalanche-config/src/field_docs.rs
+++ b/crates/data-avalanche-config/src/field_docs.rs
@@ -4,6 +4,778 @@
 //! key has exactly one row, in the model's order, and every default stated as a value is the one
 //! the resolved config applies.
 
+use serde::Serialize;
+
+/// A key's default, as the reference states it.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
+#[serde(rename_all = "snake_case", tag = "kind", content = "text")]
+pub enum DocDefault {
+    /// No default: required, or absent means off.
+    None,
+    /// The value the resolved config applies, spelled as it would be written.
+    Value(&'static str),
+    /// A default said in words: one that depends on other keys, or that the resolved config
+    /// holds as the block's absence (`tls: null` for `tls.enabled: false`).
+    Note(&'static str),
+}
+
+/// The reference's row for one key.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
+pub struct FieldDoc {
+    /// The key's path, as [`crate::Key::path`] spells it.
+    pub path: &'static str,
+    pub default: DocDefault,
+    /// One or two sentences: what the key does, and the constraint an operator meets first.
+    pub doc: &'static str,
+}
+
+use DocDefault::{None as Required, Note, Value};
+
+const fn f(path: &'static str, default: DocDefault, doc: &'static str) -> FieldDoc {
+    FieldDoc { path, default, doc }
+}
+
+/// One row per key, in [`crate::config_keys`]'s order.
+pub const FIELD_DOCS: &[FieldDoc] = &[
+    f(
+        "config_version",
+        Required,
+        "The file's format version. Required; the only version is `1`.",
+    ),
+    // ---- globals (config §6) ----
+    f(
+        "globals.cloud",
+        Value("aws"),
+        "The object-storage provider for every bucket in the file. Credentials come from the provider's default chain (instance role, workload identity, environment); the file holds none. On `azure` every `bucket` is a container.",
+    ),
+    f(
+        "globals.state.bucket",
+        Required,
+        "The bucket of the cluster's own state: the anchor, config generations, membership and the pipelines' leases.",
+    ),
+    f(
+        "globals.state.prefix",
+        Value(""),
+        "The key prefix of the cluster's state under `bucket`; empty is the bucket's root.",
+    ),
+    f(
+        "globals.api.address",
+        Value("0.0.0.0:8080"),
+        "Where the management API, `/metrics` and the health probes listen.",
+    ),
+    f(
+        "globals.api.auth",
+        Value("none"),
+        "How the API authenticates a caller: `none` (open), `basic` (one user, `admin`), or a bearer JWT (`jwt`, `oauth2`) whose roles map through `role_mapping`. The health probes and `/metrics` never ask.",
+    ),
+    f(
+        "globals.api.user",
+        Required,
+        "The user for `auth: basic`, or give `credentials_file`.",
+    ),
+    f(
+        "globals.api.password",
+        Required,
+        "The password for `auth: basic`; write it as `${VAR}`.",
+    ),
+    f(
+        "globals.api.credentials_file",
+        Required,
+        "A JSON file `{\"user\": …, \"password\": …}` in place of `user` and `password`; replace its contents and reload to rotate them.",
+    ),
+    f(
+        "globals.api.jwt.issuer",
+        Required,
+        "The `iss` a token must carry (`auth: jwt`).",
+    ),
+    f(
+        "globals.api.jwt.audience",
+        Required,
+        "The `aud` a token must carry, when set.",
+    ),
+    f(
+        "globals.api.jwt.jwks_url",
+        Required,
+        "Where the API fetches the issuer's signing keys.",
+    ),
+    f(
+        "globals.api.jwt.roles_claim",
+        Value("roles"),
+        "The claim, a dotted JSON path, that holds the caller's roles.",
+    ),
+    f(
+        "globals.api.oauth2.issuer",
+        Required,
+        "The `iss` a token must carry (`auth: oauth2`).",
+    ),
+    f(
+        "globals.api.oauth2.audience",
+        Required,
+        "The `aud` a token must carry, when set.",
+    ),
+    f(
+        "globals.api.oauth2.jwks_url",
+        Required,
+        "Where the API fetches the signing keys; optional when `discovery_url` is set.",
+    ),
+    f(
+        "globals.api.oauth2.discovery_url",
+        Required,
+        "The issuer's OpenID discovery document, which names its keys.",
+    ),
+    f(
+        "globals.api.oauth2.roles_claim",
+        Value("roles"),
+        "The claim, a dotted JSON path, that holds the caller's roles.",
+    ),
+    f(
+        "globals.api.role_mapping.*",
+        Note("each role maps to itself"),
+        "Maps the local roles `viewer`, `operator` and `admin` to the IdP's role names; a caller gets the highest role any of its roles maps to.",
+    ),
+    f(
+        "globals.api.tls.enabled",
+        Note("false"),
+        "Serve the API over TLS. Off when a proxy in front terminates TLS.",
+    ),
+    f(
+        "globals.api.tls.cert",
+        Required,
+        "The server certificate: a path, inline PEM, or `${VAR}`. Required with `enabled`.",
+    ),
+    f(
+        "globals.api.tls.key",
+        Required,
+        "The certificate's private key: a path, inline PEM, or `${VAR}`. Required with `enabled`.",
+    ),
+    f(
+        "globals.log.level",
+        Value("info"),
+        "The log filter: a level (`info`) or `tracing` directives (`info,data_avalanche_sink_delta=debug`). The `--log-level` flag overrides it.",
+    ),
+    f(
+        "globals.log.format",
+        Value("json"),
+        "The log format on stderr: `json`, `compact` or `pretty`. The `--log-format` flag overrides it.",
+    ),
+    f(
+        "globals.cluster.tokens",
+        Value("128"),
+        "Virtual nodes per node on the hash ring. The first node pins it in the cluster's state; a node with another value refuses to start.",
+    ),
+    f(
+        "globals.cluster.lock_backend",
+        Value("storage"),
+        "What arbitrates a pipeline's lease: `storage`, a compare-and-swap object in `globals.state`. `kubernetes` is reserved and refused in v1.",
+    ),
+    f(
+        "globals.cluster.lease_duration",
+        Value("15s"),
+        "How long a pipeline's lease lasts without a renewal: the longest a dead node's pipelines wait for another node. Pinned cluster-wide.",
+    ),
+    f(
+        "globals.cluster.lease_renewal_interval",
+        Value("5s"),
+        "How often the holder renews its leases; at most a third of `lease_duration`.",
+    ),
+    f(
+        "globals.cluster.lease_safety_margin",
+        Value("0.15"),
+        "The share of `lease_duration` a holder keeps in hand: it stops writing before the lease could lapse.",
+    ),
+    f(
+        "globals.cluster.rebalance_stabilization",
+        Value("15s"),
+        "How long membership must stay unchanged before pipelines move to a joined node. A dead node's pipelines don't wait for it.",
+    ),
+    f(
+        "globals.cluster.max_concurrent_handoffs",
+        Value("2"),
+        "How many pipelines a node drains at once when it hands them off.",
+    ),
+    f(
+        "globals.cluster.max_concurrent_takeovers",
+        Value("4"),
+        "How many pipelines a node starts at once when it takes over a dead node's.",
+    ),
+    f(
+        "globals.cluster.drain_timeout",
+        Value("60s"),
+        "The longest a graceful hand-off may take (flush, commit, checkpoint, release) before the next owner replays from the last checkpoint instead.",
+    ),
+    f(
+        "globals.cluster.discovery",
+        Value("static"),
+        "How a node finds its peers: `static` from `memberlist.join_members`, `dns` from `discovery_dns_name`.",
+    ),
+    f(
+        "globals.cluster.discovery_dns_name",
+        Required,
+        "The name whose addresses are the peers (a Kubernetes headless Service). Required with `discovery: dns`.",
+    ),
+    f(
+        "globals.cluster.memberlist.join_members",
+        Value("[]"),
+        "Peers to join, as `host:port` of their advertised addresses. Empty, with no discovery, is a single node.",
+    ),
+    f(
+        "globals.cluster.memberlist.node_name",
+        Note("the host name"),
+        "The node's stable identity on the ring. On Kubernetes, the StatefulSet pod's name (`${POD_NAME}`).",
+    ),
+    f(
+        "globals.cluster.memberlist.advertise_addr",
+        Note("the bind address, or the first private IP"),
+        "The address peers reach this node at. On Kubernetes, `${POD_IP}`.",
+    ),
+    f(
+        "globals.cluster.memberlist.advertise_port",
+        Value("7946"),
+        "The port peers reach this node at.",
+    ),
+    f(
+        "globals.cluster.memberlist.bind_addr",
+        Value("[0.0.0.0]"),
+        "The addresses the gossip listens on.",
+    ),
+    f(
+        "globals.cluster.memberlist.bind_port",
+        Value("7946"),
+        "The UDP port the gossip listens on.",
+    ),
+    f(
+        "globals.cluster.memberlist.gossip_interval",
+        Value("200ms"),
+        "How often a node sends its pending membership updates.",
+    ),
+    f(
+        "globals.cluster.memberlist.gossip_nodes",
+        Value("3"),
+        "How many peers each round of updates goes to.",
+    ),
+    f(
+        "globals.cluster.memberlist.probe_interval",
+        Value("1s"),
+        "How often a node probes a peer. A suspect is declared down after `4 · max(1, log10 n)` probe intervals.",
+    ),
+    f(
+        "globals.cluster.memberlist.probe_timeout",
+        Value("500ms"),
+        "How long a probe waits before asking other peers to probe; below `probe_interval`.",
+    ),
+    f(
+        "globals.cluster.memberlist.push_pull_interval",
+        Value("30s"),
+        "How often a node exchanges its full member list with a peer, so a healed partition rejoins.",
+    ),
+    f(
+        "globals.cluster.memberlist.retransmit_factor",
+        Value("4"),
+        "How many times an update is resent, times `ceil(log10(n + 1))` for `n` members.",
+    ),
+    f(
+        "globals.tracing.otlp_endpoint",
+        Required,
+        "The OTLP/HTTP collector's base URL (`http://otel-collector:4318`); spans go to `<endpoint>/v1/traces`. Absent turns tracing off.",
+    ),
+    f(
+        "globals.tracing.sample_ratio",
+        Note("0.05 with an endpoint, else 0"),
+        "The share of flushes traced, from 0 to 1.",
+    ),
+    f(
+        "globals.azure.account_name",
+        Note("from the credential chain"),
+        "The Azure storage account of every container (`cloud: azure`).",
+    ),
+    // ---- a pipeline (config §9) ----
+    f(
+        "pipelines.*.id",
+        Required,
+        "The pipeline's immutable identity: its state, its default prefixes and its lease are keyed by it. Changing it removes the pipeline and adds a new one. Lowercase letters, digits, `_` and `-`, at most 63.",
+    ),
+    f(
+        "pipelines.*.source.type",
+        Value("postgres"),
+        "The source engine: `postgres`, or `mysql` (MySQL and MariaDB).",
+    ),
+    f(
+        "pipelines.*.source.host",
+        Required,
+        "The source server's host name or address.",
+    ),
+    f(
+        "pipelines.*.source.port",
+        Note("5432 for PostgreSQL, 3306 for MySQL"),
+        "The source server's port.",
+    ),
+    f(
+        "pipelines.*.source.db",
+        Required,
+        "The one database the pipeline replicates. PostgreSQL table patterns are `schema.table` within it; MySQL's are bare table names.",
+    ),
+    f(
+        "pipelines.*.source.user",
+        Required,
+        "The replication user, or give `credentials_file`.",
+    ),
+    f(
+        "pipelines.*.source.password",
+        Required,
+        "The user's password; write it as `${VAR}`. Read again at every reconnect.",
+    ),
+    f(
+        "pipelines.*.source.credentials_file",
+        Required,
+        "A JSON file `{\"user\": …, \"password\": …}` in place of `user` and `password`; replace its contents and reload to rotate them.",
+    ),
+    f(
+        "pipelines.*.source.ssl_mode",
+        Value("require"),
+        "TLS to the source: `disable`, `allow`, `prefer`, `require`, `verify-ca` or `verify-full`. `allow` isn't valid for MySQL.",
+    ),
+    f(
+        "pipelines.*.source.ssl_cert",
+        Required,
+        "A client certificate for the source: a path, inline PEM, or `${VAR}`.",
+    ),
+    f(
+        "pipelines.*.source.ssl_key",
+        Required,
+        "The client certificate's key: a path, inline PEM, or `${VAR}`.",
+    ),
+    f(
+        "pipelines.*.source.ssl_ca",
+        Required,
+        "The CA that signed the source's certificate, for `verify-ca` and `verify-full`.",
+    ),
+    f(
+        "pipelines.*.source.timeouts.connect",
+        Value("10s"),
+        "The longest a connection may take to set up: TCP, TLS and authentication.",
+    ),
+    f(
+        "pipelines.*.source.timeouts.command",
+        Value("30s"),
+        "The longest a catalog query or command may take.",
+    ),
+    f(
+        "pipelines.*.source.timeouts.snapshot",
+        Value("6h"),
+        "The longest one table's snapshot may take; at least `10s`.",
+    ),
+    f(
+        "pipelines.*.source.toast_strategy",
+        Value("carry_forward"),
+        "PostgreSQL: an UPDATE that leaves a large (TOAST) column out keeps its last value (`carry_forward`) or reads the row again (`re_query`).",
+    ),
+    f(
+        "pipelines.*.source.postgres.publication",
+        Required,
+        "The publication the pipeline creates and keeps in step with its tables. Unique per database across pipelines; case-sensitive.",
+    ),
+    f(
+        "pipelines.*.source.postgres.slot",
+        Required,
+        "The logical replication slot. Lowercase letters, digits and `_`; unique per server and database across pipelines.",
+    ),
+    f(
+        "pipelines.*.source.postgres.auto_cleanup",
+        Value("true"),
+        "Drop the slot and the publication when the pipeline is removed or its `id` changes.",
+    ),
+    f(
+        "pipelines.*.source.postgres.heartbeat",
+        Value("60s"),
+        "How often the pipeline confirms its position to the server; a stream silent for three heartbeats is reconnected.",
+    ),
+    f(
+        "pipelines.*.source.postgres.slot_management",
+        Value("managed"),
+        "`managed` creates the slot when absent; `require_existing` uses a slot a DBA created and needs `auto_cleanup: false`.",
+    ),
+    f(
+        "pipelines.*.source.postgres.slot_on_orphan",
+        Value("halt"),
+        "A slot that exists with no state of the pipeline's: `halt` refuses to start, naming it; `adopt_resnapshot` takes it over and snapshots every table again.",
+    ),
+    f(
+        "pipelines.*.source.postgres.max_wal_lag_bytes",
+        Value("null"),
+        "Halt the pipeline when its slot holds more WAL than this, to protect the server's disk; at least `64MiB`. `null` only reports `wal_lag_bytes`.",
+    ),
+    f(
+        "pipelines.*.source.mysql.replication_type",
+        Value("gtid"),
+        "`gtid` follows GTIDs and survives a failover (MySQL `gtid_mode=ON`, MariaDB `gtid_strict_mode=1`); `binlog` follows file and position, and halts `BINLOG_SOURCE_CHANGED` when the server changes.",
+    ),
+    f(
+        "pipelines.*.source.mysql.tinyint1_as_bool",
+        Value("true"),
+        "Read `TINYINT(1)` columns as booleans.",
+    ),
+    f(
+        "pipelines.*.source.mysql.tinyint1_detection",
+        Value("information_schema"),
+        "Where `TINYINT(1)` is recognised: the catalog (`information_schema`), or `none` to read every `TINYINT` as an integer.",
+    ),
+    f(
+        "pipelines.*.source.mysql.binlog_purge_action",
+        Value("error"),
+        "When the server purged the binlog the pipeline must resume from: `error` halts; `re_snapshot` snapshots every table again.",
+    ),
+    f(
+        "pipelines.*.source.mysql.server_id",
+        Note("derived from the pipeline's id, in 2147483648…4294967295"),
+        "The replica id the binlog stream registers with; each pipeline needs its own, or the server ends one of the streams.",
+    ),
+    f(
+        "pipelines.*.source.mysql.heartbeat",
+        Value("60s"),
+        "How often an idle server sends a binlog heartbeat; a stream silent for three heartbeats is reconnected.",
+    ),
+    f(
+        "pipelines.*.tables.include",
+        Required,
+        "The tables to replicate, as globs: `schema.table` on PostgreSQL (`public.*`), bare names on MySQL (`orders`, `payment_*`). `*` stays within one name. Required, non-empty.",
+    ),
+    f(
+        "pipelines.*.tables.exclude",
+        Value("[]"),
+        "Globs subtracted from `include`, in the same form.",
+    ),
+    f(
+        "pipelines.*.tables.snapshots.mode",
+        Value("never"),
+        "When a table is copied before its changes stream: `never`, `initial` (once, for a table with no confirmed position), `always` (at every start), or `dump_initial` and `dump_always` (the same, with no streaming after).",
+    ),
+    f(
+        "pipelines.*.tables.snapshots.parallelism",
+        Value("1"),
+        "How many tables are snapshotted at once. More than 4 warns: each PostgreSQL snapshot holds several connections.",
+    ),
+    f(
+        "pipelines.*.tables.snapshots.chunk_size",
+        Value("10000"),
+        "Rows per snapshot chunk; a snapshot resumes from its last committed chunk.",
+    ),
+    f(
+        "pipelines.*.tables.snapshots.overrides.*",
+        Required,
+        "A snapshot mode for one table, named exactly (no globs).",
+    ),
+    f(
+        "pipelines.*.tables.column_type_overrides.*",
+        Required,
+        "A column's target type, keyed `table.column`, in the sink-neutral grammar: `int64`, `decimal(38,10)`, `string`, `timestamp_tz`, `list<int32>`…",
+    ),
+    f(
+        "pipelines.*.tables.numeric_overflow_action",
+        Value("string"),
+        "A `numeric` wider than 38 digits: `string` writes its text; `error` halts the table.",
+    ),
+    f(
+        "pipelines.*.tables.primary_key.*",
+        Required,
+        "The key columns of one table, in place of its primary key (for example a unique index behind `REPLICA IDENTITY USING INDEX`). Changing it means a re-snapshot.",
+    ),
+    f(
+        "pipelines.*.tables.partitioning.*[].column",
+        Required,
+        "A partition column of one table (`_ts`, the commit time, is allowed).",
+    ),
+    f(
+        "pipelines.*.tables.partitioning.*[].transform",
+        Required,
+        "The partition transform; the Delta sink takes `identity` only.",
+    ),
+    f(
+        "pipelines.*.tables.partitioning.*[].n",
+        Required,
+        "The bucket count of a `bucket` transform.",
+    ),
+    f(
+        "pipelines.*.tables.partitioning.*[].width",
+        Required,
+        "The width of a `truncate` transform.",
+    ),
+    f(
+        "pipelines.*.sink.type",
+        Required,
+        "The table format: `delta`. Required, with no default. `iceberg` is accepted but refused at start: the Iceberg sink ships in v2.",
+    ),
+    f(
+        "pipelines.*.sink.write_mode",
+        Value("mirror"),
+        "`mirror`: the tables mirror the source, rows upserted and deleted. `changelog` is reserved.",
+    ),
+    f(
+        "pipelines.*.sink.bucket",
+        Required,
+        "The bucket (Azure: container) of the pipeline's tables.",
+    ),
+    f(
+        "pipelines.*.sink.prefix",
+        Note("the pipeline's id"),
+        "The key prefix of the pipeline's tables; each table is `<prefix>/<schema>/<table>/`.",
+    ),
+    f(
+        "pipelines.*.sink.flush.seconds",
+        Value("30"),
+        "Flush when the oldest buffered change is this old. `0` flushes at every transaction; `null` turns the trigger off.",
+    ),
+    f(
+        "pipelines.*.sink.flush.rows",
+        Value("100000"),
+        "Flush at this many buffered rows; `null` turns the trigger off.",
+    ),
+    f(
+        "pipelines.*.sink.flush.bytes",
+        Value("512MiB"),
+        "Flush at this much buffered data; `null` turns the trigger off.",
+    ),
+    f(
+        "pipelines.*.sink.max_transaction_batch_rows",
+        Value("null"),
+        "Commit a source transaction larger than this in slices (its resume point stays at its start). `null` keeps every transaction whole.",
+    ),
+    f(
+        "pipelines.*.sink.max_transaction_buffer_bytes",
+        Value("1GiB"),
+        "The most one transaction may buffer. Without slicing, reaching it halts the pipeline rather than run the node out of memory.",
+    ),
+    f(
+        "pipelines.*.sink.buffer_capacity_rows",
+        Value("200000"),
+        "The capacity of the pipeline's internal queues: when the sink can't keep up, reading stops once they fill.",
+    ),
+    f(
+        "pipelines.*.sink.metrics.per_table",
+        Value("false"),
+        "Add the `table` label to the per-table metrics; it multiplies the series.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.strategy",
+        Value("mor"),
+        "v2 (the Iceberg sink): merge-on-read (`mor`) or copy-on-write (`cow`) for new tables.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.type",
+        Note("the one block present"),
+        "v2: the Iceberg catalog, `glue`, `hive` or `rest`.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.glue.name",
+        Required,
+        "v2: the Glue catalog id.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.glue.database",
+        Note("source.db"),
+        "v2: the Glue database.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.glue.strip_schema",
+        Value("false"),
+        "v2: name Glue tables `<table>` rather than `<schema>__<table>`.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.url",
+        Required,
+        "v2: the Hive Metastore's Thrift address.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.auth",
+        Note("none"),
+        "v2: the Metastore's SASL mode: `none`, `ldap` or `kerberos`.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.user",
+        Required,
+        "v2: the LDAP user.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.password",
+        Required,
+        "v2: the LDAP password.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.credentials_file",
+        Required,
+        "v2: a JSON file of the LDAP user and password.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.kerberos.principal",
+        Required,
+        "v2: the Kerberos principal.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.kerberos.keytab",
+        Required,
+        "v2: the Kerberos keytab.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.kerberos.service_name",
+        Note("hive"),
+        "v2: the Metastore's Kerberos service name.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.tls.enabled",
+        Note("false"),
+        "v2: TLS to the Metastore.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.tls.ca",
+        Required,
+        "v2: the Metastore certificate's CA.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.hive.database",
+        Note("source.db"),
+        "v2: the Hive database.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.url",
+        Required,
+        "v2: the REST catalog's base URL.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.warehouse",
+        Required,
+        "v2: the catalog's warehouse.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.prefix",
+        Required,
+        "v2: a path before the catalog's `/v1/`.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.auth",
+        Value("none"),
+        "v2: `none`, `bearer`, `oauth2` or `sigv4`.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.token",
+        Required,
+        "v2: the bearer token.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.token_file",
+        Required,
+        "v2: a file holding the bearer token.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.oauth2.client_id",
+        Required,
+        "v2: the OAuth2 client id.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.oauth2.client_secret",
+        Required,
+        "v2: the OAuth2 client secret.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.oauth2.credentials_file",
+        Required,
+        "v2: a JSON file of the client id and secret.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.oauth2.token_url",
+        Required,
+        "v2: the IdP's token endpoint.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.oauth2.scope",
+        Required,
+        "v2: the scopes asked for.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.sigv4.region",
+        Required,
+        "v2: the SigV4 region.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.sigv4.service",
+        Required,
+        "v2: the SigV4 service.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.rest.tls.ca",
+        Required,
+        "v2: the catalog certificate's CA.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.retry.max_attempts",
+        Value("5"),
+        "v2: catalog retries before the pipeline degrades.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.retry.base_backoff",
+        Value("1s"),
+        "v2: the first retry's backoff.",
+    ),
+    f(
+        "pipelines.*.sink.iceberg.catalog.retry.max_backoff",
+        Value("30s"),
+        "v2: the longest backoff.",
+    ),
+    f(
+        "pipelines.*.sink.delta.column_mapping",
+        Value("none"),
+        "Delta column mapping for new tables; `name` is refused in v1, because delta-rs can't add a column to a column-mapped table.",
+    ),
+    f(
+        "pipelines.*.sink.delta.checkpoint_interval",
+        Value("10"),
+        "Commits between Delta checkpoints, for new tables (`delta.checkpointInterval`).",
+    ),
+    f(
+        "pipelines.*.sink.delta.log_retention",
+        Value("30d"),
+        "How long the Delta log keeps its entries, for new tables (`delta.logRetentionDuration`).",
+    ),
+    f(
+        "pipelines.*.sink.delta.timestamp_ntz_as",
+        Value("timestamp_ntz"),
+        "The Delta type of a `timestamp without time zone` column in a new table: `timestamp_ntz` (reader 3 / writer 7), `timestamp` (as a UTC instant), or `string`.",
+    ),
+    f(
+        "pipelines.*.sink.delta.table_properties.*",
+        Required,
+        "A table property for new tables; a `delta.*` key must be on the allow-list.",
+    ),
+    f(
+        "pipelines.*.sink.delta.catalog.type",
+        Value("none"),
+        "Catalog registration of the Delta tables: `none` (path-based). The others are reserved.",
+    ),
+    f(
+        "pipelines.*.sink.delta.merge_memory_limit",
+        Value("null"),
+        "The memory ceiling of one table's MERGE; at least `16MiB` when set. A MERGE over it halts the table `SINK_REJECTED`.",
+    ),
+    f(
+        "pipelines.*.state.bucket",
+        Note("sink.bucket"),
+        "The bucket of the pipeline's state (positions, snapshot cursors, status). By default it sits under the sink's prefix, in `.pipeline-state/`.",
+    ),
+    f(
+        "pipelines.*.state.prefix",
+        Note("the pipeline's id"),
+        "The state's prefix when `state.bucket` is set; `defaults.state.prefix` is its parent.",
+    ),
+    f(
+        "pipelines.*.schema_evolution.enabled",
+        Value("true"),
+        "Apply the source's safe schema changes to the tables; off, any change halts its table.",
+    ),
+    f(
+        "pipelines.*.schema_evolution.rescan_interval",
+        Value("60m"),
+        "How often an idle table's schema is read again; `0` turns the rescan off.",
+    ),
+];
+
 #[cfg(test)]
 mod tests {
     use std::collections::BTreeMap;
````

`crates/data-avalanche-config/src/keys.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/keys.rs b/crates/data-avalanche-config/src/keys.rs
new file mode 100644
index 0000000..4821f84
--- /dev/null
+++ b/crates/data-avalanche-config/src/keys.rs
@@ -0,0 +1,160 @@
+//! Every key the raw model takes (config §3–§9), with what its value expects and the rule that
+//! checks it: the spine of the generated configuration reference (M11b). `walk_fields!` generates
+//! [`Keys`] for each raw struct from the same field list as [`crate::walk::Walk`], so the
+//! reference can't drift from the model.
+
+use std::collections::BTreeMap;
+
+use serde::Serialize;
+
+use crate::coerce::LeafType;
+use crate::leaf::{Leaf, Tri};
+use crate::mutability::{Mutability, classify};
+use crate::raw::{RawConfig, RawPartitionField, RawPipeline};
+
+/// What a key holds.
+#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
+#[serde(rename_all = "snake_case")]
+pub enum KeyShape {
+    /// One value.
+    Scalar,
+    /// One value, or `null` to turn the setting off (§9.4.1).
+    Nullable,
+    /// A list of values.
+    List,
+    /// A list of values, or `null` for none (§8.3).
+    NullableList,
+    /// A map whose keys the operator names (the path ends in `*`), of one value each.
+    Map,
+    /// A map whose keys the operator names, of a list of values each.
+    MapOfLists,
+}
+
+/// One key of the config file.
+#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
+pub struct Key {
+    /// Dotted, with `*` for a segment the operator names: `pipelines.*.source.port`.
+    pub path: String,
+    pub shape: KeyShape,
+    /// What the value must be (`"a duration"`, `"one of: aws, google, azure"`).
+    pub expects: &'static str,
+    /// The rule that reports a value that isn't one (`TYPE`, or the enum's V-rule).
+    pub rule: &'static str,
+}
+
+/// Every key of the config file, in the model's order: `config_version`, `globals`, then a
+/// pipeline's keys under `pipelines.*` (the same keys `defaults` takes, §7).
+pub fn config_keys() -> Vec<Key> {
+    let RawConfig {
+        config_version,
+        globals,
+        defaults: _,
+        pipelines: _,
+    } = RawConfig::default();
+    let mut out = Vec::new();
+    config_version.keys("config_version", &mut out);
+    globals.keys("globals", &mut out);
+    RawPipeline::default().keys("pipelines.*", &mut out);
+    out
+}
+
+pub(crate) trait Keys {
+    fn keys(&self, path: &str, out: &mut Vec<Key>);
+}
+
+pub(crate) fn child(path: &str, key: &str) -> String {
+    format!("{path}.{key}")
+}
+
+fn leaf<T: LeafType>(path: &str, shape: KeyShape, out: &mut Vec<Key>) {
+    out.push(Key {
+        path: path.to_string(),
+        shape,
+        expects: T::EXPECTED,
+        rule: T::RULE,
+    });
+}
+
+impl<T: LeafType> Keys for Option<Leaf<T>> {
+    fn keys(&self, path: &str, out: &mut Vec<Key>) {
+        leaf::<T>(path, KeyShape::Scalar, out);
+    }
+}
+
+impl<T: LeafType> Keys for Tri<T> {
+    fn keys(&self, path: &str, out: &mut Vec<Key>) {
+        leaf::<T>(path, KeyShape::Nullable, out);
+    }
+}
+
+impl<T: LeafType> Keys for Option<Vec<Leaf<T>>> {
+    fn keys(&self, path: &str, out: &mut Vec<Key>) {
+        leaf::<T>(path, KeyShape::List, out);
+    }
+}
+
+impl<T: LeafType> Keys for Option<Option<Vec<Leaf<T>>>> {
+    fn keys(&self, path: &str, out: &mut Vec<Key>) {
+        leaf::<T>(path, KeyShape::NullableList, out);
+    }
+}
+
+impl<T: LeafType> Keys for Option<BTreeMap<String, Leaf<T>>> {
+    fn keys(&self, path: &str, out: &mut Vec<Key>) {
+        leaf::<T>(&child(path, "*"), KeyShape::Map, out);
+    }
+}
+
+impl<T: LeafType> Keys for Option<BTreeMap<String, Vec<Leaf<T>>>> {
+    fn keys(&self, path: &str, out: &mut Vec<Key>) {
+        leaf::<T>(&child(path, "*"), KeyShape::MapOfLists, out);
+    }
+}
+
+impl Keys for Option<BTreeMap<String, Vec<RawPartitionField>>> {
+    fn keys(&self, path: &str, out: &mut Vec<Key>) {
+        RawPartitionField::default().keys(&format!("{path}.*[]"), out);
+    }
+}
+
+/// The resolved config's spelling of a key the resolved form renames: a credential pair and its
+/// file are one `credentials`, a bearer token and its file one `token`, the Delta catalog's type
+/// `catalog_type`.
+pub(crate) fn resolved_path(path: &str) -> &str {
+    const RENAMED: &[(&str, &str)] = &[
+        ("globals.api.user", "globals.api.credentials"),
+        ("globals.api.password", "globals.api.credentials"),
+        ("globals.api.credentials_file", "globals.api.credentials"),
+        (
+            "pipelines.*.sink.iceberg.catalog.hive.user",
+            "pipelines.*.sink.iceberg.catalog.hive.credentials",
+        ),
+        (
+            "pipelines.*.sink.iceberg.catalog.hive.password",
+            "pipelines.*.sink.iceberg.catalog.hive.credentials",
+        ),
+        (
+            "pipelines.*.sink.iceberg.catalog.hive.credentials_file",
+            "pipelines.*.sink.iceberg.catalog.hive.credentials",
+        ),
+        (
+            "pipelines.*.sink.iceberg.catalog.rest.token_file",
+            "pipelines.*.sink.iceberg.catalog.rest.token",
+        ),
+        (
+            "pipelines.*.sink.delta.catalog.type",
+            "pipelines.*.sink.delta.catalog_type",
+        ),
+    ];
+    RENAMED
+        .iter()
+        .find(|(raw, _)| *raw == path)
+        .map_or(path, |(_, resolved)| resolved)
+}
+
+/// What a change to the key does on a reload (config §1.2): applied in place, a pipeline
+/// restart, or a process restart. `None` only for a path that isn't a key.
+pub fn key_mutability(path: &str) -> Option<Mutability> {
+    let segments: Vec<&str> = resolved_path(path).split('.').collect();
+    classify(&segments)
+}
````

`crates/data-avalanche-config/src/lib.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/lib.rs b/crates/data-avalanche-config/src/lib.rs
index 9d27786..7ed1fec 100644
--- a/crates/data-avalanche-config/src/lib.rs
+++ b/crates/data-avalanche-config/src/lib.rs
@@ -73,6 +73,7 @@ mod diff;
 mod enums;
 mod external;
 mod field_docs;
+mod keys;
 mod leaf;
 mod load;
 mod material;
@@ -97,6 +98,8 @@ pub use external::{
     ClusterAnchor, check_cluster_anchor, check_partitioning, check_table_set,
     check_unbounded_numeric, glob_match, key_table,
 };
+pub use field_docs::{DocDefault, FIELD_DOCS, FieldDoc};
+pub use keys::{Key, KeyShape, config_keys, key_mutability};
 pub use leaf::Loc;
 pub use load::{Loaded, load_file, load_posted_str, load_str, validate_str};
 pub use material::{FileRef, Origin, Pem};
````

`crates/data-avalanche-config/src/walk.rs`:

````diff
diff --git a/crates/data-avalanche-config/src/walk.rs b/crates/data-avalanche-config/src/walk.rs
index 9a90474..9e584e3 100644
--- a/crates/data-avalanche-config/src/walk.rs
+++ b/crates/data-avalanche-config/src/walk.rs
@@ -77,6 +77,19 @@ macro_rules! walk_fields {
                 ); )+
             }
         }
+        impl $crate::keys::Keys for $t {
+            fn keys(&self, path: &str, out: &mut Vec<$crate::keys::Key>) {
+                let $t { $($f),+ } = self;
+                $( $crate::keys::Keys::keys(
+                    $f, &$crate::keys::child(path, $crate::walk::walk_key!($f)), out,
+                ); )+
+            }
+        }
+        impl $crate::keys::Keys for Option<$t> {
+            fn keys(&self, path: &str, out: &mut Vec<$crate::keys::Key>) {
+                $crate::keys::Keys::keys(&$t::default(), path, out);
+            }
+        }
         impl $crate::walk::Walk for Option<$t> {
             fn drop_empty(&mut self, ctx: &$crate::ctx::Ctx) {
                 if let Some(v) = self {
````


- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test -p data-avalanche-config --lib field_docs`
Expected: PASS, 4 tests (`every_key_of_the_model_has_one_row_in_its_order`,
`every_key_has_a_mutability_class`, `every_default_stated_as_a_value_is_the_resolved_one`,
`durations_compare_by_value`). Then the Global Constraints' gates.

- [ ] **Step 5: Commit**

```bash
git add crates/data-avalanche-config/src/keys.rs crates/data-avalanche-config/src/field_docs.rs \
  crates/data-avalanche-config/src/walk.rs crates/data-avalanche-config/src/lib.rs
git commit -m "feat(config): the config's keys and their documentation — config_keys() from the raw model through walk_fields!, key_mutability, FIELD_DOCS (147 rows) tied to the model by tests: one row per key in order, a reload class for each, every stated default the resolved one"
```

---

### Task 2: `cargo xtask docs` and the reference pages

**Files:**
- Create: `xtask/Cargo.toml`, `xtask/src/{lib,main,config,rules,api}.rs`, `xtask/tests/docs.rs`,
  `.cargo/config.toml`; generated: `docs/src/content/docs/reference/{configuration,validation-rules,api}.md`,
  `docs/public/openapi.json`
- Modify: `Cargo.toml` (the members), `Cargo.lock`

**Interfaces:**
- Consumes: Task 1's `config_keys`, `key_mutability`, `FIELD_DOCS`, `DocDefault`, `Key`,
  `KeyShape`; `data_avalanche_config::{RULES, RuleSeverity, Mutability}`;
  `data_avalanche_api::openapi()`.
- Produces (used by Task 3): `xtask::{BASE, CONTENT, repo_root, generated}`
  (`BASE = "/data-avalanche"`, `CONTENT = "docs/src/content/docs"`,
  `generated() -> Vec<(PathBuf, String)>`); the command `cargo xtask docs [--check]`.

- [ ] **Step 1: Write the failing test**

`xtask/tests/docs.rs`:

````diff
diff --git a/xtask/tests/docs.rs b/xtask/tests/docs.rs
new file mode 100644
index 0000000..7a525cd
--- /dev/null
+++ b/xtask/tests/docs.rs
@@ -0,0 +1,12 @@
+//! The site's generated pages match the code.
+
+#[test]
+fn the_generated_pages_are_current() {
+    let root = xtask::repo_root();
+    let stale: Vec<_> = xtask::generated()
+        .into_iter()
+        .filter(|(path, text)| std::fs::read_to_string(root.join(path)).ok().as_ref() != Some(text))
+        .map(|(path, _)| path)
+        .collect();
+    assert!(stale.is_empty(), "stale, run `cargo xtask docs`: {stale:?}");
+}
````


- [ ] **Step 2: Run it to see it fail**

Run: `cargo test -p xtask`
Expected: FAIL: `error: package ID specification 'xtask' did not match any packages`.

- [ ] **Step 3: Implement the crate**

The workspace takes it as a member, and `cargo xtask` is an alias:

`.cargo/config.toml`:

````diff
diff --git a/.cargo/config.toml b/.cargo/config.toml
new file mode 100644
index 0000000..e06bde7
--- /dev/null
+++ b/.cargo/config.toml
@@ -0,0 +1,3 @@
+# `cargo xtask <command>`: the repository's automation (roadmap §2), e.g. `cargo xtask docs`.
+[alias]
+xtask = "run --quiet --package xtask --"
````

`Cargo.toml`:

````diff
diff --git a/Cargo.toml b/Cargo.toml
index 74df10b..2b68606 100644
--- a/Cargo.toml
+++ b/Cargo.toml
@@ -1,6 +1,6 @@
 [workspace]
 resolver = "3"
-members = ["crates/*"]
+members = ["crates/*", "xtask"]
 exclude = ["prototypes"]
 
 [workspace.package]
````

`xtask/Cargo.toml`:

````diff
diff --git a/xtask/Cargo.toml b/xtask/Cargo.toml
new file mode 100644
index 0000000..f2c08b8
--- /dev/null
+++ b/xtask/Cargo.toml
@@ -0,0 +1,15 @@
+[package]
+name = "xtask"
+version.workspace = true
+edition.workspace = true
+rust-version.workspace = true
+publish.workspace = true
+description = "Repository automation: the documentation site's generated pages (cargo xtask docs)."
+
+[lints]
+workspace = true
+
+[dependencies]
+data-avalanche-config.workspace = true
+data-avalanche-api.workspace = true
+serde_json.workspace = true
````

`xtask/src/api.rs`:

````diff
diff --git a/xtask/src/api.rs b/xtask/src/api.rs
new file mode 100644
index 0000000..16b3317
--- /dev/null
+++ b/xtask/src/api.rs
@@ -0,0 +1,138 @@
+//! `reference/api.md`: the management API's `/v1` routes, read from the OpenAPI document the
+//! node serves (`data_avalanche_api::openapi`), and `openapi.json` beside the site's pages.
+
+use std::fmt::Write;
+
+use serde_json::Value;
+
+use crate::{BASE, cell, header};
+
+/// The document's tags, in the page's order, with what each group does.
+const TAGS: &[(&str, &str)] = &[
+    (
+        "pipelines",
+        "Pipelines: their state, and the lifecycle commands.",
+    ),
+    ("snapshots", "Snapshot jobs started through the API."),
+    (
+        "config",
+        "The config generation the cluster runs: validate, publish, status.",
+    ),
+    ("cluster", "Membership and leases."),
+    ("admin", "The node's process."),
+    ("openapi.json", "The document this page is rendered from."),
+];
+
+fn openapi_value() -> Value {
+    serde_json::to_value(data_avalanche_api::openapi()).expect("the document serializes")
+}
+
+/// `openapi.json` as the node serves it, pretty-printed by utoipa (as the API crate's own
+/// `openapi.json` is): a `serde_json::Value` would order its keys as the build's `serde_json`
+/// features say, and a workspace build turns on `preserve_order` (DataFusion does).
+pub(crate) fn document() -> String {
+    data_avalanche_api::openapi()
+        .to_pretty_json()
+        .expect("the document serializes")
+        + "\n"
+}
+
+/// An object's entries sorted by key, whatever order the build's `serde_json` keeps them in.
+fn sorted(v: &Value) -> Vec<(&String, &Value)> {
+    let mut entries: Vec<_> = v.as_object().expect("an object").iter().collect();
+    entries.sort_by_key(|(k, _)| *k);
+    entries
+}
+
+pub(crate) fn page() -> String {
+    let doc = openapi_value();
+    let mut operations: Vec<(&str, &str, &Value)> = Vec::new();
+    for (path, item) in sorted(&doc["paths"]) {
+        for (method, op) in sorted(item) {
+            operations.push((path, method, op));
+        }
+    }
+
+    let mut out = header(
+        "Management API",
+        "The node's REST API: every /v1 route, its least role, its parameters and its answers.",
+        3,
+        "the OpenAPI document (`data_avalanche_api::openapi`)",
+    );
+    let _ = write!(
+        out,
+        "Every node serves the API on `globals.api.address`. {} The \
+         [`openapi.json`]({BASE}/openapi.json) this page is rendered from is \
+         `GET /v1/openapi.json` on a node.\n\n\
+         **Roles.** A route's *least role* is the lowest that may call it: `viewer` reads, \
+         `operator` also runs the pipeline commands, snapshots and config reloads, `admin` also \
+         makes a node leave the cluster or shut down. With `auth: none` every caller is `admin`; with `auth: basic` the one user \
+         is `admin`; with `jwt` or `oauth2` the token's roles are mapped by `roles_claim` and \
+         `role_mapping`.\n\n\
+         **Answers.** Every body is a JSON object; a refusal carries an `error` string. Every \
+         route can also answer `401` (no credentials, or ones that don't verify), `403` (the \
+         caller's role is below the route's) and `503` (the node is starting or stopping, or \
+         its state can't be read). A `202` means the command is accepted and runs on the node \
+         that holds the pipeline's lease.\n\n\
+         **Outside `/v1`**, with no credentials: `GET /health/live` (the process answers), \
+         `GET /health/ready` (`200` once the node runs its config, `503` before and while it \
+         stops) and `GET /metrics` (Prometheus text).\n\n",
+        cell(doc["info"]["description"].as_str().unwrap_or_default()),
+    );
+    for (tag, about) in TAGS {
+        let _ = writeln!(out, "## {tag}\n\n{about}\n");
+        for (path, method, op) in operations.iter().filter(|(_, _, op)| op["tags"][0] == *tag) {
+            let _ = writeln!(
+                out,
+                "### `{} {path}`\n\n{}\n\nLeast role: `{}`.\n",
+                method.to_uppercase(),
+                op["summary"].as_str().expect("a summary"),
+                op["x-min-role"].as_str().expect("a role"),
+            );
+            for p in op["parameters"].as_array().into_iter().flatten() {
+                let _ = writeln!(
+                    out,
+                    "- Path parameter `{}`: {}.",
+                    p["name"].as_str().expect("a name"),
+                    p["description"].as_str().unwrap_or_default(),
+                );
+            }
+            if let Some(body) = op["requestBody"]["content"].as_object() {
+                let types: Vec<String> = body.keys().map(|t| format!("`{t}`")).collect();
+                let _ = writeln!(out, "- Body (optional): {}.", types.join(", "));
+            }
+            out.push_str("\n| Status | When |\n|---|---|\n");
+            for (code, answer) in sorted(&op["responses"]) {
+                if ["401", "403", "503"].contains(&code.as_str()) {
+                    continue;
+                }
+                let _ = writeln!(
+                    out,
+                    "| `{code}` | {} |",
+                    cell(answer["description"].as_str().unwrap_or_default()),
+                );
+            }
+            out.push('\n');
+        }
+    }
+    out
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+
+    #[test]
+    fn every_tag_of_the_document_has_a_section() {
+        let doc = openapi_value();
+        for item in doc["paths"].as_object().unwrap().values() {
+            for op in item.as_object().unwrap().values() {
+                let tag = op["tags"][0].as_str().unwrap();
+                assert!(
+                    TAGS.iter().any(|(t, _)| *t == tag),
+                    "{tag}: no section in TAGS"
+                );
+            }
+        }
+    }
+}
````

`xtask/src/config.rs`:

````diff
diff --git a/xtask/src/config.rs b/xtask/src/config.rs
new file mode 100644
index 0000000..4590155
--- /dev/null
+++ b/xtask/src/config.rs
@@ -0,0 +1,289 @@
+//! `reference/configuration.md`: every key of the config file, block by block, with its type,
+//! default, reload class, rules and meaning.
+
+use std::collections::BTreeMap;
+use std::fmt::Write;
+
+use data_avalanche_config::{
+    DocDefault, FIELD_DOCS, Key, KeyShape, Mutability, config_keys, key_mutability,
+};
+
+use crate::rules::{RuleTarget, targets};
+use crate::{BASE, cell, header};
+
+/// What each block is, said once above its table.
+const BLOCKS: &[(&str, &str)] = &[
+    (
+        "",
+        "The file's top level: its version, `globals`, and the `pipelines` map (`defaults` takes the same keys as a pipeline, merged into each, the pipeline winning).",
+    ),
+    (
+        "globals",
+        "Settings of the whole cluster, the same on every node.",
+    ),
+    ("globals.state", "Where the cluster keeps its own state."),
+    (
+        "globals.api",
+        "The management API, `/metrics` and the health probes.",
+    ),
+    ("globals.api.jwt", "Token verification for `auth: jwt`."),
+    (
+        "globals.api.oauth2",
+        "Token verification for `auth: oauth2`.",
+    ),
+    ("globals.api.tls", "TLS for the API's listener."),
+    ("globals.log", "The node's log."),
+    (
+        "globals.cluster",
+        "Membership, the hash ring and the pipelines' leases.",
+    ),
+    (
+        "globals.cluster.memberlist",
+        "The gossip (SWIM over UDP). Per-node values come from the environment through `${VAR}`.",
+    ),
+    (
+        "globals.tracing",
+        "OTLP/HTTP traces of flushes, commits, snapshot chunks and schema changes.",
+    ),
+    ("globals.azure", "Azure Blob Storage (`cloud: azure`)."),
+    (
+        "pipelines.*",
+        "One pipeline: a source database, its tables, and the sink they replicate to. The map's key is the pipeline's name.",
+    ),
+    (
+        "pipelines.*.source",
+        "The source database and the connection to it.",
+    ),
+    (
+        "pipelines.*.source.timeouts",
+        "The source connection's timeouts.",
+    ),
+    (
+        "pipelines.*.source.postgres",
+        "PostgreSQL logical replication (`type: postgres`).",
+    ),
+    (
+        "pipelines.*.source.mysql",
+        "The MySQL or MariaDB binlog stream (`type: mysql`).",
+    ),
+    (
+        "pipelines.*.tables",
+        "Which tables replicate, and per-table settings. Per-table keys name the table exactly: `schema.table` on PostgreSQL, the bare name on MySQL.",
+    ),
+    (
+        "pipelines.*.tables.snapshots",
+        "Copying a table's existing rows before its changes stream.",
+    ),
+    (
+        "pipelines.*.tables.partitioning.*[]",
+        "A table's partition fields, a list of `{ column, transform }`.",
+    ),
+    (
+        "pipelines.*.sink",
+        "The table format and where the tables are written.",
+    ),
+    (
+        "pipelines.*.sink.flush",
+        "When buffered changes are committed: the first trigger met fires.",
+    ),
+    ("pipelines.*.sink.metrics", "The pipeline's metrics."),
+    (
+        "pipelines.*.sink.delta",
+        "Delta Lake tables (`type: delta`).",
+    ),
+    (
+        "pipelines.*.sink.delta.catalog",
+        "Catalog registration of the Delta tables.",
+    ),
+    (
+        "pipelines.*.state",
+        "Where the pipeline keeps its state, when not beside its tables.",
+    ),
+    (
+        "pipelines.*.schema_evolution",
+        "Applying the source's schema changes.",
+    ),
+];
+
+/// The Iceberg sink's blocks: accepted in the file, refused at start in v1 (it ships in v2), so
+/// they are listed together at the end.
+const ICEBERG: &str = "pipelines.*.sink.iceberg";
+
+/// The block a key sits in: its path less its last segment, where a map's `*` stays with its
+/// name (`snapshots.overrides.*` is the `overrides` key of `snapshots`).
+fn block_of(path: &str) -> &str {
+    let named = path.strip_suffix(".*").unwrap_or(path);
+    named.rsplit_once('.').map_or("", |(block, _)| block)
+}
+
+/// How a path reads on the page: `<name>` for a key the operator names.
+fn shown(path: &str) -> String {
+    path.replace("pipelines.*", "pipelines.<name>")
+        .replace(".*[]", ".<table>[]")
+        .replace(".*", ".<key>")
+}
+
+fn noun(expects: &str) -> String {
+    if let Some(values) = expects.strip_prefix("one of: ") {
+        return values
+            .split(", ")
+            .map(|v| format!("`{v}`"))
+            .collect::<Vec<_>>()
+            .join(" | ");
+    }
+    match expects {
+        "text" => "string".into(),
+        "a non-negative integer" => "integer".into(),
+        "a finite number" => "number".into(),
+        "a boolean" => "boolean".into(),
+        "a duration" => "duration".into(),
+        "a byte size" => "byte size".into(),
+        "a host:port" => "host:port".into(),
+        "an IP address" => "IP address".into(),
+        "a log filter" => "log filter".into(),
+        "a logical type" => "logical type".into(),
+        other => other.into(),
+    }
+}
+
+fn type_of(key: &Key) -> String {
+    let t = noun(key.expects);
+    match key.shape {
+        KeyShape::Scalar => t,
+        KeyShape::Nullable => format!("{t} or `null`"),
+        KeyShape::List => format!("list of {t}"),
+        KeyShape::NullableList => format!("list of {t}, or `null`"),
+        KeyShape::Map => format!("map of {t}"),
+        KeyShape::MapOfLists => format!("map of lists of {t}"),
+    }
+}
+
+fn default_of(d: DocDefault) -> String {
+    match d {
+        DocDefault::None => "—".into(),
+        DocDefault::Value("") => "`\"\"`".into(),
+        DocDefault::Value(v) => format!("`{v}`"),
+        DocDefault::Note(n) => cell(n),
+    }
+}
+
+fn reload(m: Mutability) -> &'static str {
+    match m {
+        Mutability::R => "applied",
+        Mutability::PrIfTableSetChanges => "pipeline restart, if the table set changes",
+        Mutability::Pr => "pipeline restart",
+        Mutability::Proc => "process restart",
+    }
+}
+
+fn rule_link(id: &str) -> String {
+    format!(
+        "[{id}]({BASE}/reference/validation-rules/#{})",
+        id.to_lowercase()
+    )
+}
+
+pub(crate) fn page() -> String {
+    let docs: BTreeMap<&str, _> = FIELD_DOCS.iter().map(|d| (d.path, d)).collect();
+    let mut rules: BTreeMap<RuleTarget, Vec<&str>> = BTreeMap::new();
+    for (id, target) in targets() {
+        rules.entry(target).or_default().push(id);
+    }
+
+    // The keys in the model's order, grouped by the block that holds them (a block where its
+    // first key is); the Iceberg blocks go last, as one.
+    let mut blocks: Vec<(&str, Vec<Key>)> = Vec::new();
+    let mut iceberg = Vec::new();
+    for key in config_keys() {
+        if key.path.starts_with(ICEBERG) {
+            iceberg.push(key);
+            continue;
+        }
+        let block = BLOCKS
+            .iter()
+            .map(|(b, _)| *b)
+            .find(|b| block_of(&key.path) == *b)
+            .unwrap_or_else(|| panic!("{}: no block in BLOCKS holds it", key.path));
+        match blocks.iter_mut().find(|(b, _)| *b == block) {
+            Some((_, keys)) => keys.push(key),
+            None => blocks.push((block, vec![key])),
+        }
+    }
+    blocks.push((ICEBERG, iceberg));
+
+    let mut out = header(
+        "Configuration reference",
+        "Every key of the config file: its type, default, reload behaviour, rules and meaning.",
+        1,
+        "the config model (`data_avalanche_config::config_keys` and `FIELD_DOCS`)",
+    );
+    let _ = write!(
+        out,
+        "Each key's **On reload** column says what a changed value does through \
+         `POST /v1/config/reload`: *applied* in place, a *pipeline restart* (that pipeline starts \
+         again from its last checkpoint), or a *process restart* (the reload is refused, naming \
+         the key). \
+         **Rules** link to the [validation rules]({BASE}/reference/validation-rules/) that check \
+         the key. `<name>` is a pipeline's name, `<key>` a key the operator chooses. The \
+         [configuration guide]({BASE}/configuration/overview/) explains the file's structure, \
+         `${{VAR}}` substitution and how `defaults` merge.\n\n",
+    );
+    for (block, keys) in &blocks {
+        let title = match *block {
+            "" => "Top level".to_string(),
+            ICEBERG => format!("`{}` (v2)", shown(block)),
+            b => format!("`{}`", shown(b)),
+        };
+        let _ = writeln!(out, "## {title}\n");
+        let about = if *block == ICEBERG {
+            "The Iceberg sink is designed but ships in v2: a pipeline with `sink.type: iceberg` \
+             validates, and is refused at start while the other pipelines run."
+        } else {
+            BLOCKS
+                .iter()
+                .find(|(b, _)| b == block)
+                .map_or("", |(_, a)| a)
+        };
+        let _ = writeln!(out, "{about}\n");
+        if let Some(ids) = rules.get(&RuleTarget::Block(block.to_string())) {
+            let links: Vec<String> = ids.iter().map(|id| rule_link(id)).collect();
+            let _ = writeln!(out, "Rules for the whole block: {}.\n", links.join(", "));
+        }
+        out.push_str(
+            "| Key | Type | Default | On reload | Rules | Description |\n|---|---|---|---|---|---|\n",
+        );
+        for key in keys {
+            let doc = docs[key.path.as_str()];
+            let name = match key.path.strip_prefix(&format!("{block}.")) {
+                Some(name) => shown(name),
+                None => shown(&key.path),
+            };
+            let mut ids: Vec<&str> = Vec::new();
+            if key.rule.starts_with("V-") {
+                ids.push(key.rule);
+            }
+            for id in rules
+                .get(&RuleTarget::Key(key.path.clone()))
+                .into_iter()
+                .flatten()
+            {
+                if !ids.contains(id) {
+                    ids.push(id);
+                }
+            }
+            let links: Vec<String> = ids.iter().map(|id| rule_link(id)).collect();
+            let _ = writeln!(
+                out,
+                "| `{}` | {} | {} | {} | {} | {} |",
+                name,
+                cell(&type_of(key)),
+                default_of(doc.default),
+                key_mutability(&key.path).map_or("", reload),
+                links.join(" "),
+                cell(doc.doc),
+            );
+        }
+        out.push('\n');
+    }
+    out
+}
````

`xtask/src/lib.rs`:

````diff
diff --git a/xtask/src/lib.rs b/xtask/src/lib.rs
new file mode 100644
index 0000000..6f50a40
--- /dev/null
+++ b/xtask/src/lib.rs
@@ -0,0 +1,66 @@
+//! Repository automation (roadmap §2): the documentation site's generated pages (M11b). The
+//! configuration reference comes from the config model (`config_keys`, `FIELD_DOCS`, `RULES`,
+//! `key_mutability`), the API reference from the OpenAPI document, so neither can drift from the
+//! code: `cargo xtask docs` writes them, and a workspace test fails while a committed copy is stale.
+
+mod api;
+mod config;
+mod rules;
+
+use std::path::{Path, PathBuf};
+
+/// The site's base path on GitHub Pages (`astro.config.mjs`'s `base`): every link between pages
+/// starts with it, which the links validator checks.
+pub const BASE: &str = "/data-avalanche";
+
+/// The site's content directory, under the repository's root (`docs/`, a Starlight site).
+pub const CONTENT: &str = "docs/src/content/docs";
+
+/// The repository's root: the parent of this crate's directory.
+pub fn repo_root() -> PathBuf {
+    Path::new(env!("CARGO_MANIFEST_DIR"))
+        .parent()
+        .expect("xtask sits under the repository's root")
+        .to_path_buf()
+}
+
+/// Every generated file, as its path from the repository's root and its contents.
+pub fn generated() -> Vec<(PathBuf, String)> {
+    let reference = Path::new(CONTENT).join("reference");
+    vec![
+        (reference.join("configuration.md"), config::page()),
+        (reference.join("validation-rules.md"), rules::page()),
+        (reference.join("api.md"), api::page()),
+        (PathBuf::from("docs/public/openapi.json"), api::document()),
+    ]
+}
+
+/// A table cell's text: `|` escaped everywhere (GitHub's tables split on it even in code), and
+/// the characters Markdown would read as HTML escaped outside code spans.
+fn cell(text: &str) -> String {
+    let mut out = String::with_capacity(text.len());
+    let mut in_code = false;
+    for c in text.chars() {
+        match c {
+            '`' => {
+                in_code = !in_code;
+                out.push(c);
+            }
+            '|' => out.push_str("\\|"),
+            '<' if !in_code => out.push_str("&lt;"),
+            '>' if !in_code => out.push_str("&gt;"),
+            '&' if !in_code => out.push_str("&amp;"),
+            _ => out.push(c),
+        }
+    }
+    out
+}
+
+/// The page's header: Starlight's frontmatter (`order` is its place in the sidebar's Reference
+/// group; no "Edit page" link, since the page is generated), and a note that it is generated.
+fn header(title: &str, description: &str, order: u8, source: &str) -> String {
+    format!(
+        "---\ntitle: {title:?}\ndescription: {description:?}\neditUrl: false\nsidebar:\n  order: {order}\n---\n\n\
+         <!-- Generated by `cargo xtask docs` from {source}. Don't edit: change the source and run it again. -->\n\n"
+    )
+}
````

`xtask/src/main.rs`:

````diff
diff --git a/xtask/src/main.rs b/xtask/src/main.rs
new file mode 100644
index 0000000..132f2a6
--- /dev/null
+++ b/xtask/src/main.rs
@@ -0,0 +1,39 @@
+//! `cargo xtask docs` writes the documentation site's generated pages; `cargo xtask docs --check`
+//! exits 1, naming them, when a committed copy differs.
+
+use std::process::ExitCode;
+
+fn main() -> ExitCode {
+    let args: Vec<String> = std::env::args().skip(1).collect();
+    let check = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
+        ["docs"] => false,
+        ["docs", "--check"] => true,
+        _ => {
+            eprintln!("usage: cargo xtask docs [--check]");
+            return ExitCode::from(2);
+        }
+    };
+    let root = xtask::repo_root();
+    let mut stale = Vec::new();
+    for (path, text) in xtask::generated() {
+        let at = root.join(&path);
+        if std::fs::read_to_string(&at).ok().as_deref() == Some(text.as_str()) {
+            continue;
+        }
+        if check {
+            stale.push(path);
+        } else {
+            std::fs::create_dir_all(at.parent().expect("a file's directory"))
+                .and_then(|()| std::fs::write(&at, text))
+                .unwrap_or_else(|e| panic!("{}: {e}", at.display()));
+            println!("wrote {}", path.display());
+        }
+    }
+    if stale.is_empty() {
+        return ExitCode::SUCCESS;
+    }
+    for path in stale {
+        eprintln!("{} is stale: run `cargo xtask docs`", path.display());
+    }
+    ExitCode::FAILURE
+}
````

`xtask/src/rules.rs`:

````diff
diff --git a/xtask/src/rules.rs b/xtask/src/rules.rs
new file mode 100644
index 0000000..495b0a2
--- /dev/null
+++ b/xtask/src/rules.rs
@@ -0,0 +1,149 @@
+//! `reference/validation-rules.md`: every V-rule of config §13, and which key or block each one
+//! checks (read from the rule's "Path / Scope" column).
+
+use std::collections::BTreeSet;
+use std::fmt::Write;
+
+use data_avalanche_config::{RULES, RuleSeverity, config_keys};
+
+use crate::{cell, header};
+
+/// What a rule's "Path / Scope" names, as the configuration reference places it.
+#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
+pub(crate) enum RuleTarget {
+    /// One key (`globals.cluster.tokens`).
+    Key(String),
+    /// A block (`pipelines.*.source`): the rule relates its keys.
+    Block(String),
+    /// Neither: the whole file, a pipeline's name, any `${VAR}`.
+    Elsewhere,
+}
+
+/// The paths a rule's "Path / Scope" text names: its leading dotted path (`globals.api.auth` of
+/// `globals.api.auth = basic`), each alternative of `a / b`, each branch of `{x,y}`; a pipeline's
+/// field (`source.host`) under `pipelines.*`.
+pub(crate) fn named_paths(field: &str) -> Vec<String> {
+    let mut out = Vec::new();
+    for alternative in field.split(" / ") {
+        let lead: String = alternative
+            .chars()
+            .take_while(|c| c.is_ascii_alphanumeric() || "._{},".contains(*c))
+            .collect();
+        let lead = lead
+            .strip_prefix("pipeline.")
+            .unwrap_or(&lead)
+            .trim_end_matches('.');
+        let expanded = match (lead.find('{'), lead.find('}')) {
+            (Some(open), Some(close)) if open < close => lead[open + 1..close]
+                .split(',')
+                .map(|branch| format!("{}{branch}{}", &lead[..open], &lead[close + 1..]))
+                .collect(),
+            _ => vec![lead.to_string()],
+        };
+        for path in expanded {
+            if path.is_empty() {
+                continue;
+            }
+            out.push(if path.starts_with("globals") || path == "config_version" {
+                path
+            } else {
+                format!("pipelines.*.{path}")
+            });
+        }
+    }
+    out
+}
+
+/// Each rule with the key or block of the config file it checks; a rule naming several gets one
+/// entry each.
+pub(crate) fn targets() -> Vec<(&'static str, RuleTarget)> {
+    let keys: BTreeSet<String> = config_keys().into_iter().map(|k| k.path).collect();
+    let mut out = Vec::new();
+    for rule in RULES {
+        let mut placed = false;
+        for path in named_paths(rule.field) {
+            let target = if keys.contains(&path) {
+                RuleTarget::Key(path)
+            } else if keys.iter().any(|k| k.starts_with(&format!("{path}."))) {
+                RuleTarget::Block(path)
+            } else {
+                continue;
+            };
+            placed = true;
+            out.push((rule.id, target));
+        }
+        if !placed {
+            out.push((rule.id, RuleTarget::Elsewhere));
+        }
+    }
+    out
+}
+
+pub(crate) fn page() -> String {
+    let mut out = header(
+        "Validation rules",
+        "The rules a config file must pass, from the config's master validation table.",
+        2,
+        "the config model (`data_avalanche_config::RULES`)",
+    );
+    out.push_str(
+        "A config with any **error** is rejected whole: at startup the node refuses to start, \
+         and on a reload the running config stays in force. A **warning** is logged and the config \
+         loads. `POST /v1/config/validate` runs the same checks without applying anything. \
+         Rules marked *error and warning* error outside a hard range and warn outside the \
+         recommended one.\n\n\
+         | Rule | Severity | Where | Rule text |\n|---|---|---|---|\n",
+    );
+    for rule in RULES {
+        let severity = match rule.severity {
+            RuleSeverity::Error => "error",
+            RuleSeverity::Warning => "warning",
+            RuleSeverity::ErrorAndWarning => "error and warning",
+        };
+        let _ = writeln!(
+            out,
+            "| <span id=\"{}\"></span>{} | {severity} | {} | {} |",
+            rule.id.to_lowercase(),
+            rule.id,
+            cell(&format!("`{}`", rule.field.replace('`', ""))),
+            cell(rule.text),
+        );
+    }
+    out
+}
+
+#[cfg(test)]
+mod tests {
+    use super::*;
+
+    #[test]
+    fn a_rule_names_its_key_its_block_or_neither() {
+        let target = |id: &str| {
+            targets()
+                .into_iter()
+                .filter(|(i, _)| *i == id)
+                .map(|(_, t)| t)
+                .collect::<Vec<_>>()
+        };
+        assert_eq!(
+            target("V-026"),
+            [RuleTarget::Key("globals.cluster.tokens".into())]
+        );
+        assert_eq!(
+            target("V-001"),
+            [RuleTarget::Block("pipelines.*.source".into())]
+        );
+        assert_eq!(
+            target("V-011"),
+            [
+                RuleTarget::Key("pipelines.*.source.postgres.publication".into()),
+                RuleTarget::Key("pipelines.*.source.postgres.slot".into()),
+            ]
+        );
+        assert_eq!(
+            target("V-049"),
+            [RuleTarget::Key("globals.api.auth".into())]
+        );
+        assert_eq!(target("V-063"), [RuleTarget::Elsewhere]);
+    }
+}
````


- [ ] **Step 4: Run the test to see it find the pages missing**

Run: `cargo test -p xtask`
Expected: the unit tests `every_tag_of_the_document_has_a_section` and
`a_rule_names_its_key_its_block_or_neither` PASS; `the_generated_pages_are_current` FAILS:
`stale, run 'cargo xtask docs': ["docs/src/content/docs/reference/configuration.md",
"docs/src/content/docs/reference/validation-rules.md", "docs/src/content/docs/reference/api.md",
"docs/public/openapi.json"]`.

- [ ] **Step 5: Generate the pages**

Run: `cargo xtask docs`
Expected: `wrote` each of the four files. `docs/public/openapi.json` is byte-identical to
`crates/data-avalanche-api/openapi.json` (`cmp` prints nothing).

- [ ] **Step 6: Run the tests to see them pass**

Run: `cargo test -p xtask`, then `cargo test --workspace the_generated_pages` (the workspace's
`serde_json` features), then `cargo xtask docs --check`
Expected: PASS each time; `--check` prints nothing and exits 0. Then the gates.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock .cargo/config.toml xtask docs/src/content/docs/reference docs/public/openapi.json
git commit -m "feat(xtask): cargo xtask docs — the configuration reference (a table per block: type, default, reload class, rules, meaning), the validation rules and the management API from the code; --check, and a test that fails while a committed page is stale"
```

---

### Task 3: The site and its pages

**Files:**
- Create: `xtask/src/examples.rs`; `docs/package.json`, `docs/package-lock.json`,
  `docs/astro.config.mjs`, `docs/.nvmrc`, `docs/.gitignore`, `docs/src/content.config.ts`,
  `docs/src/styles/custom.css`; the 25 pages under `docs/src/content/docs/`;
  `.github/workflows/docs.yml`
- Modify: `xtask/src/lib.rs`, `xtask/Cargo.toml` (`rcgen` for the tests), `xtask/tests/docs.rs`,
  `deploy/helm/data-avalanche/{values.yaml,templates/statefulset.yaml,README.md}`,
  `crates/data-avalanche/tests/packaging.rs`, `Cargo.lock`

**Interfaces:**
- Consumes: Task 2's `xtask::{repo_root, CONTENT}`; `data_avalanche_config::{Env, validate_str}`.
- Produces: `pub struct ConfigExample { pub file: PathBuf, pub line: usize, pub text: String }`,
  `pub fn config_examples(root: &Path) -> Vec<ConfigExample>`; the site's URL
  `https://alex-karpenko.github.io/data-avalanche/`.

- [ ] **Step 1: Write the failing tests**

The examples test, its throwaway certificate's crate, and the chart's rollout test:

`crates/data-avalanche/tests/packaging.rs`:

````diff
diff --git a/crates/data-avalanche/tests/packaging.rs b/crates/data-avalanche/tests/packaging.rs
index 6e4c05e..e1ba98e 100644
--- a/crates/data-avalanche/tests/packaging.rs
+++ b/crates/data-avalanche/tests/packaging.rs
@@ -122,6 +122,26 @@ fn a_deleted_pod_has_longer_than_the_drain_to_hand_off() {
     assert!(grace > drain.as_secs(), "{grace} s, the drain {drain:?}");
 }
 
+#[test]
+fn a_rollout_waits_for_the_pipelines_to_come_back() {
+    // A new pod gets its pipelines back once it has been a member for rebalance_stabilization;
+    // the next pod rolls only after that (M11b).
+    let config = block(&chart("ci/example-values.yaml"), "config");
+    let loaded = load_str(&config, &pod_env(), &FingerprintKey::from_bytes([0; 32])).unwrap();
+    let settle = loaded.config.globals.cluster.rebalance_stabilization;
+    let ready: u64 = value(&chart("values.yaml"), "minReadySeconds: ")
+        .parse()
+        .unwrap();
+    assert!(
+        ready > settle.as_secs(),
+        "{ready} s, the stabilization {settle:?}"
+    );
+    assert!(
+        chart("templates/statefulset.yaml")
+            .contains("minReadySeconds: {{ .Values.minReadySeconds }}")
+    );
+}
+
 #[test]
 fn a_changed_config_is_said_to_go_through_the_reload() {
     // A starting node adopts the cluster's config generation, not its file (config §12): the
````

`xtask/Cargo.toml`:

````diff
diff --git a/xtask/Cargo.toml b/xtask/Cargo.toml
index f2c08b8..f9cd7ce 100644
--- a/xtask/Cargo.toml
+++ b/xtask/Cargo.toml
@@ -13,3 +13,6 @@ workspace = true
 data-avalanche-config.workspace = true
 data-avalanche-api.workspace = true
 serde_json.workspace = true
+
+[dev-dependencies]
+rcgen.workspace = true
````

`xtask/tests/docs.rs`:

````diff
diff --git a/xtask/tests/docs.rs b/xtask/tests/docs.rs
index 7a525cd..b1a4d08 100644
--- a/xtask/tests/docs.rs
+++ b/xtask/tests/docs.rs
@@ -1,4 +1,9 @@
-//! The site's generated pages match the code.
+//! The site's generated pages match the code, and its config examples are valid configs.
+
+use std::path::{Path, PathBuf};
+
+use data_avalanche_config::{Env, validate_str};
+use rcgen::{CertifiedKey, generate_simple_self_signed};
 
 #[test]
 fn the_generated_pages_are_current() {
@@ -10,3 +15,60 @@ fn the_generated_pages_are_current() {
         .collect();
     assert!(stale.is_empty(), "stale, run `cargo xtask docs`: {stale:?}");
 }
+
+/// The examples' `${VAR}`s: a throwaway certificate, key or CA for a name ending `_CERT`, `_KEY`
+/// or `_CA`; a credentials file for `_CREDENTIALS`; an address for `_IP`; a stand-in text for the
+/// rest.
+struct Placeholders {
+    pair: CertifiedKey<rcgen::KeyPair>,
+    credentials: PathBuf,
+}
+
+impl Env for Placeholders {
+    fn var(&self, name: &str) -> Option<String> {
+        Some(if name.ends_with("_CERT") || name.ends_with("_CA") {
+            self.pair.cert.pem()
+        } else if name.ends_with("_KEY") {
+            self.pair.signing_key.serialize_pem()
+        } else if name.ends_with("_CREDENTIALS") {
+            self.credentials.display().to_string()
+        } else if name.ends_with("_IP") {
+            "10.0.0.1".into()
+        } else {
+            format!("placeholder-{}", name.to_lowercase())
+        })
+    }
+}
+
+#[test]
+fn every_config_example_in_the_docs_validates() {
+    let credentials = Path::new(env!("CARGO_TARGET_TMPDIR")).join("docs-credentials.json");
+    std::fs::write(
+        &credentials,
+        r#"{"user": "cdc", "password": "placeholder"}"#,
+    )
+    .unwrap();
+    let env = Placeholders {
+        pair: generate_simple_self_signed(["localhost".to_string()]).unwrap(),
+        credentials,
+    };
+    let examples = xtask::config_examples(&xtask::repo_root());
+    assert!(!examples.is_empty(), "the pages hold config examples");
+    let mut wrong = Vec::new();
+    for example in &examples {
+        let report = validate_str(&example.text, &env);
+        if !report.is_ok() {
+            wrong.push(format!(
+                "{}:{}: {:?}",
+                example.file.display(),
+                example.line,
+                report
+                    .errors
+                    .iter()
+                    .map(|e| format!("{} {}: {}", e.rule, e.path, e.message))
+                    .collect::<Vec<_>>()
+            ));
+        }
+    }
+    assert!(wrong.is_empty(), "invalid examples:\n{}", wrong.join("\n"));
+}
````


- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p xtask`, then `cargo test -p data-avalanche --test packaging a_rollout`
Expected: FAIL: `cannot find function 'config_examples' in crate 'xtask'`; then
`a_rollout_waits_for_the_pipelines_to_come_back` panics at `packaging.rs:27:28` with
`no "minReadySeconds: "`.

- [ ] **Step 3: Write the site, its pages, the workflow and the chart's value**

`docs/package-lock.json` (6529 lines) isn't shown: copy the verified one from the planning
scratchpad (`m11b/wt2/docs/package-lock.json`, sha256
`58b60a26b823be7dbb1bc68db87f3a0f241dc7eaf2ee7f077a0ecb1c0db8771f`) when it is there; otherwise
`npm install` in `docs/` writes it from `package.json`'s exact pins, and the build below is what
checks it.

`.github/workflows/docs.yml`:

````diff
diff --git a/.github/workflows/docs.yml b/.github/workflows/docs.yml
new file mode 100644
index 0000000..7bfb0a3
--- /dev/null
+++ b/.github/workflows/docs.yml
@@ -0,0 +1,55 @@
+name: docs
+
+# The documentation site (docs/, Starlight): built on every pull request that touches it, built
+# and deployed to GitHub Pages on a push to main (the repository's Pages source is "GitHub
+# Actions"). The build fails on a link to a missing page or anchor (starlight-links-validator).
+# That the generated reference pages match the code is ci.yml's to check: the workspace tests run
+# xtask's `the_generated_pages_are_current` and `every_config_example_in_the_docs_validates`.
+on:
+  pull_request:
+    paths: [docs/**, .github/workflows/docs.yml]
+  push:
+    branches: [main]
+    paths: [docs/**, .github/workflows/docs.yml]
+  workflow_dispatch:
+
+permissions:
+  contents: read
+
+concurrency:
+  group: docs-${{ github.ref }}
+  cancel-in-progress: ${{ github.event_name == 'pull_request' }}
+
+jobs:
+  build:
+    runs-on: ubuntu-latest
+    defaults:
+      run:
+        working-directory: docs
+    steps:
+      - uses: actions/checkout@v7
+      - uses: actions/setup-node@v7
+        with:
+          node-version-file: docs/.nvmrc
+          cache: npm
+          cache-dependency-path: docs/package-lock.json
+      - run: npm ci
+      - run: npm run build
+      - if: github.ref == 'refs/heads/main'
+        uses: actions/upload-pages-artifact@v5
+        with:
+          path: docs/dist
+
+  deploy:
+    if: github.ref == 'refs/heads/main'
+    needs: build
+    runs-on: ubuntu-latest
+    permissions:
+      pages: write
+      id-token: write
+    environment:
+      name: github-pages
+      url: ${{ steps.deployment.outputs.page_url }}
+    steps:
+      - id: deployment
+        uses: actions/deploy-pages@v5
````

`deploy/helm/data-avalanche/README.md`:

````diff
diff --git a/deploy/helm/data-avalanche/README.md b/deploy/helm/data-avalanche/README.md
index 7cc5738..3156ac1 100644
--- a/deploy/helm/data-avalanche/README.md
+++ b/deploy/helm/data-avalanche/README.md
@@ -57,6 +57,7 @@ would change.
 | `serviceAccount.create`, `.name`, `.annotations` | `true`, `""`, `{}` | |
 | `service.type`, `.nodePort`, `.annotations` | `ClusterIP`, `null`, `{}` | The API, `/metrics` and the probes |
 | `terminationGracePeriodSeconds` | `90` | Above `drain_timeout` (60 s): a deleted pod hands its pipelines off first; raise it with `globals.cluster.drain_timeout` |
+| `minReadySeconds` | `30` | Above `rebalance_stabilization` (15 s): a rolling update moves on once the replaced pod's pipelines are back |
 | `livenessProbe`, `readinessProbe`, `startupProbe` | `/health/live`, `/health/ready`, `/health/live` | HTTPS when the API's TLS is on |
 | `resources` | requests 500m, 1Gi; limit 2Gi | |
 | `podDisruptionBudget.enabled`, `.maxUnavailable` | `true`, `1` | |
````

`deploy/helm/data-avalanche/templates/statefulset.yaml`:

````diff
diff --git a/deploy/helm/data-avalanche/templates/statefulset.yaml b/deploy/helm/data-avalanche/templates/statefulset.yaml
index fa33a22..c00b75c 100644
--- a/deploy/helm/data-avalanche/templates/statefulset.yaml
+++ b/deploy/helm/data-avalanche/templates/statefulset.yaml
@@ -9,6 +9,7 @@ spec:
   serviceName: {{ include "data-avalanche.headless" . }}
   # Every pod starts at once: a pod's name, not its start order, places it on the ring (D-1).
   podManagementPolicy: Parallel
+  minReadySeconds: {{ .Values.minReadySeconds }}
   selector:
     matchLabels:
       {{- include "data-avalanche.selectorLabels" . | nindent 6 }}
````

`deploy/helm/data-avalanche/values.yaml`:

````diff
diff --git a/deploy/helm/data-avalanche/values.yaml b/deploy/helm/data-avalanche/values.yaml
index 7654a9e..24926f4 100644
--- a/deploy/helm/data-avalanche/values.yaml
+++ b/deploy/helm/data-avalanche/values.yaml
@@ -84,6 +84,11 @@ resources:
 # before it is killed.
 terminationGracePeriodSeconds: 90
 
+# A rolling update waits this long after a new pod is ready before it replaces the next: the
+# pipelines handed off from the replaced pod come back once the new one has been a member for
+# globals.cluster.rebalance_stabilization (15s by default).
+minReadySeconds: 30
+
 livenessProbe:
   httpGet:
     path: /health/live
````

`docs/.gitignore`:

````diff
diff --git a/docs/.gitignore b/docs/.gitignore
new file mode 100644
index 0000000..ddce69b
--- /dev/null
+++ b/docs/.gitignore
@@ -0,0 +1,3 @@
+node_modules/
+dist/
+.astro/
````

`docs/.nvmrc`:

````diff
diff --git a/docs/.nvmrc b/docs/.nvmrc
new file mode 100644
index 0000000..a45fd52
--- /dev/null
+++ b/docs/.nvmrc
@@ -0,0 +1 @@
+24
````

`docs/astro.config.mjs`:

````diff
diff --git a/docs/astro.config.mjs b/docs/astro.config.mjs
new file mode 100644
index 0000000..da8d5f1
--- /dev/null
+++ b/docs/astro.config.mjs
@@ -0,0 +1,40 @@
+// The documentation site (M11b): Starlight, built by `npm run build` into `dist/`, deployed to
+// GitHub Pages by `.github/workflows/docs.yml`. The links validator fails the build on a link to
+// a missing page or anchor. `src/content/docs/reference/` and `public/openapi.json` are
+// generated: `cargo xtask docs`.
+import { defineConfig } from 'astro/config';
+import starlight from '@astrojs/starlight';
+import starlightLinksValidator from 'starlight-links-validator';
+import mermaid from 'astro-mermaid';
+
+export default defineConfig({
+  site: 'https://alex-karpenko.github.io',
+  base: '/data-avalanche',
+  integrations: [
+    mermaid({ autoTheme: true }),
+    starlight({
+      title: 'data-avalanche',
+      customCss: ['./src/styles/custom.css'],
+      description: 'Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake tables on object storage.',
+      social: [
+        { icon: 'github', label: 'GitHub', href: 'https://github.com/alex-karpenko/data-avalanche' },
+      ],
+      editLink: { baseUrl: 'https://github.com/alex-karpenko/data-avalanche/edit/main/docs/' },
+      plugins: [
+        starlightLinksValidator({
+          // A file of `public/`, not a page.
+          exclude: ['/data-avalanche/openapi.json'],
+        }),
+      ],
+      sidebar: [
+        { label: 'Overview', slug: 'index' },
+        { label: 'Getting started', slug: 'getting-started' },
+        { label: 'Configuration', items: [{ autogenerate: { directory: 'configuration' } }] },
+        { label: 'Architecture', items: [{ autogenerate: { directory: 'architecture' } }] },
+        { label: 'Deployment', items: [{ autogenerate: { directory: 'deployment' } }] },
+        { label: 'Runbooks', items: [{ autogenerate: { directory: 'runbooks' } }] },
+        { label: 'Reference', items: [{ autogenerate: { directory: 'reference' } }] },
+      ],
+    }),
+  ],
+});
````

`docs/package.json`:

````diff
diff --git a/docs/package.json b/docs/package.json
new file mode 100644
index 0000000..276adea
--- /dev/null
+++ b/docs/package.json
@@ -0,0 +1,18 @@
+{
+  "name": "data-avalanche-docs",
+  "private": true,
+  "type": "module",
+  "scripts": {
+    "dev": "astro dev",
+    "build": "astro build",
+    "preview": "astro preview"
+  },
+  "dependencies": {
+    "@astrojs/starlight": "0.42.7",
+    "astro": "7.4.4",
+    "astro-mermaid": "2.2.0",
+    "mermaid": "12.2.0",
+    "sharp": "0.35.5",
+    "starlight-links-validator": "0.27.0"
+  }
+}
````

`docs/src/content.config.ts`:

````diff
diff --git a/docs/src/content.config.ts b/docs/src/content.config.ts
new file mode 100644
index 0000000..6a7b7a0
--- /dev/null
+++ b/docs/src/content.config.ts
@@ -0,0 +1,7 @@
+import { defineCollection } from 'astro:content';
+import { docsLoader } from '@astrojs/starlight/loaders';
+import { docsSchema } from '@astrojs/starlight/schema';
+
+export const collections = {
+  docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
+};
````

`docs/src/content/docs/architecture/cluster.md`:

````diff
diff --git a/docs/src/content/docs/architecture/cluster.md b/docs/src/content/docs/architecture/cluster.md
new file mode 100644
index 0000000..167514c
--- /dev/null
+++ b/docs/src/content/docs/architecture/cluster.md
@@ -0,0 +1,81 @@
+---
+title: Cluster and failover
+description: How nodes share the pipelines, why a pipeline never has two writers, and what happens when a node leaves or dies.
+sidebar:
+  order: 3
+---
+
+## Two planes
+
+- **Membership is gossip.** Nodes probe each other over UDP (SWIM): a node that stops answering
+  becomes *suspect*, then *down*, after about `4 × max(1, log10 n) × probe_interval` for `n`
+  members. Every node computes the hash ring from the members it sees (`tokens` virtual nodes
+  each), keyed on the pipeline's `id`, so every node proposes the same owner for a pipeline. A
+  suspect node stays on the ring: a short network hiccup moves nothing.
+- **Ownership is a lease.** The ring only proposes. Before a node reads a row of a pipeline it
+  takes that pipeline's lease, an object in `globals.state`, with a compare-and-swap write, and it
+  renews the lease every `lease_renewal_interval`. While one node holds the lease no other can take
+  it, whatever the gossip says.
+
+## Why there is one writer
+
+A lease alone can't stop a node that has stalled (a long pause, a partition) and wakes up still
+believing it owns the pipeline. Two more rules close that:
+
+- **The safety deadline.** A holder stops writing on its own once its last successful renewal is
+  older than `lease_duration × (1 − lease_safety_margin)` (12.75 s at the defaults). A challenger
+  waits a full `lease_duration` (15 s) after the last renewal it saw before taking over, so the
+  holder has stopped before anyone else starts.
+- **The epoch fence.** Each lease term has an epoch, and every Delta commit carries the epoch of
+  the term that wrote it. A commit from an older term than the table's last one is refused, and the
+  node that tried stops the pipeline at once.
+
+## A node leaves
+
+A node that gets `SIGTERM` or `POST /v1/cluster/leave` hands its pipelines off:
+
+```mermaid
+sequenceDiagram
+  participant A as Leaving node
+  participant S as Object storage
+  participant B as Next owner
+  A->>A: stop reading, flush
+  A->>S: commit the tables, write the checkpoint
+  A->>S: release the lease
+  B->>S: take the lease (compare-and-swap)
+  B->>S: read the checkpoint
+  B->>B: resume the stream from it
+```
+
+Nothing is replayed, and the pipeline is back within seconds. A drain longer than `drain_timeout`
+gives up: the pipeline is left to the next owner as if the node had died. The node answers the API
+until it exits; its readiness probe fails while it leaves.
+
+## A node dies
+
+Its leases stop being renewed. Once the gossip marks it down, the next node on the ring for each
+of its pipelines waits out the lease (`lease_duration` after the last renewal), takes it, and
+resumes from the last checkpoint. The changes since then are replayed, and the sink's idempotence
+makes the replay harmless (see [Delivery guarantees](/data-avalanche/architecture/delivery/)).
+
+At the defaults a pipeline is unowned for about 20 seconds: a few seconds of detection, the 15 s
+lease and a write. A node that inherits many pipelines starts at most
+`max_concurrent_takeovers` (4) at once, with jitter.
+
+## A node joins
+
+A new node takes the pipelines the ring now gives it once membership has been stable for
+`rebalance_stabilization` (15 s). Each moves by a planned hand-off from its current owner, at most
+`max_concurrent_handoffs` (2) at a time per node. A node that restarts keeps its `node_name`, so
+it gets the same pipelines back.
+
+## The config
+
+The cluster's config is a sequence of **generations** in `globals.state`. `POST
+/v1/config/reload` on any node validates that node's file and publishes it as the next generation;
+the gossip tells the other nodes, and each adopts it. A node resolves the generation's `${VAR}`s
+with its own environment. `GET /v1/config/status` shows each member's generation; a node that
+can't apply one (a variable it lacks) keeps the one it runs, logs why, and tries again.
+
+A node that starts adopts the latest generation, not its file: see
+[Config changes](/data-avalanche/runbooks/config-changes/).
````

`docs/src/content/docs/architecture/data-path.md`:

````diff
diff --git a/docs/src/content/docs/architecture/data-path.md b/docs/src/content/docs/architecture/data-path.md
new file mode 100644
index 0000000..d91b15f
--- /dev/null
+++ b/docs/src/content/docs/architecture/data-path.md
@@ -0,0 +1,67 @@
+---
+title: The data path
+description: How one change travels from the source's log to a Delta commit, and when the source may forget it.
+sidebar:
+  order: 2
+---
+
+```mermaid
+flowchart TB
+  a[A transaction commits at the source] --> b[The reader decodes its rows<br/>insert, update, delete]
+  b --> c[The buffer appends them to each table's batch]
+  c --> d{A flush trigger?<br/>seconds, rows, bytes}
+  d -- not yet --> c
+  d -- yes, at a transaction boundary --> e[Collapse: one change per key, the last wins]
+  e --> f[One Delta MERGE per changed table<br/>deletes and upserts in one commit]
+  f --> g[Checkpoint: the position every commit covers]
+  g --> h[PostgreSQL: confirm the position to the slot]
+```
+
+## Reading
+
+- **PostgreSQL**: the pipeline's replication slot streams `pgoutput` messages from its
+  publication. A transaction arrives whole, at its commit.
+- **MySQL and MariaDB**: a binlog dump streams row events, positioned by GTID or by file and
+  offset. The table's column names and types come with each event (`binlog_row_metadata = FULL`)
+  and are checked against the schema the pipeline knows.
+
+Each row is decoded into typed values, and gets its order in the source (`_seq`) and its
+transaction (`_lsn`, `_ts`).
+
+## Buffering and flushing
+
+Rows wait in memory, per table, until a flush. A flush happens between transactions, never inside
+one, when the first trigger of `sink.flush` is met. A transaction bigger than the buffer's cap
+halts the pipeline, unless `max_transaction_batch_rows` lets it be committed in slices (see
+[Sink and flushing](/data-avalanche/configuration/sink/)).
+
+## Committing
+
+The flush collapses each table's changes to at most one delete and one upsert per key, keeping the
+last by `_seq`, and the sink runs one Delta MERGE per changed table: a row whose key the source
+deleted is removed, the others are inserted or replaced. Each table's MERGE is one Delta commit,
+atomic on its own; tables are committed one after the other, so a reader may see one table of a
+flush before another.
+
+A TRUNCATE at the source becomes a metadata-only delete of every row.
+
+## Checkpointing
+
+When every table of the flush is committed, the pipeline writes its checkpoint, the source position
+the commits cover, to its state location. Then, on PostgreSQL, it confirms that position to the
+slot, and the server may recycle the WAL behind it. MySQL and MariaDB keep no position for a
+reader: the binlog stays as long as the server's own retention (`binlog_expire_logs_seconds`), so
+a pipeline stopped for longer loses its position (see
+[A lost position](/data-avalanche/runbooks/position-lost/)).
+
+A crash between a commit and its checkpoint replays the changes since the last checkpoint. The
+replay writes the same values again, and the sink skips a commit it has already made: see
+[Delivery guarantees](/data-avalanche/architecture/delivery/).
+
+## Order and visibility
+
+- Within a table, changes apply in the source's order.
+- A source transaction that wrote several tables appears in each table's commit of the same flush,
+  but the tables' commits are separate: there's no atomic view across tables.
+- The lag a reader sees is the time from the source's commit to the flush that carries it: up to
+  `sink.flush.seconds` at steady state, plus the commit's time.
````

`docs/src/content/docs/architecture/delivery.md`:

````diff
diff --git a/docs/src/content/docs/architecture/delivery.md b/docs/src/content/docs/architecture/delivery.md
new file mode 100644
index 0000000..41eba34
--- /dev/null
+++ b/docs/src/content/docs/architecture/delivery.md
@@ -0,0 +1,49 @@
+---
+title: Delivery guarantees
+description: At-least-once delivery with an exactly-once effect, what makes a replay harmless, and what a reader of the tables can rely on.
+sidebar:
+  order: 4
+---
+
+data-avalanche delivers every change **at least once** and makes the result **as if once**: a
+table always converges to the source's rows, however many times a change is replayed.
+
+## Why a replay is harmless
+
+A pipeline replays the changes since its last checkpoint after a crash, a failover, a restart, or
+a lost connection. Three things make that safe, the first one alone sufficient:
+
+1. **Idempotent writes.** Every table is keyed by its primary key (or `tables.primary_key`), and a
+   flush writes each key's last state: an upsert, or a delete. Writing the same change set twice
+   leaves the same rows.
+2. **The position guard.** Each of the pipeline's Delta commits records the source position it
+   covers. Before a commit, the sink reads the table's last own commit (walking past commits other
+   writers made, such as an `OPTIMIZE`) and skips a change set that commit already covers.
+3. **The epoch fence.** Each commit also records the lease term that wrote it, and a commit from
+   an older term is refused: a node that lost its lease while stalled can't write over its
+   successor.
+
+## What a reader can rely on
+
+- **Each table's commit is atomic.** A reader sees all of a flush's changes to a table, or none.
+- **Each table converges.** After the source stops changing and the pipeline has flushed, every
+  table holds the source's rows.
+- **No atomicity across tables.** A source transaction that wrote two tables is committed to each
+  in its own Delta commit; a reader may see it in one before the other.
+- **A row may be rewritten with the same values.** After a replay, a row's Delta file may change
+  although its values don't; `_lsn` and `_seq` stay those of the source change.
+- **A large transaction may be seen in parts** when `max_transaction_batch_rows` splits it.
+
+## What stops a table instead
+
+When a change can't be applied correctly (an unsafe schema change, a value the table can't hold,
+a key column that is NULL) the pipeline **halts that table** and records why: the table keeps the
+rows it had, and the pipeline's other tables go on. It never skips the change and carries on. See
+[A halted table](/data-avalanche/runbooks/halted-table/).
+
+## Snapshots
+
+A snapshot row is written as an upsert too, and a change streamed while the snapshot runs wins over
+the snapshot's copy of the same row (see [Snapshots](/data-avalanche/architecture/snapshots/)). A
+snapshot never deletes: a row deleted at the source while its table wasn't being streamed stays in
+the lake until the table is copied afresh.
````

`docs/src/content/docs/architecture/overview.md`:

````diff
diff --git a/docs/src/content/docs/architecture/overview.md b/docs/src/content/docs/architecture/overview.md
new file mode 100644
index 0000000..38e6d9a
--- /dev/null
+++ b/docs/src/content/docs/architecture/overview.md
@@ -0,0 +1,83 @@
+---
+title: Overview
+description: The parts of a data-avalanche node, and where each one keeps what it knows.
+sidebar:
+  order: 1
+---
+
+A node is one process. It runs the pipelines the cluster gives it, and serves the management API
+and the metrics. Everything it must remember is in object storage, so any node can take over any
+pipeline.
+
+```mermaid
+flowchart TB
+  subgraph sources [Source databases]
+    pg[(PostgreSQL<br/>slot + publication)]
+    my[(MySQL / MariaDB<br/>binlog)]
+  end
+  subgraph node [A node]
+    reader[Reader<br/>decodes the change stream]
+    buffer[Buffer<br/>Arrow batches per table]
+    sink[Delta sink<br/>one MERGE per table per flush]
+    coord[Cluster<br/>gossip, ring, leases]
+    api[REST API, /metrics]
+  end
+  subgraph storage [Object storage]
+    tables[(Delta tables)]
+    pstate[(Pipeline state<br/>positions, snapshots, halts)]
+    cstate[(Cluster state<br/>anchor, config, leases)]
+  end
+  pg --> reader
+  my --> reader
+  reader --> buffer --> sink --> tables
+  sink --> pstate
+  coord <--> cstate
+  api --> coord
+```
+
+## A pipeline
+
+A pipeline joins one source database to one sink location:
+
+- **The reader** holds the source's change stream (a PostgreSQL replication slot, or a MySQL or
+  MariaDB binlog dump) and decodes each row into typed values, with the table's schema as of that
+  point in the stream.
+- **The buffer** collects the decoded rows per table as Apache Arrow batches, a transaction at a
+  time.
+- **The flush** collapses the buffered changes to one per key (the last one wins), and the
+  **sink** commits each changed table in one Delta MERGE: upserts and deletes together.
+- **The checkpoint** then records the source position the commits cover, and the reader confirms
+  it to the source, which may then release the WAL or binlog behind it.
+
+A pipeline also runs its **snapshots** (copying existing rows beside the stream) and follows the
+**schema** of its tables. [The data path](/data-avalanche/architecture/data-path/) follows one
+change through these steps.
+
+## The cluster
+
+Each node gossips with the others (SWIM), and every node computes the same hash ring from the
+members it sees. The ring proposes an owner for each pipeline; a **lease** in object storage,
+taken with a compare-and-swap write, makes the owner the only writer. A node that dies stops
+renewing its leases, and the next node on the ring takes them over when they expire.
+[Cluster and failover](/data-avalanche/architecture/cluster/) has the details.
+
+The cluster's config is also in object storage: a reload on any node publishes a new **config
+generation**, and every node adopts it.
+
+## State
+
+| Where | What |
+|---|---|
+| a pipeline's state location | its desired state (running, paused, stopped), its source position, its snapshot cursors and jobs, its halted tables, its cached table schemas |
+| `globals.state` | the cluster's anchor, the config generations, the members, one lease per pipeline |
+| the Delta log | each commit's position and owner, which the sink reads back to never apply a change twice |
+
+Each state file is small, versioned, and written with a conditional write, so two writers can't
+overwrite each other.
+
+## Further reading
+
+The design is specified in the repository:
+[System overview](https://github.com/alex-karpenko/data-avalanche/blob/main/doc/specs/System-Overview.md),
+[Data flow](https://github.com/alex-karpenko/data-avalanche/blob/main/doc/specs/Data-Flow.md),
+[Clustering](https://github.com/alex-karpenko/data-avalanche/blob/main/doc/research/Clustering_Architecture_Research.md).
````

`docs/src/content/docs/architecture/schema-changes.md`:

````diff
diff --git a/docs/src/content/docs/architecture/schema-changes.md b/docs/src/content/docs/architecture/schema-changes.md
new file mode 100644
index 0000000..a652340
--- /dev/null
+++ b/docs/src/content/docs/architecture/schema-changes.md
@@ -0,0 +1,58 @@
+---
+title: Schema changes
+description: How a pipeline notices a source schema change, which changes the Delta table follows, and which halt the table.
+sidebar:
+  order: 5
+---
+
+A pipeline keeps the schema of each table it replicates, and applies a change at its place in the
+stream: it flushes what came before, changes the Delta table, and goes on. A change the Delta table
+can't follow halts that table, with its reason; the other tables go on.
+
+## Noticing a change
+
+- **At every start**, the pipeline reads each table's schema from the source's catalog and compares
+  it with the one it kept.
+- **In the stream.** MySQL and MariaDB write DDL to the binlog, and each row event carries its
+  table's columns. PostgreSQL writes no DDL to the replication stream: a changed table is noticed
+  when its next row arrives with a new column list.
+- **On a timer.** Every `schema_evolution.rescan_interval` (60 minutes) the pipeline checks the
+  tables that had no changes, which is how PostgreSQL notices a dropped table.
+
+The pipeline then reads the table's catalog entry to tell a rename from a drop and an add, and to
+get each column's exact type.
+
+## What the Delta table does
+
+| Source change | The Delta table |
+|---|---|
+| add a column | the column is added, nullable; older rows read NULL |
+| drop a column | **kept**, NULL from then on (a soft drop, with a warning) |
+| rename a column | the new name is added, the old one soft-dropped; history stays under the old name |
+| change a column's type | **halts** the table (`EVOLUTION_UNSUPPORTED` or `UNSAFE_DDL`): Delta can't widen a column in place |
+| `NULL` ↔ `NOT NULL` | nothing: every non-key column is nullable |
+| change the primary key, or a `REPLICA IDENTITY` that no longer covers it | **halts** the table (`UNSAFE_DDL`) |
+| add or rename to a lineage column's name | **halts** the table (`RESERVED_COLUMN_COLLISION`) |
+| drop the table | **halts** the table (`TABLE_DROPPED`); the Delta table is kept |
+| MySQL: reorder or remove `ENUM` / `SET` values | **halts** the table |
+| MySQL: a partition or tablespace operation that changes rows without row events (`TRUNCATE PARTITION`, `EXCHANGE PARTITION`, …) | **halts** the table |
+| a DDL the pipeline can't parse, on a tracked table | **halts** the table |
+| indexes, comments, storage parameters, `AUTO_INCREMENT` | nothing |
+
+`data_avalanche_sink_soft_dropped_columns` counts the soft-dropped columns: drop them yourself, if
+you want, with your Delta engine once nothing reads them.
+
+`schema_evolution.enabled: false` halts a table at any schema change (`EVOLUTION_DISABLED`).
+
+## Online migrations
+
+MySQL tools that copy a table to a shadow and swap it in (gh-ost's `_t_gho`, pt-online-schema-change's
+`_t_new`) are followed: the swap, one `RENAME TABLE t TO …, _t_gho TO t` statement, is read as the
+column changes between the two tables, each handled as above. Any other rename of a tracked table
+away halts it as dropped.
+
+## A re-created table
+
+A table dropped and created again under the same name is a new table: the pipeline keeps it halted
+(`TABLE_DROPPED`) until a snapshot job copies it afresh. See
+[A halted table](/data-avalanche/runbooks/halted-table/).
````

`docs/src/content/docs/architecture/snapshots.md`:

````diff
diff --git a/docs/src/content/docs/architecture/snapshots.md b/docs/src/content/docs/architecture/snapshots.md
new file mode 100644
index 0000000..cee4209
--- /dev/null
+++ b/docs/src/content/docs/architecture/snapshots.md
@@ -0,0 +1,62 @@
+---
+title: Snapshots
+description: "Copying a table's existing rows beside the live stream: the snapshot modes, how a snapshot resumes, and snapshot jobs."
+sidebar:
+  order: 6
+---
+
+A snapshot copies a table's existing rows into its Delta table. It never stops the stream and never
+holds a long transaction: the pipeline streams first, and reads each table in short keyset chunks
+between stream transactions.
+
+## Modes
+
+`tables.snapshots.mode` (per table with `snapshots.overrides`) decides what a pipeline start does:
+
+| Mode | At a pipeline start |
+|---|---|
+| `never` (the default) | no snapshot: stream the changes from now on |
+| `initial` | snapshot a table that has never been replicated, then stream; skip one that has |
+| `always` | snapshot every table at every start, then stream |
+| `dump_initial` | snapshot a table that has never been replicated, then stop streaming it |
+| `dump_always` | snapshot every table at every start, and stream nothing |
+
+A pipeline whose tables are all `dump_*` and done shows `actual_state: dumped`.
+
+## How a snapshot reads
+
+```mermaid
+flowchart TB
+  a[Stream established at position P] --> b[Read the next chunk:<br/>rows with key greater than the cursor,<br/>chunk_size of them, in a short transaction]
+  b --> c[Drop the chunk's rows that a streamed<br/>transaction the read didn't see has changed]
+  c --> d[Flush the stream's changes, then commit the chunk]
+  d --> e[Checkpoint the cursor with the flush]
+  e --> f{Last chunk?}
+  f -- no --> b
+  f -- yes --> g[Table done: it simply streams]
+```
+
+- Each chunk is `chunk_size` rows (10000) in key order. The cursor is checkpointed with the flush
+  that commits the chunk, so a crash resumes at the next chunk, never at the first row.
+- A row the stream changes while the snapshot runs converges: the streamed change wins over the
+  chunk's older copy.
+- `parallelism` (1) tables are snapshotted at once; one table is read in order. On PostgreSQL each
+  table being snapshotted holds about three connections: more than 4 at once warns (V-038).
+- `source.timeouts.snapshot` (6 h) bounds one table's snapshot in one run; past it the table halts
+  `SNAPSHOT_TIMEOUT`. Raise it and start a job for the table.
+
+## Snapshot jobs
+
+`POST /v1/pipelines/<id>/snapshots` with `{"tables": ["public.orders"]}` copies tables on demand,
+named exactly as the config's per-table keys are (no patterns). The answer is `202` with the job's
+id; `GET /v1/snapshots/<job_id>` follows it, `GET /v1/pipelines/<id>/snapshots` lists the
+pipeline's jobs. Jobs are kept in the pipeline's state and survive restarts and failover.
+
+- The pipeline restarts to take the job: its owner replays from its checkpoint and snapshots the
+  job's tables from their first row, whatever their mode.
+- A job's table that is halted has its halt cleared and its schema taken afresh from the catalog.
+- A table the config doesn't replicate is copied once, and not streamed.
+- A job **upserts**: it never deletes. A row deleted at the source while the table was halted, or
+  not replicated, stays in the Delta table. To start clean, drop or move the Delta table first.
+
+[Snapshot jobs](/data-avalanche/runbooks/snapshot-jobs/) is the runbook.
````

`docs/src/content/docs/configuration/cluster.md`:

````diff
diff --git a/docs/src/content/docs/configuration/cluster.md b/docs/src/content/docs/configuration/cluster.md
new file mode 100644
index 0000000..0924f1b
--- /dev/null
+++ b/docs/src/content/docs/configuration/cluster.md
@@ -0,0 +1,104 @@
+---
+title: Clusters
+description: One node or several, node identity, discovery, and the values that differ per node.
+sidebar:
+  order: 4
+---
+
+Every node is a member of a cluster, and one node alone is a cluster of one: the same code, with
+its lease. Nodes that share a `globals.state` location are one cluster. They find each other by
+gossip (SWIM over UDP, port 7946), place every pipeline on a hash ring, and the node a pipeline
+lands on takes its lease in object storage before it reads a row. How that holds up when nodes
+fail is in [Cluster and failover](/data-avalanche/architecture/cluster/).
+
+## One node
+
+Leave `globals.cluster` out. The node's name is its hostname, it has no peers, and it runs every
+pipeline. A second process started on the same `globals.state` by mistake (an overlapping
+deploy) has no peer to join, so it places every pipeline on itself too; but each lease has one
+holder, so it waits for the leases the first one holds and never writes those pipelines.
+
+## Several nodes
+
+Every node runs **the same config**, the cluster's config generation in `globals.state`, not its
+own file: the file only bootstraps the first generation and is the text a reload publishes (see
+[Config changes](/data-avalanche/runbooks/config-changes/)). So the values that differ per node are
+written as `${VAR}`, which each node resolves from its own environment:
+
+- `globals.cluster.memberlist.node_name`: the node's identity, and its place on the ring. It must
+  stay the same across the node's restarts: a pod name of a StatefulSet, or a name kept on the
+  host, never a pod IP or a Deployment's random pod name.
+- `memberlist.advertise_addr` (and `advertise_port`): the address the other nodes reach this one
+  at.
+- the API's `globals.api.address`, when nodes share a host.
+
+A value written as a literal in one node's file is ignored once the cluster has a generation: the
+node takes the generation's, and warns at startup naming each key that differs.
+
+```yaml
+config_version: 1
+globals:
+  cloud: aws
+  state:
+    bucket: example-cdc-state
+    prefix: cdc
+  cluster:
+    discovery: static
+    memberlist:
+      node_name: ${NODE_NAME}
+      advertise_addr: ${NODE_IP}
+      join_members:
+        - cdc-1.example.internal:7946
+        - cdc-2.example.internal:7946
+        - cdc-3.example.internal:7946
+pipelines: {}
+```
+
+## Discovery
+
+- `discovery: static` (the default) joins the addresses in `memberlist.join_members`. Each is a
+  peer's advertised address, not a load balancer's or a NAT's, whose packets the gossip drops.
+- `discovery: dns` resolves `discovery_dns_name`, a Kubernetes headless Service, and joins every
+  address it returns. The [Helm chart](/data-avalanche/deployment/kubernetes/) sets it up.
+
+:::caution[Pods started together with DNS discovery]
+A node asks DNS again only while it has no peer. When four or more pods start at once, nodes that
+each resolved some of the others can settle in separate groups that never merge: each group
+places every pipeline, the leases keep one writer per pipeline, but the groups contend for them.
+After a cold start, check that `GET /v1/cluster/status` on any node lists every pod. If it
+doesn't, restart the pods of the smaller group one at a time: a starting node resolves the name
+again.
+:::
+
+## Settings fixed for the cluster's life
+
+The first node of a cluster writes its **anchor** to `globals.state`: `tokens` (virtual nodes per
+member, 128), `lock_backend` (`storage`) and `lease_duration` (15 s). A node whose config has
+other values refuses to start. They can't change for that `globals.state`.
+
+The gossip's packets carry a MAC keyed from the anchor, so a node of another cluster can't join
+or disturb this one. They aren't encrypted: they carry node names, addresses and states, never
+data or secrets.
+
+## Leases and hand-offs
+
+| Key | Default | |
+|---|---|---|
+| `lease_duration` | `15s` | how long a dead node's pipelines wait before another node takes them |
+| `lease_renewal_interval` | `5s` | at most a third of `lease_duration` |
+| `drain_timeout` | `60s` | how long a node leaving gracefully spends flushing a pipeline before it lets it go |
+| `rebalance_stabilization` | `15s` | how long membership must be stable before pipelines move to a node that joined |
+| `max_concurrent_handoffs` | `2` | pipelines a leaving node drains at once |
+| `max_concurrent_takeovers` | `4` | pipelines a node starts at once after another node died |
+
+A node that leaves gracefully (`SIGTERM`, `POST /v1/cluster/leave`) drains its pipelines: it
+flushes and commits each one, then releases its lease, and the next owner starts at once. Size the
+time a supervisor waits before it kills the process (Kubernetes' `terminationGracePeriodSeconds`)
+for the worst case:
+
+```text
+drain_timeout × ceil(pipelines on the node / max_concurrent_handoffs) + 2 × lease_renewal_interval + drain_timeout
+```
+
+130 s for one pipeline at the defaults. A node killed earlier isn't a loss: its pipelines wait
+for their leases to expire and the next owner replays from the last checkpoint.
````

`docs/src/content/docs/configuration/examples.md`:

````diff
diff --git a/docs/src/content/docs/configuration/examples.md b/docs/src/content/docs/configuration/examples.md
new file mode 100644
index 0000000..f965f01
--- /dev/null
+++ b/docs/src/content/docs/configuration/examples.md
@@ -0,0 +1,171 @@
+---
+title: Examples
+description: Complete config files for Google Cloud Storage, Azure, API authentication, tracing and split state.
+sidebar:
+  order: 6
+---
+
+Each example is a whole config file. The repository's tests load every one of them, so they stay
+valid as the config changes. `${…}` values come from the environment.
+
+## MySQL into Google Cloud Storage, with JWT auth
+
+The API takes bearer tokens from an identity provider and maps its groups to the local roles.
+
+```yaml
+config_version: 1
+globals:
+  cloud: google
+  state:
+    bucket: example-cdc-state
+  api:
+    address: 0.0.0.0:8080
+    auth: jwt
+    jwt:
+      issuer: https://idp.example.com/
+      audience: data-avalanche
+      jwks_url: https://idp.example.com/.well-known/jwks.json
+      roles_claim: groups
+    role_mapping:
+      viewer: [cdc-readers]
+      operator: [cdc-operators]
+      admin: [cdc-admins]
+  log:
+    level: info
+    format: json
+pipelines:
+  inventory:
+    id: inventory
+    source:
+      type: mysql
+      host: inventory-db.example.internal
+      db: inventory
+      credentials_file: ${INVENTORY_DB_CREDENTIALS}
+      ssl_mode: verify-full
+      mysql:
+        replication_type: gtid
+        binlog_purge_action: re_snapshot
+    tables:
+      include: ["*"]
+      exclude: [audit_*]
+      snapshots:
+        mode: initial
+        parallelism: 2
+    sink:
+      type: delta
+      bucket: example-lake
+      prefix: inventory
+      flush:
+        seconds: 60
+        rows: 500000
+```
+
+`credentials_file` names a JSON file `{"user": …, "password": …}` that a mounted Secret keeps
+current: replace it and reload to rotate the password without a restart.
+
+## PostgreSQL into Azure, with basic auth over TLS
+
+```yaml
+config_version: 1
+globals:
+  cloud: azure
+  azure:
+    account_name: examplelake
+  state:
+    bucket: cdc-state
+  api:
+    auth: basic
+    user: ops
+    password: ${API_PASSWORD}
+    tls:
+      enabled: true
+      cert: ${API_TLS_CERT}
+      key: ${API_TLS_KEY}
+pipelines:
+  payments:
+    id: payments
+    source:
+      type: postgres
+      host: payments.postgres.database.azure.com
+      db: payments
+      user: cdc
+      password: ${PAYMENTS_DB_PASSWORD}
+      ssl_mode: verify-full
+      ssl_ca: ${PAYMENTS_DB_CA}
+      postgres:
+        publication: payments_pub
+        slot: payments_slot
+        max_wal_lag_bytes: 50GiB
+    tables:
+      include: [public.payments, public.refunds]
+      primary_key:
+        public.refunds: [payment_id, refund_no]
+    sink:
+      type: delta
+      bucket: lake
+      delta:
+        timestamp_ntz_as: timestamp
+```
+
+`API_TLS_CERT`, `API_TLS_KEY` and `PAYMENTS_DB_CA` hold PEM text (or a path to a PEM file). On
+Azure `bucket` is a container of the `examplelake` account.
+
+## Shared defaults, split state and tracing
+
+Two pipelines on one PostgreSQL server; their state lives in a bucket of its own, and flushes are
+traced to an OpenTelemetry collector.
+
+```yaml
+config_version: 1
+globals:
+  cloud: aws
+  state:
+    bucket: example-cdc-state
+  tracing:
+    otlp_endpoint: http://otel-collector.observability:4318
+    sample_ratio: 0.1
+defaults:
+  source:
+    type: postgres
+    host: erp-db.example.internal
+    db: erp
+    user: cdc
+    password: ${ERP_DB_PASSWORD}
+    postgres:
+      max_wal_lag_bytes: 20GiB
+  tables:
+    snapshots:
+      mode: initial
+  sink:
+    type: delta
+    bucket: example-lake
+    metrics:
+      per_table: true
+  state:
+    bucket: example-pipeline-state
+    prefix: erp
+pipelines:
+  erp-sales:
+    id: erp-sales
+    source:
+      postgres:
+        publication: erp_sales_pub
+        slot: erp_sales_slot
+    tables:
+      include: [sales.*]
+  erp-stock:
+    id: erp-stock
+    source:
+      postgres:
+        publication: erp_stock_pub
+        slot: erp_stock_slot
+    tables:
+      include: [stock.*]
+      snapshots:
+        mode: never
+```
+
+`erp-sales` keeps its state at `example-pipeline-state/erp/erp-sales/`, `erp-stock` at
+`example-pipeline-state/erp/erp-stock/`; their tables at `example-lake/erp-sales/…` and
+`example-lake/erp-stock/…`. `erp-stock` overrides only `snapshots.mode`: the other `tables`
+keys still come from `defaults`.
````

`docs/src/content/docs/configuration/overview.md`:

````diff
diff --git a/docs/src/content/docs/configuration/overview.md b/docs/src/content/docs/configuration/overview.md
new file mode 100644
index 0000000..ea130aa
--- /dev/null
+++ b/docs/src/content/docs/configuration/overview.md
@@ -0,0 +1,141 @@
+---
+title: The config file
+description: The config file's structure, how defaults merge, environment variables, secrets, and when a change applies.
+sidebar:
+  order: 1
+---
+
+A node reads one YAML file, named by `--config`. The file holds the cluster's settings and every
+pipeline; a node runs every pipeline the ring gives it. The
+[configuration reference](/data-avalanche/reference/configuration/) lists every key.
+
+## Structure
+
+```yaml
+config_version: 1
+globals:
+  cloud: aws
+  state:
+    bucket: example-cdc-state
+defaults:
+  source:
+    type: postgres
+    host: analytics-db.example.internal
+    db: analytics
+    user: cdc
+    password: ${ANALYTICS_DB_PASSWORD}
+  sink:
+    type: delta
+    bucket: example-lake
+pipelines:
+  orders:
+    id: orders
+    source:
+      postgres:
+        publication: orders_pub
+        slot: orders_slot
+    tables:
+      include: [sales.orders, sales.order_lines]
+  customers:
+    id: customers
+    source:
+      postgres:
+        publication: customers_pub
+        slot: customers_slot
+    tables:
+      include: [crm.*]
+      exclude: [crm.tmp_*]
+```
+
+- **`config_version`** is required; `1` is the only version.
+- **`globals`** holds what the whole cluster shares: the cloud, where the cluster keeps its state,
+  the API, logging, the cluster's own settings, tracing.
+- **`defaults`** is shaped like a pipeline. It is merged into every pipeline, and the pipeline
+  wins. It may not hold a pipeline's `id`, its publication or its slot, which must be unique.
+- **`pipelines`** maps a pipeline's name to its config. It may be empty (`pipelines: {}`): the
+  cluster runs and waits.
+
+The parser is strict: an unknown key is an error, at every level. A misspelt key never silently
+falls back to its default.
+
+## A pipeline's name and its id
+
+The map key (`orders` above) is the pipeline's **name**: the `_pipeline` column of every row and
+the `pipeline` label of its metrics. Renaming it is harmless.
+
+`id` is the pipeline's **identity**: its state, its lease, its place on the ring, its default
+table prefix (`sink.prefix`) and the default MySQL `server_id` derive from it. Changing `id`
+deletes the pipeline and creates a new one: a new state, and a new snapshot per its
+`snapshots.mode`. Lower-case letters, digits, `_` and `-`, at most 63 characters (V-055).
+
+:::caution[Reusing an id or its resources]
+When a pipeline is removed and another one takes its `id`, slot, publication, sink prefix or state
+location, start the new one only after the old one's lease is released (watch
+`GET /v1/cluster/status`), or in the same reload as the removal. A node that starts or joins in
+between, or a second reload, can start the new pipeline while the old one still drains.
+:::
+
+## How defaults merge
+
+- **Values and maps** merge key by key; the pipeline's value wins.
+- **Lists are replaced**, never concatenated: a pipeline's `tables.include` replaces the one in
+  `defaults`.
+- **The engine and format blocks are alternatives.** `source.postgres` and `source.mysql` are
+  chosen by `source.type`, `sink.delta` and `sink.iceberg` by `sink.type`: a pipeline that sets
+  `source.type: mysql` drops a `postgres` block it inherits, and keeps the common keys (`host`,
+  `port`, `user`, …).
+- A value that resolves to empty (`${VAR:-}` with the variable unset) counts as absent and
+  inherits, except a `prefix`, where empty means the bucket's root.
+
+## Values
+
+- **Durations** are `humantime` strings: `200ms`, `30s`, `1h30m`, `30d`.
+- **Byte sizes** take units, `512MiB` (binary) or `512MB` (decimal), or a bare number of bytes.
+- **`${VAR}`** in any string is replaced by the environment variable, whole or in part
+  (`bucket: ${ACCOUNT}-lake`). An unset variable is an error (V-063); `${VAR:-text}` gives a
+  default, and `$${VAR}` writes a literal `${VAR}`. The result is then read as the key's type, so
+  `port: ${DB_PORT}` is a number.
+- **TLS material** (`cert`, `key`, `ca`, `ssl_cert`, …) is a file path, or the PEM text itself (a
+  value starting `-----BEGIN`), or a `${VAR}` holding either.
+
+## Secrets
+
+The file never needs a secret in clear. Two ways in:
+
+- **`${VAR}`**, from the process's environment (a Kubernetes Secret through `env`). A changed
+  variable takes a process restart.
+- **A credentials file**, `credentials_file` in place of `user` and `password`: a JSON file
+  `{"user": "…", "password": "…"}`, which a secret manager's sidecar or a mounted Secret keeps
+  current. Replace its contents and reload: the reload compares the resolved values, sees the new
+  credentials and applies them without restarting the process.
+
+A resolved secret stays in memory: it never reaches the logs, the state in object storage, or the
+answers of the API, which show a templated value as its `${VAR}` text.
+
+## Validation
+
+A node checks the whole file before it applies anything, in three passes: the YAML and its
+keys; the rules over the keys as written (structure, required keys, value ranges, cross-key
+rules); then every `${VAR}`, file and certificate resolved and checked. Any **error** rejects the
+whole file: a starting node exits, a reload leaves the running config in force. A **warning** is
+logged and the file loads. Each finding names its rule and the key's path; the
+[validation rules](/data-avalanche/reference/validation-rules/) list them all.
+
+What validation can't see waits for a pipeline's start: an unreachable host, a wrong password, a
+missing privilege, a table without a key. Those stop that pipeline or that table, and the others
+run.
+
+## When a change applies
+
+A running cluster takes a new config through `POST /v1/config/reload`, never by itself. Each key
+of the [reference](/data-avalanche/reference/configuration/) has a reload class:
+
+| On reload | What happens |
+|---|---|
+| applied | in place, at once |
+| pipeline restart | that pipeline stops, drops what it hasn't committed, and starts again from its last checkpoint with the new value; the replay is idempotent, and the other pipelines don't notice |
+| pipeline restart, if the table set changes | a restart only when the tables the patterns match change |
+| process restart | the reload is refused, naming the key (V-061); see [Config changes](/data-avalanche/runbooks/config-changes/#keys-a-reload-refuses) |
+
+A node that restarts takes the config the cluster runs, not its own file: see
+[Config changes](/data-avalanche/runbooks/config-changes/).
````

`docs/src/content/docs/configuration/sink.md`:

````diff
diff --git a/docs/src/content/docs/configuration/sink.md b/docs/src/content/docs/configuration/sink.md
new file mode 100644
index 0000000..8a5b6a4
--- /dev/null
+++ b/docs/src/content/docs/configuration/sink.md
@@ -0,0 +1,99 @@
+---
+title: Sink and flushing
+description: The Delta sink's settings, when changes are committed, what a commit costs, and how source types land in Delta.
+sidebar:
+  order: 5
+---
+
+Every pipeline names its table format with `sink.type`, which has no default. v1 writes Delta Lake
+(`delta`); `iceberg` is accepted by the config but refused at start ("the Iceberg sink ships in
+v2"), while the other pipelines run.
+
+## Flushing
+
+A pipeline buffers changes in memory and commits them in a **flush**: one Delta MERGE per table that
+changed, each table committed atomically, then the pipeline's checkpoint. A flush fires when the
+first of its triggers is met, always at a source transaction boundary:
+
+| `sink.flush` | Default | |
+|---|---|---|
+| `seconds` | `30` | the oldest buffered change is this old; `0` flushes at every transaction |
+| `rows` | `100000` | this many rows are buffered |
+| `bytes` | `512MiB` | the buffer holds this much |
+
+Each trigger can be turned off with `null`, but not all three (V-035). `POST
+/v1/pipelines/<id>/flush` flushes at once.
+
+**What a flush costs.** A Delta MERGE rewrites every data file that holds a key the flush changed.
+Changes to recent rows (appends, updates to new orders) touch a few files; updates spread over a
+large table's whole key range touch most of its files, so each flush rewrites most of the table.
+Watch `data_avalanche_sink_merge_files_rewritten_total` and
+`data_avalanche_sink_merge_rows_copied_total`: when they grow much faster than the rows changed,
+flush less often (raise `seconds` and `rows`), so each MERGE carries more changes for the same
+rewrite. The cost of a lower bound is lag: the
+[replication lag](/data-avalanche/deployment/observability/) is up to about the flush interval
+at steady state.
+
+**Large transactions.** A transaction is committed whole, so it is buffered whole:
+`max_transaction_buffer_bytes` (1 GiB) caps it, and a bigger one halts the pipeline rather than
+the process running out of memory. Set `max_transaction_batch_rows` to commit a large transaction
+in slices instead: readers then see it in parts, and a crash replays it from its start.
+
+**Back-pressure.** The buffer between reading and writing holds `buffer_capacity_rows` (200000)
+rows. While the storage fails, the pipeline retries its commit with backoff and, once the buffer
+is full, stops reading the source until the storage answers.
+
+## Delta settings
+
+`sink.delta` applies when the sink creates a table; an existing table keeps its properties.
+
+- `timestamp_ntz_as`: how a `timestamp without time zone` column is written. The default,
+  `timestamp_ntz`, keeps its meaning but creates the table at reader version 3 and writer version 7
+  (the `timestampNtz` table feature), which older readers can't open. `timestamp` writes the wall
+  clock as a UTC instant at protocol 1/2; `string` writes its text.
+- `checkpoint_interval` (10) and `log_retention` (30 days): Delta's own `delta.checkpointInterval`
+  and `delta.logRetentionDuration`.
+- `merge_memory_limit` caps one MERGE's memory. A MERGE that needs more fails on every replay, so
+  the table halts `SINK_REJECTED` naming the key: raise it or lower the flush bounds, then
+  re-snapshot the table.
+- `table_properties` passes table properties through; `delta.*` keys must be on the allow-list
+  (V-093).
+- `column_mapping: name` is refused in v1: delta-rs can't add a column to a column-mapped table.
+
+Partitioning (`tables.partitioning`) takes `identity` fields only on Delta.
+
+## What lands in the table
+
+Each table gets the source's columns, then six lineage columns:
+
+| Column | |
+|---|---|
+| `_op` | `I` insert, `U` update, `r` a snapshot row (a delete removes the row) |
+| `_ts` | the source transaction's commit time |
+| `_lsn` | the source transaction: its commit LSN, or its GTID, or its binlog file and offset |
+| `_seq` | the change's place in the source's order, as text that sorts the same way |
+| `_source_db` | `host/database` of the source |
+| `_pipeline` | the pipeline's name |
+
+A source column with one of these names (in any case), or starting `__data_avalanche_`, halts its
+table with `RESERVED_COLUMN_COLLISION`: rename it at the source.
+
+Identifier columns are `NOT NULL`; every other column is nullable, whatever the source says, so
+that a column added later can be added.
+
+Types Delta lacks are converted, and every column records its source's logical type in its Delta
+metadata:
+
+| Source | Delta |
+|---|---|
+| `uuid` | `string`, the canonical lower-case form |
+| `time` | `string`, `HH:MM:SS.ffffff` |
+| MySQL `BIGINT UNSIGNED` | `decimal(20,0)` |
+| PostgreSQL `numeric` wider than 38 digits | `string` (or a halt, `numeric_overflow_action: error`) |
+| `json`, `jsonb`, `enum`, and most types with no native mapping | `string`, the source's text |
+| PostgreSQL `±infinity` and dates outside 0001-01-01 … 9999-12-31 | clamped to that range |
+
+`tables.column_type_overrides` sets a column's type, in the sink-neutral grammar (`int64`,
+`decimal(38,10)`, `string`, `timestamp_tz`, …). An override must be the column's own type, `string`,
+or a lossless conversion (`int32` to `int64`, `timestamp_ntz` to `timestamp_tz`, …); any other
+halts the table at start with `SCHEMA_MISMATCH`.
````

`docs/src/content/docs/configuration/sources.md`:

````diff
diff --git a/docs/src/content/docs/configuration/sources.md b/docs/src/content/docs/configuration/sources.md
new file mode 100644
index 0000000..e9b5d6a
--- /dev/null
+++ b/docs/src/content/docs/configuration/sources.md
@@ -0,0 +1,141 @@
+---
+title: Sources
+description: What PostgreSQL, MySQL and MariaDB need, and the source settings of a pipeline.
+sidebar:
+  order: 2
+---
+
+A pipeline reads one database: a PostgreSQL database, or one MySQL or MariaDB database (schema).
+`source.type` picks the engine; the common keys (`host`, `port`, `db`, `user`, `password` or
+`credentials_file`, `ssl_mode`, `timeouts`) are the same for both. `ssl_mode` defaults to
+`require`.
+
+The server settings and privileges below are checked when the pipeline starts. A missing one stops
+that pipeline (or that table) with a message naming it; the other pipelines run.
+[Source privileges](/data-avalanche/runbooks/source-privileges/) has the grants.
+
+## PostgreSQL
+
+Tested on PostgreSQL 14 to 18.
+
+**Server settings**
+
+- `wal_level = logical`.
+- A free replication slot and WAL sender per pipeline (`max_replication_slots`,
+  `max_wal_senders`).
+
+**Each table**
+
+- has a primary key, or a `tables.primary_key` override naming NOT NULL, non-floating columns;
+- has a `REPLICA IDENTITY` other than `NOTHING` that covers the key (the default, the primary key,
+  does);
+- is a logged table: PostgreSQL doesn't publish `UNLOGGED` tables.
+
+A table without a key that a pattern matches is skipped with a warning; one named exactly halts.
+
+**The slot and the publication.** `source.postgres.publication` and `slot` name them; both are
+required and unique per database. By default (`slot_management: managed`) the pipeline creates the
+slot and the publication, keeps the publication's tables equal to the tables it replicates, and
+drops both when the pipeline is removed (`auto_cleanup: true`). With
+`slot_management: require_existing` it creates neither: a DBA creates them, and the pipeline
+refuses to start without them (and needs `auto_cleanup: false`). The managed publication needs
+the pipeline's role to own the tables;
+[Source privileges](/data-avalanche/runbooks/source-privileges/) has both set-ups.
+
+**Partitioned tables.** Name the partitions, or a pattern that matches them: each partition
+replicates as its own table. A publication that publishes through the partition root
+(`publish_via_partition_root = true`) is refused, since the partitions' changes would arrive under
+the parent's name.
+
+**Large values.** PostgreSQL leaves an unchanged TOASTed value out of an update. With
+`toast_strategy: carry_forward` (the default) the pipeline keeps the value it wrote last;
+`re_query` reads the row again from the database.
+
+**The WAL a slot holds.** A slot keeps the server's WAL until the pipeline confirms it, so a
+stopped or failing pipeline grows the server's disk. `data_avalanche_wal_lag_bytes` measures it;
+`source.postgres.max_wal_lag_bytes` halts the pipeline when it passes a bound (the slot stays).
+Set the server's `max_slot_wal_keep_size` too, so a forgotten slot can't fill the disk: a slot past
+it is invalidated, and the pipeline then starts over with a new slot and a snapshot (see
+[A lost position](/data-avalanche/runbooks/position-lost/)).
+
+## MySQL
+
+Tested on MySQL 8.4.
+
+**Server settings**
+
+- `log_bin` on, `binlog_format = ROW`, `binlog_row_image = FULL`, `binlog_row_metadata = FULL`;
+- for `replication_type: gtid` (the default): `gtid_mode = ON` and
+  `enforce_gtid_consistency = ON`;
+- no binlog filter (`binlog_do_db`, `binlog_ignore_db` empty): a filter drops statements by the
+  session's database, so changes to the pipeline's database made from another one would never
+  reach it;
+- on a server that replicates from another, `log_replica_updates = ON`.
+
+**Tables.** `tables.include` and `exclude` take bare table names (`orders`, `payment_*`), never
+`db.table`: `source.db` is the one database the pipeline reads. Each table needs a primary key or
+a `tables.primary_key` override.
+
+**Positions.** `source.mysql.replication_type`:
+
+- `gtid` (the default) follows GTIDs, which survive a failover of the source to a replica.
+- `binlog` follows a file and an offset, for servers without GTIDs. A position is only valid on
+  the server that wrote it: when the pipeline finds another server behind the host (another
+  `server_uuid`), it halts with `BINLOG_SOURCE_CHANGED`.
+
+**`server_id`.** The binlog stream registers as a replica with a server id, and the server ends any
+other stream with the same id. Each pipeline takes its own, derived from its `id` (between
+2147483648 and 4294967295); set `source.mysql.server_id` when that range collides with the
+server's replicas.
+
+**A purged binlog.** When the server has purged the binlog a pipeline still needs,
+`binlog_purge_action: error` (the default) stops it for the operator;
+`re_snapshot` starts again from the server's current position and snapshots every table.
+
+**`TINYINT(1)`** is read as a boolean (`tinyint1_as_bool: true`); set it `false` to keep the
+number.
+
+```yaml
+config_version: 1
+globals:
+  cloud: aws
+  state:
+    bucket: example-cdc-state
+pipelines:
+  billing:
+    id: billing
+    source:
+      type: mysql
+      host: billing-db.example.internal
+      db: billing
+      user: cdc
+      password: ${BILLING_DB_PASSWORD}
+      mysql:
+        replication_type: gtid
+    tables:
+      include: [invoices, payment_*]
+      snapshots:
+        mode: initial
+    sink:
+      type: delta
+      bucket: example-lake
+```
+
+## MariaDB
+
+MariaDB is read through the MySQL source (`type: mysql`); tested on MariaDB 10.11, 11.4 and 11.8.
+What differs:
+
+- GTIDs need `gtid_strict_mode = 1`; positions are MariaDB's `domain-server-sequence` GTIDs.
+- `log_slave_updates` is off by default; turn it on for a server that replicates from another.
+- Reading a replica's status takes `SLAVE MONITOR` (10.5 and later). Without it the pipeline
+  can't check `log_slave_updates`, and warns.
+
+## Connections
+
+- `timeouts.connect` (10 s) bounds the whole connection setup; `timeouts.command` (30 s) each
+  statement; `timeouts.snapshot` (6 h) one table's snapshot in one run.
+- A lost connection is retried with backoff (1 s to 60 s), and the stream resumes from the last
+  committed position.
+- `heartbeat` (60 s) keeps an idle stream alive. A stream silent for three heartbeats, whose
+  connection the server no longer shows as streaming, is reconnected.
````

`docs/src/content/docs/configuration/storage.md`:

````diff
diff --git a/docs/src/content/docs/configuration/storage.md b/docs/src/content/docs/configuration/storage.md
new file mode 100644
index 0000000..26d1930
--- /dev/null
+++ b/docs/src/content/docs/configuration/storage.md
@@ -0,0 +1,80 @@
+---
+title: Storage
+description: The clouds, where the tables and the state are written, the credentials and permissions a node needs, and S3-compatible stores.
+sidebar:
+  order: 3
+---
+
+Everything a node keeps is in object storage: the Delta tables, each pipeline's state (positions,
+snapshot progress, halted tables), and the cluster's own state (its anchor, the config
+generations, the members, the leases). A node writes nothing to its local disk.
+
+## The cloud
+
+`globals.cloud` names the provider for every bucket of the file: `aws` (the default), `google` or
+`azure`. On Azure every `bucket` is a container, and `globals.azure.account_name` names the storage
+account when the credentials don't.
+
+The file holds no storage credentials. A node takes them from the provider's default chain:
+
+| Cloud | Where the credentials come from |
+|---|---|
+| `aws` | an IAM role (EKS IRSA or Pod Identity, an EC2 instance profile), or `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`; the region from `AWS_REGION` |
+| `google` | Workload Identity or the instance's service account, or a service-account key named by `GOOGLE_SERVICE_ACCOUNT_PATH` |
+| `azure` | Workload Identity or a managed identity, or `AZURE_STORAGE_ACCOUNT_NAME` with `AZURE_STORAGE_ACCOUNT_KEY` |
+
+The variables are the [`object_store`](https://docs.rs/object_store/latest/object_store/) crate's,
+which reads the rest of its settings from the environment too.
+
+## Permissions
+
+| Cloud | On every bucket the file names |
+|---|---|
+| `aws` | `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on the objects; `s3:ListBucket` on the bucket |
+| `google` | `storage.objects.create`, `.get`, `.delete`, `.list` (the `Storage Object User` role holds them) |
+| `azure` | the `Storage Blob Data Contributor` role on the container |
+
+The leases and the state documents are compare-and-swap writes (`If-Match`, `If-None-Match`, GCS
+generation preconditions), and every Delta commit is a create-if-absent write; none needs a
+permission beyond writing an object, but the store must honour the conditions: one that ignores
+them lets two writers both win.
+
+## S3-compatible stores
+
+Point the S3 client at another endpoint through the environment:
+
+```sh
+AWS_ENDPOINT_URL=https://s3.example.internal:9000
+AWS_REGION=us-east-1            # any region the store accepts
+AWS_ALLOW_HTTP=true             # only for a plain-HTTP endpoint
+AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
+```
+
+The project's tests run against [RustFS](https://github.com/rustfs/rustfs) this way. The store must
+support conditional writes: `If-None-Match: *` and `If-Match` on `PutObject`.
+
+## Where things are written
+
+| What | Where |
+|---|---|
+| a table | `<sink.bucket>/<sink.prefix>/<schema>/<table>/`: `_delta_log/` and the Parquet files |
+| a pipeline's state, by default | `<sink.bucket>/<sink.prefix>/.pipeline-state/` |
+| a pipeline's state, with `state.bucket` | `<state.bucket>/[<defaults.state.prefix>/]<state.prefix>/` |
+| the cluster's state | `<globals.state.bucket>/<globals.state.prefix>/` |
+
+- `sink.prefix` and `state.prefix` default to the pipeline's `id`.
+- `<schema>` is the PostgreSQL schema; for MySQL and MariaDB it is `source.db`.
+- A table location that already holds another table format's metadata is refused.
+- Moving a pipeline's state (`state.bucket`, `state.prefix`) on a running pipeline leaves its
+  history behind: the pipeline starts over at the new location as if new.
+
+data-avalanche never deletes a table, its data files or its catalog entry: removing a pipeline
+leaves its tables. It writes the Delta checkpoints (every `sink.delta.checkpoint_interval`
+commits) and removes expired log entries (`log_retention`, 30 days); compaction and `VACUUM` are
+yours to run.
+
+## Names outside ASCII
+
+Table and column names are written as the source has them, non-ASCII letters included. Some
+readers can't open such a table: DuckDB 1.5.6's `delta_scan` refuses a path with non-ASCII text.
+Check your engines before replicating such a table.
````

`docs/src/content/docs/deployment/container.md`:

````diff
diff --git a/docs/src/content/docs/deployment/container.md b/docs/src/content/docs/deployment/container.md
new file mode 100644
index 0000000..7d15ddd
--- /dev/null
+++ b/docs/src/content/docs/deployment/container.md
@@ -0,0 +1,56 @@
+---
+title: Container image and CLI
+description: The container image, its tags and platforms, the command line, signals and exit codes.
+sidebar:
+  order: 1
+---
+
+## The image
+
+`ghcr.io/alex-karpenko/data-avalanche`, for `linux/amd64` and `linux/arm64`, is published by each
+release:
+
+| Tag | |
+|---|---|
+| `X.Y.Z` | that release |
+| `X.Y`, `latest` | the latest release of that line, and the latest release; a pre-release moves neither |
+
+The image is built on `gcr.io/distroless/cc-debian13:nonroot`: no shell, user 65532, nothing
+written but to object storage, so it runs with a read-only root filesystem. It listens on 8080/tcp
+(the API) and 7946/udp (the gossip). Each release carries a build-provenance attestation and an
+SPDX SBOM per platform.
+
+Its entrypoint is `data-avalanche`, with the default arguments
+`run --config /etc/data-avalanche/config.yaml`: mount the config there, or pass other arguments.
+
+Until the first release, build it from the repository:
+
+```sh
+docker build -f deploy/docker/Dockerfile -t data-avalanche:dev .
+```
+
+## The command line
+
+```text
+data-avalanche run --config <file> [--log-level <filter>] [--log-format json|compact|pretty]
+```
+
+- `--config` names the YAML file. Nothing else is read from the disk.
+- `--log-level` overrides `globals.log.level`: a level (`info`) or a filter
+  (`data_avalanche=debug,warn`).
+- `--log-format` overrides `globals.log.format`.
+
+The log goes to stderr. `data-avalanche --version` prints the version.
+
+## Signals and exit codes
+
+`SIGTERM` or `SIGINT` (or `POST /v1/admin/shutdown`) makes the node leave the cluster: it hands
+off its pipelines (flush, commit, checkpoint, release each lease), then exits. Give it the time
+[Clusters](/data-avalanche/configuration/cluster/#leases-and-hand-offs) works out before a
+`SIGKILL`.
+
+| Exit code | |
+|---|---|
+| `0` | stopped by a signal or the API |
+| `1` | couldn't start: the state storage is unreachable, the cluster's anchor disagrees, … |
+| `2` | the config is invalid (each error is printed with its rule and key), or the command line is |
````

`docs/src/content/docs/deployment/kubernetes.md`:

````diff
diff --git a/docs/src/content/docs/deployment/kubernetes.md b/docs/src/content/docs/deployment/kubernetes.md
new file mode 100644
index 0000000..f8a6b62
--- /dev/null
+++ b/docs/src/content/docs/deployment/kubernetes.md
@@ -0,0 +1,102 @@
+---
+title: Kubernetes
+description: "Running a cluster with the Helm chart: the StatefulSet, discovery, secrets, the config, probes and rolling updates."
+sidebar:
+  order: 2
+---
+
+The chart runs the nodes as a **StatefulSet**: a pod's name is its node name, so a restarted pod
+keeps its place on the ring. The pods find each other through a headless Service
+(`discovery: dns`). They have no volumes: their state is in object storage.
+
+```sh
+helm install cdc oci://ghcr.io/alex-karpenko/charts/data-avalanche --version <version> \
+  --set-file config=config.yaml -f values.yaml
+```
+
+Until the first release, install it from a checkout: `helm install cdc deploy/helm/data-avalanche
+…`. The chart's [README](https://github.com/alex-karpenko/data-avalanche/tree/main/deploy/helm/data-avalanche)
+lists every value.
+
+## The config
+
+`config` is the config, as a map in the values or as the file's text (`--set-file`); or
+`existingConfigMap` names a ConfigMap of your own. The chart puts in every pod's environment
+`POD_NAME`, `POD_NAMESPACE`, `POD_IP` and `DATA_AVALANCHE_DISCOVERY_DNS_NAME`, so the cluster
+block reads:
+
+```yaml
+config_version: 1
+globals:
+  cloud: aws
+  state:
+    bucket: example-cdc-state
+  cluster:
+    discovery: dns
+    discovery_dns_name: ${DATA_AVALANCHE_DISCOVERY_DNS_NAME}
+    memberlist:
+      node_name: ${POD_NAME}
+      advertise_addr: ${POD_IP}
+pipelines: {}
+```
+
+Secrets reach the config's `${VAR}`s through `env` or `envFrom`, and a cloud identity through
+`serviceAccount.annotations` (an EKS role, a GKE or Azure workload identity):
+
+```yaml
+env:
+  - name: SHOP_DB_PASSWORD
+    valueFrom:
+      secretKeyRef: { name: shop-db, key: password }
+serviceAccount:
+  annotations:
+    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/data-avalanche
+```
+
+:::caution[`helm upgrade` doesn't change the config]
+A pod that starts takes the cluster's config generation, not its file, and the chart doesn't
+restart pods when the config changes. After `helm upgrade` with a new config, wait for the
+kubelet to update the mounted ConfigMap (up to about a minute), check it with
+`POST /v1/config/validate`, then apply it with `POST /v1/config/reload`. See
+[Config changes](/data-avalanche/runbooks/config-changes/).
+:::
+
+## Stopping a pod
+
+A deleted or evicted pod gets `SIGTERM` and hands its pipelines off before it exits. The chart's
+`terminationGracePeriodSeconds` is 90 s, above the default `drain_timeout` (60 s). The worst case
+of a hand-off is longer (130 s for one pipeline, more for many: the formula is in
+[Clusters](/data-avalanche/configuration/cluster/#leases-and-hand-offs)); a pod killed before it
+finishes isn't a loss, its pipelines fail over and replay. Raise the grace with `drain_timeout` and
+with the number of pipelines per pod.
+
+The chart's PodDisruptionBudget lets one pod go at a time.
+
+## Probes
+
+| Probe | Path | Settings |
+|---|---|---|
+| startup | `/health/live` | every 5 s, up to 60 failures (5 minutes to start) |
+| liveness | `/health/live` | every 10 s, 3 failures |
+| readiness | `/health/ready` | every 5 s, 2 failures: ready once the node runs the cluster's config, not ready while it leaves |
+
+A node is ready before its pipelines run: readiness says the node serves the API, not that it has
+caught up. Watch the [metrics](/data-avalanche/deployment/observability/) for that.
+
+## Rolling updates
+
+The StatefulSet replaces its pods one at a time, the highest ordinal first, each once the one before
+is ready. The pipelines of the pod being replaced move to the others (a planned hand-off), and come
+back once the new pod has been a member for `rebalance_stabilization` (15 s). `minReadySeconds`
+(30 s by default) keeps the rollout from moving on before they have: lower it to roll faster, at
+the cost of moving some pipelines twice.
+
+The pods start in parallel (`podManagementPolicy: Parallel`), since a pod's name, not its start
+order, places it. With four or more pods started at once, check after a cold start that every node
+sees every other: see [the caution on DNS discovery](/data-avalanche/configuration/cluster/#discovery).
+
+## TLS for the API
+
+With `globals.api.tls.enabled: true` the API and the probes speak HTTPS: set `apiTls: true` when the
+config isn't a map the chart can read it from. Pass the certificate and key as `${VAR}`s from a
+Secret, or mount them with `extraVolumes` and name their paths.
````

`docs/src/content/docs/deployment/observability.md`:

````diff
diff --git a/docs/src/content/docs/deployment/observability.md b/docs/src/content/docs/deployment/observability.md
new file mode 100644
index 0000000..4f8f804
--- /dev/null
+++ b/docs/src/content/docs/deployment/observability.md
@@ -0,0 +1,59 @@
+---
+title: Observability
+description: The metrics a node exposes, the alerts that come with it, the log, and the traces.
+sidebar:
+  order: 3
+---
+
+## Metrics
+
+`GET /metrics` on every node, with no credentials, in the Prometheus text format. Each node
+exposes the pipelines it owns. Every series has the labels `pipeline`, `source_host` and
+`source_db`; `sink.metrics.per_table: true` adds a `table` label to the snapshot gauges.
+
+| Family | Metrics |
+|---|---|
+| Replication | `data_avalanche_replication_lag_seconds`, `data_avalanche_rows_read_total{op}`, `data_avalanche_rows_written_total`, `data_avalanche_flush_duration_seconds`, `data_avalanche_flush_rows`, `data_avalanche_transaction_buffer_bytes` |
+| Source | `data_avalanche_wal_lag_bytes` (PostgreSQL), `data_avalanche_binlog_lag_seconds` (MySQL, MariaDB), `data_avalanche_source_reconnects_total` |
+| Snapshots | `data_avalanche_snapshot_rows_done`, `data_avalanche_snapshot_state`, `data_avalanche_snapshot_chunk_duration_seconds`, `data_avalanche_reconcile_log_entries`, `data_avalanche_reconcile_log_keys` |
+| Sink | `data_avalanche_sink_commit_duration_seconds{sink_type}`, `data_avalanche_sink_commit_failures_total`, `data_avalanche_sink_merge_files_rewritten_total`, `data_avalanche_sink_merge_rows_copied_total`, `data_avalanche_sink_soft_dropped_columns` |
+| Cluster | `data_avalanche_lease_state`, `data_avalanche_lease_renew_failures_total`, `data_avalanche_lease_renew_latency_seconds`, `data_avalanche_cluster_members`, `data_avalanche_owned_pipelines`, `data_avalanche_ring_generation` |
+| Status | `data_avalanche_pipeline_status` (running, paused, stopped, error), `data_avalanche_halted_tables` |
+
+The ones to watch first:
+
+- **`data_avalanche_replication_lag_seconds`**: the age of the oldest change read but not yet
+  committed; `0` when nothing waits. v1's target is a p99 under 60 s at steady state, and back
+  under it within 2 minutes of a node failure. A snapshot, a transaction larger than the flush
+  bounds, and heavy random-key MERGEs on large tables are outside the target.
+- **`data_avalanche_wal_lag_bytes`**: the WAL a PostgreSQL slot holds on the server's disk.
+- **`data_avalanche_halted_tables`** and **`data_avalanche_pipeline_status`**: something waits for
+  an operator.
+- **`data_avalanche_sink_merge_files_rewritten_total`**: what the flushes cost (see
+  [Sink and flushing](/data-avalanche/configuration/sink/#flushing)).
+
+## Alerts
+
+[`deploy/prometheus/alerts.yaml`](https://github.com/alex-karpenko/data-avalanche/blob/main/deploy/prometheus/alerts.yaml)
+is a starting set of Prometheus rules, with `promtool` tests beside it:
+
+| Alert | When | Runbook |
+|---|---|---|
+| `DataAvalancheReplicationLag` | lag above 60 s for 5 minutes | [Failover](/data-avalanche/runbooks/failover/), [Sink and flushing](/data-avalanche/configuration/sink/) |
+| `DataAvalancheLeaseDegraded` | a lease not renewed for 2 minutes | [Failover](/data-avalanche/runbooks/failover/) |
+| `DataAvalancheTablesHalted` | a halted table | [A halted table](/data-avalanche/runbooks/halted-table/) |
+| `DataAvalanchePipelineError` | a pipeline in `error` | [A halted table](/data-avalanche/runbooks/halted-table/#a-pipeline-in-error) |
+| `DataAvalancheWalLag` | a slot holds more than 10 GiB of WAL for 10 minutes | [A lost position](/data-avalanche/runbooks/position-lost/) |
+
+## The log
+
+To stderr, as `globals.log.format`: `json` (one object a line), `compact` or `pretty`; at
+`globals.log.level`, a level or a filter such as `info,data_avalanche_pipeline=debug`. Both apply
+on reload, and `--log-level` / `--log-format` override them. A log line never carries a secret.
+
+## Traces
+
+With `globals.tracing.otlp_endpoint` set, a node sends spans over OTLP/HTTP (protobuf) to
+`<endpoint>/v1/traces`: one per flush and its commits, per snapshot chunk, per schema change
+applied, never one per row. `sample_ratio` (0.05 with an endpoint) samples them. A JSON log line
+written inside a sampled span carries its `trace_id` and `span_id`.
````

`docs/src/content/docs/getting-started.md`:

````diff
diff --git a/docs/src/content/docs/getting-started.md b/docs/src/content/docs/getting-started.md
new file mode 100644
index 0000000..69abbc0
--- /dev/null
+++ b/docs/src/content/docs/getting-started.md
@@ -0,0 +1,112 @@
+---
+title: Getting started
+description: Replicate one PostgreSQL database into Delta tables on S3 with a single node.
+sidebar:
+  order: 1
+---
+
+This page runs one node with one pipeline: a PostgreSQL database replicated into Delta tables in
+an S3 bucket. The [configuration guide](/data-avalanche/configuration/overview/) covers MySQL,
+MariaDB, the other clouds and clusters.
+
+## 1. Prepare the source
+
+The database needs logical replication on, and a role that may replicate and read the tables:
+
+```sql
+-- postgresql.conf: wal_level = logical (a server restart applies it)
+CREATE ROLE cdc LOGIN REPLICATION PASSWORD '…';
+GRANT CREATE ON DATABASE shop TO cdc;          -- the pipeline creates its publication
+GRANT USAGE ON SCHEMA public TO cdc;
+GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc;
+```
+
+The pipeline creates its publication, and PostgreSQL lets only a table's owner add the table to
+one: `cdc` must own the tables (or be a superuser). Otherwise the pipeline stops in `error`, "must
+be owner of table …". For a role that doesn't own them, a DBA creates the publication and the slot:
+see [Source privileges](/data-avalanche/runbooks/source-privileges/).
+
+Every table needs a primary key (or a `tables.primary_key` override), and its `REPLICA IDENTITY`
+must not be `NOTHING`.
+
+## 2. Prepare the storage
+
+Two buckets, or one for both: one for the cluster's own state, one for the Delta tables. The node
+takes its credentials from the cloud's default chain (an instance role, workload identity, or the
+`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` variables); the config holds none. It needs to read,
+write, list and delete objects in both:
+[Storage](/data-avalanche/configuration/storage/) lists the permissions per cloud.
+
+## 3. Write the config
+
+```yaml
+config_version: 1
+globals:
+  cloud: aws
+  state:
+    bucket: example-cdc-state
+pipelines:
+  shop:
+    id: shop
+    source:
+      type: postgres
+      host: shop-db.example.internal
+      db: shop
+      user: cdc
+      password: ${SHOP_DB_PASSWORD}
+      postgres:
+        publication: shop_pub
+        slot: shop_slot
+    tables:
+      include: [public.*]
+      snapshots:
+        mode: initial
+    sink:
+      type: delta
+      bucket: example-lake
+```
+
+- `id` is the pipeline's identity: its state, its lease and its default table prefix. Don't change
+  it once the pipeline runs.
+- `snapshots.mode: initial` copies each table's existing rows once, at its first start; the default
+  (`never`) streams only changes from now on.
+- `${SHOP_DB_PASSWORD}` is read from the environment.
+- Each table lands at `s3://example-lake/shop/<schema>/<table>/`.
+
+## 4. Run it
+
+```sh
+export SHOP_DB_PASSWORD=…  AWS_REGION=eu-west-1
+data-avalanche run --config config.yaml
+```
+
+or with the container image (see [Container image](/data-avalanche/deployment/container/)):
+
+```sh
+docker run --rm -p 8080:8080 -e SHOP_DB_PASSWORD -e AWS_REGION \
+  -v "$PWD/config.yaml:/etc/data-avalanche/config.yaml:ro" \
+  ghcr.io/alex-karpenko/data-avalanche:<version>
+```
+
+The node checks the whole config before it starts anything: a mistake is reported with its
+rule (`V-…`, see [Validation rules](/data-avalanche/reference/validation-rules/)) and the path of
+the key, and the node exits. Check a config without running it with `POST /v1/config/validate`
+on a running node.
+
+## 5. Watch it
+
+```sh
+curl -s localhost:8080/v1/pipelines/shop        # desired and actual state, position, halted tables
+curl -s localhost:8080/metrics | grep data_avalanche_replication_lag_seconds
+```
+
+The pipeline's `actual_state` is `running` once it streams. Its tables appear under the sink's prefix as the first
+flush commits, within `sink.flush.seconds` (30 s by default) of a change. Read them with any Delta
+reader: Spark, DuckDB's `delta_scan`, Polars, delta-rs.
+
+## Next
+
+- Change the config of a running node: [Config changes](/data-avalanche/runbooks/config-changes/).
+- Run more than one node: [Clusters](/data-avalanche/configuration/cluster/) and
+  [Kubernetes](/data-avalanche/deployment/kubernetes/).
+- Know what to watch: [Observability](/data-avalanche/deployment/observability/).
````

`docs/src/content/docs/index.md`:

````diff
diff --git a/docs/src/content/docs/index.md b/docs/src/content/docs/index.md
new file mode 100644
index 0000000..07110fb
--- /dev/null
+++ b/docs/src/content/docs/index.md
@@ -0,0 +1,63 @@
+---
+title: data-avalanche
+description: Change data capture from PostgreSQL, MySQL and MariaDB into Delta Lake tables on S3, Google Cloud Storage and Azure Blob Storage.
+sidebar:
+  label: Overview
+---
+
+data-avalanche replicates row-level changes from **PostgreSQL**, **MySQL** and **MariaDB** into
+**Delta Lake** tables on object storage (Amazon S3, Google Cloud Storage, Azure Blob Storage, or an
+S3-compatible store). Each source table becomes a Delta table that mirrors it: inserts and updates
+are upserts by primary key, deletes remove the row, and every flush commits all of a table's changes
+in one Delta transaction.
+
+:::caution[Not for production yet]
+data-avalanche is under development. v1 is being built and hasn't been tested at scale or in
+production. Expect missing features, breaking changes to the config and the state layout, and bugs.
+Try it on data you can rebuild, and report what you find in the
+[issue tracker](https://github.com/alex-karpenko/data-avalanche/issues).
+:::
+
+## What it does
+
+- **Reads the source's own change stream.** PostgreSQL logical replication (`pgoutput`, a
+  replication slot and a publication the pipeline manages); the MySQL and MariaDB binlog in row
+  format, positioned by GTID or by file and position.
+- **Copies existing rows first, without stopping the stream.** A snapshot reads each table in
+  keyset chunks beside the live stream and resumes from its last chunk after a crash.
+- **Follows schema changes.** An added column appears in the Delta table; a dropped or renamed
+  column is kept and filled with nulls from then on; a change it can't apply safely stops that
+  table only, with a reason.
+- **Runs as a cluster, or as one process.** Nodes find each other by gossip, share the pipelines
+  out by consistent hashing, and hold each pipeline by a lease in object storage, so one pipeline
+  never has two writers. One process is a cluster of one.
+- **Keeps no local state.** Positions, snapshot progress, leases and the config the cluster runs
+  are all in object storage.
+- **Is managed over a REST API**: pause, stop, resume, restart and flush pipelines, start
+  snapshots, reload the config, with Prometheus metrics and optional OpenTelemetry traces.
+
+## What v1 doesn't do
+
+- **Apache Iceberg** is designed but ships in v2: a pipeline with `sink.type: iceberg` is refused at
+  start while the others run.
+- **Table maintenance** (OPTIMIZE, VACUUM, file compaction) is left to your engine (Spark,
+  Databricks, Trino, delta-rs tooling). data-avalanche writes Delta checkpoints and cleans up the
+  expired transaction log, nothing more.
+- **Transactions across tables** aren't atomic in the lake: each table commits on its own.
+- **Tables without a primary key** replicate only with a `tables.primary_key` override.
+
+## Where to go next
+
+- [Getting started](/data-avalanche/getting-started/): one pipeline from PostgreSQL to Delta.
+- [Configuration](/data-avalanche/configuration/overview/): the config file, its sources and its
+  storage.
+- [Architecture](/data-avalanche/architecture/overview/): how a change travels, and what holds
+  when a node fails.
+- [Deployment](/data-avalanche/deployment/kubernetes/): the container image and the Helm chart.
+- [Runbooks](/data-avalanche/runbooks/source-privileges/): what to do when something stops.
+- [Reference](/data-avalanche/reference/configuration/): every config key, every validation rule,
+  every API route.
+
+The design behind these pages is in the repository's
+[specifications](https://github.com/alex-karpenko/data-avalanche/tree/main/doc/specs) and
+[research notes](https://github.com/alex-karpenko/data-avalanche/tree/main/doc/research).
````

`docs/src/content/docs/runbooks/config-changes.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/config-changes.md b/docs/src/content/docs/runbooks/config-changes.md
new file mode 100644
index 0000000..e83f531
--- /dev/null
+++ b/docs/src/content/docs/runbooks/config-changes.md
@@ -0,0 +1,90 @@
+---
+title: Config changes
+description: Validating and reloading a config, why a restart doesn't apply one, rotating secrets, and the keys a reload refuses.
+sidebar:
+  order: 7
+---
+
+## Change the config
+
+1. Change the file a node reads (on Kubernetes: the ConfigMap, then wait up to about a minute for
+   the kubelet to update the pod's copy).
+2. Check it on that node:
+
+   ```sh
+   curl -s -X POST https://cdc-0.example.internal/v1/config/validate
+   ```
+
+   With no body it checks the node's file; with a YAML body, that text. The answer is the report
+   (each error and warning with its rule and key) and what a reload would change: which pipelines
+   are added, removed, restarted, or changed in place. Nothing is applied.
+3. Apply it:
+
+   ```sh
+   curl -s -X POST https://cdc-0.example.internal/v1/config/reload
+   ```
+
+   The node validates its file again and publishes it as the cluster's next config generation;
+   every node adopts it within seconds. A file with any error is refused whole, and the running
+   config stays.
+4. Check that every node runs it: `GET /v1/config/status` lists the generation and each member's.
+
+A reload doesn't restart a pipeline in `error`, even when it fixes the cause: once the new
+generation runs, `POST /v1/pipelines/<id>/restart` it.
+
+Both calls need the `operator` role. Any node will do, but it publishes **its own file**: send the
+reload to a node whose file is the one you changed.
+
+## A restart doesn't apply a config
+
+A node that starts takes the cluster's latest config generation from `globals.state`, not its own
+file. The file matters only for the very first start of a cluster, and as the text a reload
+publishes. So editing the file and restarting the node changes nothing, and the node warns that
+it runs the generation, not its file. On Kubernetes, `helm upgrade` with a new config is the same:
+follow it with a reload.
+
+The per-node values are the exception: a node resolves the generation's `${VAR}`s with its own
+environment, so `node_name`, `advertise_addr` and secrets passed as `${VAR}` are each node's own.
+A per-node key written as a literal that differs from the generation is ignored, with a warning
+naming it, and a reload from that node is refused until the file matches.
+
+## Rotate a secret
+
+- **A credentials file** (`credentials_file`): replace its contents on every node (a mounted
+  Secret does), then reload. The reload sees the new value and applies it as the key's class says
+  (the source's credentials restart that pipeline; the API's apply in place).
+- **An environment variable** (`${VAR}`): a running process can't see a new value. Update it and
+  restart the nodes one at a time.
+- **The API's TLS certificate** (`globals.api.tls.cert` and `key`): replace the files or the
+  variables, then restart the nodes one at a time. A reload meanwhile is refused, naming the key.
+
+## Keys a reload refuses
+
+A reload refuses a change to a key whose class is *process restart*
+([the reference](/data-avalanche/reference/configuration/) marks them), naming it (V-061). Because
+a restarted node adopts the cluster's generation, restarting doesn't apply such a change either:
+
+- `tokens`, `lock_backend` and `lease_duration` are pinned in the cluster's anchor and never
+  change for that `globals.state`.
+- `cloud`, `globals.state` and `globals.azure` say where the cluster is: changing them makes a new
+  cluster.
+- The rest (`lease_renewal_interval`, `lease_safety_margin`, `discovery`, `discovery_dns_name`,
+  `join_members`, the gossip's ports and timers, `globals.api.tls`) change only through each
+  node's environment. Write the ones you may want to change as `${VAR}`, with a
+  default, from the start, and change the variable with a rolling restart:
+
+  ```yaml
+  globals:
+    cluster:
+      lease_renewal_interval: ${LEASE_RENEWAL_INTERVAL:-5s}
+  ```
+
+  A key already written as a literal can't be changed in a running cluster. The way out is a new
+  cluster: stop every node, then start them with a new `globals.state.prefix`, so that the file
+  bootstraps a new anchor and generation. Never run nodes on the old and the new location at
+  once: they would be two clusters holding the same pipelines. This path isn't covered by the
+  project's tests yet.
+
+`node_name` and `globals.api.address` are read from each node's own file when it starts, so a
+restart applies a changed value; a reload is still refused while the generation holds another
+one. Write them as `${VAR}` too.
````

`docs/src/content/docs/runbooks/failover.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/failover.md b/docs/src/content/docs/runbooks/failover.md
new file mode 100644
index 0000000..4cf0559
--- /dev/null
+++ b/docs/src/content/docs/runbooks/failover.md
@@ -0,0 +1,67 @@
+---
+title: Nodes, leases and failover
+description: A node down, a lease that isn't renewed, a pipeline no node runs, taking a node out for maintenance.
+sidebar:
+  order: 4
+---
+
+How the cluster places pipelines and moves them is in
+[Cluster and failover](/data-avalanche/architecture/cluster/). Start from what the cluster sees:
+
+```sh
+curl -s https://cdc.example.internal/v1/cluster/status
+```
+
+It lists the members this node sees, with their state, and each pipeline's lease holder. Ask more
+than one node: they should agree.
+
+## A node died
+
+Nothing to do for the data. Each of its pipelines is taken over by the next node on the ring once
+its lease expires (about 20 s at the defaults), and resumes from its last checkpoint; the
+replication lag rises for that time and recovers within about two minutes. When the node comes
+back under the same `node_name` it rejoins and gets its pipelines back after
+`rebalance_stabilization`.
+
+If a pipeline stays without a holder:
+
+- check that the surviving nodes see each other (`members` on each);
+- check `data_avalanche_owned_pipelines` on each node: a node that owns it but can't start it logs
+  why (its source unreachable, its storage refusing);
+- check that every node can write `globals.state`: a node that can't take leases can't own
+  anything.
+
+## A lease isn't renewed
+
+**Symptom:** `DataAvalancheLeaseDegraded`, a rising `data_avalanche_lease_renew_failures_total` or
+`data_avalanche_lease_renew_latency_seconds`.
+
+A holder that can't renew keeps writing only until its safety deadline (`lease_duration ×
+(1 − lease_safety_margin)` after its last renewal, 12.75 s by default), then stops the pipeline
+so that another node can take it safely. A lease degraded for minutes means the storage of
+`globals.state` is slow or failing for that node: look at its network path and the storage's
+status. If renewals fail often, a longer `lease_duration` tolerates more, at the price of a
+slower failover; it is fixed for the life of a cluster's `globals.state`.
+
+## The nodes split into groups
+
+With `discovery: dns`, nodes started together can form groups that don't see each other: each
+group's `members` lists only its own, and the groups contend for the leases. Restart the pods of
+the smaller group one at a time. See
+[the caution on DNS discovery](/data-avalanche/configuration/cluster/#discovery).
+
+## Taking a node out
+
+```sh
+curl -s -X POST https://cdc-2.example.internal/v1/cluster/leave    # admin
+```
+
+The node hands its pipelines off (flush, commit, checkpoint, release) and stays up, not ready,
+still answering the API, until you stop it; a restart joins again. `POST /v1/admin/shutdown` (or
+`SIGTERM`) leaves and then exits.
+
+:::caution[A node that left keeps its last config]
+A node that left the cluster no longer follows the config generations: a later reload's
+credentials or `role_mapping` don't reach it, and it still accepts the pipeline commands. Stop a
+node soon after it leaves, and send commands to the nodes that are members.
+:::
````

`docs/src/content/docs/runbooks/halted-table.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/halted-table.md b/docs/src/content/docs/runbooks/halted-table.md
new file mode 100644
index 0000000..3ce2ee7
--- /dev/null
+++ b/docs/src/content/docs/runbooks/halted-table.md
@@ -0,0 +1,82 @@
+---
+title: A halted table
+description: A table that stopped replicating while its pipeline goes on, and a pipeline in error.
+sidebar:
+  order: 2
+---
+
+**Symptom:** `DataAvalancheTablesHalted` fires, `data_avalanche_halted_tables` is above 0, or a table
+stops changing in the lake.
+
+A table halts when a change can't be applied correctly. The pipeline records the halt with its
+reason, keeps the table's Delta rows as they were, and goes on with its other tables. It drops the
+halted table's later changes, so the table can't simply resume: a **snapshot job** copies it afresh
+once the cause is fixed. The halt stays across restarts and failovers until then.
+
+## 1. Find the reason
+
+```sh
+curl -s https://cdc.example.internal/v1/pipelines/shop
+```
+
+Its `halted_tables` lists each halted table with its code and a detail.
+
+## 2. Fix the cause
+
+| Code | Cause | Fix |
+|---|---|---|
+| `UNSAFE_DDL` | a schema change the table can't follow: a type change, a key change, `ENUM` values reordered, a partition operation, an unparsable DDL | decide whether the Delta table can keep its history; if not, drop or move it |
+| `EVOLUTION_UNSUPPORTED` | a change Delta can't apply, such as widening a column | as for `UNSAFE_DDL` |
+| `EVOLUTION_DISABLED` | any schema change, with `schema_evolution.enabled: false` | enable it, or accept the change by hand |
+| `TABLE_DROPPED` | the table was dropped, or renamed away, at the source | remove it from the config (reload), or, if it was created again, go on to step 3 |
+| `RESERVED_COLUMN_COLLISION` | a column named `_op`, `_ts`, `_lsn`, `_seq`, `_source_db`, `_pipeline` or `__data_avalanche_…` | rename the column at the source |
+| `NO_IDENTIFIER` | no primary key and no `tables.primary_key` | add a primary key, or a `tables.primary_key` entry (reload) |
+| `IDENTIFIER_NOT_USABLE` | a key column that is nullable, floating-point, or not a plain column | choose other columns in `tables.primary_key` |
+| `IDENTIFIER_NOT_IN_REPLICA_IDENTITY` | PostgreSQL: the key isn't in the table's `REPLICA IDENTITY` | `ALTER TABLE … REPLICA IDENTITY …` to cover it |
+| `NULL_IDENTIFIER` | a row arrived with a NULL key column | fix the source rows, or the key |
+| `TABLE_NOT_REPLICABLE` | an `UNLOGGED` PostgreSQL table, or a table the source can't stream | make it logged, or drop it from the config |
+| `NUMERIC_OVERFLOW` | a value that doesn't fit its decimal type | widen the type with `tables.column_type_overrides`, or `numeric_overflow_action: string` |
+| `INVALID_DATE_VALUE` | MySQL: a zero or invalid date such as `2024-00-15` | override the column to `string`, or fix the rows |
+| `SCHEMA_MISMATCH` | a column whose type the stream and the table disagree on, or an override that can't apply | correct the `column_type_overrides` entry |
+| `SINK_REJECTED` | Delta refused the table's changes for good, for example a MERGE over `merge_memory_limit` | the detail names it: raise the limit, or lower the `flush` bounds |
+| `SNAPSHOT_TIMEOUT` | the table's snapshot ran past `source.timeouts.snapshot` | raise the timeout |
+
+A config fix goes through `POST /v1/config/reload`; most of these keys restart the pipeline.
+
+## 3. Copy the table again
+
+```sh
+curl -s -X POST https://cdc.example.internal/v1/pipelines/shop/snapshots \
+  -H 'content-type: application/json' -d '{"tables": ["public.orders"]}'
+```
+
+The job clears the halt, takes the table's schema from the source's catalog, copies the table, and
+streams it again. Follow it with `GET /v1/snapshots/<job_id>` (see
+[Snapshot jobs](/data-avalanche/runbooks/snapshot-jobs/)).
+
+:::caution[Rows deleted while the table was halted]
+A job upserts every row it reads and deletes nothing. A row deleted at the source while the table
+was halted stays in the Delta table. To start clean, drop or move the Delta table before the job.
+:::
+
+:::note[A table created again while the pipeline was down]
+If the table was dropped and created again while the pipeline was stopped, its first job halts it
+`TABLE_DROPPED` at the job's start and fails. Start a second job: it clears the halt and copies the
+new table.
+:::
+
+## A pipeline in error
+
+**Symptom:** `DataAvalanchePipelineError`, or `actual_state: error`. Unlike a halted table, the
+whole pipeline has stopped and waits for an operator. `GET /v1/pipelines/<id>` gives the reason;
+the common ones:
+
+- the binlog server changed under a file-and-position pipeline (`BINLOG_SOURCE_CHANGED`), or the
+  position was purged (see [A lost position](/data-avalanche/runbooks/position-lost/));
+- the slot is missing, invalidated or orphaned, and the settings say not to recover by themselves;
+- the WAL behind the slot passed `max_wal_lag_bytes`;
+- a transaction passed `max_transaction_buffer_bytes`;
+- the sink's table is missing, or a commit failed in a way a retry won't fix.
+
+Fix the cause, then `POST /v1/pipelines/<id>/restart` (or a `stop` and a `resume`). A pipeline in
+`retrying` needs nothing: it hit an error worth another try, and tries again by itself.
````

`docs/src/content/docs/runbooks/pipeline-commands.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/pipeline-commands.md b/docs/src/content/docs/runbooks/pipeline-commands.md
new file mode 100644
index 0000000..4b5adca
--- /dev/null
+++ b/docs/src/content/docs/runbooks/pipeline-commands.md
@@ -0,0 +1,41 @@
+---
+title: Pausing, stopping and restarting
+description: The pipeline commands, what each one does to the source and the lease, and their answers.
+sidebar:
+  order: 5
+---
+
+Every command is a `POST` to `/v1/pipelines/<id>/…` with the `operator` role, accepted by any node.
+It records what you asked in the pipeline's state, and the pipeline's owner carries it out within
+a poll. The command you gave last is the pipeline's **desired state**: it survives restarts,
+failovers and config reloads, so a reload never resumes a paused or stopped pipeline.
+
+| Command | From | Does | The source | The lease |
+|---|---|---|---|---|
+| `pause` | running | flushes to a transaction boundary and commits, then reads nothing | connection kept | kept |
+| `stop` | running, paused | flushes and commits, then closes the source connection | closed; a PostgreSQL slot stays and holds WAL | kept |
+| `resume` | paused, stopped | streams again from the checkpoint | reopened | kept |
+| `restart` | running (`error` included), with a lease holder | drops what isn't committed and replays from the last checkpoint | reconnected | kept |
+| `flush` | running, with a lease holder | commits what is buffered now, and restarts the flush timer; a paused pipeline has nothing buffered | — | — |
+
+- `202`: recorded; the owner acts at its next poll.
+- `200`: the pipeline is in that state already (or, for `flush`, paused with nothing to flush);
+  nothing changes.
+- `409`: the state doesn't allow it (`pause` of a stopped pipeline; `restart` or `flush` of a
+  paused or stopped one, or of one no node holds).
+
+:::caution[A stopped PostgreSQL pipeline grows the server's WAL]
+`stop` keeps the replication slot, which keeps the WAL from the checkpoint on. Watch
+`data_avalanche_wal_lag_bytes`, and remove the pipeline from the config if it won't come back.
+:::
+
+## Retrying a command
+
+`restart` and `flush` are recorded as requests, one per HTTP call. A client that retries after
+losing the answer asks twice, and the pipeline may restart twice. That costs a second replay from
+the checkpoint, never data. Check `GET /v1/pipelines/<id>` before retrying.
+
+## Restart and reload
+
+`restart` reconnects with the config the pipeline runs. A changed config reaches it only through
+`POST /v1/config/reload`: see [Config changes](/data-avalanche/runbooks/config-changes/).
````

`docs/src/content/docs/runbooks/position-lost.mdx`:

````diff
diff --git a/docs/src/content/docs/runbooks/position-lost.mdx b/docs/src/content/docs/runbooks/position-lost.mdx
new file mode 100644
index 0000000..4601bce
--- /dev/null
+++ b/docs/src/content/docs/runbooks/position-lost.mdx
@@ -0,0 +1,92 @@
+---
+title: A lost position
+description: A PostgreSQL slot lost, invalidated or orphaned, a WAL backlog, a purged binlog, a changed binlog server.
+sidebar:
+  order: 3
+---
+
+import { Tabs, TabItem } from '@astrojs/starlight/components';
+
+A pipeline resumes from its checkpoint: a position in the source's log. When the source can no
+longer serve that position, the changes in between are gone, and the only way to a correct table
+is a fresh copy. Depending on its settings the pipeline makes that copy by itself, or stops in
+`error` for you to decide.
+
+:::caution[A fresh copy keeps rows deleted in the gap]
+The copy upserts every row it reads and deletes nothing: a row deleted at the source during the
+gap stays in the Delta table. When the tables must be exact, stop the pipeline, drop or move its
+Delta tables, then let it copy them.
+:::
+
+<Tabs>
+<TabItem label="PostgreSQL">
+
+## The WAL a slot holds grows
+
+**Symptom:** `DataAvalancheWalLag`, a growing `data_avalanche_wal_lag_bytes`, the server's disk
+filling.
+
+The slot holds the WAL from the pipeline's last checkpoint. It grows while the pipeline is stopped,
+paused, failing to commit, or far behind.
+
+1. Check the pipeline: `GET /v1/pipelines/<id>`. A stopped or paused pipeline still holds its slot;
+   resume it, or remove it from the config if it's gone for good (the slot is dropped with it,
+   with `auto_cleanup: true`).
+2. A pipeline that runs but doesn't keep up: look at `data_avalanche_sink_commit_failures_total`
+   (the storage) and `data_avalanche_flush_duration_seconds` (the MERGE cost).
+3. With `max_wal_lag_bytes` set, a pipeline past it stops in `error` and keeps its slot. Fix the
+   cause, then `POST /v1/pipelines/<id>/restart`.
+
+## The slot is gone or invalidated
+
+A slot past `max_slot_wal_keep_size` is invalidated by the server; a slot can also be dropped by
+hand, or advanced past the pipeline's checkpoint.
+
+- With `slot_management: managed` (the default) the pipeline creates the slot again at its next
+  start and copies every table afresh, streaming from the new slot. The log says why.
+- With `require_existing` the pipeline stops in `error`: have the slot created again, then
+  restart the pipeline. It copies every table afresh.
+
+## An orphan slot
+
+The slot exists, but the pipeline has no state: its state location was lost or moved, or a new
+pipeline took an old slot's name. With `slot_on_orphan: halt` (the default) the pipeline refuses to
+start. Then either:
+
+- drop the slot, for a first start (a snapshot per `snapshots.mode`);
+- restore the pipeline's state location, if it was lost by mistake;
+- or set `slot_on_orphan: adopt_resnapshot`: the pipeline takes the slot over, moves it to the
+  current WAL position and copies every table afresh.
+
+</TabItem>
+<TabItem label="MySQL and MariaDB">
+
+## The binlog was purged
+
+**Symptom:** the pipeline stops in `error`, its reason saying the server purged the binlog it
+needs. A pipeline stopped for longer than the server keeps its binlog
+(`binlog_expire_logs_seconds`) meets this when it starts again.
+
+- With `binlog_purge_action: error` (the default) the pipeline waits for you. To copy afresh, set
+  `binlog_purge_action: re_snapshot` (a reload applies it) and restart the pipeline: it streams
+  from the server's position now and copies every table.
+- With `re_snapshot` it does that by itself, with a warning.
+
+## The server changed (`BINLOG_SOURCE_CHANGED`)
+
+With `replication_type: binlog`, a position is a file and an offset of one server. When the host
+now leads to another server (a failover, a restore), the pipeline stops in `error` with
+`BINLOG_SOURCE_CHANGED`.
+
+1. Prefer GTIDs: set `replication_type: gtid` (the server needs them on) and
+   `binlog_purge_action: re_snapshot`, and reload. The pipeline can't carry a file position over
+   to a GTID one, so it copies every table afresh once, then follows GTIDs, which survive later
+   failovers.
+2. Without GTIDs, the position can't be repaired in place: replace the pipeline. Give it a new
+   `id` and a new `sink.prefix` (or none, so the prefix is the new `id`), with
+   `snapshots.mode: initial`, in one reload. The new pipeline copies every table into fresh Delta
+   tables and streams from the server's position now; drop the old tables once nothing reads
+   them. The old pipeline's state stays behind, unused.
+
+</TabItem>
+</Tabs>
````

`docs/src/content/docs/runbooks/snapshot-jobs.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/snapshot-jobs.md b/docs/src/content/docs/runbooks/snapshot-jobs.md
new file mode 100644
index 0000000..51939d7
--- /dev/null
+++ b/docs/src/content/docs/runbooks/snapshot-jobs.md
@@ -0,0 +1,57 @@
+---
+title: Snapshot jobs
+description: Copying tables on demand through the API, following a job, and what a job does and doesn't do.
+sidebar:
+  order: 6
+---
+
+A snapshot job copies tables into the lake while the pipeline keeps streaming: to re-copy a halted
+table, to backfill a table after a gap, or to copy once a table the config doesn't replicate. How
+a snapshot reads is in [Snapshots](/data-avalanche/architecture/snapshots/).
+
+## Start a job
+
+```sh
+curl -s -X POST https://cdc.example.internal/v1/pipelines/shop/snapshots \
+  -H 'content-type: application/json' \
+  -d '{"tables": ["public.orders", "public.order_lines"]}'
+```
+
+- Name each table exactly, as the config's per-table keys do: `schema.table` on PostgreSQL, the
+  bare name on MySQL. No patterns.
+- `202` answers with the job's id, `<pipeline id>~<suffix>`.
+- `409`: the pipeline isn't running (paused, stopped), no node holds its lease, or an unfinished
+  job already holds one of the tables.
+- `422`: no table, a pattern, or a name not in the source's form.
+
+The call needs the `operator` role. Any node accepts it; the pipeline's owner restarts the
+pipeline to take the job, which replays from the last checkpoint and is harmless.
+
+## Follow it
+
+```sh
+curl -s https://cdc.example.internal/v1/snapshots/shop~0192f0c4-7b9e-7c3a-9d5e-2f0a1b3c4d5e
+curl -s https://cdc.example.internal/v1/pipelines/shop/snapshots
+```
+
+A job's status, and each of its tables', is kept in the pipeline's state: it survives restarts and
+failover, and a job interrupted by a crash goes on from its last chunk.
+
+## What a job does
+
+- Copies each table from its first row, whatever the table's `snapshots.mode`.
+- For a halted table: clears the halt, takes the schema from the source's catalog, and streams the
+  table again once it's copied.
+- For a table the config doesn't replicate: copies it once, and doesn't stream it.
+
+## What it doesn't do
+
+- **Delete.** A job upserts. Rows deleted at the source while the table wasn't streamed stay in
+  the lake: drop or move the Delta table first when you need an exact copy.
+- **Stop the stream.** The pipeline keeps streaming every table; a change to a row the job is
+  copying wins over the job's copy.
+
+:::note
+A job's table that halts while it is being copied fails in the job only at the pipeline's next
+flush, up to `sink.flush.seconds` later.
+:::
````

`docs/src/content/docs/runbooks/source-privileges.mdx`:

````diff
diff --git a/docs/src/content/docs/runbooks/source-privileges.mdx b/docs/src/content/docs/runbooks/source-privileges.mdx
new file mode 100644
index 0000000..210bc7e
--- /dev/null
+++ b/docs/src/content/docs/runbooks/source-privileges.mdx
@@ -0,0 +1,134 @@
+---
+title: Source privileges
+description: The roles, grants and server settings a source database needs before a pipeline starts.
+sidebar:
+  order: 1
+---
+
+import { Tabs, TabItem } from '@astrojs/starlight/components';
+
+A pipeline checks its source when it starts. A missing privilege or setting stops that pipeline
+(or that table) with a message naming it, and the other pipelines run. Set these up before the
+first start. Storage permissions are in [Storage](/data-avalanche/configuration/storage/#permissions).
+
+<Tabs>
+<TabItem label="PostgreSQL">
+
+**Settings** (`postgresql.conf`; `wal_level` needs a server restart):
+
+```ini
+wal_level = logical
+max_replication_slots = 10      # at least one per pipeline, plus your others
+max_wal_senders = 10
+max_slot_wal_keep_size = 100GB  # so a forgotten slot can't fill the disk
+```
+
+**A role the pipeline manages its slot and publication with** (the default,
+`slot_management: managed`):
+
+```sql
+CREATE ROLE cdc LOGIN REPLICATION PASSWORD '…';
+GRANT CREATE ON DATABASE shop TO cdc;                 -- CREATE PUBLICATION
+GRANT USAGE ON SCHEMA public TO cdc;
+GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc;   -- snapshots and the catalog checks
+ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc;
+```
+
+This role must own the tables: PostgreSQL lets only a table's owner add it to a publication, and
+without ownership the pipeline stops in `error` with "must be owner of table …". When `cdc`
+doesn't own the tables, have the owner create the slot and the publication, and let the pipeline
+use them as they are:
+
+```sql
+-- as the tables' owner
+CREATE PUBLICATION shop_pub FOR TABLE public.orders, public.order_lines;
+SELECT pg_create_logical_replication_slot('shop_slot', 'pgoutput');
+```
+
+```yaml
+source:
+  postgres:
+    publication: shop_pub
+    slot: shop_slot
+    slot_management: require_existing
+    auto_cleanup: false
+    slot_on_orphan: adopt_resnapshot   # for the first start only
+```
+
+At its first start the pipeline has no state for the slot, so the slot is an orphan, and the
+default `slot_on_orphan: halt` refuses it. `adopt_resnapshot` takes it over: the pipeline moves it
+to the current WAL position and copies every table, whatever `snapshots.mode` says. Once the
+pipeline runs, set `slot_on_orphan` back to `halt` (a reload applies it), so that a pipeline whose
+state is lost later stops rather than starting over by itself.
+
+With `require_existing`, a table the pipeline replicates but the publication doesn't publish is a
+warning: its changes never arrive. Keep the publication in step with `tables.include`.
+
+On a managed service, follow its guide to logical replication: on Amazon RDS and Aurora the role
+is granted `rds_replication` and `rds.logical_replication` is set; on Cloud SQL the
+`cloudsql.logical_decoding` flag is set.
+
+**Each table**: a primary key (or `tables.primary_key`), a `REPLICA IDENTITY` that covers it, and
+not `UNLOGGED`.
+
+</TabItem>
+<TabItem label="MySQL">
+
+**Settings** (`my.cnf`):
+
+```ini
+log_bin = ON
+binlog_format = ROW
+binlog_row_image = FULL
+binlog_row_metadata = FULL
+gtid_mode = ON                    # for replication_type: gtid
+enforce_gtid_consistency = ON
+log_replica_updates = ON          # when the server replicates from another
+binlog_expire_logs_seconds = 604800  # longer than a pipeline may be stopped
+```
+
+and no `binlog_do_db` or `binlog_ignore_db`.
+
+**A user**:
+
+```sql
+CREATE USER 'cdc'@'%' IDENTIFIED BY '…';
+GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
+```
+
+`SELECT` on `*.*` covers the tables and the catalog; grant it on the pipeline's database
+(`shop.*`) instead to narrow it, keeping `REPLICATION SLAVE` and `REPLICATION CLIENT` global.
+
+</TabItem>
+<TabItem label="MariaDB">
+
+**Settings**:
+
+```ini
+log_bin = ON
+binlog_format = ROW
+binlog_row_image = FULL
+binlog_row_metadata = FULL
+gtid_strict_mode = 1              # for replication_type: gtid
+log_slave_updates = ON            # off by default: needed when the server replicates from another
+```
+
+**A user**:
+
+```sql
+CREATE USER 'cdc'@'%' IDENTIFIED BY '…';
+GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, SLAVE MONITOR ON *.* TO 'cdc'@'%';
+```
+
+`REPLICATION CLIENT` is MariaDB's `BINLOG MONITOR` since 10.5. `SLAVE MONITOR` lets the pipeline
+read a replica's status to check `log_slave_updates`; without it the pipeline warns.
+
+</TabItem>
+</Tabs>
+
+:::note[Where these come from]
+The PostgreSQL privileges are PostgreSQL's own requirements for slots, publications and reads;
+the project's tests run as a superuser. The MySQL grant passes the pipeline's start checks in the
+project's TLS tests; its other tests run as `root`.
+Report a privilege these pages miss in the issue tracker.
+:::
````

`docs/src/content/docs/runbooks/upgrade.md`:

````diff
diff --git a/docs/src/content/docs/runbooks/upgrade.md b/docs/src/content/docs/runbooks/upgrade.md
new file mode 100644
index 0000000..ada6a56
--- /dev/null
+++ b/docs/src/content/docs/runbooks/upgrade.md
@@ -0,0 +1,45 @@
+---
+title: Upgrades
+description: Upgrading the nodes of a cluster, and what v1 does and doesn't promise across versions.
+sidebar:
+  order: 8
+---
+
+:::caution[No compatibility promise before 1.0]
+Until 1.0 a release may change the config, the state it writes to object storage, or its metrics.
+Read the release notes of every version between yours and the target before upgrading, and keep
+the old version's image and chart at hand.
+:::
+
+## Before
+
+- Check `GET /v1/pipelines`: no pipeline in `error`, no halted table you haven't dealt with.
+- Once the first node runs the new version, `POST /v1/config/validate` on it checks your config
+  against the new version's rules, before the other nodes roll.
+
+## Rolling the nodes
+
+On Kubernetes, `helm upgrade` with the new chart (or `--set image.tag=<version>`) replaces the pods
+one at a time, the highest ordinal first. Each pod hands its pipelines off before it exits, its
+pipelines run on the others meanwhile, and they come back once the new pod has been a member for
+`rebalance_stabilization`. The chart's `minReadySeconds` paces the rollout so that they have.
+
+Outside Kubernetes do the same by hand: stop a node with `SIGTERM` (or
+`POST /v1/admin/shutdown`), wait for it to exit, start it on the new version, wait until
+`GET /v1/cluster/status` lists it and its pipelines are back, then the next node.
+
+During the roll both versions run the cluster's one config generation and share its leases. If
+the release notes say a version can't run beside its predecessor, stop every node first and start
+them on the new version.
+
+## A new config with the upgrade
+
+`helm upgrade` doesn't apply a changed config (see
+[Config changes](/data-avalanche/runbooks/config-changes/)). Upgrade first, then reload the new
+config once every node runs the new version, so that every node can apply it.
+
+## Rolling back
+
+Roll the nodes back to the old version the same way. The state documents are strict: a version
+refuses a document with a field it doesn't know, so state that a newer version wrote may stop an
+older one. The release notes say when that happens.
````

`docs/src/styles/custom.css`:

````diff
diff --git a/docs/src/styles/custom.css b/docs/src/styles/custom.css
new file mode 100644
index 0000000..3d2b204
--- /dev/null
+++ b/docs/src/styles/custom.css
@@ -0,0 +1,11 @@
+/* The reference pages' tables are wide: each scrolls in its own box, so a narrow screen keeps
+   the page's width, and a rule's id (V-001) never breaks at its hyphen. */
+.sl-markdown-content table {
+  display: block;
+  max-width: 100%;
+  overflow-x: auto;
+}
+.sl-markdown-content td a[href*='/validation-rules/#'],
+.sl-markdown-content td:first-child {
+  white-space: nowrap;
+}
````

`xtask/src/examples.rs`:

````diff
diff --git a/xtask/src/examples.rs b/xtask/src/examples.rs
new file mode 100644
index 0000000..856911b
--- /dev/null
+++ b/xtask/src/examples.rs
@@ -0,0 +1,68 @@
+//! The documentation's config examples: a fenced `yaml` block whose first line is
+//! `config_version:` is a whole config file, and a test validates each one.
+
+use std::path::{Path, PathBuf};
+
+/// One example, where it sits, and its text.
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub struct ConfigExample {
+    /// The page, from the repository's root.
+    pub file: PathBuf,
+    /// The line of the opening fence, from 1.
+    pub line: usize,
+    pub text: String,
+}
+
+/// Every config example in the site's pages (`.md` and `.mdx` under [`crate::CONTENT`]), in
+/// path order.
+pub fn config_examples(root: &Path) -> Vec<ConfigExample> {
+    let mut pages = Vec::new();
+    collect(&root.join(crate::CONTENT), &mut pages);
+    pages.sort();
+    let mut out = Vec::new();
+    for page in pages {
+        let text = std::fs::read_to_string(&page).expect("a page reads");
+        let file = page
+            .strip_prefix(root)
+            .expect("under the root")
+            .to_path_buf();
+        let mut open: Option<(usize, String)> = None;
+        for (i, line) in text.lines().enumerate() {
+            match &mut open {
+                None if line.trim_start().starts_with("```yaml") => {
+                    open = Some((i + 1, String::new()))
+                }
+                Some((at, body)) if line.trim_start().starts_with("```") => {
+                    if body.starts_with("config_version:") {
+                        out.push(ConfigExample {
+                            file: file.clone(),
+                            line: *at,
+                            text: body.clone(),
+                        });
+                    }
+                    open = None;
+                }
+                Some((_, body)) => {
+                    body.push_str(line);
+                    body.push('\n');
+                }
+                None => {}
+            }
+        }
+    }
+    out
+}
+
+fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
+    let Ok(entries) = std::fs::read_dir(dir) else {
+        return;
+    };
+    for entry in entries {
+        let path = entry.expect("a directory entry").path();
+        if path.is_dir() {
+            collect(&path, out);
+        } else if path.extension().is_some_and(|e| e == "md" || e == "mdx") {
+            out.push(path);
+        }
+    }
+}
````

`xtask/src/lib.rs`:

````diff
diff --git a/xtask/src/lib.rs b/xtask/src/lib.rs
index 6f50a40..8413ce4 100644
--- a/xtask/src/lib.rs
+++ b/xtask/src/lib.rs
@@ -5,10 +5,13 @@
 
 mod api;
 mod config;
+mod examples;
 mod rules;
 
 use std::path::{Path, PathBuf};
 
+pub use examples::{ConfigExample, config_examples};
+
 /// The site's base path on GitHub Pages (`astro.config.mjs`'s `base`): every link between pages
 /// starts with it, which the links validator checks.
 pub const BASE: &str = "/data-avalanche";
````


- [ ] **Step 4: Run the tests and build the site**

Run: `cargo test -p xtask`, `cargo test -p data-avalanche --test packaging`,
`cargo xtask docs --check`, then in `docs/`: `npm ci && npm run build`
Expected: PASS (`xtask`: 2 integration tests, 2 unit tests; `packaging`: 8); the build ends with
"All internal links are valid." and "29 page(s) built". Then the gates.

- [ ] **Step 5: Check the validator bites**

Append `[a](/data-avalanche/nope/) [b](/data-avalanche/runbooks/failover/#nope) [c](../failover/)`
to `runbooks/upgrade.md`, run `npm run build`, then restore the page.
Expected: exit 1, with "invalid link", "invalid hash" and "relative link" for the three.

- [ ] **Step 6: Commit**

```bash
git add xtask docs .github/workflows/docs.yml deploy/helm/data-avalanche crates/data-avalanche/tests/packaging.rs Cargo.lock
git commit -m "docs(site): the documentation site on Starlight — the config guide, the architecture, the deployment, the runbooks and the generated reference, its links validated at build; docs.yml deploys it to GitHub Pages from main; every config example of the pages validated; the chart's minReadySeconds (30)"
```

---

### Task 4: The doc obligations in the gap tracker

**Files:**
- Modify: `doc/analysis/CDC_Design_Gap_Analysis_v1.md`

- [ ] **Step 1: Record what the pages close**

Bump `updated:` and add a section after §49 (the plan's), in the shape of §48: a paragraph naming
the site and its pages, then a table `| ID | Change | Evidence |` with these rows, and the same
status change and one-line rationale in each ID's domain-table row:

| ID | Change | Evidence |
|---|---|---|
| DA-OPS-8 | 🔄 → ✅: the operator docs say a node runs the generation, per-node values go through `${VAR}`, a literal is ignored with a warning and a reload from that node refused | `configuration/cluster.md`, `runbooks/config-changes.md` |
| DA-OPS-10 | 🟥 → 🔄: the docs say to wait for the old lease's release before reusing an `id` or its resources, or to do both in one reload; the tombstone stays | `configuration/overview.md` |
| DA-API-14 | 🟥 → ✅ (documented): a node that left keeps its last generation's auth and still takes commands; stop it soon after | `runbooks/failover.md` |
| DA-API-15 | 🟥 → ✅ (documented): a retried `restart` or `flush` asks twice, which costs a replay, never data | `runbooks/pipeline-commands.md` |
| DA-SNAP-15 | 🟥 → 🔄: the docs say a job upserts, rows deleted meanwhile stay, drop or move the table first; the truncate option stays open | `architecture/snapshots.md`, `runbooks/snapshot-jobs.md`, `runbooks/halted-table.md` |
| DA-SNAP-17 | 🟥 → ✅ (documented): a job table's halt fails the job at the next flush | `runbooks/snapshot-jobs.md` |
| DA-SNAP-18 | 🟥 → ✅ (documented): a table created again while the pipeline was down takes two jobs | `runbooks/halted-table.md` |
| DA-CLUS-15 | 🟥 (stays): the docs warn and give the check (`GET /v1/cluster/status` on each node) and the remedy (restart the smaller group's pods); the fix stays in the code | `configuration/cluster.md`, `runbooks/failover.md` |

and, in the M11-carried rows of the roadmap's risk table (§6), R-5 and R-12 point at
`configuration/sink.md` (the flush guidance) and `configuration/storage.md` (the non-ASCII
limit, the date clamp).

- [ ] **Step 2: Commit**

```bash
git add doc/analysis/CDC_Design_Gap_Analysis_v1.md doc/v1.0/00-roadmap.md
git commit -m "gap: v1 §50 — M11b's docs: DA-OPS-8, DA-API-14/15, DA-SNAP-17/18 documented; DA-OPS-10 and DA-SNAP-15 partly; DA-CLUS-15 warned, its fix open"
```

---

## Exit

- [ ] `npm ci && npm run build` in `docs/` passes with its link validation (the roadmap's exit,
  amended by Q-M11b-1), in `docs.yml` on the PR.
- [ ] Every config example of the pages validates (`every_config_example_in_the_docs_validates`),
  and the generated pages are current (`the_generated_pages_are_current`), in `ci.yml` on the PR.
- [ ] After the merge, with the Pages source set to "GitHub Actions": `docs.yml`'s run on `main`
  deploys, and `https://alex-karpenko.github.io/data-avalanche/` serves the site.

## Deviations during execution

(none yet)
