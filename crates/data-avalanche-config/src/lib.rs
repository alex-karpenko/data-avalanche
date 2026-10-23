//! data-avalanche configuration (`doc/specs/Configuration_Specification.md`): the YAML model,
//! strict parse, `${VAR}` and file resolution, merge of `defaults`, the V-rule validators, the
//! resolved config the runtime uses, and the reload diff.
//!
//! How the runtime uses it:
//!
//! - **Startup and reload:** [`load_file`] (or [`load_str`]) returns a [`ResolvedConfig`] plus
//!   warnings, or a [`Report`] of every error. Any error rejects the whole config (§1.4). Both
//!   take a [`FingerprintKey`] (config §6.2): 32 random bytes the node that creates the
//!   `globals.state` cluster anchor ([`ClusterAnchor`]) generates once, with the same
//!   create-if-absent conditional write that pins `tokens`, and never changes afterward, so every
//!   node's fingerprints of the same secret or file agree. On a cold start the key isn't known yet
//!   — it lives *in* the config the crate is about to load — so M2's startup sequence runs
//!   [`load_file`] twice: (1) once with [`FingerprintKey::random`], discarding the result except
//!   for `globals.state`, `cloud` and the cluster anchor fields (their fingerprints don't matter
//!   here); (2) read the anchor at that `globals.state` location, or create it (generating
//!   `fingerprint_key` there); (3) adopt the cloud config generation
//!   (`data_avalanche_state`'s `ConfigGenerations::bootstrap_or_adopt`, which bootstraps it from
//!   this file only when none exists) and load its text with the anchor's key — that result is
//!   the running config (Clustering §9.3). A dedicated `load_globals` entry point is deliberately
//!   not added for step 1; a later reload only loads the new text with the already-known key
//!   (and publishes it as the next generation).
//! - **Validation endpoint:** [`validate_str`] runs the same phases and returns only the report;
//!   it never exposes the resolved config (§11.1), and its fingerprints never leave the call, so
//!   it keys them with [`FingerprintKey::random`] rather than taking a key. [`Report::to_json`] is
//!   the response body.
//! - **Reload:** [`diff()`] compares the running and the new config, classifying each changed
//!   field by [`Mutability`]; a changed restart-only field rejects the reload (V-061). Both
//!   configs must be loaded with the same [`FingerprintKey`] (the cluster anchor's) — with two
//!   different keys every secret and file fingerprint differs whether or not the value changed,
//!   so a key mismatch reports every secret field as changed (a spurious V-061 for a PROC field),
//!   never the reverse: it cannot hide a real rotation.
//! - **Rules that need more than the file:** [`check_cluster_anchor`] at startup,
//!   [`check_table_set`] when a pipeline's tables resolve, [`check_unbounded_numeric`] at schema
//!   resolution.
//!
//! Every diagnostic carries its V-rule ID ([`RULES`] is the §13 master table), or [`PARSE`] /
//! [`TYPE`] for YAML and value-type errors. [`Secret`] values never print: `Debug` shows `***` and
//! `Serialize` a keyed BLAKE3 fingerprint (§11.1). [`FingerprintKey`]'s own `Debug` hides its
//! bytes too, and it carries no `Serialize` impl — it is not a config field.
//!
//! ## Public surface
//!
//! The public surface is exactly what the runtime (milestones M2–M8) consumes to load, validate,
//! diff and act on a config; everything else (the parser, the raw model, merge, the evaluation
//! context, the resolved-config build, the rule checks, scalar coercion, template expansion, raw
//! leaves, unit-parsing helpers and file-material readers) is an internal implementation detail and
//! is not reachable from outside this crate. Every public type is re-exported at the crate root (the
//! workspace API convention, `data-avalanche-core`'s crate docs): callers import
//! `data_avalanche_config::X`, never a module path.
//!
//! The surface is:
//! - **loaders:** [`load_file`], [`load_str`], [`validate_str`], [`Loaded`], [`FingerprintKey`];
//! - **the resolved model:** every type in the (private) `resolved` module, the config enums and
//!   their shared `FromStr::Err`, [`EnumValueError`] (public because a public `FromStr` impl's
//!   associated `Err` type must be, E0446), the value types with their own text grammar
//!   ([`ByteSize`]/[`ByteSizeError`],
//!   [`HostPort`]/[`HostPortError`]), [`Secret`], [`Pem`], [`Origin`], [`FileRef`], [`LogFilter`]
//!   (a resolved `Log.level`), and [`Loc`] (a [`Diagnostic`]'s source location);
//! - **diagnostics:** [`Report`], [`Diagnostic`], [`Severity`], [`Locus`], [`PARSE`], [`TYPE`];
//! - **the registry:** [`RULES`], [`RuleMeta`], [`RuleSeverity`], [`RuleLocus`], [`RuleInput`],
//!   [`rule_meta`];
//! - **the reload diff:** [`diff`], [`diff_with`], [`Fingerprints`], [`ConfigDiff`],
//!   [`FieldChange`], [`PipelineChange`], [`Mutability`];
//! - **external checks:** [`ClusterAnchor`] and the three `check_*` functions;
//! - **env:** [`Env`], [`ProcessEnv`].

mod build;
mod coerce;
mod ctx;
mod diag;
mod diff;
mod enums;
mod external;
mod leaf;
mod load;
mod material;
mod merge;
mod mutability;
mod parse;
mod path;
mod raw;
mod registry;
mod resolved;
mod rules;
mod secret;
mod template;
mod units;
mod walk;

pub use build::derived_server_id;
pub use coerce::LogFilter;
pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
pub use diff::{ConfigDiff, FieldChange, Fingerprints, PipelineChange, diff, diff_with};
pub use external::{
    ClusterAnchor, check_cluster_anchor, check_partitioning, check_table_set,
    check_unbounded_numeric, glob_match,
};
pub use leaf::Loc;
pub use load::{Loaded, load_file, load_str, validate_str};
pub use material::{FileRef, Origin, Pem};
pub use mutability::Mutability;
pub use registry::{RULES, RuleInput, RuleLocus, RuleMeta, RuleSeverity, rule_meta};
pub use secret::{FingerprintKey, Secret};
pub use template::{Env, ProcessEnv};
pub use units::{ByteSize, ByteSizeError, HostPort, HostPortError};

// Every public type is reachable from the crate root (API convention): the resolved model and the
// config enums by glob.
pub use enums::*;
pub use resolved::*;
