//! data-avalanche configuration (`doc/specs/Configuration_Specification.md`): the YAML model,
//! strict parse, `${VAR}` and file resolution, merge of `defaults`, the V-rule validators, the
//! resolved config the runtime uses, and the reload diff.
//!
//! How the runtime uses it:
//!
//! - **Startup and reload:** [`load_file`] (or [`load_str`]) returns a [`ResolvedConfig`] plus
//!   warnings, or a [`Report`] of every error. Any error rejects the whole config (§1.4).
//! - **Validation endpoint:** [`validate_str`] runs the same phases and returns only the report;
//!   it never exposes the resolved config (§11.1). [`Report::to_json`] is the response body.
//! - **Reload:** [`diff()`] compares the running and the new config, classifying each changed
//!   field by [`Mutability`]; a changed restart-only field rejects the reload (V-061).
//! - **Rules that need more than the file:** [`check_cluster_anchor`] at startup,
//!   [`check_table_set`] when a pipeline's tables resolve, [`check_unbounded_numeric`] at schema
//!   resolution.
//!
//! Every diagnostic carries its V-rule ID ([`RULES`] is the §13 master table), or [`PARSE`] /
//! [`TYPE`] for YAML and value-type errors. [`Secret`] values never print: `Debug` shows `***` and
//! `Serialize` a fingerprint.

pub mod build;
pub mod coerce;
pub mod ctx;
pub mod diag;
pub mod diff;
pub mod enums;
pub mod external;
pub mod leaf;
pub mod load;
pub mod material;
pub mod merge;
pub mod mutability;
pub mod parse;
pub mod path;
pub mod raw;
pub mod registry;
pub mod resolved;
pub mod rules;
pub mod secret;
pub mod template;
pub mod units;
mod walk;

pub use coerce::{LeafType, LogFilter};
pub use ctx::{At, Ctx};
pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
pub use diff::{ConfigDiff, FieldChange, PipelineChange, diff};
pub use external::{ClusterAnchor, check_cluster_anchor, check_table_set, check_unbounded_numeric};
pub use leaf::{Leaf, Loc, Tri};
pub use load::{Loaded, load_file, load_str, validate_str};
pub use material::{FileRef, Origin, Pem};
pub use merge::merge_pipeline;
pub use mutability::Mutability;
pub use parse::parse_document;
pub use path::YamlPath;
pub use raw::RawConfig;
pub use registry::{RULES, RuleInput, RuleMeta, RuleSeverity, rule_meta};
pub use resolved::{Pipeline, ResolvedConfig};
pub use secret::Secret;
pub use template::{Env, ProcessEnv};
pub use units::{ByteSize, HostPort};

// Every public type is reachable from the crate root (API convention): the resolved model, the raw
// model and the config enums by glob; the rest by name.
pub use enums::*;
pub use raw::*;
pub use resolved::*;

pub use material::PemKind;
pub use merge::Merge;
pub use template::{Expanded, TemplateError, expand};
pub use units::{ByteSizeError, HostPortError};
