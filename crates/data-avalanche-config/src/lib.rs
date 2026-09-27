//! data-avalanche configuration (`doc/specs/Configuration_Specification.md`): the YAML model,
//! strict parse, `${VAR}` and file resolution, merge of `defaults`, the V-rule validators, the
//! resolved config the runtime uses, and the reload diff.

pub mod coerce;
pub mod ctx;
pub mod diag;
pub mod enums;
pub mod leaf;
pub mod merge;
pub mod parse;
pub mod path;
pub mod raw;
pub mod registry;
pub mod template;
pub mod units;

pub use coerce::{LeafType, LogFilter};
pub use ctx::{At, Ctx};
pub use diag::{Diagnostic, Locus, PARSE, Report, Severity, TYPE};
pub use leaf::{Leaf, Loc, Tri};
pub use merge::merge_pipeline;
pub use parse::parse_document;
pub use path::YamlPath;
pub use raw::RawConfig;
pub use registry::{RULES, RuleInput, RuleMeta, RuleSeverity, rule_meta};
pub use template::{Env, ProcessEnv};
pub use units::{ByteSize, HostPort};
