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
pub(crate) struct Leaf<T> {
    text: String,
    loc: Option<Loc>,
    _type: PhantomData<fn() -> T>,
}

impl<T> Leaf<T> {
    /// A leaf with no file location. Only used by unit tests inline in this crate — production
    /// code always gets a `Leaf` from deserializing the document (`Leaf::deserialize`).
    #[cfg(test)]
    pub(crate) fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            loc: None,
            _type: PhantomData,
        }
    }

    /// Only used by this module's own unit tests.
    #[cfg(test)]
    pub(crate) fn with_loc(text: impl Into<String>, loc: Loc) -> Self {
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
    /// Never prints the text: a leaf can hold a secret's literal value (before it becomes a
    /// [`crate::secret::Secret`]), and `Debug` output ends up in test failure messages, panics
    /// and `{:#?}`-formatted diagnostics dumps alike. Only the location survives, e.g.
    /// `Leaf(***@12:5)` or `Leaf(***)` with no location.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.loc {
            Some(loc) => write!(f, "Leaf(***@{}:{})", loc.line, loc.column),
            None => write!(f, "Leaf(***)"),
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
pub(crate) type Tri<T> = Option<Option<Leaf<T>>>;

/// `#[serde(default, deserialize_with = "crate::leaf::tri")]` for [`Tri`] fields: serde maps an absent
/// field to `None` through `default`, so reaching this function means the key was present.
pub(crate) fn tri<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_prints_the_text() {
        let with_loc = Leaf::<String>::with_loc(
            "s3cr3t",
            Loc {
                line: 12,
                column: 5,
            },
        );
        assert_eq!(format!("{with_loc:?}"), "Leaf(***@12:5)");
        assert!(!format!("{with_loc:?}").contains("s3cr3t"));

        let without_loc = Leaf::<String>::new("s3cr3t");
        assert_eq!(format!("{without_loc:?}"), "Leaf(***)");
        assert!(!format!("{without_loc:?}").contains("s3cr3t"));
    }
}
