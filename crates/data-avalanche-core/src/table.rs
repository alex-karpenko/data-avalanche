use std::fmt;

use serde::{Deserialize, Serialize};

/// A source table. For MySQL the `schema` slot holds the database (`source.db`), so the layout is
/// uniform across engines (config §10, `Data-Flow.md` `TableRef`).
/// Unknown fields are refused, like every stored state document (D-M2.6).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

impl TableRef {
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_fields_are_refused() {
        let ok: TableRef = serde_json::from_str(r#"{"schema":"public","name":"orders"}"#).unwrap();
        assert_eq!(ok, TableRef::new("public", "orders"));
        let e = serde_json::from_str::<TableRef>(r#"{"schema":"public","name":"orders","db":"x"}"#)
            .unwrap_err();
        assert!(e.to_string().contains("unknown field `db`"), "{e}");
    }

    #[test]
    fn displays_as_schema_dot_name() {
        assert_eq!(
            TableRef::new("public", "orders").to_string(),
            "public.orders"
        );
    }

    #[test]
    fn orders_by_schema_then_name() {
        let mut v = vec![
            TableRef::new("b", "a"),
            TableRef::new("a", "z"),
            TableRef::new("a", "b"),
        ];
        v.sort();
        assert_eq!(
            v,
            vec![
                TableRef::new("a", "b"),
                TableRef::new("a", "z"),
                TableRef::new("b", "a")
            ]
        );
    }

    #[test]
    fn lineage_names_are_reserved_in_any_case() {
        for n in [
            "_op",
            "_OP",
            "_Ts",
            "_lsn",
            "_SEQ",
            "_source_db",
            "_Pipeline",
        ] {
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
        for n in [
            "op",
            "_opx",
            "x_op",
            "__data_avalanche",
            "_data_avalanche_del",
            "source_db",
            "",
        ] {
            assert!(!is_reserved_column(n), "{n:?} should not be reserved");
        }
    }

    #[test]
    fn reports_first_collision_in_column_order() {
        assert_eq!(first_reserved_collision(["id", "_TS", "_op"]), Some("_TS"));
        assert_eq!(first_reserved_collision(["id", "name"]), None);
    }
}
