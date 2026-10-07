use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::MAX_DECIMAL_PRECISION;

/// Deepest `list<…>` nesting the grammar accepts. Config input is operator-written, so the recursive
/// parser needs a bound; real schemas nest two or three levels at most.
pub const MAX_LIST_DEPTH: usize = 8;

/// Sink-neutral logical type — the grammar of `tables.column_type_overrides` (config §9.3).
///
/// This is *not* the grammar of the `data-avalanche.logical` Arrow field tag
/// (`Type_Mapping.md`): that tag carries additional provenance-only values (e.g. `uint64`,
/// `enum`, `set`) that have no `LogicalType` variant and must not be parsed with
/// [`LogicalType::from_str`].
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
        parse_type(s.trim(), 0).map_err(|reason| LogicalTypeError {
            input: s.to_string(),
            reason,
        })
    }
}

fn parse_type(s: &str, depth: usize) -> Result<LogicalType, String> {
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
        let precision: u8 = p
            .trim()
            .parse()
            .map_err(|_| format!("bad precision {:?}", p.trim()))?;
        let scale: i8 = sc
            .trim()
            .parse()
            .map_err(|_| format!("bad scale {:?}", sc.trim()))?;
        if !(1..=MAX_DECIMAL_PRECISION).contains(&precision) {
            return Err(format!(
                "precision {precision} is outside 1..={MAX_DECIMAL_PRECISION}"
            ));
        }
        if scale < 0 || scale.unsigned_abs() > precision {
            return Err(format!("scale {scale} is outside 0..={precision}"));
        }
        return Ok(LogicalType::Decimal { precision, scale });
    }
    if let Some(args) = s.strip_prefix("list") {
        if depth == MAX_LIST_DEPTH {
            return Err(format!("list nesting is deeper than {MAX_LIST_DEPTH}"));
        }
        let inner = args
            .trim()
            .strip_prefix('<')
            .and_then(|a| a.strip_suffix('>'))
            .ok_or("list needs <T>")?;
        return Ok(LogicalType::List(Box::new(parse_type(
            inner.trim(),
            depth + 1,
        )?)));
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

/// Stored as its text (`"decimal(10,2)"`, `"list<int64>"`).
impl Serialize for LogicalType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for LogicalType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIMPLE: [&str; 14] = [
        "bool",
        "int16",
        "int32",
        "int64",
        "float32",
        "float64",
        "string",
        "binary",
        "date",
        "time",
        "timestamp_tz",
        "timestamp_ntz",
        "uuid",
        "json",
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
        assert_eq!(
            t,
            LogicalType::Decimal {
                precision: 38,
                scale: 10
            }
        );
        assert_eq!(t.to_string(), "decimal(38,10)");
    }

    #[test]
    fn lists_nest() {
        let t: LogicalType = "list<list<string>>".parse().unwrap();
        assert_eq!(
            t,
            LogicalType::List(Box::new(LogicalType::List(Box::new(LogicalType::String))))
        );
        assert_eq!(t.to_string(), "list<list<string>>");
        assert_eq!(
            "list< int64 >".parse::<LogicalType>().unwrap().to_string(),
            "list<int64>"
        );
    }

    #[test]
    fn rejects_out_of_range_decimals() {
        for s in [
            "decimal(0,0)",
            "decimal(39,1)",
            "decimal(10,11)",
            "decimal(10,-1)",
            "decimal(10)",
            "decimal",
        ] {
            assert!(s.parse::<LogicalType>().is_err(), "{s}");
        }
    }

    #[test]
    fn rejects_ambiguous_and_unknown_names() {
        // `timestamp` means opposite things in Iceberg and Delta, so the grammar has no bare `timestamp` (D-MS.7).
        for s in [
            "timestamp",
            "TIMESTAMP_TZ",
            "Int64",
            "",
            "list<>",
            "list<foo>",
            "list<int64",
            "varchar",
        ] {
            assert!(s.parse::<LogicalType>().is_err(), "{s:?}");
        }
    }

    #[test]
    fn bounds_list_nesting() {
        let nested = |n: usize| format!("{}int64{}", "list<".repeat(n), ">".repeat(n));
        assert!(nested(MAX_LIST_DEPTH).parse::<LogicalType>().is_ok());
        let err = nested(MAX_LIST_DEPTH + 1)
            .parse::<LogicalType>()
            .unwrap_err();
        assert!(err.reason.contains("deeper than 8"), "{err}");
        // Far past the limit: rejected without exhausting the stack.
        assert!(nested(100_000).parse::<LogicalType>().is_err());
    }

    #[test]
    fn error_names_the_input() {
        let err = "decimal(99,1)".parse::<LogicalType>().unwrap_err();
        assert_eq!(err.input, "decimal(99,1)");
        assert!(err.to_string().contains("decimal(99,1)"));
    }
    #[test]
    fn serializes_as_its_text() {
        let t: LogicalType = "list<decimal(10,2)>".parse().unwrap();
        let json = serde_json::to_string(&t).unwrap();
        assert_eq!(json, r#""list<decimal(10,2)>""#);
        assert_eq!(serde_json::from_str::<LogicalType>(&json).unwrap(), t);
        let e = serde_json::from_str::<LogicalType>(r#""timestamp""#).unwrap_err();
        assert!(e.to_string().contains("invalid logical type"), "{e}");
    }
}
