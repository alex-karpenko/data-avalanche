//! Type planning (`Type_Mapping.md`, the MySQL and MariaDB tables): a column as the catalog
//! describes it → its `ColumnValueType` and the [`Decode`] for its values. One plan serves both
//! inputs, the binlog and a snapshot chunk (S-MY type probe, 2026-10-14).

use data_avalanche_config::NumericOverflowAction;
use data_avalanche_core::{
    ColumnValueType as V, HaltReason, LogicalType, MAX_DECIMAL_PRECISION, canonical_logical,
    conversion, hop2_compatible,
};

use crate::value::{Charset, Decode, int_type};

/// A column as `information_schema.COLUMNS` describes it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CatalogColumn {
    pub name: String,
    /// `DATA_TYPE`, lower case: `int`, `varchar`, `enum`, MariaDB `uuid`, `inet6`.
    pub data_type: String,
    /// `COLUMN_TYPE`: `int unsigned`, `tinyint(1)`, `enum('a','b')`, `binary(4)`.
    pub column_type: String,
    /// `CHARACTER_SET_NAME`, for character columns.
    pub charset: Option<String>,
    /// `COLLATION_NAME`, for character columns: the order a snapshot reads a text key in.
    pub collation: Option<String>,
    /// `NUMERIC_PRECISION` / `NUMERIC_SCALE`, for decimals.
    pub precision: Option<u32>,
    pub scale: Option<u32>,
    /// `DATETIME_PRECISION`: the fractional digits of a `TIME`, `DATETIME` or `TIMESTAMP`.
    pub fsp: Option<u32>,
    /// `CHARACTER_MAXIMUM_LENGTH`: `n` of a `BINARY(n)`.
    pub max_length: Option<u64>,
    /// `ORDINAL_POSITION`, from 1.
    pub ordinal: u32,
    /// `IS_NULLABLE`.
    pub nullable: bool,
}

impl CatalogColumn {
    fn unsigned(&self) -> bool {
        self.column_type.contains("unsigned")
    }

    fn fsp(&self) -> u8 {
        self.fsp.map_or(0, |f| f.min(6) as u8)
    }
}

/// The `tinyint1` settings (`source.mysql.tinyint1_as_bool` and `tinyint1_detection`): `true`
/// maps a `tinyint(1)` column to `bool`.
pub type Tinyint1AsBool = bool;

/// A planned column: what the decoder produces and what the target stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPlan {
    pub value_type: V,
    pub logical: LogicalType,
    pub decode: Decode,
    /// `Some(type)` when the column is kept as its text because its decimal is wider than 38
    /// digits (`numeric_overflow_action: string`).
    pub read_as_text: Option<String>,
    /// An identifier column: an all-zero date there has no NULL to become (Q-M7-1).
    pub identifier: bool,
}

/// The members of an `enum(…)` or `set(…)` `COLUMN_TYPE`, quotes undoubled.
pub fn members(column_type: &str) -> Vec<String> {
    let Some(start) = column_type.find('(') else {
        return vec![];
    };
    let inner = &column_type[start + 1..column_type.rfind(')').unwrap_or(column_type.len())];
    let mut out = vec![];
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut member = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    member.push('\'');
                }
                '\'' => break,
                '\\' => member.extend(chars.next()),
                other => member.push(other),
            }
        }
        out.push(member);
    }
    out
}

/// The decoding of a character column in `charset` (Q-M7a-2): the UTF-8 family and latin1 read as
/// text, `binary` as bytes; any other charset can't be read.
fn character(c: &CatalogColumn) -> Result<(V, Decode), HaltReason> {
    let name = c.charset.as_deref().unwrap_or("binary");
    match Charset::of(name) {
        Some(Charset::Binary) => Ok((V::Bytes, Decode::Bytes { pad: None })),
        Some(charset) => Ok((V::Text, Decode::Text { charset })),
        None => Err(HaltReason::SchemaMismatch {
            column: c.name.clone(),
            detail: format!(
                "character set {name} isn't read by the source (UTF-8 family, latin1, binary); \
                 convert the column, or override it to binary"
            ),
        }),
    }
}

fn int(bits: u32, unsigned: bool) -> (V, Decode) {
    (int_type(bits, unsigned), Decode::Int { bits, unsigned })
}

/// Plans `c` without an override. `Ok(None)` keeps an over-wide decimal as text.
fn plan(
    c: &CatalogColumn,
    tinyint1_as_bool: Tinyint1AsBool,
    action: NumericOverflowAction,
) -> Result<(V, Decode, Option<String>), HaltReason> {
    let unsigned = c.unsigned();
    let fsp = c.fsp();
    let (t, d) = match c.data_type.as_str() {
        "tinyint" if tinyint1_as_bool && !unsigned && c.column_type.starts_with("tinyint(1)") => {
            (V::Bool, Decode::Bool)
        }
        "tinyint" => int(8, unsigned),
        "smallint" => int(16, unsigned),
        "mediumint" => int(24, unsigned),
        "int" | "integer" => int(32, unsigned),
        "bigint" => int(64, unsigned),
        "float" => (V::Float32, Decode::Float32),
        "double" | "real" => (V::Float64, Decode::Float64),
        "decimal" | "numeric" => {
            let (p, s) = (c.precision.unwrap_or(10), c.scale.unwrap_or(0));
            if p > u32::from(MAX_DECIMAL_PRECISION) {
                return match action {
                    NumericOverflowAction::String => Ok((
                        V::Text,
                        Decode::DecimalText,
                        Some(format!("decimal({p},{s})")),
                    )),
                    NumericOverflowAction::Error => Err(HaltReason::NumericOverflow {
                        column: c.name.clone(),
                    }),
                };
            }
            (
                V::Numeric {
                    precision: p as u8,
                    scale: s as i8,
                },
                Decode::Decimal,
            )
        }
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => character(c)?,
        "binary" => (
            V::Bytes,
            Decode::Bytes {
                pad: c.max_length.map(|n| n as usize),
            },
        ),
        "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" | "vector" => {
            (V::Bytes, Decode::Bytes { pad: None })
        }
        "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" | "geomcollection" => {
            (V::Bytes, Decode::Bytes { pad: None })
        }
        "date" => (V::Date, Decode::Date),
        "time" => (V::Time, Decode::Time { fsp }),
        "datetime" => (V::Timestamp { with_tz: false }, Decode::DateTime { fsp }),
        "timestamp" => (V::Timestamp { with_tz: true }, Decode::Timestamp { fsp }),
        "year" => (V::Int32, Decode::Year),
        "json" => (V::Json, Decode::Json),
        "enum" => (
            V::Text,
            Decode::Enum {
                members: members(&c.column_type),
            },
        ),
        "set" => (
            V::Array(Box::new(V::Text)),
            Decode::Set {
                members: members(&c.column_type),
            },
        ),
        "uuid" => (V::Uuid, Decode::Uuid),
        "inet4" => (V::Text, Decode::Inet4),
        "inet6" => (V::Text, Decode::Inet6),
        other => {
            return Err(HaltReason::SchemaMismatch {
                column: c.name.clone(),
                detail: format!("type {other} ({}) isn't supported", c.column_type),
            });
        }
    };
    Ok((t, d, None))
}

/// Plans column `c` with its `tables.column_type_overrides` entry, if any (config §9.3):
/// - `string` keeps the server's text of the value (an invalid date's raw text, Q-M7-1); a byte
///   column has no text and halts;
/// - `decimal(P,S)` on a decimal column decodes it as that decimal, whatever its declared
///   precision (the V-079 remedy);
/// - any other override is one Hop 2 can store the values as ([`hop2_compatible`]) or a lossless
///   [`conversion`]; otherwise the table halts (`SCHEMA_MISMATCH`).
pub fn plan_column(
    c: &CatalogColumn,
    tinyint1_as_bool: Tinyint1AsBool,
    action: NumericOverflowAction,
    over: Option<&LogicalType>,
    identifier: bool,
) -> Result<ColumnPlan, HaltReason> {
    let column = |value_type: V, logical: LogicalType, decode: Decode, read_as_text| ColumnPlan {
        value_type,
        logical,
        decode,
        read_as_text,
        identifier,
    };
    let mismatch = |detail: String| HaltReason::SchemaMismatch {
        column: c.name.clone(),
        detail,
    };
    match over {
        None => {
            let (t, d, text) = plan(c, tinyint1_as_bool, action)?;
            Ok(column(t.clone(), canonical_logical(&t), d, text))
        }
        Some(LogicalType::String) => {
            let (t, d, _) = plan(c, tinyint1_as_bool, NumericOverflowAction::String)?;
            if t == V::Bytes {
                return Err(mismatch(
                    "column_type_overrides string: a byte column has no text form".into(),
                ));
            }
            Ok(column(
                V::Text,
                LogicalType::String,
                Decode::String(Box::new(d)),
                None,
            ))
        }
        Some(LogicalType::Decimal { precision, scale })
            if matches!(c.data_type.as_str(), "decimal" | "numeric") =>
        {
            let t = V::Numeric {
                precision: *precision,
                scale: *scale,
            };
            Ok(column(
                t.clone(),
                canonical_logical(&t),
                Decode::Decimal,
                None,
            ))
        }
        Some(l) => {
            let (t, d, _) = plan(c, tinyint1_as_bool, NumericOverflowAction::String)?;
            if hop2_compatible(&t, l) {
                Ok(column(t, l.clone(), d, None))
            } else if let Some(to) = conversion(&t, l) {
                let d = Decode::Convert {
                    from: Box::new(d),
                    to: to.clone(),
                };
                Ok(column(to, l.clone(), d, None))
            } else {
                Err(mismatch(format!(
                    "column_type_overrides {l} doesn't fit values of {t:?}; use string, a \
                     compatible type or a lossless conversion"
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(data_type: &str, column_type: &str) -> CatalogColumn {
        CatalogColumn {
            name: "c".into(),
            data_type: data_type.into(),
            column_type: column_type.into(),
            charset: Some("utf8mb4".into()),
            ..Default::default()
        }
    }

    fn planned(c: &CatalogColumn) -> (V, Decode) {
        let p = plan_column(c, true, NumericOverflowAction::Error, None, false).unwrap();
        (p.value_type, p.decode)
    }

    #[test]
    fn integers_widen_unsigned_and_tinyint1_is_bool() {
        let t = |d: &str, ct: &str| planned(&col(d, ct)).0;
        assert_eq!(t("tinyint", "tinyint"), V::Int16);
        assert_eq!(t("tinyint", "tinyint unsigned"), V::Int16);
        assert_eq!(t("tinyint", "tinyint(1)"), V::Bool);
        assert_eq!(t("tinyint", "tinyint(1) unsigned"), V::Int16);
        assert_eq!(t("smallint", "smallint unsigned"), V::Int32);
        assert_eq!(t("mediumint", "mediumint"), V::Int32);
        assert_eq!(t("int", "int unsigned"), V::Int64);
        assert_eq!(t("bigint", "bigint"), V::Int64);
        assert_eq!(t("bigint", "bigint unsigned"), V::UInt64);
        let p = plan_column(
            &col("tinyint", "tinyint(1)"),
            false,
            NumericOverflowAction::Error,
            None,
            false,
        )
        .unwrap();
        assert_eq!(p.value_type, V::Int16, "tinyint1_as_bool off");
    }

    #[test]
    fn decimals_wider_than_38_digits_follow_the_overflow_action() {
        let mut c = col("decimal", "decimal(65,30)");
        (c.precision, c.scale) = (Some(65), Some(30));
        assert_eq!(
            plan_column(&c, true, NumericOverflowAction::Error, None, false).unwrap_err(),
            HaltReason::NumericOverflow { column: "c".into() }
        );
        let p = plan_column(&c, true, NumericOverflowAction::String, None, false).unwrap();
        assert_eq!(p.value_type, V::Text);
        assert_eq!(p.read_as_text.as_deref(), Some("decimal(65,30)"));
        // The V-079 remedy: an override to a decimal that fits.
        let d = LogicalType::Decimal {
            precision: 38,
            scale: 10,
        };
        let p = plan_column(&c, true, NumericOverflowAction::Error, Some(&d), false).unwrap();
        assert_eq!(
            p.value_type,
            V::Numeric {
                precision: 38,
                scale: 10
            }
        );
        (c.precision, c.scale) = (Some(10), Some(2));
        assert_eq!(
            planned(&c).0,
            V::Numeric {
                precision: 10,
                scale: 2
            }
        );
    }

    #[test]
    fn characters_read_by_charset() {
        let mut c = col("varchar", "varchar(20)");
        for cs in ["utf8mb4", "utf8mb3", "utf8", "ascii"] {
            c.charset = Some(cs.into());
            assert_eq!(
                planned(&c).1,
                Decode::Text {
                    charset: Charset::Utf8
                },
                "{cs}"
            );
        }
        c.charset = Some("latin1".into());
        assert_eq!(
            planned(&c).1,
            Decode::Text {
                charset: Charset::Latin1
            }
        );
        c.charset = Some("binary".into());
        assert_eq!(planned(&c), (V::Bytes, Decode::Bytes { pad: None }));
        c.charset = Some("sjis".into());
        let e = plan_column(&c, true, NumericOverflowAction::Error, None, false).unwrap_err();
        assert!(
            matches!(&e, HaltReason::SchemaMismatch { detail, .. } if detail.contains("sjis")),
            "{e:?}"
        );
    }

    #[test]
    fn the_other_types_follow_the_table() {
        let mut bin = col("binary", "binary(4)");
        bin.max_length = Some(4);
        assert_eq!(planned(&bin), (V::Bytes, Decode::Bytes { pad: Some(4) }));
        let mut dt = col("datetime", "datetime(3)");
        dt.fsp = Some(3);
        assert_eq!(
            planned(&dt),
            (V::Timestamp { with_tz: false }, Decode::DateTime { fsp: 3 })
        );
        assert_eq!(
            planned(&col("timestamp", "timestamp")).0,
            V::Timestamp { with_tz: true }
        );
        assert_eq!(planned(&col("year", "year")), (V::Int32, Decode::Year));
        assert_eq!(planned(&col("json", "json")).0, V::Json);
        assert_eq!(
            planned(&col("enum", "enum('a','b')")),
            (
                V::Text,
                Decode::Enum {
                    members: vec!["a".into(), "b".into()]
                }
            )
        );
        assert_eq!(
            planned(&col("set", "set('x','y')")).0,
            V::Array(Box::new(V::Text))
        );
        assert_eq!(planned(&col("geometry", "point")).0, V::Bytes);
        assert_eq!(planned(&col("uuid", "uuid")).0, V::Uuid);
        assert_eq!(planned(&col("inet6", "inet6")), (V::Text, Decode::Inet6));
        assert_eq!(planned(&col("vector", "vector(3)")).0, V::Bytes);
        assert!(
            plan_column(
                &col("hyperloglog", "x"),
                true,
                NumericOverflowAction::Error,
                None,
                false
            )
            .is_err()
        );
    }

    #[test]
    fn enum_and_set_members_undo_their_quoting() {
        assert_eq!(members("enum('a','b','c')"), ["a", "b", "c"]);
        assert_eq!(members("set('it''s','a,b')"), ["it's", "a,b"]);
        assert_eq!(members("enum('it''s','x\\\\y','')"), ["it's", "x\\y", ""]);
        assert_eq!(members("enum()"), Vec::<String>::new());
    }

    #[test]
    fn overrides_keep_text_convert_losslessly_or_halt() {
        let mut d = col("date", "date");
        let s = plan_column(
            &d,
            true,
            NumericOverflowAction::Error,
            Some(&LogicalType::String),
            false,
        )
        .unwrap();
        assert_eq!(s.value_type, V::Text);
        assert_eq!(s.decode, Decode::String(Box::new(Decode::Date)));
        let ts = plan_column(
            &d,
            true,
            NumericOverflowAction::Error,
            Some(&LogicalType::TimestampNtz),
            false,
        )
        .unwrap();
        assert_eq!(ts.value_type, V::Timestamp { with_tz: false });
        assert!(matches!(ts.decode, Decode::Convert { .. }));
        let i = plan_column(
            &col("int", "int"),
            true,
            NumericOverflowAction::Error,
            Some(&LogicalType::Int64),
            false,
        )
        .unwrap();
        assert_eq!(i.value_type, V::Int64);
        d.data_type = "blob".into();
        assert!(
            plan_column(
                &d,
                true,
                NumericOverflowAction::Error,
                Some(&LogicalType::String),
                false
            )
            .is_err()
        );
        assert!(
            plan_column(
                &col("bigint", "bigint"),
                true,
                NumericOverflowAction::Error,
                Some(&LogicalType::Int32),
                false
            )
            .is_err()
        );
    }
}
