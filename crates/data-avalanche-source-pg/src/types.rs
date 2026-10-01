//! Type planning (`Type_Mapping.md`, PostgreSQL): a column's type OID and `atttypmod` → its
//! `ColumnValueType` and the [`Decode`] for its text. Built-in scalars are known by OID; every
//! other type (arrays, domains, enums, composites, ranges, extension types) is planned from its
//! `pg_type` row ([`TypeInfo`]).

use std::collections::HashMap;

use data_avalanche_config::NumericOverflowAction;
use data_avalanche_core::{
    ColumnValueType as V, HaltReason, LogicalType, MAX_DECIMAL_PRECISION, canonical_logical,
    hop2_compatible,
};

use crate::value::Decode;

/// The `pg_type` columns the planner reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeInfo {
    pub oid: u32,
    pub name: String,
    pub schema: String,
    /// `typtype`: `b` base, `c` composite, `d` domain, `e` enum, `p` pseudo, `r` range,
    /// `m` multirange.
    pub kind: u8,
    /// `typcategory`: `A` for arrays.
    pub category: u8,
    /// `typelem`: an array's element type.
    pub element: u32,
    /// `typbasetype`: a domain's base type.
    pub base: u32,
    /// `typtypmod`: a domain's typmod.
    pub base_typmod: i32,
    /// `typdelim`: the array delimiter of this element type.
    pub delimiter: u8,
}

/// `pg_type` rows by OID: the planner's view of the catalog.
pub type TypeCatalog = HashMap<u32, TypeInfo>;

/// The planned decoding of one column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub value_type: V,
    pub decode: Decode,
    /// A type kept as its text because it has no closer mapping (`Type_Mapping.md` fallback chain:
    /// text with a warning).
    pub read_as_text: Option<String>,
    /// A `numeric` column without a declared precision (V-079).
    pub unconstrained_numeric: bool,
}

impl Plan {
    fn of(value_type: V, decode: Decode) -> Self {
        Self {
            value_type,
            decode,
            read_as_text: None,
            unconstrained_numeric: false,
        }
    }

    fn text() -> Self {
        Self::of(V::Text, Decode::Text)
    }
}

/// Why a column can't be planned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// `numeric(P,S)` beyond 38 digits under `numeric_overflow_action: error`.
    NumericOverflow,
    /// A type OID with no `pg_type` row in the catalog given: the caller fetches it and plans
    /// again.
    Missing(u32),
}

/// Built-in types kept as their PostgreSQL text (`Type_Mapping.md`: network, geometric, range,
/// text-search, XML, interval, `timetz`, identifier and system types).
#[rustfmt::skip]
const TEXT_OIDS: &[u32] = &[
    // timetz, interval, xml, jsonpath
    1266, 1186, 142, 4072,
    // inet, cidr, macaddr, macaddr8
    869, 650, 829, 774,
    // point, lseg, path, box, polygon, line, circle
    600, 601, 602, 603, 604, 628, 718,
    // tsvector, tsquery
    3614, 3615,
    // ranges
    3904, 3906, 3908, 3910, 3912, 3926,
    // multiranges
    4451, 4532, 4533, 4534, 4535, 4536,
    // xid, xid8, cid, tid, pg_lsn, txid_snapshot, pg_snapshot
    28, 5069, 29, 27, 3220, 2970, 5038,
    // reg* types
    24, 2202, 2203, 2204, 2205, 2206, 4096, 4089, 3734, 3769, 4191,
    // aclitem
    1033,
    // int2vector, oidvector: category `A`, but their text is space-separated, without braces
    22, 30,
];

/// Extension types kept as text without a warning (they are text already).
const KNOWN_TEXT_TYPES: &[&str] = &["citext", "ltree", "lquery", "ltxtquery"];

/// `numeric`'s typmod → `(precision, scale)`, `None` without one. The scale is an 11-bit signed
/// field since PostgreSQL 15.
pub fn numeric_typmod(typmod: i32) -> Option<(i32, i32)> {
    if typmod < 4 {
        return None;
    }
    let t = typmod - 4;
    Some(((t >> 16) & 0xFFFF, ((t & 0x7FF) ^ 1024) - 1024))
}

/// Q-M5-8: a `numeric` without a declared precision, under `numeric_overflow_action: error`.
pub const UNCONSTRAINED_NUMERIC: V = V::Numeric {
    precision: 38,
    scale: 18,
};

fn numeric(typmod: i32, action: NumericOverflowAction) -> Result<Plan, PlanError> {
    let as_text = |t: String| Plan {
        read_as_text: Some(t),
        ..Plan::text()
    };
    let Some((p, s)) = numeric_typmod(typmod) else {
        let mut plan = match action {
            NumericOverflowAction::String => as_text("numeric".into()),
            NumericOverflowAction::Error => Plan::of(UNCONSTRAINED_NUMERIC, Decode::Numeric),
        };
        plan.unconstrained_numeric = true;
        return Ok(plan);
    };
    // A negative scale rounds to tens: numeric(3,-2) holds 12300 = decimal(5,0). A scale above
    // the precision holds only fractions: numeric(2,5) holds 0.00012 = decimal(5,5).
    let declared = format!("numeric({p},{s})");
    let (p, s) = if s < 0 { (p - s, 0) } else { (p.max(s), s) };
    if p > i32::from(MAX_DECIMAL_PRECISION) {
        return match action {
            NumericOverflowAction::String => Ok(as_text(declared)),
            NumericOverflowAction::Error => Err(PlanError::NumericOverflow),
        };
    }
    Ok(Plan::of(
        V::Numeric {
            precision: p as u8,
            scale: s as i8,
        },
        Decode::Numeric,
    ))
}

fn builtin(
    oid: u32,
    typmod: i32,
    action: NumericOverflowAction,
) -> Option<Result<Plan, PlanError>> {
    let p = |t: V, d: Decode| Some(Ok(Plan::of(t, d)));
    match oid {
        16 => p(V::Bool, Decode::Bool),
        17 => p(V::Bytes, Decode::Bytea),
        18 => p(V::Int16, Decode::Char),
        19 | 25 | 1043 => p(V::Text, Decode::Text),
        1042 => p(V::Text, Decode::BpChar),
        20 => p(V::Int64, Decode::Int64),
        21 => p(V::Int16, Decode::Int16),
        23 => p(V::Int32, Decode::Int32),
        26 => p(V::Int64, Decode::Int64),
        700 => p(V::Float32, Decode::Float32),
        701 => p(V::Float64, Decode::Float64),
        1700 => Some(numeric(typmod, action)),
        790 => p(
            V::Numeric {
                precision: 19,
                scale: 2,
            },
            Decode::Money,
        ),
        1082 => p(V::Date, Decode::Date),
        1083 => p(V::Time, Decode::Time),
        1114 => p(
            V::Timestamp { with_tz: false },
            Decode::Timestamp { with_tz: false },
        ),
        1184 => p(
            V::Timestamp { with_tz: true },
            Decode::Timestamp { with_tz: true },
        ),
        2950 => p(V::Uuid, Decode::Uuid),
        114 | 3802 => p(V::Json, Decode::Json),
        1560 => p(V::Bytes, Decode::Bit),
        1562 => p(V::Bytes, Decode::VarBit),
        o if TEXT_OIDS.contains(&o) => p(V::Text, Decode::Text),
        _ => None,
    }
}

/// Plans a column of type `oid` with `typmod`. Types not built in come from `types`.
pub fn plan(
    oid: u32,
    typmod: i32,
    types: &TypeCatalog,
    action: NumericOverflowAction,
) -> Result<Plan, PlanError> {
    if let Some(p) = builtin(oid, typmod, action) {
        return p;
    }
    let t = types.get(&oid).ok_or(PlanError::Missing(oid))?;
    Ok(match t.kind {
        b'd' => {
            let typmod = if typmod < 0 { t.base_typmod } else { typmod };
            return plan(t.base, typmod, types, action);
        }
        b'e' | b'c' | b'r' | b'm' => Plan::text(),
        _ if t.category == b'A' && t.element != 0 => {
            let element = plan(t.element, typmod, types, action)?;
            Plan {
                value_type: V::Array(Box::new(element.value_type.clone())),
                decode: array_of(t.element, types, element.value_type, element.decode),
                ..element
            }
        }
        _ if t.name == "hstore" => Plan::of(V::Json, Decode::Hstore),
        _ if matches!(t.name.as_str(), "geometry" | "geography") => {
            Plan::of(V::Bytes, Decode::HexBytes)
        }
        _ if KNOWN_TEXT_TYPES.contains(&t.name.as_str()) => Plan::text(),
        _ => Plan {
            read_as_text: Some(format!("{}.{}", t.schema, t.name)),
            ..Plan::text()
        },
    })
}

/// The [`Decode`] of an array whose elements, of type `element`, decode as `decode`.
fn array_of(element: u32, types: &TypeCatalog, element_type: V, decode: Decode) -> Decode {
    let delimiter = match types.get(&element) {
        Some(e) => e.delimiter,
        None if element == 603 => b';',
        None => b',',
    };
    Decode::Array {
        element: Box::new(decode),
        element_type,
        delimiter,
    }
}

/// The OIDs `plan` will look up in the catalog for a column of type `oid`, given what is known.
pub fn missing(oid: u32, types: &TypeCatalog) -> Option<u32> {
    if builtin(oid, -1, NumericOverflowAction::String).is_some() {
        return None;
    }
    let Some(t) = types.get(&oid) else {
        return Some(oid);
    };
    match t.kind {
        b'd' => missing(t.base, types),
        _ if t.category == b'A' && t.element != 0 => missing(t.element, types),
        _ => None,
    }
}

/// A planned column: what the decoder produces and what the target stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPlan {
    pub value_type: V,
    pub logical: LogicalType,
    pub decode: Decode,
    pub read_as_text: Option<String>,
    pub unconstrained_numeric: bool,
}

/// Plans `column` with its `tables.column_type_overrides` entry, if any (config §9.3):
/// - `string` keeps the column's PostgreSQL text (`value_type` Text);
/// - `decimal(P,S)` on a `numeric` or `money` column decodes it as that decimal, whatever its
///   declared precision (the V-079 remedy), and `list<decimal(P,S)>` does the same for an array
///   of them;
/// - any other override must be one Hop 2 can store the column's values as
///   ([`hop2_compatible`]); otherwise the table halts (`SCHEMA_MISMATCH`).
pub fn plan_column(
    column: &str,
    oid: u32,
    typmod: i32,
    types: &TypeCatalog,
    action: NumericOverflowAction,
    over: Option<&LogicalType>,
) -> Result<ColumnPlan, HaltReason> {
    let plan_or_halt = |action| {
        plan(oid, typmod, types, action).map_err(|e| match e {
            PlanError::NumericOverflow => HaltReason::NumericOverflow {
                column: column.to_owned(),
            },
            PlanError::Missing(o) => HaltReason::SchemaMismatch {
                column: column.to_owned(),
                detail: format!("type {o} isn't in the catalog"),
            },
        })
    };
    if let Some((value_type, decode)) = over.and_then(|l| decimal_override(oid, types, l)) {
        return Ok(ColumnPlan {
            logical: canonical_logical(&value_type),
            value_type,
            decode,
            read_as_text: None,
            unconstrained_numeric: false,
        });
    }
    match over {
        Some(LogicalType::String) => Ok(ColumnPlan {
            value_type: V::Text,
            logical: LogicalType::String,
            decode: Decode::Text,
            read_as_text: None,
            unconstrained_numeric: false,
        }),
        Some(l) => {
            // The `string` action plans an over-wide numeric as text; the override decides here.
            let p = plan_or_halt(NumericOverflowAction::String)?;
            if !hop2_compatible(&p.value_type, l) {
                return Err(HaltReason::SchemaMismatch {
                    column: column.to_owned(),
                    detail: format!(
                        "column_type_overrides {l} doesn't fit values of {:?}; use string or a compatible type",
                        p.value_type
                    ),
                });
            }
            Ok(ColumnPlan {
                logical: l.clone(),
                value_type: p.value_type,
                decode: p.decode,
                read_as_text: None,
                unconstrained_numeric: false,
            })
        }
        None => {
            let p = plan_or_halt(action)?;
            Ok(ColumnPlan {
                logical: canonical_logical(&p.value_type),
                value_type: p.value_type,
                decode: p.decode,
                read_as_text: p.read_as_text,
                unconstrained_numeric: p.unconstrained_numeric,
            })
        }
    }
}

/// A `decimal(P,S)` override on a `numeric` or `money` column, or a `list<…>` of one on an array
/// of them, through domains: the value type and decoding it asks for, whatever the declared
/// precision. `None` when the override isn't of that shape.
fn decimal_override(oid: u32, types: &TypeCatalog, over: &LogicalType) -> Option<(V, Decode)> {
    let base = base_oid(oid, types);
    match over {
        LogicalType::Decimal { precision, scale } if matches!(base, 1700 | 790) => {
            let decode = if base == 790 {
                Decode::Money
            } else {
                Decode::Numeric
            };
            Some((
                V::Numeric {
                    precision: *precision,
                    scale: *scale,
                },
                decode,
            ))
        }
        LogicalType::List(e) => {
            let t = types
                .get(&base)
                .filter(|t| t.category == b'A' && t.element != 0)?;
            let (element_type, decode) = decimal_override(t.element, types, e)?;
            Some((
                V::Array(Box::new(element_type.clone())),
                array_of(t.element, types, element_type, decode),
            ))
        }
        _ => None,
    }
}

/// A domain's base type, through any number of domains.
fn base_oid(oid: u32, types: &TypeCatalog) -> u32 {
    match types.get(&oid) {
        Some(t) if t.kind == b'd' => base_oid(t.base, types),
        _ => oid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: NumericOverflowAction = NumericOverflowAction::String;
    const E: NumericOverflowAction = NumericOverflowAction::Error;

    fn info(
        oid: u32,
        name: &str,
        kind: u8,
        category: u8,
        element: u32,
        base: u32,
        base_typmod: i32,
    ) -> TypeInfo {
        TypeInfo {
            oid,
            name: name.into(),
            schema: if oid < 16384 { "pg_catalog" } else { "public" }.into(),
            kind,
            category,
            element,
            base,
            base_typmod,
            delimiter: if name == "box" { b';' } else { b',' },
        }
    }

    /// Catalog rows for the types the tests use beyond the built-ins.
    fn catalog() -> TypeCatalog {
        [
            info(1007, "_int4", b'b', b'A', 23, 0, -1),
            info(1231, "_numeric", b'b', b'A', 1700, 0, -1),
            info(1020, "_box", b'b', b'A', 603, 0, -1),
            info(603, "box", b'b', b'G', 600, 0, -1),
            info(1000, "_bool", b'b', b'A', 16, 0, -1),
            info(70000, "mood", b'e', b'E', 0, 0, -1),
            info(70001, "_mood", b'b', b'A', 70000, 0, -1),
            info(70002, "price", b'd', b'N', 0, 1700, numeric_mod(10, 2)),
            info(70003, "address", b'c', b'C', 0, 0, -1),
            info(70004, "hstore", b'b', b'U', 0, 0, -1),
            info(70005, "geometry", b'b', b'U', 0, 0, -1),
            info(70006, "citext", b'b', b'S', 0, 0, -1),
            info(70007, "vector", b'b', b'U', 0, 0, -1),
            info(70008, "floatrange", b'r', b'R', 0, 0, -1),
            info(70009, "wide", b'd', b'N', 0, 1700, -1),
            info(70010, "cash", b'd', b'N', 0, 790, -1),
            info(70011, "_price", b'b', b'A', 70002, 0, -1),
        ]
        .into_iter()
        .map(|t| (t.oid, t))
        .collect()
    }

    fn numeric_mod(p: i32, s: i32) -> i32 {
        ((p << 16) | (s & 0x7FF)) + 4
    }

    fn vt(oid: u32, typmod: i32) -> V {
        plan(oid, typmod, &catalog(), S).unwrap().value_type
    }

    #[test]
    fn built_in_scalars() {
        let c = catalog();
        for (oid, t) in [
            (16, V::Bool),
            (21, V::Int16),
            (23, V::Int32),
            (20, V::Int64),
            (26, V::Int64),
            (18, V::Int16),
            (700, V::Float32),
            (701, V::Float64),
            (25, V::Text),
            (1043, V::Text),
            (1042, V::Text),
            (19, V::Text),
            (17, V::Bytes),
            (1082, V::Date),
            (1083, V::Time),
            (1114, V::Timestamp { with_tz: false }),
            (1184, V::Timestamp { with_tz: true }),
            (2950, V::Uuid),
            (114, V::Json),
            (3802, V::Json),
            (1560, V::Bytes),
            (1562, V::Bytes),
            (
                790,
                V::Numeric {
                    precision: 19,
                    scale: 2,
                },
            ),
            (1266, V::Text),
            (1186, V::Text),
            (869, V::Text),
            (3904, V::Text),
            (4451, V::Text),
            (3614, V::Text),
            (142, V::Text),
            (22, V::Text),
            (30, V::Text),
        ] {
            let p = plan(oid, -1, &c, S).unwrap();
            assert_eq!(p.value_type, t, "oid {oid}");
            assert_eq!(p.read_as_text, None, "oid {oid}");
        }
        assert_eq!(plan(1042, -1, &c, S).unwrap().decode, Decode::BpChar);
    }

    #[test]
    fn numeric_precision_rules() {
        assert_eq!(numeric_typmod(-1), None);
        assert_eq!(numeric_typmod(numeric_mod(10, 2)), Some((10, 2)));
        assert_eq!(numeric_typmod(numeric_mod(3, -2)), Some((3, -2)));
        assert_eq!(
            vt(1700, numeric_mod(10, 2)),
            V::Numeric {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            vt(1700, numeric_mod(38, 0)),
            V::Numeric {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(
            vt(1700, numeric_mod(3, -2)),
            V::Numeric {
                precision: 5,
                scale: 0
            },
            "negative scale widens"
        );
        assert_eq!(
            vt(1700, numeric_mod(2, 5)),
            V::Numeric {
                precision: 5,
                scale: 5
            },
            "scale above precision"
        );
        // Beyond 38 digits: text under `string`, a halt under `error`.
        assert_eq!(vt(1700, numeric_mod(39, 2)), V::Text);
        assert_eq!(
            plan(1700, numeric_mod(39, 2), &catalog(), S)
                .unwrap()
                .read_as_text
                .as_deref(),
            Some("numeric(39,2)"),
            "text with a warning (Type_Mapping)"
        );
        assert_eq!(vt(1700, numeric_mod(37, -2)), V::Text);
        assert_eq!(
            plan(1700, numeric_mod(39, 2), &catalog(), E),
            Err(PlanError::NumericOverflow)
        );
        // No declared precision: text under `string`, decimal(38,18) under `error` (Q-M5-8).
        let bare = plan(1700, -1, &catalog(), S).unwrap();
        assert_eq!(
            (bare.value_type, bare.unconstrained_numeric),
            (V::Text, true)
        );
        assert_eq!(bare.read_as_text.as_deref(), Some("numeric"));
        let bare = plan(1700, -1, &catalog(), E).unwrap();
        assert_eq!(
            (bare.value_type, bare.unconstrained_numeric),
            (UNCONSTRAINED_NUMERIC, true)
        );
    }

    #[test]
    fn arrays_follow_their_element() {
        assert_eq!(vt(1007, -1), V::Array(Box::new(V::Int32)));
        assert_eq!(
            vt(1231, numeric_mod(12, 3)),
            V::Array(Box::new(V::Numeric {
                precision: 12,
                scale: 3
            })),
            "the column typmod applies to the element"
        );
        assert_eq!(
            vt(70001, -1),
            V::Array(Box::new(V::Text)),
            "an array of an enum"
        );
        assert_eq!(
            vt(70011, -1),
            V::Array(Box::new(V::Numeric {
                precision: 10,
                scale: 2
            })),
            "an array of a domain"
        );
        let boxes = plan(1020, -1, &catalog(), S).unwrap();
        assert!(
            matches!(
                boxes.decode,
                Decode::Array {
                    delimiter: b';',
                    ..
                }
            ),
            "box arrays use ';'"
        );
    }

    #[test]
    fn user_and_extension_types() {
        assert_eq!(vt(70000, -1), V::Text, "enum");
        assert_eq!(vt(70003, -1), V::Text, "composite");
        assert_eq!(vt(70008, -1), V::Text, "range");
        assert_eq!(
            vt(70002, -1),
            V::Numeric {
                precision: 10,
                scale: 2
            },
            "a domain takes its typmod"
        );
        assert_eq!(vt(70004, -1), V::Json, "hstore");
        assert_eq!(vt(70005, -1), V::Bytes, "PostGIS");
        assert_eq!(vt(70006, -1), V::Text, "citext");
        let unknown = plan(70007, -1, &catalog(), S).unwrap();
        assert_eq!(unknown.value_type, V::Text);
        assert_eq!(unknown.read_as_text.as_deref(), Some("public.vector"));
        assert_eq!(
            plan(99999, -1, &catalog(), S),
            Err(PlanError::Missing(99999))
        );
    }

    #[test]
    fn missing_names_what_to_fetch() {
        let c = catalog();
        assert_eq!(missing(23, &c), None);
        assert_eq!(missing(1007, &c), None);
        assert_eq!(missing(99999, &c), Some(99999));
        let mut partial = c.clone();
        partial.remove(&70000);
        assert_eq!(
            missing(70001, &partial),
            Some(70000),
            "the element of a known array"
        );
    }

    #[test]
    fn overrides() {
        let c = catalog();
        let col = |oid, typmod, action, over: Option<LogicalType>| {
            plan_column("c", oid, typmod, &c, action, over.as_ref())
        };
        let s = col(2950, -1, S, Some(LogicalType::String)).unwrap();
        assert_eq!(
            (s.value_type, s.logical, s.decode),
            (V::Text, LogicalType::String, Decode::Text)
        );
        let d = col(
            1700,
            -1,
            E,
            Some(LogicalType::Decimal {
                precision: 38,
                scale: 10,
            }),
        )
        .unwrap();
        assert_eq!(
            d.value_type,
            V::Numeric {
                precision: 38,
                scale: 10
            }
        );
        assert_eq!(d.decode, Decode::Numeric);
        let wide = col(
            1700,
            numeric_mod(50, 2),
            E,
            Some(LogicalType::Decimal {
                precision: 38,
                scale: 2,
            }),
        )
        .unwrap();
        assert_eq!(
            wide.value_type,
            V::Numeric {
                precision: 38,
                scale: 2
            },
            "an override rescues an over-wide numeric"
        );
        let domain = col(
            70009,
            -1,
            S,
            Some(LogicalType::Decimal {
                precision: 20,
                scale: 4,
            }),
        )
        .unwrap();
        assert_eq!(domain.decode, Decode::Numeric, "through a domain");
        let money = col(
            70010,
            -1,
            S,
            Some(LogicalType::Decimal {
                precision: 20,
                scale: 2,
            }),
        )
        .unwrap();
        assert_eq!(money.decode, Decode::Money);
        let widened = col(
            1700,
            numeric_mod(10, 2),
            S,
            Some(LogicalType::Decimal {
                precision: 20,
                scale: 4,
            }),
        )
        .unwrap();
        assert_eq!(
            widened.logical,
            LogicalType::Decimal {
                precision: 20,
                scale: 4
            }
        );
        let e = col(2950, -1, S, Some(LogicalType::Int64)).unwrap_err();
        assert_eq!(e.code(), "SCHEMA_MISMATCH");
        assert_eq!(
            col(23, -1, S, Some(LogicalType::Int32)).unwrap().logical,
            LogicalType::Int32
        );
        assert_eq!(
            col(1700, numeric_mod(39, 0), E, None),
            Err(HaltReason::NumericOverflow { column: "c".into() })
        );
        let plain = col(1700, -1, S, None).unwrap();
        assert!(plain.unconstrained_numeric);
        assert_eq!(plain.logical, LogicalType::String);
    }

    #[test]
    fn a_decimal_list_override_rescues_a_numeric_array() {
        let c = catalog();
        let list = |p, s| {
            LogicalType::List(Box::new(LogicalType::Decimal {
                precision: p,
                scale: s,
            }))
        };
        let col = |oid, typmod, action, over: &LogicalType| {
            plan_column("c", oid, typmod, &c, action, Some(over))
        };
        let numeric = V::Numeric {
            precision: 38,
            scale: 10,
        };
        for action in [S, E] {
            for typmod in [-1, numeric_mod(50, 2)] {
                let p = col(1231, typmod, action, &list(38, 10)).unwrap();
                assert_eq!(p.value_type, V::Array(Box::new(numeric.clone())));
                assert_eq!(p.logical, list(38, 10));
                assert_eq!(
                    p.decode,
                    Decode::Array {
                        element: Box::new(Decode::Numeric),
                        element_type: numeric.clone(),
                        delimiter: b',',
                    }
                );
                assert!(!p.unconstrained_numeric);
            }
        }
        let domain = col(70011, -1, S, &list(20, 4)).unwrap();
        assert_eq!(
            domain.value_type,
            V::Array(Box::new(V::Numeric {
                precision: 20,
                scale: 4
            })),
            "an array of a numeric domain"
        );
        let e = col(1007, -1, S, &list(38, 10)).unwrap_err();
        assert_eq!(
            e.code(),
            "SCHEMA_MISMATCH",
            "an int4[] isn't a numeric array"
        );
    }
}
