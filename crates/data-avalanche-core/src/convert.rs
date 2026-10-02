//! The conversions a `tables.column_type_overrides` entry may ask for (config §9.3), applied by
//! the source at Hop 1: Hop 2 converts nothing (D-M3.6). Only conversions that keep every value of
//! the source type are offered; anything else is `string`, a decimal override or a halt.

use crate::{ColumnValue, ColumnValueType, Decimal, LogicalType};

/// Microseconds in a day.
const DAY_MICROS: i64 = 86_400_000_000;

/// The value type a column of `from` is converted to for an override of `to`, or `None` when no
/// conversion applies (the types already match, or the conversion could lose a value):
/// - `int16` → `int32`, `int64`, `float64`; `int32` → `int64`, `float64`;
/// - an integer → `decimal(P,S)` with room for its every value: `P - S` at least 5 (`int16`), 10
///   (`int32`) or 19 (`int64`);
/// - `float32` → `float64`;
/// - `timestamp_ntz` ↔ `timestamp_tz`: the wall clock read as UTC, as `timestamp_ntz_as:
///   timestamp` does;
/// - `date` → `timestamp_ntz` or `timestamp_tz`: midnight;
/// - `list<T>` → `list<U>`: element by element.
pub fn conversion(from: &ColumnValueType, to: &LogicalType) -> Option<ColumnValueType> {
    use ColumnValueType as V;
    use LogicalType as L;
    let digits = |t: &V| match t {
        V::Int16 => Some(5),
        V::Int32 => Some(10),
        V::Int64 => Some(19),
        _ => None,
    };
    Some(match (from, to) {
        (V::Int16, L::Int32) => V::Int32,
        (V::Int16 | V::Int32, L::Int64) => V::Int64,
        (V::Int16 | V::Int32 | V::Float32, L::Float64) => V::Float64,
        (t, L::Decimal { precision, scale })
            if digits(t).is_some_and(|d| i16::from(*precision) - i16::from(*scale) >= d) =>
        {
            V::Numeric {
                precision: *precision,
                scale: *scale,
            }
        }
        (V::Timestamp { with_tz: false }, L::TimestampTz) => V::Timestamp { with_tz: true },
        (V::Timestamp { with_tz: true }, L::TimestampNtz) => V::Timestamp { with_tz: false },
        (V::Date, L::TimestampTz) => V::Timestamp { with_tz: true },
        (V::Date, L::TimestampNtz) => V::Timestamp { with_tz: false },
        (V::Array(e), L::List(l)) => V::Array(Box::new(conversion(e, l)?)),
        _ => return None,
    })
}

impl ColumnValue {
    /// The value as type `to`, which [`conversion`] returned for the value's own type. NULL and
    /// [`ColumnValue::Unchanged`] stay as they are. A `float32` becomes the `float64` of its
    /// shortest decimal form (`0.1` stays `0.1`, as PostgreSQL prints it). A date beyond the
    /// `i64` microsecond range clamps to `i64` MAX/MIN, as a timestamp there does (Q-M5-1).
    pub fn convert(self, to: &ColumnValueType) -> ColumnValue {
        use ColumnValue as C;
        use ColumnValueType as V;
        match (self, to) {
            (C::Int16(i), V::Int32) => C::Int32(i.into()),
            (C::Int16(i), V::Int64) => C::Int64(i.into()),
            (C::Int32(i), V::Int64) => C::Int64(i.into()),
            (C::Int16(i), V::Float64) => C::Float64(i.into()),
            (C::Int32(i), V::Float64) => C::Float64(i.into()),
            (C::Float32(f), V::Float64) => C::Float64(
                f.to_string()
                    .parse()
                    .expect("an f32's text is an f64's text"),
            ),
            (C::Int16(i), V::Numeric { .. }) => C::Numeric(Decimal::new(i.into(), 0)),
            (C::Int32(i), V::Numeric { .. }) => C::Numeric(Decimal::new(i.into(), 0)),
            (C::Int64(i), V::Numeric { .. }) => C::Numeric(Decimal::new(i.into(), 0)),
            (C::Timestamp { micros, .. }, V::Timestamp { with_tz }) => C::Timestamp {
                micros,
                with_tz: *with_tz,
            },
            (C::Date(d), V::Timestamp { with_tz }) => C::Timestamp {
                micros: match d {
                    i32::MAX => i64::MAX,
                    i32::MIN => i64::MIN,
                    d => i64::from(d).checked_mul(DAY_MICROS).unwrap_or(if d < 0 {
                        i64::MIN
                    } else {
                        i64::MAX
                    }),
                },
                with_tz: *with_tz,
            },
            (C::Array { values, .. }, V::Array(e)) => C::Array {
                element_type: e.as_ref().clone(),
                values: values.into_iter().map(|v| v.convert(e)).collect(),
            },
            (v, _) => v,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ColumnValue as C;
    use ColumnValueType as V;
    use LogicalType as L;

    fn dec(precision: u8, scale: i8) -> L {
        L::Decimal { precision, scale }
    }

    #[test]
    fn integers_widen() {
        assert_eq!(conversion(&V::Int16, &L::Int32), Some(V::Int32));
        assert_eq!(conversion(&V::Int16, &L::Int64), Some(V::Int64));
        assert_eq!(conversion(&V::Int32, &L::Int64), Some(V::Int64));
        assert_eq!(conversion(&V::Int32, &L::Float64), Some(V::Float64));
        assert_eq!(conversion(&V::Float32, &L::Float64), Some(V::Float64));
        assert_eq!(C::Int16(i16::MIN).convert(&V::Int64), C::Int64(-32_768));
        assert_eq!(
            C::Int32(i32::MAX).convert(&V::Int64),
            C::Int64(2_147_483_647)
        );
        assert_eq!(C::Int32(-7).convert(&V::Float64), C::Float64(-7.0));
        assert_eq!(C::Float32(0.1).convert(&V::Float64), C::Float64(0.1));
        assert_eq!(
            C::Float32(f32::NEG_INFINITY).convert(&V::Float64),
            C::Float64(f64::NEG_INFINITY)
        );
        assert!(matches!(
            C::Float32(f32::NAN).convert(&V::Float64),
            C::Float64(f) if f.is_nan()
        ));
    }

    #[test]
    fn integers_to_decimals_with_room() {
        assert_eq!(
            conversion(&V::Int32, &dec(12, 2)),
            Some(V::Numeric {
                precision: 12,
                scale: 2
            })
        );
        assert_eq!(
            conversion(&V::Int32, &dec(11, 2)),
            None,
            "10 digits need P - S ≥ 10"
        );
        assert!(conversion(&V::Int16, &dec(5, 0)).is_some());
        assert!(conversion(&V::Int64, &dec(19, 0)).is_some());
        assert_eq!(conversion(&V::Int64, &dec(38, 20)), None);
        let v = C::Int64(i64::MIN).convert(&V::Numeric {
            precision: 19,
            scale: 0,
        });
        assert_eq!(v, C::Numeric(Decimal::new(i64::MIN.into(), 0)));
        assert!(v.fits(&V::Numeric {
            precision: 19,
            scale: 0
        }));
        assert!(
            C::Int32(5)
                .convert(&V::Numeric {
                    precision: 12,
                    scale: 2
                })
                .fits(&V::Numeric {
                    precision: 12,
                    scale: 2
                })
        );
    }

    #[test]
    fn timestamps_and_dates() {
        let (ntz, tz) = (
            V::Timestamp { with_tz: false },
            V::Timestamp { with_tz: true },
        );
        assert_eq!(conversion(&ntz, &L::TimestampTz), Some(tz.clone()));
        assert_eq!(conversion(&tz, &L::TimestampNtz), Some(ntz.clone()));
        assert_eq!(conversion(&V::Date, &L::TimestampTz), Some(tz.clone()));
        assert_eq!(conversion(&V::Date, &L::TimestampNtz), Some(ntz.clone()));
        let at = C::Timestamp {
            micros: 1_700_000_000_000_000,
            with_tz: false,
        };
        assert_eq!(
            at.convert(&tz),
            C::Timestamp {
                micros: 1_700_000_000_000_000,
                with_tz: true
            },
            "the wall clock read as UTC"
        );
        assert_eq!(
            C::Date(19_723).convert(&ntz),
            C::Timestamp {
                micros: 19_723 * DAY_MICROS,
                with_tz: false
            }
        );
        assert_eq!(
            C::Date(-1).convert(&tz),
            C::Timestamp {
                micros: -DAY_MICROS,
                with_tz: true
            }
        );
        for (d, micros) in [
            (i32::MAX, i64::MAX),
            (i32::MIN, i64::MIN),
            (2_000_000_000, i64::MAX),
            (-2_000_000_000, i64::MIN),
        ] {
            assert_eq!(
                C::Date(d).convert(&tz),
                C::Timestamp {
                    micros,
                    with_tz: true
                },
                "±infinity and dates beyond the range clamp"
            );
        }
    }

    #[test]
    fn lists_convert_their_elements() {
        let to = conversion(&V::Array(Box::new(V::Int32)), &L::List(Box::new(L::Int64)));
        assert_eq!(to, Some(V::Array(Box::new(V::Int64))));
        let v = C::Array {
            element_type: V::Int32,
            values: vec![C::Int32(1), C::Null],
        };
        assert_eq!(
            v.convert(to.as_ref().unwrap()),
            C::Array {
                element_type: V::Int64,
                values: vec![C::Int64(1), C::Null]
            }
        );
        assert_eq!(
            conversion(&V::Array(Box::new(V::Int64)), &L::List(Box::new(L::Int32))),
            None
        );
    }

    #[test]
    fn nothing_else_converts() {
        for (from, to) in [
            (V::Int64, L::Int32),
            (V::Int32, L::Int16),
            (V::Int64, L::Float64),
            (V::Int32, L::Float32),
            (V::Float64, L::Float32),
            (V::Float64, dec(38, 10)),
            (
                V::Numeric {
                    precision: 10,
                    scale: 2,
                },
                L::Float64,
            ),
            (V::Timestamp { with_tz: false }, L::Date),
            (V::Timestamp { with_tz: false }, L::TimestampNtz),
            (V::Text, L::Uuid),
            (V::Text, L::Json),
            (V::Bool, L::Int32),
            (V::UInt64, L::Float64),
            (V::Int32, L::List(Box::new(L::Int32))),
        ] {
            assert_eq!(conversion(&from, &to), None, "{from:?} → {to}");
        }
        assert_eq!(C::Null.convert(&V::Int64), C::Null);
        assert_eq!(C::Unchanged.convert(&V::Int64), C::Unchanged);
    }
}
