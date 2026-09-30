use uuid::Uuid;

use crate::Decimal;

/// The type of a column as decoded from the source (Hop 1 output, `Type_Mapping.md`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnValueType {
    Bool,
    Int16,
    Int32,
    Int64,
    UInt64,
    Float32,
    Float64,
    Numeric { precision: u8, scale: i8 },
    Text,
    Bytes,
    Timestamp { with_tz: bool },
    Date,
    Time,
    Uuid,
    Json,
    Array(Box<ColumnValueType>),
}

/// A source-decoded value (`Data-Flow.md` §CdcEvent Structure).
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    /// MySQL/MariaDB `BIGINT UNSIGNED`.
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Numeric(Decimal),
    Text(String),
    Bytes(Vec<u8>),
    /// Microseconds since the Unix epoch. `with_tz = true`: a UTC instant (PG `timestamptz`, MySQL
    /// `TIMESTAMP`). `false`: a wall-clock value without a zone (PG `timestamp`, MySQL `DATETIME`),
    /// stored as if it were UTC.
    Timestamp {
        micros: i64,
        with_tz: bool,
    },
    /// Days since the Unix epoch — exactly Arrow `Date32`, so PG `±infinity` can clamp to `i32::MIN` /
    /// `i32::MAX` (Q-M5-1). chrono's `NaiveDate` is too narrow for that.
    Date(i32),
    /// Signed microseconds of elapsed time (Q-M3-3): PG `time` is `0..=86_400_000_000`
    /// (`24:00:00` included), MySQL/MariaDB `TIME` is an interval of `-838:59:59..=838:59:59`.
    /// [`time_text`] renders it.
    Time(i64),
    Uuid(Uuid),
    Json(serde_json::Value),
    Array {
        element_type: ColumnValueType,
        values: Vec<ColumnValue>,
    },
    /// The source did not send this value: a PostgreSQL unchanged-TOAST column in an UPDATE
    /// (pgoutput `u`, D-IMPL-3). It is not NULL. The collapse fills it from an earlier image of the
    /// same key; a value still unresolved at flush keeps the target's value.
    Unchanged,
}

impl ColumnValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// True when the value can be stored in a column of type `t`. NULL and [`Self::Unchanged`] fit
    /// every type (nullability is checked separately); arrays check the declared element type and
    /// every element.
    pub fn fits(&self, t: &ColumnValueType) -> bool {
        use ColumnValueType as T;
        match (self, t) {
            (Self::Null | Self::Unchanged, _) => true,
            (Self::Bool(_), T::Bool)
            | (Self::Int16(_), T::Int16)
            | (Self::Int32(_), T::Int32)
            | (Self::Int64(_), T::Int64)
            | (Self::UInt64(_), T::UInt64)
            | (Self::Float32(_), T::Float32)
            | (Self::Float64(_), T::Float64)
            | (Self::Text(_), T::Text)
            | (Self::Bytes(_), T::Bytes)
            | (Self::Date(_), T::Date)
            | (Self::Time(_), T::Time)
            | (Self::Uuid(_), T::Uuid)
            | (Self::Json(_), T::Json) => true,
            (Self::Numeric(d), T::Numeric { precision, scale }) => d.fits(*precision, *scale),
            (Self::Timestamp { with_tz, .. }, T::Timestamp { with_tz: column_tz }) => {
                with_tz == column_tz
            }
            (
                Self::Array {
                    element_type,
                    values,
                },
                T::Array(column_element),
            ) => {
                element_type == column_element.as_ref()
                    && values.iter().all(|v| v.fits(column_element))
            }
            _ => false,
        }
    }
}

/// Renders a [`ColumnValue::Time`] as `[-]HH:MM:SS.ffffff`, with at least two hour digits:
/// `24:00:00.000000`, `838:59:59.000000`, `-01:30:00.500000`.
pub fn time_text(micros: i64) -> String {
    let sign = if micros < 0 { "-" } else { "" };
    let m = micros.unsigned_abs();
    let (secs, frac) = (m / 1_000_000, m % 1_000_000);
    format!(
        "{sign}{:02}:{:02}:{:02}.{frac:06}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

/// A row image: `(column name, value)` pairs in source column order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Row(pub Vec<(String, ColumnValue)>);

impl Row {
    pub fn get(&self, column: &str) -> Option<&ColumnValue> {
        self.0
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, v)| v)
    }

    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(|(name, _)| name.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Decimal;

    #[test]
    fn null_fits_every_type() {
        for t in [
            ColumnValueType::Bool,
            ColumnValueType::Json,
            ColumnValueType::Array(Box::new(ColumnValueType::Text)),
        ] {
            assert!(ColumnValue::Null.fits(&t));
        }
        assert!(ColumnValue::Null.is_null());
    }

    #[test]
    fn scalar_values_fit_only_their_own_type() {
        assert!(ColumnValue::Int32(7).fits(&ColumnValueType::Int32));
        assert!(!ColumnValue::Int32(7).fits(&ColumnValueType::Int64));
        assert!(ColumnValue::UInt64(u64::MAX).fits(&ColumnValueType::UInt64));
        assert!(!ColumnValue::Text("x".into()).fits(&ColumnValueType::Bytes));
    }

    #[test]
    fn numeric_respects_declared_precision_and_scale() {
        let v = ColumnValue::Numeric(Decimal::parse("123.45").unwrap());
        assert!(v.fits(&ColumnValueType::Numeric {
            precision: 5,
            scale: 2
        }));
        assert!(!v.fits(&ColumnValueType::Numeric {
            precision: 4,
            scale: 2
        }));
    }

    #[test]
    fn date_and_timestamp_hold_the_full_arrow_range() {
        // Clamped PG edge values (Q-M5-1) must be representable.
        for d in [i32::MIN, 0, i32::MAX] {
            assert!(ColumnValue::Date(d).fits(&ColumnValueType::Date));
        }
        for micros in [i64::MIN, i64::MAX] {
            assert!(
                ColumnValue::Timestamp {
                    micros,
                    with_tz: false
                }
                .fits(&ColumnValueType::Timestamp { with_tz: false })
            );
        }
    }

    #[test]
    fn timestamp_must_match_time_zone_flavour() {
        let tz = ColumnValue::Timestamp {
            micros: 0,
            with_tz: true,
        };
        assert!(tz.fits(&ColumnValueType::Timestamp { with_tz: true }));
        assert!(!tz.fits(&ColumnValueType::Timestamp { with_tz: false }));
    }

    #[test]
    fn array_checks_declared_and_actual_element_types() {
        let ints = ColumnValue::Array {
            element_type: ColumnValueType::Int32,
            values: vec![ColumnValue::Int32(1), ColumnValue::Null],
        };
        assert!(ints.fits(&ColumnValueType::Array(Box::new(ColumnValueType::Int32))));
        assert!(!ints.fits(&ColumnValueType::Array(Box::new(ColumnValueType::Int64))));
        let lying = ColumnValue::Array {
            element_type: ColumnValueType::Int32,
            values: vec![ColumnValue::Text("x".into())],
        };
        assert!(!lying.fits(&ColumnValueType::Array(Box::new(ColumnValueType::Int32))));
    }

    #[test]
    fn time_holds_elapsed_intervals_beyond_a_day() {
        // PG `24:00:00` and MySQL `TIME` extremes (Q-M3-3).
        for micros in [0, 86_400_000_000, 3_020_399_000_000, -3_020_399_000_000] {
            assert!(ColumnValue::Time(micros).fits(&ColumnValueType::Time));
        }
        assert_eq!(time_text(86_400_000_000), "24:00:00.000000");
        assert_eq!(time_text(3_020_399_000_000), "838:59:59.000000");
        assert_eq!(time_text(-5_400_500_000), "-01:30:00.500000");
        assert_eq!(time_text(0), "00:00:00.000000");
        assert_eq!(time_text(i64::MIN), "-2562047788:00:54.775808");
    }

    #[test]
    fn unchanged_fits_every_type_and_is_not_null() {
        for t in [
            ColumnValueType::Text,
            ColumnValueType::Json,
            ColumnValueType::Array(Box::new(ColumnValueType::Int32)),
        ] {
            assert!(ColumnValue::Unchanged.fits(&t));
        }
        assert!(!ColumnValue::Unchanged.is_null());
    }

    #[test]
    fn row_looks_up_by_name_in_column_order() {
        let row = Row(vec![
            ("id".into(), ColumnValue::Int64(1)),
            ("name".into(), ColumnValue::Null),
        ]);
        assert_eq!(row.get("id"), Some(&ColumnValue::Int64(1)));
        assert_eq!(row.get("missing"), None);
        assert_eq!(row.columns().collect::<Vec<_>>(), vec!["id", "name"]);
    }
}
