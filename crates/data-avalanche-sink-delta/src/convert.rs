//! Hop 3b for values: a Hop 2 column → the Arrow array of its Delta column, and back.
//!
//! The conversion is chosen by the pair (logical type, the table's Delta type), so a table keeps the
//! representation it was created with (`timestamp_ntz_as` applies to new columns only):
//! - `uuid` → `string`: canonical lowercase text;
//! - `time` → `string`: [`time_text`], which keeps intervals beyond a day;
//! - `timestamp_ntz` → `timestamp`: the same microseconds as a UTC instant;
//! - `timestamp_ntz` → `string`: `YYYY-MM-DD HH:MM:SS.ffffff`;
//! - `date`, `timestamp`, `timestamp_ntz` cells are clamped to the Delta protocol's range,
//!   0001-01-01 … 9999-12-31 23:59:59.999999 (Q-M4-2): delta-rs can't write dates much beyond it
//!   and readers can't read them. PostgreSQL `±infinity` lands on the bounds;
//! - lists convert element-wise; every other pair is the same Arrow type on both sides.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Time64MicrosecondType, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, ListArray, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_schema::Field;
use data_avalanche_core::{LIST_ELEMENT, LogicalType, arrow_type, time_text};
use deltalake::kernel::{DataType as DeltaType, PrimitiveType};

use crate::types::physical_type;

/// 0001-01-01, the first day of the Delta protocol's `date` range.
pub const DELTA_MIN_DATE: i32 = -719_162;
/// 9999-12-31.
pub const DELTA_MAX_DATE: i32 = 2_932_896;
/// 0001-01-01 00:00:00.000000 in microseconds since the Unix epoch.
pub const DELTA_MIN_MICROS: i64 = -62_135_596_800_000_000;
/// 9999-12-31 23:59:59.999999.
pub const DELTA_MAX_MICROS: i64 = 253_402_300_799_999_999;

fn is_string(d: &DeltaType) -> bool {
    matches!(d, DeltaType::Primitive(PrimitiveType::String))
}

fn mismatch(logical: &LogicalType, delta: &DeltaType) -> String {
    format!("a {logical} column can't be stored as Delta {delta}")
}

/// Converts a Hop 2 column of `logical` type to the Arrow array delta-rs writes into a column of
/// type `delta`.
pub fn to_delta(
    array: &ArrayRef,
    logical: &LogicalType,
    delta: &DeltaType,
) -> Result<ArrayRef, String> {
    use LogicalType as L;
    use PrimitiveType as P;
    let DeltaType::Primitive(p) = delta else {
        let (L::List(element), DeltaType::Array(a)) = (logical, delta) else {
            return Err(mismatch(logical, delta));
        };
        let list = array.as_list::<i32>();
        let values = to_delta(list.values(), element, a.element_type())?;
        let field = Arc::new(Field::new(LIST_ELEMENT, values.data_type().clone(), true));
        return Ok(Arc::new(ListArray::new(
            field,
            list.offsets().clone(),
            values,
            list.nulls().cloned(),
        )));
    };
    Ok(match (logical, p) {
        (L::Uuid, P::String) => {
            let a = array.as_fixed_size_binary();
            if a.value_length() != 16 {
                return Err(format!(
                    "a uuid column holds {}-byte values, not 16",
                    a.value_length()
                ));
            }
            Arc::new(
                (0..a.len())
                    .map(|i| {
                        a.is_valid(i).then(|| {
                            uuid::Uuid::from_slice(a.value(i))
                                .expect("16 bytes")
                                .hyphenated()
                                .to_string()
                        })
                    })
                    .collect::<StringArray>(),
            )
        }
        (L::Time, P::String) => Arc::new(
            array
                .as_primitive::<Time64MicrosecondType>()
                .iter()
                .map(|v| v.map(time_text))
                .collect::<StringArray>(),
        ),
        (L::Date, P::Date) => Arc::new(
            array
                .as_primitive::<Date32Type>()
                .unary::<_, Date32Type>(|d| d.clamp(DELTA_MIN_DATE, DELTA_MAX_DATE)),
        ),
        (L::TimestampTz | L::TimestampNtz, P::Timestamp | P::TimestampNtz) => {
            if matches!((logical, p), (L::TimestampTz, P::TimestampNtz)) {
                return Err(mismatch(logical, delta));
            }
            let clamped: TimestampMicrosecondArray = array
                .as_primitive::<TimestampMicrosecondType>()
                .unary(|t| t.clamp(DELTA_MIN_MICROS, DELTA_MAX_MICROS));
            match p {
                P::Timestamp => Arc::new(clamped.with_timezone("UTC")),
                _ => Arc::new(clamped),
            }
        }
        (L::TimestampNtz, P::String) => Arc::new(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .map(|v| v.map(ntz_text))
                .collect::<StringArray>(),
        ),
        _ => {
            let want = physical_type(delta)?;
            if array.data_type() != &want || arrow_type(logical) != want {
                return Err(mismatch(logical, delta));
            }
            array.clone()
        }
    })
}

/// Converts an array read from a Delta column of type `delta` (in whatever Arrow form the reader
/// produced: views, dictionaries) back to the Hop 2 array of `logical` type.
pub fn from_delta(
    array: &ArrayRef,
    logical: &LogicalType,
    delta: &DeltaType,
) -> Result<ArrayRef, String> {
    use LogicalType as L;
    let physical = physical_type(delta)?;
    let array = arrow_cast::cast(array, &physical).map_err(|e| e.to_string())?;
    if let (L::List(element), DeltaType::Array(a)) = (logical, delta) {
        let list = array.as_list::<i32>();
        let values = from_delta(list.values(), element, a.element_type())?;
        let field = Arc::new(Field::new(LIST_ELEMENT, values.data_type().clone(), true));
        return Ok(Arc::new(ListArray::new(
            field,
            list.offsets().clone(),
            values,
            list.nulls().cloned(),
        )));
    }
    Ok(match (logical, is_string(delta)) {
        (L::Uuid, true) => {
            let s = array.as_string::<i32>();
            let bytes = (0..s.len())
                .map(|i| {
                    s.is_valid(i)
                        .then(|| uuid::Uuid::parse_str(s.value(i)).map(|u| u.into_bytes()))
                        .transpose()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("a uuid cell: {e}"))?;
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(bytes.into_iter(), 16)
                    .map_err(|e| e.to_string())?,
            )
        }
        (L::Time, true) => {
            let s = array.as_string::<i32>();
            Arc::new(
                s.iter()
                    .map(|v| v.map(parse_time_text).transpose())
                    .collect::<Result<Time64MicrosecondArray, _>>()?,
            )
        }
        (L::TimestampNtz, true) => {
            let s = array.as_string::<i32>();
            Arc::new(
                s.iter()
                    .map(|v| v.map(parse_ntz_text).transpose())
                    .collect::<Result<TimestampMicrosecondArray, _>>()?,
            )
        }
        _ => arrow_cast::cast(&array, &arrow_type(logical)).map_err(|e| e.to_string())?,
    })
}

/// Days since the Unix epoch → (year, month, day), proleptic Gregorian, for every `i64` (Howard
/// Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// The inverse of [`civil_from_days`].
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = i64::from((m + 9) % 12);
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `YYYY-MM-DD HH:MM:SS.ffffff` for a wall-clock timestamp in microseconds. Years outside
/// 0…9999 print their digits with a sign for negative years.
pub fn ntz_text(micros: i64) -> String {
    let (days, rem) = (
        micros.div_euclid(86_400_000_000),
        micros.rem_euclid(86_400_000_000),
    );
    let (y, m, d) = civil_from_days(days);
    let (secs, frac) = (rem / 1_000_000, rem % 1_000_000);
    let year = if y < 0 {
        format!("-{:04}", -y)
    } else {
        format!("{y:04}")
    };
    format!(
        "{year}-{m:02}-{d:02} {:02}:{:02}:{:02}.{frac:06}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

fn bad(text: &str, what: &str) -> String {
    format!("{text:?} isn't a {what}")
}

/// The largest year `parse_ntz_text` accepts (either sign).
const MAX_TEXT_YEAR: i64 = 300_000;

/// The inverse of [`ntz_text`].
pub fn parse_ntz_text(text: &str) -> Result<i64, String> {
    let err = || bad(text, "timestamp text");
    let (date, time) = text.split_once(' ').ok_or_else(err)?;
    let (negative, date) = match date.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, date),
    };
    let mut parts = date.splitn(3, '-');
    let mut next = || parts.next().ok_or_else(err);
    let (y, m, d) = (next()?, next()?, next()?);
    let y: i64 = y.parse().map_err(|_| err())?;
    let m: u32 = m.parse().map_err(|_| err())?;
    let d: u32 = d.parse().map_err(|_| err())?;
    // `i64` microseconds span years -290308 … 294247; the bound keeps the arithmetic in range.
    if !(0..=MAX_TEXT_YEAR).contains(&y) || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(err());
    }
    let y = if negative { -y } else { y };
    let days = days_from_civil(y, m, d);
    // A day the month doesn't have (`2024-02-31`) comes back as another date.
    if civil_from_days(days) != (y, m, d) {
        return Err(err());
    }
    let days = i128::from(days);
    let micros = days * 86_400_000_000 + i128::from(parse_time_text(time)?);
    i64::try_from(micros).map_err(|_| err())
}

/// The inverse of [`time_text`]: `[-]H…H:MM:SS.ffffff` → signed microseconds.
pub fn parse_time_text(text: &str) -> Result<i64, String> {
    let err = || bad(text, "time text");
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (hms, frac) = body.split_once('.').ok_or_else(err)?;
    let mut parts = hms.splitn(3, ':');
    let mut next =
        || -> Result<i128, String> { parts.next().ok_or_else(err)?.parse().map_err(|_| err()) };
    let (h, m, s) = (next()?, next()?, next()?);
    if frac.len() != 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(err());
    }
    if h < 0 || !(0..=59).contains(&m) || !(0..=59).contains(&s) {
        return Err(err());
    }
    let frac: i128 = frac.parse().map_err(|_| err())?;
    let magnitude = h
        .checked_mul(60)
        .and_then(|v| v.checked_add(m))
        .and_then(|v| v.checked_mul(60))
        .and_then(|v| v.checked_add(s))
        .and_then(|v| v.checked_mul(1_000_000))
        .and_then(|v| v.checked_add(frac))
        .ok_or_else(err)?;
    let micros = if negative { -magnitude } else { magnitude };
    i64::try_from(micros).map_err(|_| err())
}

#[cfg(test)]
mod tests {
    use arrow_array::builder::{FixedSizeBinaryBuilder, ListBuilder, StringBuilder};
    use arrow_array::{Date32Array, FixedSizeBinaryArray, Int64Array, StringViewArray};
    use arrow_schema::DataType;
    use deltalake::kernel::ArrayType;
    use proptest::prelude::*;

    use super::*;

    fn round_trip(array: ArrayRef, logical: LogicalType, delta: DeltaType) -> ArrayRef {
        let stored = to_delta(&array, &logical, &delta).unwrap();
        assert_eq!(stored.data_type(), &physical_type(&delta).unwrap());
        from_delta(&stored, &logical, &delta).unwrap()
    }

    #[test]
    fn uuid_is_canonical_lowercase_text() {
        let mut b = FixedSizeBinaryBuilder::new(16);
        b.append_value([0xAB; 16]).unwrap();
        b.append_null();
        let a: ArrayRef = Arc::new(b.finish());
        let stored = to_delta(&a, &LogicalType::Uuid, &DeltaType::STRING).unwrap();
        let s = stored.as_string::<i32>();
        assert_eq!(s.value(0), "abababab-abab-abab-abab-abababababab");
        assert!(s.is_null(1));
        assert_eq!(
            round_trip(a.clone(), LogicalType::Uuid, DeltaType::STRING).as_ref(),
            a.as_ref()
        );
    }

    #[test]
    fn time_keeps_intervals_beyond_a_day() {
        let a: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![
            Some(86_400_000_000),
            Some(-5_400_500_000),
            Some(i64::MIN),
            None,
        ]));
        let stored = to_delta(&a, &LogicalType::Time, &DeltaType::STRING).unwrap();
        let s = stored.as_string::<i32>();
        assert_eq!(s.value(0), "24:00:00.000000");
        assert_eq!(s.value(1), "-01:30:00.500000");
        assert_eq!(
            round_trip(a.clone(), LogicalType::Time, DeltaType::STRING).as_ref(),
            a.as_ref()
        );
    }

    #[test]
    fn dates_and_timestamps_clamp_to_the_delta_range() {
        let d: ArrayRef = Arc::new(Date32Array::from(vec![
            Some(i32::MIN),
            Some(0),
            Some(i32::MAX),
            None,
        ]));
        let stored = to_delta(&d, &LogicalType::Date, &DeltaType::DATE).unwrap();
        assert_eq!(
            stored
                .as_primitive::<Date32Type>()
                .iter()
                .collect::<Vec<_>>(),
            [Some(DELTA_MIN_DATE), Some(0), Some(DELTA_MAX_DATE), None]
        );
        let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            i64::MIN,
            -1,
            i64::MAX,
        ]));
        for (logical, delta, tz) in [
            (LogicalType::TimestampNtz, DeltaType::TIMESTAMP_NTZ, None),
            (LogicalType::TimestampNtz, DeltaType::TIMESTAMP, Some("UTC")),
        ] {
            let stored = to_delta(&ts, &logical, &delta).unwrap();
            let p = stored.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(
                p.values().as_ref(),
                [DELTA_MIN_MICROS, -1, DELTA_MAX_MICROS]
            );
            assert_eq!(p.timezone(), tz);
        }
        let tz: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX]).with_timezone("UTC"));
        let stored = to_delta(&tz, &LogicalType::TimestampTz, &DeltaType::TIMESTAMP).unwrap();
        assert_eq!(
            stored.as_primitive::<TimestampMicrosecondType>().value(0),
            DELTA_MAX_MICROS
        );
        assert!(to_delta(&tz, &LogicalType::TimestampTz, &DeltaType::TIMESTAMP_NTZ).is_err());
    }

    #[test]
    fn ntz_as_string_renders_and_parses_the_whole_range() {
        assert_eq!(ntz_text(0), "1970-01-01 00:00:00.000000");
        assert_eq!(ntz_text(-1), "1969-12-31 23:59:59.999999");
        assert_eq!(ntz_text(DELTA_MIN_MICROS), "0001-01-01 00:00:00.000000");
        assert_eq!(ntz_text(DELTA_MAX_MICROS), "9999-12-31 23:59:59.999999");
        assert_eq!(ntz_text(i64::MIN), "-290308-12-21 19:59:05.224192");
        assert_eq!(ntz_text(i64::MAX), "294247-01-10 04:00:54.775807");
        for v in [0, -1, i64::MIN, i64::MAX, DELTA_MIN_MICROS - 1] {
            assert_eq!(parse_ntz_text(&ntz_text(v)).unwrap(), v, "{v}");
        }
        assert!(parse_ntz_text("2024-01-01").is_err());
    }

    #[test]
    fn lists_convert_element_wise() {
        let mut b = ListBuilder::new(FixedSizeBinaryBuilder::new(16)).with_field(Arc::new(
            Field::new(LIST_ELEMENT, DataType::FixedSizeBinary(16), true),
        ));
        b.values().append_value([1; 16]).unwrap();
        b.values().append_null();
        b.append(true);
        b.append_null();
        let a: ArrayRef = Arc::new(b.finish());
        let logical = LogicalType::List(Box::new(LogicalType::Uuid));
        let delta: DeltaType = ArrayType::new(DeltaType::STRING, true).into();
        let stored = to_delta(&a, &logical, &delta).unwrap();
        assert_eq!(stored.data_type(), &physical_type(&delta).unwrap());
        assert_eq!(round_trip(a.clone(), logical, delta).as_ref(), a.as_ref());
    }

    #[test]
    fn reading_accepts_view_and_dictionary_arrays() {
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![Some("01:00:00.000000"), None]));
        let back = from_delta(&view, &LogicalType::Time, &DeltaType::STRING).unwrap();
        assert_eq!(
            back.as_primitive::<Time64MicrosecondType>().value(0),
            3_600_000_000
        );
        let dict: ArrayRef = Arc::new(
            vec![Some("p1"), Some("p1"), None]
                .into_iter()
                .collect::<arrow_array::DictionaryArray<arrow_array::types::UInt16Type>>(),
        );
        let back = from_delta(&dict, &LogicalType::String, &DeltaType::STRING).unwrap();
        assert_eq!(
            back.as_ref(),
            &StringArray::from(vec![Some("p1"), Some("p1"), None])
        );
    }

    #[test]
    fn a_type_the_column_cant_hold_is_an_error() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let err = to_delta(&a, &LogicalType::Int64, &DeltaType::STRING).unwrap_err();
        assert!(err.contains("can't be stored as Delta"), "{err}");
        let s: ArrayRef = Arc::new(StringArray::from(vec!["not-a-uuid"]));
        assert!(from_delta(&s, &LogicalType::Uuid, &DeltaType::STRING).is_err());
        let mut b = ListBuilder::new(StringBuilder::new());
        b.append(true);
        let l: ArrayRef = Arc::new(b.finish());
        assert!(
            to_delta(
                &l,
                &LogicalType::List(Box::new(LogicalType::String)),
                &DeltaType::STRING
            )
            .is_err()
        );
    }

    proptest! {
        #[test]
        fn time_and_ntz_text_round_trip(v in any::<i64>()) {
            prop_assert_eq!(parse_time_text(&time_text(v)).unwrap(), v);
            prop_assert_eq!(parse_ntz_text(&ntz_text(v)).unwrap(), v);
        }
    }

    #[test]
    fn malformed_text_is_an_error_not_a_panic() {
        for bad in [
            "9000000000000000000-01-01 00:00:00.000000",
            "-9223372036854775808-01-01 00:00:00.000000",
            "300001-01-01 00:00:00.000000",
            "2024-13-01 00:00:00.000000",
            "2024-00-10 00:00:00.000000",
            "2024-4294967295-01 00:00:00.000000",
            "2023-02-29 00:00:00.000000",
            "2024-04-31 00:00:00.000000",
            "2024-01-01 99999999999999999999999999999999:00:00.000000",
        ] {
            assert!(parse_ntz_text(bad).is_err(), "{bad}");
        }
        assert!(parse_ntz_text("2024-02-29 00:00:00.000000").is_ok());
        for bad in [
            "99999999999999999999999999999999:00:00.000000",
            "--1:00:00.000000",
            "00:-1:00.000000",
            "00:00:60.000000",
            "00:00:00.-00001",
        ] {
            assert!(parse_time_text(bad).is_err(), "{bad}");
        }
        let wide = FixedSizeBinaryArray::try_from_iter([[0u8; 20]].into_iter()).unwrap();
        let err = to_delta(
            &(Arc::new(wide) as ArrayRef),
            &LogicalType::Uuid,
            &DeltaType::Primitive(PrimitiveType::String),
        )
        .unwrap_err();
        assert!(err.contains("20-byte"), "{err}");
    }
}
