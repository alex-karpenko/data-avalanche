//! Text form → `ColumnValue` (Hop 1, `Type_Mapping.md` PostgreSQL tables). The text is what
//! PostgreSQL's output functions print under the pinned session settings
//! ([`crate::SESSION_OPTIONS`]): the same for a pgoutput tuple and a snapshot `::text` column.

use data_avalanche_core::{ColumnValue, ColumnValueType, Decimal, DecimalError};

/// How one column's text becomes a value. Planned once per column from its type
/// ([`crate::types`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decode {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    /// `numeric`, and `numeric(P,S)` overridden to a decimal.
    Numeric,
    /// `money`: the C-locale form `-$1,234.56` (`lc_monetary=C`).
    Money,
    /// The text as is: `text`, `varchar`, and every type kept as its PostgreSQL text.
    Text,
    /// `char(n)`: trailing spaces stripped.
    BpChar,
    /// `"char"`: one byte as `Int16`.
    Char,
    /// `bytea` hex form `\x…`.
    Bytea,
    Date,
    Time,
    Timestamp {
        with_tz: bool,
    },
    Uuid,
    Json,
    /// `bit(n)`: the bits packed MSB-first, the last byte zero-padded.
    Bit,
    /// `varbit(n)`: PostgreSQL's binary form, a 4-byte big-endian bit count then the packed bits
    /// (Q-M5-9).
    VarBit,
    /// `hstore` → a JSON object of strings (or `null`).
    Hstore,
    /// PostGIS `geometry`/`geography`: the hex EWKB text as bytes.
    HexBytes,
    /// An array literal of `element`s separated by `delimiter`.
    Array {
        element: Box<Decode>,
        element_type: ColumnValueType,
        delimiter: u8,
    },
    /// Decoded as `from`, then converted to `to` for a `column_type_overrides` entry
    /// ([`data_avalanche_core::conversion`]).
    Convert {
        from: Box<Decode>,
        to: ColumnValueType,
    },
}

/// Why a value can't be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueError {
    /// The value doesn't fit its decimal column (`NUMERIC_OVERFLOW`).
    Overflow,
    /// The value has no representation in the column's type (`SCHEMA_MISMATCH`).
    Unrepresentable(String),
}

fn bad(what: &str, text: &str) -> ValueError {
    ValueError::Unrepresentable(format!("{what} {text:?}"))
}

/// Decodes the text of one value.
pub fn decode(d: &Decode, text: &str) -> Result<ColumnValue, ValueError> {
    use ColumnValue as V;
    Ok(match d {
        Decode::Bool => match text {
            "t" => V::Bool(true),
            "f" => V::Bool(false),
            _ => return Err(bad("not a boolean:", text)),
        },
        Decode::Int16 => V::Int16(text.parse().map_err(|_| bad("not an int2:", text))?),
        Decode::Int32 => V::Int32(text.parse().map_err(|_| bad("not an int4:", text))?),
        Decode::Int64 => V::Int64(text.parse().map_err(|_| bad("not an int8:", text))?),
        Decode::Float32 => V::Float32(text.parse().map_err(|_| bad("not a float4:", text))?),
        Decode::Float64 => V::Float64(text.parse().map_err(|_| bad("not a float8:", text))?),
        Decode::Numeric => V::Numeric(numeric(text)?),
        Decode::Money => V::Numeric(numeric(&money(text))?),
        Decode::Text => V::Text(text.to_owned()),
        Decode::BpChar => V::Text(text.trim_end_matches(' ').to_owned()),
        Decode::Char => V::Int16(char_byte(text)?),
        Decode::Bytea => V::Bytes(bytea(text)?),
        Decode::Date => V::Date(date(text)?),
        Decode::Time => V::Time(time(text)?),
        Decode::Timestamp { with_tz } => V::Timestamp {
            micros: timestamp(text, *with_tz)?,
            with_tz: *with_tz,
        },
        Decode::Uuid => V::Uuid(uuid::Uuid::parse_str(text).map_err(|_| bad("not a uuid:", text))?),
        Decode::Json => V::Json(serde_json::from_str(text).map_err(|e| {
            ValueError::Unrepresentable(format!("JSON the decoder can't read: {e}"))
        })?),
        Decode::Bit => V::Bytes(bits(text)?),
        Decode::VarBit => {
            let n = u32::try_from(text.len()).map_err(|_| bad("bit string too long:", ""))?;
            let mut out = n.to_be_bytes().to_vec();
            out.extend(bits(text)?);
            V::Bytes(out)
        }
        Decode::Hstore => V::Json(hstore(text)?),
        Decode::HexBytes => V::Bytes(hex(text).ok_or_else(|| bad("not hex:", text))?),
        Decode::Array {
            element,
            element_type,
            delimiter,
        } => {
            let values = array(text, *delimiter)?
                .into_iter()
                .map(|e| match e {
                    None => Ok(V::Null),
                    Some(s) => decode(element, &s),
                })
                .collect::<Result<_, _>>()?;
            V::Array {
                element_type: element_type.clone(),
                values,
            }
        }
        Decode::Convert { from, to } => decode(from, text)?.convert(to),
    })
}

/// `numeric` text. `NaN` and `±Infinity` have no decimal representation; more than 38 digits
/// overflow.
fn numeric(text: &str) -> Result<Decimal, ValueError> {
    Decimal::parse(text).map_err(|e| match e {
        DecimalError::TooManyDigits(_) => ValueError::Overflow,
        DecimalError::NotFinite(_) => {
            ValueError::Unrepresentable(format!("{text} can't be stored in a decimal column"))
        }
        _ => bad("not a numeric:", text),
    })
}

/// `-$1,234.56` → `-1234.56`.
fn money(text: &str) -> String {
    text.chars().filter(|c| !matches!(c, '$' | ',')).collect()
}

/// `"char"` prints a byte as itself, or `\ooo` (octal) when it isn't printable ASCII.
fn char_byte(text: &str) -> Result<i16, ValueError> {
    let b = text.as_bytes();
    match b {
        [] => Ok(0),
        [c] => Ok(i16::from(*c)),
        [b'\\', o @ ..] if o.len() == 3 => u8::from_str_radix(&text[1..], 8)
            .map(i16::from)
            .map_err(|_| bad("not a \"char\":", text)),
        _ => Err(bad("not a \"char\":", text)),
    }
}

fn hex(text: &str) -> Option<Vec<u8>> {
    let b = text.as_bytes();
    if !b.len().is_multiple_of(2) {
        return None;
    }
    let nibble = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    b.chunks(2)
        .map(|p| Some(nibble(p[0])? << 4 | nibble(p[1])?))
        .collect()
}

fn bytea(text: &str) -> Result<Vec<u8>, ValueError> {
    text.strip_prefix("\\x")
        .and_then(hex)
        .ok_or_else(|| bad("not a hex bytea:", text))
}

fn bits(text: &str) -> Result<Vec<u8>, ValueError> {
    let mut out = vec![0u8; text.len().div_ceil(8)];
    for (i, c) in text.bytes().enumerate() {
        match c {
            b'0' => {}
            b'1' => out[i / 8] |= 0x80 >> (i % 8),
            _ => return Err(bad("not a bit string:", text)),
        }
    }
    Ok(out)
}

/// Days from 1970-01-01 to the proleptic Gregorian date (astronomical year numbering).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`] (Howard Hinnant's algorithm): `(year, month, day)`,
/// astronomical year numbering.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

/// A date as PostgreSQL's ISO input reads it: `YYYY-MM-DD`, ` BC` before year 1.
fn date_text(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    if y <= 0 {
        format!("{:04}-{m:02}-{d:02} BC", 1 - y)
    } else {
        format!("{y:04}-{m:02}-{d:02}")
    }
}

/// The text PostgreSQL reads back as `v`: the inverse of [`decode`] for the types an identifier
/// can have (no floats, JSON or arrays, Ghost_Row §1). `None` for those, and for NULL.
pub fn text_of(v: &ColumnValue) -> Option<String> {
    use ColumnValue as V;
    Some(match v {
        V::Bool(b) => if *b { "t" } else { "f" }.to_owned(),
        V::Int16(i) => i.to_string(),
        V::Int32(i) => i.to_string(),
        V::Int64(i) => i.to_string(),
        V::UInt64(i) => i.to_string(),
        V::Numeric(d) => d.to_string(),
        V::Text(t) => t.clone(),
        V::Bytes(b) => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            format!("\\x{hex}")
        }
        V::Date(i32::MAX) => "infinity".into(),
        V::Date(i32::MIN) => "-infinity".into(),
        V::Date(d) => date_text(i64::from(*d)),
        V::Time(t) => data_avalanche_core::time_text(*t),
        V::Timestamp {
            micros: i64::MAX, ..
        } => "infinity".into(),
        V::Timestamp {
            micros: i64::MIN, ..
        } => "-infinity".into(),
        V::Timestamp { micros, with_tz } => {
            let days = micros.div_euclid(86_400_000_000);
            let clock = data_avalanche_core::time_text(micros.rem_euclid(86_400_000_000));
            let (date, bc) = match date_text(days).strip_suffix(" BC") {
                Some(d) => (d.to_owned(), " BC"),
                None => (date_text(days), ""),
            };
            let tz = if *with_tz { "+00" } else { "" };
            format!("{date} {clock}{tz}{bc}")
        }
        V::Uuid(u) => u.to_string(),
        V::Null | V::Unchanged | V::Float32(_) | V::Float64(_) | V::Json(_) | V::Array { .. } => {
            return None;
        }
    })
}

fn digits(s: &str, what: &str, text: &str) -> Result<i64, ValueError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad(what, text));
    }
    s.parse().map_err(|_| bad(what, text))
}

/// `YYYY-MM-DD` (the year may be longer than four digits) with an optional ` BC`: days since
/// the epoch, before the range checks of the caller.
fn civil_days(date: &str, bc: bool, text: &str) -> Result<i64, ValueError> {
    let mut parts = date.rsplitn(3, '-');
    let (d, m, y) = (parts.next(), parts.next(), parts.next());
    let (Some(d), Some(m), Some(y)) = (d, m, y) else {
        return Err(bad("not an ISO date:", text));
    };
    let (y, m, d) = (
        digits(y, "not an ISO date:", text)?,
        digits(m, "not an ISO date:", text)?,
        digits(d, "not an ISO date:", text)?,
    );
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad("not an ISO date:", text));
    }
    // 1 BC is year 0, 2 BC is year -1.
    let y = if bc { 1 - y } else { y };
    Ok(days_from_civil(y, m, d))
}

fn split_bc(text: &str) -> (&str, bool) {
    match text.strip_suffix(" BC") {
        Some(rest) => (rest, true),
        None => (text, false),
    }
}

/// `date` → days since 1970-01-01; `±infinity` clamps to `i32` MAX/MIN (Q-M5-1).
pub fn date(text: &str) -> Result<i32, ValueError> {
    match text {
        "infinity" => return Ok(i32::MAX),
        "-infinity" => return Ok(i32::MIN),
        _ => {}
    }
    let (date, bc) = split_bc(text);
    i32::try_from(civil_days(date, bc, text)?).map_err(|_| bad("date out of range:", text))
}

/// `HH:MM:SS[.ffffff]` → microseconds; `24:00:00` is 86 400 000 000.
pub fn time(text: &str) -> Result<i64, ValueError> {
    let err = || bad("not a time:", text);
    let (hms, frac) = text.split_once('.').unwrap_or((text, ""));
    let mut it = hms.split(':');
    let (Some(h), Some(m), Some(s), None) = (it.next(), it.next(), it.next(), it.next()) else {
        return Err(err());
    };
    let (h, m, s) = (
        digits(h, "not a time:", text)?,
        digits(m, "not a time:", text)?,
        digits(s, "not a time:", text)?,
    );
    if frac.len() > 6 || (!frac.is_empty() && !frac.bytes().all(|b| b.is_ascii_digit())) {
        return Err(err());
    }
    let micros = if frac.is_empty() {
        0
    } else {
        frac.parse::<i64>().map_err(|_| err())? * 10_i64.pow(6 - frac.len() as u32)
    };
    let total = ((h * 60 + m) * 60 + s) * 1_000_000 + micros;
    if m > 59 || s > 59 || total > 86_400_000_000 {
        return Err(err());
    }
    Ok(total)
}

/// A UTC offset `+HH`, `+HH:MM` or `+HH:MM:SS` → seconds east of UTC.
fn offset(text: &str, full: &str) -> Result<i64, ValueError> {
    let err = || bad("not a timestamp:", full);
    let (sign, rest) = match text.as_bytes().first() {
        Some(b'+') => (1, &text[1..]),
        Some(b'-') => (-1, &text[1..]),
        _ => return Err(err()),
    };
    let mut secs = 0;
    for (i, part) in rest.split(':').enumerate() {
        if i > 2 || part.len() != 2 {
            return Err(err());
        }
        secs += digits(part, "not a timestamp:", full)? * [3600, 60, 1][i];
    }
    Ok(sign * secs)
}

/// `timestamp` / `timestamptz` → microseconds since the epoch (UTC for `timestamptz`, wall clock
/// for `timestamp`); `±infinity`, and instants beyond `i64` microseconds, clamp to `i64` MAX/MIN.
pub fn timestamp(text: &str, with_tz: bool) -> Result<i64, ValueError> {
    match text {
        "infinity" => return Ok(i64::MAX),
        "-infinity" => return Ok(i64::MIN),
        _ => {}
    }
    let err = || bad("not a timestamp:", text);
    let (body, bc) = split_bc(text);
    let (date_part, mut clock) = body.split_once(' ').ok_or_else(err)?;
    let mut offset_secs = 0;
    if with_tz {
        let at = clock.rfind(['+', '-']).ok_or_else(err)?;
        offset_secs = offset(&clock[at..], text)?;
        clock = &clock[..at];
    }
    let days = civil_days(date_part, bc, text)?;
    let t = time(clock).map_err(|_| err())?;
    // Beyond the i64 microseconds PostgreSQL's range reaches (294247–294276 AD): clamp (Q-M5-1).
    let micros =
        i128::from(days) * 86_400_000_000 + i128::from(t) - i128::from(offset_secs) * 1_000_000;
    Ok(micros.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64)
}

/// The `hstore` text form `"k"=>"v", "k2"=>NULL` → a JSON object.
fn hstore(text: &str) -> Result<serde_json::Value, ValueError> {
    let err = || bad("not an hstore:", text);
    let b = text.as_bytes();
    let mut i = 0;
    let mut map = serde_json::Map::new();
    let quoted = |i: &mut usize| -> Result<String, ValueError> {
        if b.get(*i) != Some(&b'"') {
            return Err(err());
        }
        *i += 1;
        let mut out = Vec::new();
        loop {
            match b.get(*i) {
                None => return Err(err()),
                Some(b'"') => {
                    *i += 1;
                    return String::from_utf8(out).map_err(|_| err());
                }
                Some(b'\\') => {
                    out.push(*b.get(*i + 1).ok_or_else(err)?);
                    *i += 2;
                }
                Some(&c) => {
                    out.push(c);
                    *i += 1;
                }
            }
        }
    };
    while i < b.len() {
        let k = quoted(&mut i)?;
        if !text[i..].starts_with("=>") {
            return Err(err());
        }
        i += 2;
        let v = if text[i..].starts_with("NULL") {
            i += 4;
            serde_json::Value::Null
        } else {
            serde_json::Value::String(quoted(&mut i)?)
        };
        map.insert(k, v);
        if i < b.len() {
            if !text[i..].starts_with(", ") {
                return Err(err());
            }
            i += 2;
        }
    }
    Ok(serde_json::Value::Object(map))
}

/// The elements of a one-dimensional array literal (`{a,"b c",NULL}`, optionally after a
/// `[lo:hi]=` bounds decoration); `None` for NULL.
fn array(text: &str, delimiter: u8) -> Result<Vec<Option<String>>, ValueError> {
    let err = || bad("not an array:", text);
    let body = match text.as_bytes().first() {
        Some(b'[') => &text[text.find('=').ok_or_else(err)? + 1..],
        _ => text,
    };
    let inner = body
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(err)?;
    let b = inner.as_bytes();
    let mut out = Vec::new();
    if b.is_empty() {
        return Ok(out);
    }
    let mut i = 0;
    loop {
        match b.get(i) {
            Some(b'{') => {
                return Err(ValueError::Unrepresentable(
                    "a multi-dimensional array; override the column to string".into(),
                ));
            }
            Some(b'"') => {
                i += 1;
                let mut v = Vec::new();
                loop {
                    match b.get(i) {
                        None => return Err(err()),
                        Some(b'"') => {
                            i += 1;
                            break;
                        }
                        Some(b'\\') => {
                            v.push(*b.get(i + 1).ok_or_else(err)?);
                            i += 2;
                        }
                        Some(&c) => {
                            v.push(c);
                            i += 1;
                        }
                    }
                }
                out.push(Some(String::from_utf8(v).map_err(|_| err())?));
            }
            _ => {
                let end = b[i..]
                    .iter()
                    .position(|&c| c == delimiter)
                    .map_or(b.len(), |p| i + p);
                let v = &inner[i..end];
                if v.is_empty() {
                    return Err(err());
                }
                out.push(if v.eq_ignore_ascii_case("NULL") {
                    None
                } else {
                    Some(v.to_owned())
                });
                i = end;
            }
        }
        match b.get(i) {
            None => return Ok(out),
            Some(&c) if c == delimiter => i += 1,
            Some(_) => return Err(err()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ColumnValue as V;

    fn ok(d: Decode, text: &str) -> ColumnValue {
        decode(&d, text).unwrap_or_else(|e| panic!("{text:?}: {e:?}"))
    }

    fn unrepresentable(d: Decode, text: &str) {
        assert!(
            matches!(decode(&d, text), Err(ValueError::Unrepresentable(_))),
            "{text:?} should be refused"
        );
    }

    #[test]
    fn scalars() {
        assert_eq!(ok(Decode::Bool, "t"), V::Bool(true));
        assert_eq!(ok(Decode::Bool, "f"), V::Bool(false));
        assert_eq!(ok(Decode::Int16, "-32768"), V::Int16(i16::MIN));
        assert_eq!(ok(Decode::Int32, "2147483647"), V::Int32(i32::MAX));
        assert_eq!(
            ok(Decode::Int64, "-9223372036854775808"),
            V::Int64(i64::MIN)
        );
        assert_eq!(ok(Decode::Float32, "1.5"), V::Float32(1.5));
        assert_eq!(
            ok(Decode::Float64, "-Infinity"),
            V::Float64(f64::NEG_INFINITY)
        );
        assert_eq!(ok(Decode::Float64, "Infinity"), V::Float64(f64::INFINITY));
        assert!(matches!(ok(Decode::Float64, "NaN"), V::Float64(x) if x.is_nan()));
        assert_eq!(ok(Decode::Float64, "0.1"), V::Float64(0.1));
        assert_eq!(ok(Decode::Text, " a "), V::Text(" a ".into()));
        assert_eq!(ok(Decode::BpChar, "ab   "), V::Text("ab".into()));
        assert_eq!(ok(Decode::Char, "a"), V::Int16(97));
        assert_eq!(ok(Decode::Char, "\\377"), V::Int16(255));
        assert_eq!(ok(Decode::Char, ""), V::Int16(0));
        assert_eq!(
            ok(Decode::Bytea, "\\xdeadBEEF"),
            V::Bytes(vec![0xde, 0xad, 0xbe, 0xef])
        );
        assert_eq!(ok(Decode::Bytea, "\\x"), V::Bytes(vec![]));
        assert_eq!(
            ok(Decode::Uuid, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
            V::Uuid(uuid::Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap())
        );
        assert_eq!(
            ok(Decode::Json, r#"{"a": [1, 2.5, null]}"#),
            V::Json(serde_json::json!({"a": [1, 2.5, null]}))
        );
        for (d, t) in [
            (Decode::Bool, "true"),
            (Decode::Int16, "32768"),
            (Decode::Bytea, "deadbeef"),
            (Decode::Bytea, "\\xabc"),
            (Decode::Char, "ab"),
            (Decode::Uuid, "x"),
            (Decode::Json, "{"),
        ] {
            unrepresentable(d, t);
        }
    }

    #[test]
    fn json_numbers_keep_their_digits() {
        // jsonb numbers are `numeric`: serde_json's `arbitrary_precision` keeps their digits.
        for n in [
            "12345678901234567890123".to_owned(),
            "0.10000000000000000000001".to_owned(),
            format!("1{}", "0".repeat(400)),
        ] {
            let V::Json(v) = ok(Decode::Json, &format!(r#"{{"a": {n}}}"#)) else {
                panic!("not json");
            };
            assert_eq!(v.to_string(), format!(r#"{{"a":{n}}}"#));
        }
    }

    #[test]
    fn numerics_and_money() {
        assert_eq!(
            ok(Decode::Numeric, "-12.340"),
            V::Numeric(Decimal::new(-12340, 3))
        );
        assert_eq!(
            ok(Decode::Numeric, "12300"),
            V::Numeric(Decimal::new(12300, 0))
        );
        unrepresentable(Decode::Numeric, "NaN");
        unrepresentable(Decode::Numeric, "Infinity");
        unrepresentable(Decode::Numeric, "-Infinity");
        assert_eq!(
            decode(&Decode::Numeric, &"9".repeat(39)),
            Err(ValueError::Overflow)
        );
        assert_eq!(
            ok(Decode::Money, "-$1,234.56"),
            V::Numeric(Decimal::new(-123456, 2))
        );
        assert_eq!(
            ok(Decode::Money, "$92,233,720,368,547,758.07"),
            V::Numeric(Decimal::new(9_223_372_036_854_775_807, 2))
        );
    }

    #[test]
    fn dates() {
        assert_eq!(date("1970-01-01"), Ok(0));
        assert_eq!(date("2000-03-01"), Ok(11_017));
        assert_eq!(date("0001-01-01"), Ok(-719_162));
        assert_eq!(
            date("0001-12-31 BC"),
            Ok(-719_163),
            "1 BC is the year before 1 AD"
        );
        assert_eq!(
            date("4714-11-24 BC"),
            Ok(-2_440_588),
            "PostgreSQL's first date (Julian day 0)"
        );
        assert_eq!(
            date("5874897-12-31"),
            Ok(2_145_042_905),
            "PostgreSQL's last date"
        );
        assert_eq!(date("infinity"), Ok(i32::MAX));
        assert_eq!(date("-infinity"), Ok(i32::MIN));
        for t in ["1970-1-x", "1970-13-01", "1970/01/01", "", "-1970-01-01"] {
            assert!(date(t).is_err(), "{t}");
        }
    }

    #[test]
    fn times() {
        assert_eq!(time("00:00:00"), Ok(0));
        assert_eq!(time("13:45:06.5"), Ok(49_506_500_000));
        assert_eq!(time("23:59:59.999999"), Ok(86_399_999_999));
        assert_eq!(time("24:00:00"), Ok(86_400_000_000));
        for t in [
            "24:00:01",
            "12:60:00",
            "12:00",
            "12:00:00.1234567",
            "1a:00:00",
        ] {
            assert!(time(t).is_err(), "{t}");
        }
    }

    #[test]
    fn timestamps() {
        assert_eq!(timestamp("1970-01-01 00:00:00", false), Ok(0));
        assert_eq!(
            timestamp("2024-02-29 12:34:56.789012", false),
            Ok(1_709_210_096_789_012)
        );
        assert_eq!(
            timestamp("2024-02-29 12:34:56.789012+00", true),
            Ok(1_709_210_096_789_012)
        );
        assert_eq!(timestamp("1970-01-01 05:30:00+05:30", true), Ok(0));
        assert_eq!(timestamp("1969-12-31 23:59:59-00:00:01", true), Ok(0));
        assert_eq!(
            timestamp("0001-01-01 00:00:00 BC", false),
            Ok(-62_167_219_200_000_000)
        );
        assert_eq!(timestamp("infinity", true), Ok(i64::MAX));
        assert_eq!(timestamp("-infinity", false), Ok(i64::MIN));
        assert_eq!(
            timestamp("294276-12-31 23:59:59.999999", false),
            Ok(i64::MAX),
            "PostgreSQL's last instant is past i64 microseconds: it clamps (Q-M5-1)"
        );
        for (t, tz) in [
            ("1970-01-01", false),
            ("1970-01-01 00:00:00", true),
            ("1970-01-01 00:00:00+1", true),
        ] {
            assert!(timestamp(t, tz).is_err(), "{t}");
        }
    }

    #[test]
    fn key_values_render_as_postgresql_input() {
        assert_eq!(text_of(&V::Bool(true)).as_deref(), Some("t"));
        assert_eq!(
            text_of(&V::Numeric(Decimal::new(-1234, 2))).as_deref(),
            Some("-12.34")
        );
        assert_eq!(
            text_of(&V::Bytes(vec![0xde, 0xad])).as_deref(),
            Some("\\xdead")
        );
        assert_eq!(
            text_of(&V::Date(-719_163)).as_deref(),
            Some("0001-12-31 BC")
        );
        assert_eq!(text_of(&V::Date(i32::MAX)).as_deref(), Some("infinity"));
        assert_eq!(
            text_of(&V::Time(86_400_000_000)).as_deref(),
            Some("24:00:00.000000")
        );
        assert_eq!(
            text_of(&V::Timestamp {
                micros: 1_709_210_096_789_012,
                with_tz: true
            })
            .as_deref(),
            Some("2024-02-29 12:34:56.789012+00")
        );
        assert_eq!(
            text_of(&V::Timestamp {
                micros: -62_167_219_200_000_000,
                with_tz: false
            })
            .as_deref(),
            Some("0001-01-01 00:00:00.000000 BC")
        );
        for v in [V::Null, V::Float64(1.0), V::Json(serde_json::json!(1))] {
            assert_eq!(text_of(&v), None);
        }
    }

    #[test]
    fn bit_strings() {
        assert_eq!(ok(Decode::Bit, "101"), V::Bytes(vec![0b1010_0000]));
        assert_eq!(ok(Decode::Bit, "111111111"), V::Bytes(vec![0xff, 0x80]));
        assert_eq!(ok(Decode::Bit, ""), V::Bytes(vec![]));
        assert_eq!(
            ok(Decode::VarBit, "10"),
            V::Bytes(vec![0, 0, 0, 2, 0b1000_0000])
        );
        assert_eq!(ok(Decode::VarBit, ""), V::Bytes(vec![0, 0, 0, 0]));
        unrepresentable(Decode::Bit, "102");
    }

    #[test]
    fn hstores() {
        assert_eq!(
            ok(Decode::Hstore, r#""a"=>"1", "b c"=>NULL, "q\"k"=>"x\\y""#),
            V::Json(serde_json::json!({"a": "1", "b c": null, "q\"k": "x\\y"}))
        );
        assert_eq!(ok(Decode::Hstore, ""), V::Json(serde_json::json!({})));
        unrepresentable(Decode::Hstore, r#""a"=>1"#);
        unrepresentable(Decode::Hstore, r#""a"=>"1","b"=>"2""#);
    }

    #[test]
    fn postgis_hex() {
        assert_eq!(
            ok(Decode::HexBytes, "0101000020E6100000"),
            V::Bytes(vec![1, 1, 0, 0, 0x20, 0xe6, 0x10, 0, 0])
        );
        unrepresentable(Decode::HexBytes, "0G");
    }

    fn arr(element: Decode, element_type: ColumnValueType, delimiter: u8) -> Decode {
        Decode::Array {
            element: Box::new(element),
            element_type,
            delimiter,
        }
    }

    #[test]
    fn arrays() {
        let ints = arr(Decode::Int32, ColumnValueType::Int32, b',');
        assert_eq!(
            ok(ints.clone(), "{1,NULL,-3}"),
            V::Array {
                element_type: ColumnValueType::Int32,
                values: vec![V::Int32(1), V::Null, V::Int32(-3)]
            }
        );
        assert_eq!(
            ok(ints.clone(), "[0:1]={7,8}"),
            V::Array {
                element_type: ColumnValueType::Int32,
                values: vec![V::Int32(7), V::Int32(8)]
            }
        );
        assert_eq!(
            ok(ints.clone(), "{}"),
            V::Array {
                element_type: ColumnValueType::Int32,
                values: vec![]
            }
        );
        let texts = arr(Decode::Text, ColumnValueType::Text, b',');
        assert_eq!(
            ok(
                texts.clone(),
                r#"{plain,"with, comma","q\"uote","back\\slash","NULL",""}"#
            ),
            V::Array {
                element_type: ColumnValueType::Text,
                values: ["plain", "with, comma", "q\"uote", "back\\slash", "NULL", ""]
                    .map(|s| V::Text(s.into()))
                    .to_vec()
            }
        );
        let boxes = arr(Decode::Text, ColumnValueType::Text, b';');
        assert_eq!(
            ok(boxes, "{(1,1),(0,0);(2,2),(1,1)}"),
            V::Array {
                element_type: ColumnValueType::Text,
                values: vec![V::Text("(1,1),(0,0)".into()), V::Text("(2,2),(1,1)".into())]
            }
        );
        assert_eq!(
            decode(&ints, "{{1,2},{3,4}}"),
            Err(ValueError::Unrepresentable(
                "a multi-dimensional array; override the column to string".into()
            ))
        );
        for t in ["1,2", "{1,,2}", "{\"a}", "{1,2"] {
            unrepresentable(texts.clone(), t);
        }
        let dates = arr(Decode::Date, ColumnValueType::Date, b',');
        assert_eq!(
            ok(dates, "{1970-01-02,infinity}"),
            V::Array {
                element_type: ColumnValueType::Date,
                values: vec![V::Date(1), V::Date(i32::MAX)]
            }
        );
    }

    proptest::proptest! {
        /// Every date PostgreSQL prints round-trips through the civil-day arithmetic.
        #[test]
        fn dates_round_trip(days in -2_440_588i32..=2_145_042_905) {
            let text = text_of(&V::Date(days)).unwrap();
            proptest::prop_assert_eq!(date(&text), Ok(days));
        }

        /// Timestamps round-trip through their text, both kinds, BC included.
        #[test]
        fn timestamps_round_trip(micros in -210_866_803_200_000_000i64..=9_223_371_331_199_999_999, tz: bool) {
            let text = text_of(&V::Timestamp { micros, with_tz: tz }).unwrap();
            proptest::prop_assert_eq!(timestamp(&text, tz), Ok(micros), "{}", text);
        }

        /// Arbitrary array text never panics.
        #[test]
        fn array_text_never_panics(text in "\\PC{0,24}") {
            let _ = array(&text, b',');
            let _ = hstore(&text);
        }
    }
}
