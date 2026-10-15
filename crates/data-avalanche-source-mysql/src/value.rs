//! Values → `ColumnValue` (Hop 1, `Type_Mapping.md` MySQL and MariaDB tables). A value comes from
//! the binlog (`mysql_common`'s row decoding) or from a snapshot chunk (a prepared statement, the
//! binary protocol). The two differ per type (S-MY type probe, 2026-10-14): `mysql_common` drops
//! a signed `MEDIUMINT`'s sign and reads `YEAR 0000` as `1900`; the binlog carries a `TIMESTAMP` as
//! epoch text, an `ENUM` as its index, a `SET` as its bitmap, latin1 as its own bytes, and strips
//! the trailing zero bytes of `BINARY(n)` and MariaDB's `UUID`/`INET4`/`INET6`. Both inputs become
//! the same [`Raw`] value first, so a row reads the same from either.

use std::net::Ipv4Addr;

use chrono::{DateTime, NaiveDate};
use data_avalanche_core::{ColumnValue, ColumnValueType, Decimal, DecimalError, HaltReason};
use mysql_async::Value;
use mysql_async::binlog::value::BinlogValue;

/// The character sets the source reads (Q-M7a-2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Charset {
    /// `utf8mb4`, `utf8mb3` (`utf8`) and `ascii`.
    Utf8,
    /// MySQL's `latin1`, which is Windows-1252.
    Latin1,
    Binary,
}

impl Charset {
    pub fn of(name: &str) -> Option<Self> {
        match name {
            "utf8mb4" | "utf8mb3" | "utf8" | "ascii" => Some(Self::Utf8),
            "latin1" => Some(Self::Latin1),
            "binary" => Some(Self::Binary),
            _ => None,
        }
    }
}

/// How one column's values become `ColumnValue`s. Planned once per column ([`crate::types`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decode {
    /// `TINYINT(1)`: nonzero is true.
    Bool,
    /// An integer of `bits` bits, read with the catalog's signedness.
    Int {
        bits: u32,
        unsigned: bool,
    },
    Float32,
    Float64,
    Decimal,
    /// A decimal wider than 38 digits kept as its text (`numeric_overflow_action: string`).
    DecimalText,
    Text {
        charset: Charset,
    },
    /// Bytes; `BINARY(n)` pads to `n` the trailing zero bytes the binlog strips.
    Bytes {
        pad: Option<usize>,
    },
    Date,
    Time {
        fsp: u8,
    },
    DateTime {
        fsp: u8,
    },
    Timestamp {
        fsp: u8,
    },
    Year,
    Json,
    Enum {
        members: Vec<String>,
    },
    Set {
        members: Vec<String>,
    },
    /// MariaDB `UUID`.
    Uuid,
    /// MariaDB `INET4` / `INET6`, as MariaDB prints them.
    Inet4,
    Inet6,
    /// The server's text of the value (`column_type_overrides: string`).
    String(Box<Decode>),
    /// Decoded as `from`, then converted for a `column_type_overrides` entry.
    Convert {
        from: Box<Decode>,
        to: ColumnValueType,
    },
}

/// The value type an integer of `bits` bits widens to (`Type_Mapping.md` Integer Types).
pub fn int_type(bits: u32, unsigned: bool) -> ColumnValueType {
    use ColumnValueType as V;
    match (bits, unsigned) {
        (8, _) | (16, false) => V::Int16,
        (16, true) | (24, _) | (32, false) => V::Int32,
        (32, true) | (64, false) => V::Int64,
        _ => V::UInt64,
    }
}

/// Where a value comes from.
#[derive(Debug, Clone, Copy)]
pub enum Input<'a> {
    Binlog(&'a BinlogValue<'a>),
    /// A snapshot chunk's prepared statement.
    Row(&'a Value),
}

/// Why a value can't be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueError {
    /// The value doesn't fit its decimal column (`NUMERIC_OVERFLOW`).
    #[error("the value doesn't fit its decimal column")]
    Overflow,
    /// A zero-in-part or impossible date, or an all-zero date in an identifier column
    /// (`INVALID_DATE_VALUE`, Q-M7-1). `raw` is the server's text of it.
    #[error("invalid date {raw}")]
    InvalidDate { raw: String },
    /// The value has no representation in the column's type (`SCHEMA_MISMATCH`).
    #[error("{0}")]
    Unrepresentable(String),
}

fn bad(what: impl std::fmt::Display) -> ValueError {
    ValueError::Unrepresentable(what.to_string())
}

/// A value read from either input, before it becomes a `ColumnValue` or text.
#[derive(Debug, Clone, PartialEq)]
enum Raw {
    Null,
    Int(i128),
    F32(f32),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// `DATE`, `DATETIME` and `TIMESTAMP` (in UTC) as their parts, zero parts kept.
    Clock {
        y: u16,
        mo: u8,
        d: u8,
        h: u8,
        mi: u8,
        s: u8,
        us: u32,
    },
    Time {
        neg: bool,
        hours: u32,
        mi: u8,
        s: u8,
        us: u32,
    },
    Json(serde_json::Value),
    Labels(Vec<String>),
}

/// MySQL `latin1` (Windows-1252, its five unassigned bytes kept as the C1 controls) → UTF-8.
fn latin1(bytes: &[u8]) -> String {
    const HIGH: [u16; 32] = [
        0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160,
        0x2039, 0x0152, 0x008D, 0x017D, 0x008F, 0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022,
        0x2013, 0x2014, 0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
    ];
    bytes
        .iter()
        .map(|&b| match b {
            0x80..=0x9F => char::from_u32(u32::from(HIGH[usize::from(b - 0x80)])).expect("BMP"),
            _ => char::from(b),
        })
        .collect()
}

fn utf8(bytes: &[u8]) -> Result<String, ValueError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| bad("text that isn't valid UTF-8"))
}

/// An `INET6` as MariaDB prints it (`ipv6_to_str`, which MariaDB inherited from MySQL's
/// `INET6_NTOA`; measured on 11.8). Not RFC 5952: the first longest run of zero groups is
/// compressed even when it is a single group (`1::2:0:3:0:4:0`), and the last 32 bits are dotted
/// after a leading run of exactly six zero groups (`::192.168.0.1`, but `::0.0.0.2` is `::2`) or
/// of five and `ffff` (`::ffff:192.168.0.1`).
fn inet6(b: [u8; 16]) -> String {
    let words: Vec<u16> = b
        .chunks(2)
        .map(|w| u16::from_be_bytes([w[0], w[1]]))
        .collect();
    // The gap: the first longest run of zero words, as (start, length).
    type Run = Option<(usize, usize)>;
    let (mut gap, mut run): (Run, Run) = (None, None);
    for (i, w) in words.iter().enumerate() {
        run = match (*w, run) {
            (0, Some((start, len))) => Some((start, len + 1)),
            (0, None) => Some((i, 1)),
            (_, r) => {
                if let Some(r) = r
                    && gap.is_none_or(|g| r.1 > g.1)
                {
                    gap = Some(r);
                }
                None
            }
        };
    }
    if let Some(r) = run
        && gap.is_none_or(|g| r.1 > g.1)
    {
        gap = Some(r);
    }
    let mut out = String::new();
    let mut i = 0;
    while i < 8 {
        match gap {
            Some((start, len)) if start == i => {
                out.push_str(if i == 0 { "::" } else { ":" });
                i += len;
                continue;
            }
            Some((0, len)) if i == 6 && (len == 6 || (len == 5 && words[5] == 0xffff)) => {
                out.push_str(&Ipv4Addr::new(b[12], b[13], b[14], b[15]).to_string());
                return out;
            }
            _ => {}
        }
        out.push_str(&format!("{:x}", words[i]));
        if i != 7 {
            out.push(':');
        }
        i += 1;
    }
    out
}

fn padded(mut bytes: Vec<u8>, n: usize) -> Vec<u8> {
    if bytes.len() < n {
        bytes.resize(n, 0);
    }
    bytes
}

/// The integer of `bits` bits in `v`, read with `unsigned`: `mysql_common` reads a signed
/// `MEDIUMINT` as unsigned, and without `binlog_row_metadata = FULL` it doesn't know a column's
/// signedness at all.
fn integer(v: &Value, bits: u32, unsigned: bool) -> Result<i128, ValueError> {
    let n = match v {
        Value::Int(i) => i128::from(*i),
        Value::UInt(u) => i128::from(*u),
        other => return Err(bad(format!("an integer arrived as {other:?}"))),
    };
    // Reinterpret only the other reading of the same `bits` bits.
    let m = 1i128 << bits;
    let n = match unsigned {
        true if (-m / 2..0).contains(&n) => n + m,
        false if (m / 2..m).contains(&n) => n - m,
        _ => n,
    };
    let (lo, hi) = match unsigned {
        true => (0, m - 1),
        false => (-m / 2, m / 2 - 1),
    };
    match (lo..=hi).contains(&n) {
        true => Ok(n),
        false => Err(bad(format!("{n} is out of range for a {bits}-bit integer"))),
    }
}

fn clock(v: &Value) -> Option<Raw> {
    match *v {
        Value::Date(y, mo, d, h, mi, s, us) => Some(Raw::Clock {
            y,
            mo,
            d,
            h,
            mi,
            s,
            us,
        }),
        _ => None,
    }
}

/// The binlog's `TIMESTAMP`: seconds since the epoch, a fraction, in UTC; `0` is the zero value.
fn epoch(text: &str) -> Result<Raw, ValueError> {
    let invalid = || bad(format!("a timestamp arrived as {text:?}"));
    let (secs, frac) = text.split_once('.').unwrap_or((text, ""));
    let secs: i64 = secs.parse().map_err(|_| invalid())?;
    if secs == 0 && frac.trim_matches('0').is_empty() {
        return Ok(Raw::Clock {
            y: 0,
            mo: 0,
            d: 0,
            h: 0,
            mi: 0,
            s: 0,
            us: 0,
        });
    }
    if frac.len() > 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    let us: u32 = format!("{frac:0<6}").parse().map_err(|_| invalid())?;
    let t = DateTime::from_timestamp(secs, us * 1000).ok_or_else(invalid)?;
    use chrono::{Datelike, Timelike};
    Ok(Raw::Clock {
        y: t.year() as u16,
        mo: t.month() as u8,
        d: t.day() as u8,
        h: t.hour() as u8,
        mi: t.minute() as u8,
        s: t.second() as u8,
        us,
    })
}

/// The value of `input` as `d` reads it, before it becomes a `ColumnValue` or text.
fn raw(d: &Decode, input: Input<'_>) -> Result<Raw, ValueError> {
    let v = match input {
        Input::Binlog(BinlogValue::Value(v)) | Input::Row(v) => v,
        Input::Binlog(BinlogValue::Jsonb(j)) => {
            let json = serde_json::Value::try_from(j.clone())
                .map_err(|e| bad(format!("binary JSON the decoder can't read: {e}")))?;
            return Ok(Raw::Json(json));
        }
        Input::Binlog(BinlogValue::JsonDiff(_)) => {
            return Err(bad(
                "a partial JSON update (binlog_row_value_options) needs its before image",
            ));
        }
    };
    if matches!(v, Value::NULL) {
        return Ok(Raw::Null);
    }
    let binlog = matches!(input, Input::Binlog(_));
    let bytes = || match v {
        Value::Bytes(b) => Ok(b.clone()),
        other => Err(bad(format!("{d:?} arrived as {other:?}"))),
    };
    let unexpected = || bad(format!("{d:?} arrived as {v:?}"));
    Ok(match d {
        Decode::Bool => Raw::Int(integer(v, 8, false)?),
        Decode::Int { bits, unsigned } => Raw::Int(integer(v, *bits, *unsigned)?),
        Decode::Float32 => match v {
            Value::Float(f) => Raw::F32(*f),
            _ => return Err(unexpected()),
        },
        Decode::Float64 => match v {
            Value::Double(f) => Raw::F64(*f),
            Value::Float(f) => Raw::F64(f64::from(*f)),
            _ => return Err(unexpected()),
        },
        Decode::Decimal | Decode::DecimalText => Raw::Text(utf8(&bytes()?)?),
        Decode::Text { charset } => {
            let b = bytes()?;
            match (binlog, charset) {
                (true, Charset::Latin1) => Raw::Text(latin1(&b)),
                (_, Charset::Binary) => Raw::Bytes(b),
                _ => Raw::Text(utf8(&b)?),
            }
        }
        Decode::Bytes { pad } => Raw::Bytes(padded(bytes()?, pad.unwrap_or(0))),
        Decode::Date | Decode::DateTime { .. } => clock(v).ok_or_else(unexpected)?,
        Decode::Timestamp { .. } => match v {
            Value::Bytes(b) if binlog => epoch(&utf8(b)?)?,
            _ => clock(v).ok_or_else(unexpected)?,
        },
        Decode::Time { .. } => match *v {
            Value::Time(neg, days, h, mi, s, us) => Raw::Time {
                neg,
                hours: days * 24 + u32::from(h),
                mi,
                s,
                us,
            },
            _ => return Err(unexpected()),
        },
        // `mysql_common` reads the binlog's YEAR as text, 1900 + the stored byte: 0 is 1900, a
        // year YEAR can't hold.
        Decode::Year => match v {
            Value::Bytes(b) => match utf8(b)?.parse::<i128>() {
                Ok(1900) => Raw::Int(0),
                Ok(y) => Raw::Int(y),
                Err(_) => return Err(unexpected()),
            },
            Value::Int(_) | Value::UInt(_) => Raw::Int(integer(v, 16, true)?),
            _ => return Err(unexpected()),
        },
        Decode::Json => Raw::Json(
            serde_json::from_slice(&bytes()?)
                .map_err(|e| bad(format!("JSON the decoder can't read: {e}")))?,
        ),
        Decode::Enum { members } => match v {
            Value::Int(_) | Value::UInt(_) if binlog => {
                let i = integer(v, 16, true)? as usize;
                match i {
                    0 => Raw::Text(String::new()),
                    i => Raw::Text(members.get(i - 1).cloned().ok_or_else(|| {
                        bad(format!("ENUM index {i} of {} members", members.len()))
                    })?),
                }
            }
            _ => Raw::Text(utf8(&bytes()?)?),
        },
        Decode::Set { members } => {
            let b = bytes()?;
            if binlog {
                if let Some(i) =
                    (members.len()..b.len() * 8).find(|i| b[i / 8] & (1 << (i % 8)) != 0)
                {
                    return Err(bad(format!("SET bit {i} of {} members", members.len())));
                }
                let labels = (0..members.len())
                    .filter(|i| b.get(i / 8).is_some_and(|byte| byte & (1 << (i % 8)) != 0))
                    .map(|i| members[i].clone())
                    .collect();
                Raw::Labels(labels)
            } else {
                let text = utf8(&b)?;
                Raw::Labels(match text.is_empty() {
                    true => vec![],
                    false => text.split(',').map(str::to_owned).collect(),
                })
            }
        }
        Decode::Uuid => {
            let b = bytes()?;
            match binlog {
                true => Raw::Bytes(padded(b, 16)),
                false => {
                    let text = utf8(&b)?;
                    let u = uuid::Uuid::parse_str(&text)
                        .map_err(|_| bad(format!("not a UUID: {text}")))?;
                    Raw::Bytes(u.as_bytes().to_vec())
                }
            }
        }
        Decode::Inet4 | Decode::Inet6 => {
            let b = bytes()?;
            match (binlog, d) {
                (true, Decode::Inet4) => {
                    let b: [u8; 4] = padded(b, 4).try_into().map_err(|_| unexpected())?;
                    Raw::Text(Ipv4Addr::from(b).to_string())
                }
                (true, _) => {
                    let b: [u8; 16] = padded(b, 16).try_into().map_err(|_| unexpected())?;
                    Raw::Text(inet6(b))
                }
                (false, _) => Raw::Text(utf8(&b)?),
            }
        }
        Decode::String(inner) | Decode::Convert { from: inner, .. } => raw(inner, input)?,
    })
}

/// `us` with `fsp` fractional digits, MySQL's way: none for `fsp` 0.
fn fraction(us: u32, fsp: u8) -> String {
    match fsp {
        0 => String::new(),
        n => format!(".{:06}", us)[..usize::from(n) + 1].to_owned(),
    }
}

/// The server's text of `raw` read by `d` (`column_type_overrides: string`).
fn text(d: &Decode, raw: &Raw) -> String {
    match (d, raw) {
        (_, Raw::Null) => String::new(),
        (Decode::Year, Raw::Int(y)) => format!("{y:04}"),
        (_, Raw::Int(n)) => n.to_string(),
        (_, Raw::F32(f)) => f.to_string(),
        (_, Raw::F64(f)) => f.to_string(),
        (_, Raw::Text(t)) => t.clone(),
        (Decode::Uuid, Raw::Bytes(b)) => uuid::Uuid::from_slice(b)
            .map(|u| u.hyphenated().to_string())
            .unwrap_or_default(),
        (_, Raw::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        (
            _,
            &Raw::Clock {
                y,
                mo,
                d: day,
                h,
                mi,
                s,
                us,
            },
        ) => match d {
            Decode::Date => format!("{y:04}-{mo:02}-{day:02}"),
            Decode::DateTime { fsp } | Decode::Timestamp { fsp } => format!(
                "{y:04}-{mo:02}-{day:02} {h:02}:{mi:02}:{s:02}{}",
                fraction(us, *fsp)
            ),
            _ => String::new(),
        },
        (
            _,
            &Raw::Time {
                neg,
                hours,
                mi,
                s,
                us,
            },
        ) => {
            let fsp = match d {
                Decode::Time { fsp } => *fsp,
                _ => 6,
            };
            let sign = if neg { "-" } else { "" };
            format!("{sign}{hours:02}:{mi:02}:{s:02}{}", fraction(us, fsp))
        }
        (_, Raw::Json(j)) => j.to_string(),
        (_, Raw::Labels(l)) => l.join(","),
    }
}

/// The days and microseconds of a `Clock`: `None` for the all-zero "no date"; an error for a
/// zero-in-part or impossible date (Q-M7-1).
fn instant(d: &Decode, raw: &Raw) -> Result<Option<(i32, i64)>, ValueError> {
    let &Raw::Clock {
        y,
        mo,
        d: day,
        h,
        mi,
        s,
        us,
    } = raw
    else {
        return Err(bad(format!("{d:?} read {raw:?}")));
    };
    if y == 0 && mo == 0 && day == 0 && h == 0 && mi == 0 && s == 0 && us == 0 {
        return Ok(None);
    }
    let invalid = || ValueError::InvalidDate { raw: text(d, raw) };
    let date =
        NaiveDate::from_ymd_opt(i32::from(y), u32::from(mo), u32::from(day)).ok_or_else(invalid)?;
    let days = date
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).expect("the epoch"))
        .num_days() as i32;
    let micros = i64::from(days) * 86_400_000_000
        + (i64::from(h) * 3600 + i64::from(mi) * 60 + i64::from(s)) * 1_000_000
        + i64::from(us);
    Ok(Some((days, micros)))
}

/// The `ColumnValue` of `raw` read by `d`.
fn value(d: &Decode, raw: Raw, identifier: bool) -> Result<ColumnValue, ValueError> {
    use ColumnValue as V;
    if raw == Raw::Null {
        return Ok(V::Null);
    }
    Ok(match d {
        Decode::Date | Decode::DateTime { .. } | Decode::Timestamp { .. } => {
            match (instant(d, &raw)?, identifier) {
                (None, true) => return Err(ValueError::InvalidDate { raw: text(d, &raw) }),
                (None, false) => V::Null,
                (Some((days, _)), _) if *d == Decode::Date => V::Date(days),
                (Some((_, micros)), _) => V::Timestamp {
                    micros,
                    with_tz: matches!(d, Decode::Timestamp { .. }),
                },
            }
        }
        // A key value other than 0 or 1 halts the table: as a boolean, two keys would be one
        // (as a key value that clamps, Q-M5-18).
        Decode::Bool => match raw {
            Raw::Int(n) if identifier && n != 0 && n != 1 => {
                return Err(bad(format!(
                    "a TINYINT(1) key value {n} isn't 0 or 1, and two keys would be one boolean; \
                     turn tinyint1_as_bool off or override the column"
                )));
            }
            raw => V::Bool(raw != Raw::Int(0)),
        },
        Decode::Int { bits, unsigned } => {
            let Raw::Int(n) = raw else {
                return Err(bad(format!("an integer read {raw:?}")));
            };
            match int_type(*bits, *unsigned) {
                ColumnValueType::Int16 => V::Int16(n as i16),
                ColumnValueType::Int32 => V::Int32(n as i32),
                ColumnValueType::Int64 => V::Int64(n as i64),
                _ => V::UInt64(n as u64),
            }
        }
        Decode::Year => match raw {
            Raw::Int(y) => V::Int32(y as i32),
            other => return Err(bad(format!("a year read {other:?}"))),
        },
        Decode::Float32 => match raw {
            Raw::F32(f) => V::Float32(f),
            other => return Err(bad(format!("a float read {other:?}"))),
        },
        Decode::Float64 => match raw {
            Raw::F64(f) => V::Float64(f),
            other => return Err(bad(format!("a double read {other:?}"))),
        },
        Decode::Decimal => match raw {
            Raw::Text(t) => V::Numeric(Decimal::parse(&t).map_err(|e| match e {
                DecimalError::TooManyDigits(_) => ValueError::Overflow,
                _ => bad(format!("not a decimal: {t}")),
            })?),
            other => return Err(bad(format!("a decimal read {other:?}"))),
        },
        Decode::Time { .. } => match raw {
            Raw::Time {
                neg,
                hours,
                mi,
                s,
                us,
            } => {
                let micros = (i64::from(hours) * 3600 + i64::from(mi) * 60 + i64::from(s))
                    * 1_000_000
                    + i64::from(us);
                V::Time(if neg { -micros } else { micros })
            }
            other => return Err(bad(format!("a time read {other:?}"))),
        },
        Decode::Json => match raw {
            Raw::Json(j) => V::Json(j),
            other => return Err(bad(format!("JSON read {other:?}"))),
        },
        Decode::Set { .. } => match raw {
            Raw::Labels(l) => V::Array {
                element_type: ColumnValueType::Text,
                values: l.into_iter().map(V::Text).collect(),
            },
            other => return Err(bad(format!("a SET read {other:?}"))),
        },
        Decode::Uuid => match raw {
            Raw::Bytes(b) => {
                V::Uuid(uuid::Uuid::from_slice(&b).map_err(|_| bad("a UUID of the wrong length"))?)
            }
            other => return Err(bad(format!("a UUID read {other:?}"))),
        },
        Decode::String(inner) => V::Text(text(inner, &raw)),
        Decode::Convert { from, to } => value(from, raw, identifier)?.convert(to),
        _ => match raw {
            Raw::Text(t) => V::Text(t),
            Raw::Bytes(b) => V::Bytes(b),
            other => return Err(bad(format!("{d:?} read {other:?}"))),
        },
    })
}

/// The table halt a value that can't be decoded in `column` causes.
pub(crate) fn halt_reason(column: String, e: ValueError) -> HaltReason {
    match e {
        ValueError::Overflow => HaltReason::NumericOverflow { column },
        ValueError::InvalidDate { raw } => HaltReason::InvalidDateValue { column, raw },
        ValueError::Unrepresentable(detail) => HaltReason::SchemaMismatch { column, detail },
    }
}

/// Decodes one value of a column planned as `d`; `identifier` marks a column of the table's
/// identifier.
pub fn decode(d: &Decode, input: Input<'_>, identifier: bool) -> Result<ColumnValue, ValueError> {
    let r = raw(d, input)?;
    value(d, r, identifier)
}

/// Decodes an after image's value, which a partial JSON update logs as diffs against the before
/// image's (Q-M7a-3): the diffs are applied to `before`, and the full document is the value.
pub fn decode_after(
    d: &Decode,
    before: Option<Input<'_>>,
    after: Input<'_>,
    identifier: bool,
) -> Result<ColumnValue, ValueError> {
    let Input::Binlog(BinlogValue::JsonDiff(diffs)) = after else {
        return decode(d, after, identifier);
    };
    let before = before.ok_or_else(|| bad("a partial JSON update without a before image"))?;
    let Raw::Json(doc) = raw(d, before)? else {
        return Err(bad(
            "a partial JSON update of a value that isn't a JSON document",
        ));
    };
    let doc = crate::json::apply_all(doc, diffs).map_err(bad)?;
    value(d, Raw::Json(doc), identifier)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(d: &Decode, v: Value) -> Result<ColumnValue, ValueError> {
        decode(d, Input::Row(&v), false)
    }

    fn binlog(d: &Decode, v: Value) -> Result<ColumnValue, ValueError> {
        decode(d, Input::Binlog(&BinlogValue::Value(v)), false)
    }

    /// Both inputs as measured (S-MY type probe): the binlog's form and the prepared statement's
    /// decode to `want`.
    fn both(d: &Decode, from_binlog: Value, from_row: Value, want: ColumnValue) {
        assert_eq!(binlog(d, from_binlog).unwrap(), want, "binlog, {d:?}");
        assert_eq!(row(d, from_row).unwrap(), want, "row, {d:?}");
    }

    fn b(s: &[u8]) -> Value {
        Value::Bytes(s.to_vec())
    }

    #[test]
    fn a_boolean_key_other_than_0_or_1_is_unrepresentable() {
        let key = |v| decode(&Decode::Bool, Input::Row(&Value::Int(v)), true);
        assert_eq!(key(1), Ok(ColumnValue::Bool(true)));
        assert_eq!(key(0), Ok(ColumnValue::Bool(false)));
        assert!(matches!(key(2), Err(ValueError::Unrepresentable(_))));
        assert_eq!(
            row(&Decode::Bool, Value::Int(2)),
            Ok(ColumnValue::Bool(true))
        );
    }

    #[test]
    fn integers_read_with_the_catalogs_signedness() {
        let mi = Decode::Int {
            bits: 24,
            unsigned: false,
        };
        // mysql_common reads a signed MEDIUMINT's -8388608 as 8388608.
        both(
            &mi,
            Value::Int(8_388_608),
            Value::Int(-8_388_608),
            ColumnValue::Int32(-8_388_608),
        );
        let tiu = Decode::Int {
            bits: 8,
            unsigned: true,
        };
        // Without FULL row metadata, an unsigned 255 reads as -1.
        both(
            &tiu,
            Value::Int(-1),
            Value::Int(255),
            ColumnValue::Int16(255),
        );
        let iu = Decode::Int {
            bits: 32,
            unsigned: true,
        };
        both(
            &iu,
            Value::Int(4_294_967_295),
            Value::Int(4_294_967_295),
            ColumnValue::Int64(4_294_967_295),
        );
        let biu = Decode::Int {
            bits: 64,
            unsigned: true,
        };
        both(
            &biu,
            Value::UInt(u64::MAX),
            Value::UInt(u64::MAX),
            ColumnValue::UInt64(u64::MAX),
        );
        assert_eq!(
            binlog(&biu, Value::Int(-1)).unwrap(),
            ColumnValue::UInt64(u64::MAX)
        );
        let bi = Decode::Int {
            bits: 64,
            unsigned: false,
        };
        both(
            &bi,
            Value::Int(i64::MIN),
            Value::Int(i64::MIN),
            ColumnValue::Int64(i64::MIN),
        );
        assert!(
            binlog(
                &Decode::Int {
                    bits: 16,
                    unsigned: false
                },
                Value::Int(70_000)
            )
            .is_err()
        );
        both(
            &Decode::Bool,
            Value::Int(2),
            Value::Int(2),
            ColumnValue::Bool(true),
        );
        both(
            &Decode::Bool,
            Value::Int(0),
            Value::Int(0),
            ColumnValue::Bool(false),
        );
    }

    #[test]
    fn floats_decimals_and_nulls() {
        both(
            &Decode::Float32,
            Value::Float(1.5),
            Value::Float(1.5),
            ColumnValue::Float32(1.5),
        );
        both(
            &Decode::Float64,
            Value::Double(2.25),
            Value::Double(2.25),
            ColumnValue::Float64(2.25),
        );
        both(
            &Decode::Decimal,
            b(b"12345678.90"),
            b(b"12345678.90"),
            ColumnValue::Numeric(Decimal::parse("12345678.90").unwrap()),
        );
        both(
            &Decode::Decimal,
            Value::NULL,
            Value::NULL,
            ColumnValue::Null,
        );
        let wide = "12345678901234567890123456789012345.123456789012345678901234567891";
        assert_eq!(
            binlog(&Decode::DecimalText, b(wide.as_bytes())).unwrap(),
            ColumnValue::Text(wide.into())
        );
        assert_eq!(
            binlog(&Decode::Decimal, b(wide.as_bytes())),
            Err(ValueError::Overflow)
        );
    }

    #[test]
    fn text_reads_its_charset_from_the_binlog_and_utf8_from_a_chunk() {
        let latin = Decode::Text {
            charset: Charset::Latin1,
        };
        both(
            &latin,
            b(&[0x64, 0xe9, 0x6a, 0xe0]),
            b("déjà".as_bytes()),
            ColumnValue::Text("déjà".into()),
        );
        assert_eq!(latin1(&[0x80, 0x81, 0x9f, 0xff]), "€\u{81}Ÿÿ");
        let u = Decode::Text {
            charset: Charset::Utf8,
        };
        both(
            &u,
            b("héllo 🎉".as_bytes()),
            b("héllo 🎉".as_bytes()),
            ColumnValue::Text("héllo 🎉".into()),
        );
        assert!(binlog(&u, b(&[0xff])).is_err());
    }

    #[test]
    fn binary_n_and_mariadb_types_get_their_stripped_zeros_back() {
        both(
            &Decode::Bytes { pad: Some(4) },
            b(b"ab"),
            b(b"ab\0\0"),
            ColumnValue::Bytes(b"ab\0\0".to_vec()),
        );
        both(
            &Decode::Bytes { pad: None },
            b(b"a\0b"),
            b(b"a\0b"),
            ColumnValue::Bytes(b"a\0b".to_vec()),
        );
        let u = uuid::Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        both(
            &Decode::Uuid,
            b(&u.as_bytes()[..15]),
            b(b"123e4567-e89b-12d3-a456-426614174000"),
            ColumnValue::Uuid(u),
        );
        both(
            &Decode::Uuid,
            b(&[]),
            b(b"00000000-0000-0000-0000-000000000000"),
            ColumnValue::Uuid(uuid::Uuid::nil()),
        );
        both(
            &Decode::Inet4,
            b(&[192, 168, 0, 1]),
            b(b"192.168.0.1"),
            ColumnValue::Text("192.168.0.1".into()),
        );
        both(
            &Decode::Inet4,
            b(&[]),
            b(b"0.0.0.0"),
            ColumnValue::Text("0.0.0.0".into()),
        );
        let v6 = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        both(
            &Decode::Inet6,
            b(&v6),
            b(b"2001:db8::1"),
            ColumnValue::Text("2001:db8::1".into()),
        );
        both(
            &Decode::Inet6,
            b(&[]),
            b(b"::"),
            ColumnValue::Text("::".into()),
        );
        let compat = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 192, 168, 0, 1];
        both(
            &Decode::Inet6,
            b(&compat),
            b(b"::192.168.0.1"),
            ColumnValue::Text("::192.168.0.1".into()),
        );
        assert_eq!(inet6([0; 16]), "::");
        let mut two = [0; 16];
        two[15] = 2;
        assert_eq!(inet6(two), "::2");
        assert_eq!(inet6(std::net::Ipv6Addr::LOCALHOST.octets()), "::1");
        let w = |s: &str| inet6(s.parse::<std::net::Ipv6Addr>().unwrap().octets());
        assert_eq!(w("1:0:2:0:3:0:4:0"), "1::2:0:3:0:4:0");
        assert_eq!(w("1:0:0:2:0:0:0:3"), "1:0:0:2::3");
        assert_eq!(w("1::"), "1::");
        assert_eq!(w("::ffff:1.2.3.4"), "::ffff:1.2.3.4");
        assert_eq!(w("1:2:3:4:5:6:7:8"), "1:2:3:4:5:6:7:8");
    }

    #[test]
    fn dates_times_and_timestamps() {
        let ntz = |micros| ColumnValue::Timestamp {
            micros,
            with_tz: false,
        };
        both(
            &Decode::Date,
            Value::Date(2024, 2, 29, 0, 0, 0, 0),
            Value::Date(2024, 2, 29, 0, 0, 0, 0),
            ColumnValue::Date(19_782),
        );
        both(
            &Decode::DateTime { fsp: 6 },
            Value::Date(2024, 2, 29, 23, 59, 59, 999_999),
            Value::Date(2024, 2, 29, 23, 59, 59, 999_999),
            ntz(1_709_251_199_999_999),
        );
        // The binlog's TIMESTAMP is epoch text in UTC; a chunk's is the session's (UTC) clock.
        both(
            &Decode::Timestamp { fsp: 6 },
            b(b"1709251199.123456"),
            Value::Date(2024, 2, 29, 23, 59, 59, 123_456),
            ColumnValue::Timestamp {
                micros: 1_709_251_199_123_456,
                with_tz: true,
            },
        );
        both(
            &Decode::Timestamp { fsp: 3 },
            b(b"1.5"),
            Value::Date(1970, 1, 1, 0, 0, 1, 500_000),
            ColumnValue::Timestamp {
                micros: 1_500_000,
                with_tz: true,
            },
        );
        both(
            &Decode::Time { fsp: 6 },
            Value::Time(true, 34, 22, 59, 59, 0),
            Value::Time(true, 34, 22, 59, 59, 0),
            ColumnValue::Time(-(838 * 3600 + 59 * 60 + 59) * 1_000_000),
        );
        // YEAR: mysql_common reads 0000 as 1900 from the binlog.
        both(
            &Decode::Year,
            b(b"2024"),
            Value::Int(2024),
            ColumnValue::Int32(2024),
        );
        both(
            &Decode::Year,
            b(b"1900"),
            Value::Int(0),
            ColumnValue::Int32(0),
        );
    }

    #[test]
    fn all_zero_dates_are_null_and_invalid_dates_halt() {
        let zero_dt = Value::Date(0, 0, 0, 0, 0, 0, 0);
        both(
            &Decode::Date,
            zero_dt.clone(),
            zero_dt.clone(),
            ColumnValue::Null,
        );
        both(
            &Decode::Timestamp { fsp: 0 },
            b(b"0"),
            zero_dt.clone(),
            ColumnValue::Null,
        );
        // In an identifier, all-zero has no NULL to become.
        assert_eq!(
            decode(&Decode::DateTime { fsp: 0 }, Input::Row(&zero_dt), true),
            Err(ValueError::InvalidDate {
                raw: "0000-00-00 00:00:00".into()
            })
        );
        for (d, v, raw) in [
            (
                Decode::Date,
                Value::Date(2024, 0, 15, 0, 0, 0, 0),
                "2024-00-15",
            ),
            (
                Decode::Date,
                Value::Date(2024, 2, 31, 0, 0, 0, 0),
                "2024-02-31",
            ),
            (
                Decode::DateTime { fsp: 2 },
                Value::Date(2024, 5, 0, 10, 0, 0, 0),
                "2024-05-00 10:00:00.00",
            ),
            (
                Decode::DateTime { fsp: 0 },
                Value::Date(0, 0, 0, 10, 0, 0, 0),
                "0000-00-00 10:00:00",
            ),
        ] {
            assert_eq!(
                binlog(&d, v),
                Err(ValueError::InvalidDate { raw: raw.into() })
            );
        }
    }

    #[test]
    fn json_enum_set_from_both_forms() {
        both(
            &Decode::Json,
            b(br#"{"a": [1, 2.5]}"#),
            b(br#"{"a": [1, 2.5]}"#),
            ColumnValue::Json(serde_json::json!({"a": [1, 2.5]})),
        );
        let e = Decode::Enum {
            members: vec!["a".into(), "b".into(), "c".into()],
        };
        both(&e, Value::Int(2), b(b"b"), ColumnValue::Text("b".into()));
        both(&e, Value::Int(0), b(b""), ColumnValue::Text("".into()));
        assert!(binlog(&e, Value::Int(4)).is_err());
        let s = Decode::Set {
            members: vec!["x".into(), "y".into(), "z".into()],
        };
        let labels = |l: &[&str]| ColumnValue::Array {
            element_type: ColumnValueType::Text,
            values: l.iter().map(|x| ColumnValue::Text(x.to_string())).collect(),
        };
        both(&s, b(&[0b101]), b(b"x,z"), labels(&["x", "z"]));
        both(&s, b(&[0]), b(b""), labels(&[]));
        assert!(binlog(&s, b(&[0b1001])).is_err());
        let partial = BinlogValue::JsonDiff(vec![]);
        assert!(decode(&Decode::Json, Input::Binlog(&partial), false).is_err());
        // With its before image, an empty diff list (a statement that changed nothing) is the
        // before image; a `string` override renders the document.
        let before = b(br#"{"a": 1}"#);
        for (d, want) in [
            (Decode::Json, ColumnValue::Json(serde_json::json!({"a": 1}))),
            (
                Decode::String(Box::new(Decode::Json)),
                ColumnValue::Text(r#"{"a":1}"#.into()),
            ),
        ] {
            let got = decode_after(
                &d,
                Some(Input::Row(&before)),
                Input::Binlog(&partial),
                false,
            );
            assert_eq!(got.unwrap(), want);
        }
        assert!(decode_after(&Decode::Json, None, Input::Binlog(&partial), false).is_err());
    }

    #[test]
    fn string_overrides_keep_the_servers_text() {
        let s = |d: Decode| Decode::String(Box::new(d));
        let t = |x: &str| ColumnValue::Text(x.into());
        both(
            &s(Decode::Date),
            Value::Date(2024, 0, 15, 0, 0, 0, 0),
            Value::Date(2024, 0, 15, 0, 0, 0, 0),
            t("2024-00-15"),
        );
        both(
            &s(Decode::DateTime { fsp: 3 }),
            Value::Date(2024, 2, 29, 23, 59, 59, 999_000),
            Value::Date(2024, 2, 29, 23, 59, 59, 999_000),
            t("2024-02-29 23:59:59.999"),
        );
        both(
            &s(Decode::Timestamp { fsp: 0 }),
            b(b"0"),
            Value::Date(0, 0, 0, 0, 0, 0, 0),
            t("0000-00-00 00:00:00"),
        );
        both(
            &s(Decode::Time { fsp: 0 }),
            Value::Time(false, 0, 12, 0, 0, 0),
            Value::Time(false, 0, 12, 0, 0, 0),
            t("12:00:00"),
        );
        both(
            &s(Decode::Time { fsp: 6 }),
            Value::Time(true, 34, 22, 59, 59, 1),
            Value::Time(true, 34, 22, 59, 59, 1),
            t("-838:59:59.000001"),
        );
        both(&s(Decode::Year), b(b"1900"), Value::Int(0), t("0000"));
        both(
            &s(Decode::Int {
                bits: 24,
                unsigned: false,
            }),
            Value::Int(8_388_608),
            Value::Int(-8_388_608),
            t("-8388608"),
        );
        let set = Decode::Set {
            members: vec!["x".into(), "y".into(), "z".into()],
        };
        both(&s(set), b(&[0b110]), b(b"y,z"), t("y,z"));
        both(
            &s(Decode::Json),
            b(br#"{"a": 1}"#),
            b(br#"{"a": 1}"#),
            t(r#"{"a":1}"#),
        );
    }

    #[test]
    fn conversions_apply_after_decoding() {
        let d = Decode::Convert {
            from: Box::new(Decode::Int {
                bits: 16,
                unsigned: false,
            }),
            to: ColumnValueType::Int64,
        };
        both(&d, Value::Int(-5), Value::Int(-5), ColumnValue::Int64(-5));
    }
}
