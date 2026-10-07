//! A snapshot cursor between the source's typed key ([`KeysetCursor`]) and the state document's
//! JSON ([`SnapshotCursor`], Snapshot_Resumability §2). Each key value is stored by its column's
//! type: integers, dates, times and timestamps (microseconds) as JSON numbers, decimals and UUIDs
//! as their text, bytes as hex.

use data_avalanche_core::{
    ColumnValue as V, ColumnValueType as T, Decimal, KeysetCursor, Row, TableSchema,
};
use data_avalanche_state::{CursorKey, SnapshotCursor};
use serde_json::Value;
use uuid::Uuid;

/// The document form of `cursor`, a cursor of `schema`'s identifier.
pub(crate) fn to_doc(
    schema: &TableSchema,
    cursor: &KeysetCursor,
    chunk_size: u64,
    rows_done: u64,
) -> Result<SnapshotCursor, String> {
    let key = cursor
        .key
        .0
        .iter()
        .map(|(col, v)| {
            let c = schema
                .column(col)
                .ok_or_else(|| format!("{col:?} isn't a column"))?;
            Ok(CursorKey {
                col: col.clone(),
                val: json(v)?,
                ty: c.source_type.clone(),
            })
        })
        .collect::<Result<_, String>>()?;
    Ok(SnapshotCursor {
        key,
        chunk_size,
        rows_done,
        collation: cursor.collation.clone(),
    })
}

/// The cursor a document holds, when it still fits `schema`'s identifier.
pub(crate) fn from_doc(schema: &TableSchema, doc: &SnapshotCursor) -> Result<KeysetCursor, String> {
    let cols: Vec<&str> = doc.key.iter().map(|k| k.col.as_str()).collect();
    if cols != schema.identifier() {
        return Err(format!(
            "the cursor is over {cols:?}, the identifier is {:?}",
            schema.identifier()
        ));
    }
    let key = doc
        .key
        .iter()
        .map(|k| {
            let c = schema.column(&k.col).expect("an identifier column");
            Ok((k.col.clone(), value(&c.value_type, &k.val)?))
        })
        .collect::<Result<_, String>>()?;
    Ok(KeysetCursor {
        key: Row(key),
        collation: doc.collation.clone(),
    })
}

fn json(v: &V) -> Result<Value, String> {
    Ok(match v {
        V::Bool(b) => Value::Bool(*b),
        V::Int16(i) => Value::from(*i),
        V::Int32(i) => Value::from(*i),
        V::Int64(i) | V::Time(i) => Value::from(*i),
        V::UInt64(u) => Value::from(*u),
        V::Timestamp { micros, .. } => Value::from(*micros),
        V::Date(d) => Value::from(*d),
        V::Numeric(d) => Value::String(d.to_string()),
        V::Text(s) => Value::String(s.clone()),
        V::Uuid(u) => Value::String(u.to_string()),
        V::Bytes(b) => Value::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        other => return Err(format!("{other:?} can't be a key value")),
    })
}

fn value(t: &T, v: &Value) -> Result<V, String> {
    let int = || v.as_i64().ok_or_else(|| format!("{v} isn't an integer"));
    let text = || v.as_str().ok_or_else(|| format!("{v} isn't a string"));
    Ok(match t {
        T::Bool => V::Bool(v.as_bool().ok_or_else(|| format!("{v} isn't a boolean"))?),
        T::Int16 => V::Int16(fit(int()?, t)?),
        T::Int32 => V::Int32(fit(int()?, t)?),
        T::Int64 => V::Int64(int()?),
        T::UInt64 => V::UInt64(v.as_u64().ok_or_else(|| format!("{v} isn't unsigned"))?),
        T::Time => V::Time(int()?),
        T::Date => V::Date(fit(int()?, t)?),
        T::Timestamp { with_tz } => V::Timestamp {
            micros: int()?,
            with_tz: *with_tz,
        },
        T::Numeric { .. } => V::Numeric(Decimal::parse(text()?).map_err(|e| e.to_string())?),
        T::Text => V::Text(text()?.to_owned()),
        T::Uuid => V::Uuid(Uuid::parse_str(text()?).map_err(|e| e.to_string())?),
        T::Bytes => V::Bytes(hex(text()?)?),
        other => return Err(format!("{other:?} can't be a key type")),
    })
}

fn fit<N: TryFrom<i64>>(i: i64, t: &T) -> Result<N, String> {
    N::try_from(i).map_err(|_| format!("{i} doesn't fit {t:?}"))
}

fn hex(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) || !s.is_ascii() {
        return Err(format!("{s:?} isn't hex"));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| format!("{s:?} isn't hex")))
        .collect()
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnSchema, TableRef};

    use super::*;

    fn schema() -> TableSchema {
        let col = |n: &str, t: T, src: &str| ColumnSchema::new(n, t, src).not_null();
        TableSchema::new(
            TableRef::new("public", "k"),
            vec![
                col("b", T::Bool, "bool"),
                col("i", T::Int16, "int2"),
                col(
                    "n",
                    T::Numeric {
                        precision: 10,
                        scale: 2,
                    },
                    "numeric",
                ),
                col("ts", T::Timestamp { with_tz: true }, "timestamptz"),
                col("d", T::Date, "date"),
                col("s", T::Text, "text"),
                col("u", T::Uuid, "uuid"),
                col("x", T::Bytes, "bytea"),
                col("big", T::UInt64, "bigint unsigned"),
            ],
            ["b", "i", "n", "ts", "d", "s", "u", "x", "big"]
                .map(String::from)
                .to_vec(),
        )
        .unwrap()
    }

    fn cursor() -> KeysetCursor {
        KeysetCursor {
            key: Row(vec![
                ("b".into(), V::Bool(true)),
                ("i".into(), V::Int16(-7)),
                ("n".into(), V::Numeric(Decimal::new(12_345, 2))),
                (
                    "ts".into(),
                    V::Timestamp {
                        micros: 1_700_000_000_000_001,
                        with_tz: true,
                    },
                ),
                ("d".into(), V::Date(-3)),
                ("s".into(), V::Text("é\"x".into())),
                ("u".into(), V::Uuid(Uuid::from_u128(0x0123_4567_89ab_cdef))),
                ("x".into(), V::Bytes(vec![0, 0xab, 0xff])),
                ("big".into(), V::UInt64(u64::MAX)),
            ]),
            collation: Some("C".into()),
        }
    }

    #[test]
    fn a_cursor_round_trips_through_its_document() {
        let doc = to_doc(&schema(), &cursor(), 500, 1500).unwrap();
        assert_eq!((doc.chunk_size, doc.rows_done), (500, 1500));
        assert_eq!(doc.key[2].val, Value::String("123.45".into()));
        assert_eq!(doc.key[7].val, Value::String("00abff".into()));
        assert_eq!(doc.key[3].ty, "timestamptz");
        let json = serde_json::to_value(&doc).unwrap();
        let back: SnapshotCursor = serde_json::from_value(json).unwrap();
        assert_eq!(from_doc(&schema(), &back).unwrap(), cursor());
    }

    #[test]
    fn a_cursor_that_doesnt_fit_the_identifier_is_refused() {
        let mut doc = to_doc(&schema(), &cursor(), 500, 0).unwrap();
        doc.key.pop();
        assert!(
            from_doc(&schema(), &doc)
                .unwrap_err()
                .contains("identifier")
        );
        let mut doc = to_doc(&schema(), &cursor(), 500, 0).unwrap();
        doc.key[1].val = Value::from(70_000);
        assert!(from_doc(&schema(), &doc).is_err(), "out of int2's range");
        doc.key[1].val = Value::from(1);
        doc.key[7].val = Value::String("abc".into());
        assert!(from_doc(&schema(), &doc).unwrap_err().contains("hex"));
    }
}
