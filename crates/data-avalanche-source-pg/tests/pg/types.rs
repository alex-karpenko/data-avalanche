//! S-PG7 as a regression, and the M5 exit's type round trip: every mapped PostgreSQL type
//! (`Type_Mapping.md`) inserted, then read through the stream and through a snapshot chunk. Both
//! give the expected values, and the same ones. The writer's session uses another time zone,
//! date style and interval style than the pinned ones.

use data_avalanche_core::{
    ChangeStream, ColumnValue as V, ColumnValueType as T, Decimal, KeysetCursor, Row, StreamItem,
};
use data_avalanche_testkit::require_it;
use serde_json::json;

use crate::common::{admin, cleanup, collect, connector, first_stream, fresh, pipeline};

/// A column: its name, its type, and per row a SQL literal and the value it decodes to. A row
/// past the end of the list is NULL.
struct Case {
    column: &'static str,
    ty: &'static str,
    values: Vec<(&'static str, V)>,
}

fn case(column: &'static str, ty: &'static str, values: Vec<(&'static str, V)>) -> Case {
    Case { column, ty, values }
}

fn dec(unscaled: i128, scale: i8) -> V {
    V::Numeric(Decimal::new(unscaled, scale))
}

fn text(s: &str) -> V {
    V::Text(s.into())
}

fn array(element_type: T, values: Vec<V>) -> V {
    V::Array {
        element_type,
        values,
    }
}

fn cases(server_version: i32) -> Vec<Case> {
    let mut c = vec![
        case(
            "b",
            "bool",
            vec![("true", V::Bool(true)), ("false", V::Bool(false))],
        ),
        case(
            "i2",
            "int2",
            vec![
                ("-32768", V::Int16(i16::MIN)),
                ("32767", V::Int16(i16::MAX)),
            ],
        ),
        case(
            "i4",
            "int4",
            vec![
                ("2147483647", V::Int32(i32::MAX)),
                ("-2147483648", V::Int32(i32::MIN)),
            ],
        ),
        case(
            "i8",
            "int8",
            vec![
                ("9223372036854775807", V::Int64(i64::MAX)),
                ("-9223372036854775808", V::Int64(i64::MIN)),
            ],
        ),
        case(
            "o",
            "oid",
            vec![("4294967295", V::Int64(4_294_967_295)), ("0", V::Int64(0))],
        ),
        case(
            "ch",
            "\"char\"",
            vec![("'a'", V::Int16(97)), ("'Z'", V::Int16(90))],
        ),
        case(
            "f4",
            "float4",
            vec![
                ("1.5", V::Float32(1.5)),
                ("'-Infinity'", V::Float32(f32::NEG_INFINITY)),
            ],
        ),
        case(
            "f8",
            "float8",
            vec![
                ("0.1", V::Float64(0.1)),
                ("'Infinity'", V::Float64(f64::INFINITY)),
            ],
        ),
        case(
            "n",
            "numeric(12,3)",
            vec![
                ("123456789.123", dec(123_456_789_123, 3)),
                ("-0.001", dec(-1, 3)),
            ],
        ),
        case(
            "nb",
            "numeric",
            vec![
                ("1e-20", text("0.00000000000000000001")),
                ("'NaN'", text("NaN")),
            ],
        ),
        case(
            "m",
            "money",
            vec![
                ("1234.56", dec(123_456, 2)),
                ("'-92233720368547758.08'", dec(i64::MIN.into(), 2)),
            ],
        ),
        case(
            "bp",
            "char(5)",
            vec![("'ab'", text("ab")), ("''", text(""))],
        ),
        case(
            "vc",
            "varchar(10)",
            vec![("'héllo ✓'", text("héllo ✓")), ("''", text(""))],
        ),
        case(
            "tx",
            "text",
            vec![("'it''s \"q\" \\ {x}'", text("it's \"q\" \\ {x}"))],
        ),
        case("nm", "name", vec![("'nm'", text("nm"))]),
        case(
            "by",
            "bytea",
            vec![
                ("'\\xdeadbeef'", V::Bytes(vec![0xde, 0xad, 0xbe, 0xef])),
                ("'\\x'", V::Bytes(vec![])),
            ],
        ),
        case(
            "d",
            "date",
            vec![
                ("'2024-02-29'", V::Date(19_782)),
                ("'4714-11-24 BC'", V::Date(-2_440_588)),
                ("'infinity'", V::Date(i32::MAX)),
            ],
        ),
        case(
            "tm",
            "time",
            vec![
                ("'23:59:59.999999'", V::Time(86_399_999_999)),
                ("'24:00:00'", V::Time(86_400_000_000)),
            ],
        ),
        case(
            "tz",
            "timetz",
            vec![("'12:00:00+05:30'", text("12:00:00+05:30"))],
        ),
        case(
            "ts",
            "timestamp",
            vec![
                (
                    "'2024-02-29 12:34:56.789012'",
                    V::Timestamp {
                        micros: 1_709_210_096_789_012,
                        with_tz: false,
                    },
                ),
                (
                    "'0001-01-01 00:00:00 BC'",
                    V::Timestamp {
                        micros: -62_167_219_200_000_000,
                        with_tz: false,
                    },
                ),
                (
                    "'-infinity'",
                    V::Timestamp {
                        micros: i64::MIN,
                        with_tz: false,
                    },
                ),
            ],
        ),
        case(
            "tt",
            "timestamptz",
            vec![
                (
                    "'2024-02-29 12:34:56.789012+03'",
                    V::Timestamp {
                        micros: 1_709_199_296_789_012,
                        with_tz: true,
                    },
                ),
                (
                    "'infinity'",
                    V::Timestamp {
                        micros: i64::MAX,
                        with_tz: true,
                    },
                ),
            ],
        ),
        case(
            "iv",
            "interval",
            vec![
                ("'1 day 02:03:04'", text("P1DT2H3M4S")),
                ("'-1 mon'", text("P-1M")),
            ],
        ),
        case(
            "u",
            "uuid",
            vec![(
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
                V::Uuid(uuid::Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap()),
            )],
        ),
        case(
            "j",
            "json",
            vec![(
                r#"'{"b": 1, "a": [1, 2.5, null]}'"#,
                V::Json(json!({"b": 1, "a": [1, 2.5, null]})),
            )],
        ),
        case(
            "jb",
            "jsonb",
            vec![(
                r#"'{"b": 1, "a": [1, 2.5, null]}'"#,
                V::Json(json!({"b": 1, "a": [1, 2.5, null]})),
            )],
        ),
        case(
            "bt",
            "bit(5)",
            vec![("B'10101'", V::Bytes(vec![0b1010_1000]))],
        ),
        case(
            "vb",
            "varbit(10)",
            vec![
                ("B'101'", V::Bytes(vec![0, 0, 0, 3, 0b1010_0000])),
                ("B''", V::Bytes(vec![0, 0, 0, 0])),
            ],
        ),
        case(
            "ip",
            "inet",
            vec![("'192.168.0.1/24'", text("192.168.0.1/24"))],
        ),
        case("cd", "cidr", vec![("'10.0.0.0/8'", text("10.0.0.0/8"))]),
        case(
            "mc",
            "macaddr",
            vec![("'08:00:2b:01:02:03'", text("08:00:2b:01:02:03"))],
        ),
        case(
            "m8",
            "macaddr8",
            vec![("'08:00:2b:01:02:03:04:05'", text("08:00:2b:01:02:03:04:05"))],
        ),
        case("pt", "point", vec![("'(1.5,2)'", text("(1.5,2)"))]),
        case("bx", "box", vec![("'(1,1),(0,0)'", text("(1,1),(0,0)"))]),
        case("ci", "circle", vec![("'<(0,0),2>'", text("<(0,0),2>"))]),
        case(
            "r4",
            "int4range",
            vec![("'[1,10)'", text("[1,10)")), ("'empty'", text("empty"))],
        ),
        case(
            "tr",
            "tstzrange",
            vec![(
                "'[2024-01-01 00:00+00,2024-01-02 00:00+00)'",
                text("[\"2024-01-01 00:00:00+00\",\"2024-01-02 00:00:00+00\")"),
            )],
        ),
        case(
            "mr",
            "int4multirange",
            vec![("'{[1,3),[5,7)}'", text("{[1,3),[5,7)}"))],
        ),
        case(
            "tv",
            "tsvector",
            vec![("'a fat cat'", text("'a' 'cat' 'fat'"))],
        ),
        case("x", "xml", vec![("'<a>1</a>'", text("<a>1</a>"))]),
        case("e", "mood", vec![("'ok'", text("ok"))]),
        case("cp", "pair", vec![("ROW(1, 'x')", text("(1,x)"))]),
        case("dm", "price", vec![("12.5", dec(1250, 2))]),
        case(
            "hs",
            "public.hstore",
            vec![("'a=>1, b=>NULL'", V::Json(json!({"a": "1", "b": null})))],
        ),
        case(
            "ai",
            "int4[]",
            vec![
                (
                    "'{1,NULL,3}'",
                    array(T::Int32, vec![V::Int32(1), V::Null, V::Int32(3)]),
                ),
                ("'{}'", array(T::Int32, vec![])),
            ],
        ),
        case(
            "at",
            "text[]",
            vec![(
                r#"'{a,"b c",NULL,"NULL"}'"#,
                array(T::Text, vec![text("a"), text("b c"), V::Null, text("NULL")]),
            )],
        ),
        case(
            "an",
            "numeric(5,2)[]",
            vec![(
                "'{1.5,-2}'",
                array(
                    T::Numeric {
                        precision: 5,
                        scale: 2,
                    },
                    vec![dec(150, 2), dec(-200, 2)],
                ),
            )],
        ),
        case(
            "ad",
            "date[]",
            vec![(
                "'{2024-01-01,infinity}'",
                array(T::Date, vec![V::Date(19_723), V::Date(i32::MAX)]),
            )],
        ),
        case(
            "ab",
            "box[]",
            vec![(
                "'{(1,1),(0,0);(2,2),(1,1)}'",
                array(T::Text, vec![text("(1,1),(0,0)"), text("(2,2),(1,1)")]),
            )],
        ),
        case(
            "ae",
            "mood[]",
            vec![("'{ok,sad}'", array(T::Text, vec![text("ok"), text("sad")]))],
        ),
    ];
    // Negative scales and scales above the precision: PostgreSQL 15 and later.
    if server_version >= 150_000 {
        c.push(case("ns", "numeric(3,-2)", vec![("12345", dec(12_300, 0))]));
        c.push(case("nf", "numeric(2,5)", vec![("0.00012", dec(12, 5))]));
    }
    c
}

fn rows(cases: &[Case]) -> usize {
    cases.iter().map(|c| c.values.len()).max().unwrap() + 1
}

/// Row `i` as expected: the id, then each column's value, NULL past its list.
fn expected(cases: &[Case], i: usize) -> Row {
    let mut r = vec![("id".to_string(), V::Int32(i as i32))];
    for c in cases {
        r.push((
            c.column.into(),
            c.values.get(i).map_or(V::Null, |v| v.1.clone()),
        ));
    }
    Row(r)
}

#[tokio::test(flavor = "multi_thread")]
async fn every_mapped_type_round_trips_through_stream_and_snapshot() {
    require_it!();
    let a = admin().await;
    a.batch_execute("CREATE EXTENSION IF NOT EXISTS hstore SCHEMA public")
        .await
        .unwrap();
    let version: i32 = a
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .unwrap()
        .get(0);
    let cases = cases(version);
    let columns: Vec<String> = cases
        .iter()
        .map(|c| format!("{} {}", c.column, c.ty))
        .collect();
    let n = fresh(
        &a,
        "types",
        &format!(
            "CREATE TYPE mood AS ENUM ('sad', 'ok');
             CREATE TYPE pair AS (n int, s text);
             CREATE DOMAIN price AS numeric(10,2);
             CREATE TABLE t (id int PRIMARY KEY, {});",
            columns.join(", ")
        ),
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, tables, _) = first_stream(&c).await;
    let t = &tables[0];
    assert!(t.columns().iter().all(|c| c.source_ordinal.is_some()));

    // The writer's session differs from the pinned one in every setting the text depends on.
    let mut inserts = vec![
        "SET TimeZone = 'America/St_Johns'".to_string(),
        "SET DateStyle = 'German, DMY'".into(),
        "SET IntervalStyle = 'postgres_verbose'".into(),
        "SET extra_float_digits = 0".into(),
        format!("SET search_path = {}, public", n.schema),
        "BEGIN".into(),
    ];
    for i in 0..rows(&cases) {
        let values: Vec<&str> = cases
            .iter()
            .map(|c| c.values.get(i).map_or("NULL", |v| v.0))
            .collect();
        let names: Vec<&str> = cases.iter().map(|c| c.column).collect();
        inserts.push(format!(
            "INSERT INTO t (id, {}) VALUES ({i}, {})",
            names.join(", "),
            values.join(", ")
        ));
    }
    inserts.push("COMMIT".into());
    a.batch_execute(&inserts.join(";\n")).await.unwrap();

    let items = collect(&mut s, 1, 30).await;
    let streamed: Vec<&Row> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => e.after.as_ref(),
            StreamItem::TableHalt { reason, .. } => panic!("halted: {reason}"),
            _ => None,
        })
        .collect();
    let mut snapshot = vec![];
    let mut cursor: Option<KeysetCursor> = None;
    loop {
        let chunk = c.read_chunk(t, cursor.as_ref(), 2).await.unwrap();
        let Some(next) = chunk.next else { break };
        snapshot.extend(chunk.rows);
        cursor = Some(next);
    }
    assert_eq!(streamed.len(), rows(&cases));
    assert_eq!(snapshot.len(), rows(&cases));
    for i in 0..rows(&cases) {
        let want = expected(&cases, i);
        for (j, (column, value)) in want.0.iter().enumerate() {
            assert_eq!(
                &streamed[i].0[j],
                &(column.clone(), value.clone()),
                "stream row {i}"
            );
            assert_eq!(
                &snapshot[i].0[j],
                &(column.clone(), value.clone()),
                "snapshot row {i}"
            );
        }
        assert_eq!(streamed[i].0.len(), want.0.len());
    }
    // Every value fits the column type the schema declares.
    for r in &snapshot {
        for (column, value) in &r.0 {
            let col = t.column(column).unwrap();
            assert!(
                value.fits(&col.value_type),
                "{column}: {value:?} vs {:?}",
                col.value_type
            );
        }
    }
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Config §9.3's lossless conversions, through the stream and through snapshot chunks of one row.
/// Four of the converted columns form the key, so each chunk's cursor goes back to PostgreSQL as
/// converted values: an `int8` for an `int4`, a `timestamptz` text for a `date` and a
/// `timestamp`, a zone-less text for a `timestamptz`.
#[tokio::test(flavor = "multi_thread")]
async fn overrides_convert_through_stream_and_snapshot() {
    require_it!();
    const DAY: i64 = 86_400_000_000;
    const HOUR: i64 = 3_600_000_000;
    let a = admin().await;
    let n = fresh(
        &a,
        "convert",
        "CREATE TABLE t (id int, d date, ts timestamp, tstz timestamptz, s smallint, f real,
                         a int[], PRIMARY KEY (id, d, ts, tstz));",
    )
    .await;
    let overrides = format!(
        "      column_type_overrides: {{ {s}.t.id: int64, {s}.t.d: timestamp_tz, \
         {s}.t.ts: timestamp_tz, {s}.t.tstz: timestamp_ntz, {s}.t.s: \"decimal(7,2)\", \
         {s}.t.f: float64, {s}.t.a: \"list<int64>\" }}",
        s = n.schema
    );
    let c = connector(&pipeline(&n, &["*"], &overrides, ""));
    let (mut s, tables, _) = first_stream(&c).await;
    let t = &tables[0];
    let types: Vec<(&str, T)> = t
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), c.value_type.clone()))
        .collect();
    assert_eq!(
        types,
        [
            ("id", T::Int64),
            ("d", T::Timestamp { with_tz: true }),
            ("ts", T::Timestamp { with_tz: true }),
            ("tstz", T::Timestamp { with_tz: false }),
            (
                "s",
                T::Numeric {
                    precision: 7,
                    scale: 2
                }
            ),
            ("f", T::Float64),
            ("a", T::Array(Box::new(T::Int64))),
        ]
    );

    // The key's order is decided by each of its parts in turn.
    let keys = [
        (1, "2024-01-01", "2024-01-01 10:00", "2024-01-01 10:00+00"),
        (1, "2024-01-01", "2024-01-01 10:00", "2024-01-01 11:00+00"),
        (1, "2024-01-01", "2024-01-01 11:00", "2024-01-01 09:00+00"),
        (1, "2024-01-02", "2024-01-01 09:00", "2024-01-01 09:00+00"),
        (2, "2023-12-31", "2023-12-31 23:00", "2023-12-31 23:00+00"),
    ];
    let mut inserts = vec![
        "SET TimeZone = 'America/St_Johns'".to_string(),
        format!("SET search_path = {}", n.schema),
        "BEGIN".into(),
    ];
    for (id, d, ts, tstz) in keys {
        inserts.push(format!(
            "INSERT INTO t VALUES ({id}, '{d}', '{ts}', '{tstz}', -32768, 0.1, '{{1,NULL}}')"
        ));
    }
    inserts.push("COMMIT".into());
    a.batch_execute(&inserts.join(";\n")).await.unwrap();

    let micros = |day: i64, hour: i64| day * DAY + hour * HOUR;
    let expected: Vec<Row> = [
        (1, 19_723, 10, 10),
        (1, 19_723, 10, 11),
        (1, 19_723, 11, 9),
        (1, 19_724, 9, 9),
        (2, 19_722, 23, 23),
    ]
    .into_iter()
    .map(|(id, day, ts, tstz)| {
        let ts_day = if id == 2 { 19_722 } else { 19_723 };
        Row(vec![
            ("id".into(), V::Int64(id)),
            (
                "d".into(),
                V::Timestamp {
                    micros: micros(day, 0),
                    with_tz: true,
                },
            ),
            (
                "ts".into(),
                V::Timestamp {
                    micros: micros(ts_day, ts),
                    with_tz: true,
                },
            ),
            (
                "tstz".into(),
                V::Timestamp {
                    micros: micros(ts_day, tstz),
                    with_tz: false,
                },
            ),
            ("s".into(), dec(-32_768, 0)),
            ("f".into(), V::Float64(0.1)),
            ("a".into(), array(T::Int64, vec![V::Int64(1), V::Null])),
        ])
    })
    .collect();

    let items = collect(&mut s, 1, 30).await;
    let streamed: Vec<Row> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => e.after.clone(),
            StreamItem::TableHalt { reason, .. } => panic!("halted: {reason}"),
            _ => None,
        })
        .collect();
    assert_eq!(streamed, expected, "the stream");
    let mut snapshot = vec![];
    let mut cursor: Option<KeysetCursor> = None;
    loop {
        let chunk = c.read_chunk(t, cursor.as_ref(), 1).await.unwrap();
        let Some(next) = chunk.next else { break };
        snapshot.extend(chunk.rows);
        cursor = Some(next);
    }
    assert_eq!(snapshot, expected, "snapshot chunks of one row");
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}
