//! MySQL / MariaDB source probes (gate DA-SRC-11). Needs the `mysql` service (and `mariadb` for
//! the M-gates) from docker-compose.yml. Both candidates are probed side by side:
//!   (a) mysql-binlog-connector-rust 0.3.3     (b) mysql_async 0.37.1 binlog stream
//!
//!   SRC_PROBE=1 cargo test --features mysql --test probe_mysql -- --nocapture --test-threads=1

#![cfg(feature = "mysql")]

use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use mysql_async::prelude::Queryable;

const URL: &str = "mysql://root:probe@127.0.0.1:53306/probe";

fn enabled() -> bool {
    std::env::var("SRC_PROBE").is_ok_and(|v| !v.is_empty())
}

async fn setup(table: &str) -> anyhow::Result<(mysql_async::Conn, String)> {
    let mut c = mysql_async::Conn::from_url(URL).await.context("control connect")?;
    c.query_drop(format!("DROP TABLE IF EXISTS {table}")).await?;
    c.query_drop(format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, val VARCHAR(32))")).await?;
    let gtid: String = c.query_first("SELECT @@GLOBAL.gtid_executed").await?.unwrap_or_default();
    Ok((c, gtid.replace('\n', "")))
}

/// S-MY0a — smoke, candidate (a): GTID-positioned dump, one INSERT → TableMap + WriteRows.
#[tokio::test(flavor = "multi_thread")]
async fn s_my0a_smoke_binlog_connector() -> anyhow::Result<()> {
    use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
    use mysql_binlog_connector_rust::event::event_data::EventData;
    if !enabled() {
        println!("S-MY0a skipped: set SRC_PROBE=1 (and `docker compose up -d --wait`)");
        return Ok(());
    }
    let (mut c, gtid) = setup("s_my0a").await?;
    let mut client = BinlogClient::new(URL, 1001, StartPosition::Gtid(gtid.clone()))
        .with_master_heartbeat(Duration::from_secs(1))
        .with_read_timeout(Duration::from_secs(10));
    let mut stream = client.connect().await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
    c.query_drop("INSERT INTO s_my0a VALUES (1, 'hello')").await?;

    let (mut table_id, mut rows) = (None, 0usize);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while rows == 0 && tokio::time::Instant::now() < deadline {
        let (_h, data) = stream.read().await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
        match data {
            EventData::TableMap(t) if t.table_name == "s_my0a" => table_id = Some(t.table_id),
            EventData::WriteRows(w) if Some(w.table_id) == table_id => rows += w.rows.len(),
            _ => {}
        }
    }
    assert_eq!(rows, 1, "one WriteRows row for s_my0a after GTID set {gtid}");
    println!("S-MY0a ✅ mysql-binlog-connector-rust: GTID dump from {gtid:?} → TableMap + 1 WriteRows row");
    let _ = stream.close().await;
    Ok(())
}

/// S-MY0b — smoke, candidate (b): mysql_async binlog stream with a GTID set, one INSERT → WriteRows.
#[tokio::test(flavor = "multi_thread")]
async fn s_my0b_smoke_mysql_async() -> anyhow::Result<()> {
    use mysql_async::binlog::events::{EventData, RowsEventData};
    if !enabled() {
        println!("S-MY0b skipped: set SRC_PROBE=1 (and `docker compose up -d --wait`)");
        return Ok(());
    }
    let (mut c, gtid) = setup("s_my0b").await?;
    let sids: Vec<mysql_async::Sid<'static>> = gtid
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().parse().map_err(|e| anyhow::anyhow!("gtid {s}: {e:?}")))
        .collect::<anyhow::Result<_>>()?;
    let req = mysql_async::BinlogStreamRequest::new(1002).with_gtid().with_gtid_set(sids);
    let mut stream = mysql_async::Conn::from_url(URL).await?.get_binlog_stream(req).await?;
    c.query_drop("INSERT INTO s_my0b VALUES (1, 'hello')").await?;

    let mut rows = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while rows == 0 && tokio::time::Instant::now() < deadline {
        let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(5), stream.next()).await else { continue };
        let ev = ev?;
        if let Some(EventData::RowsEvent(RowsEventData::WriteRowsEvent(w))) = ev.read_data()? {
            let tme = stream.get_tme(w.table_id()).context("TableMap for rows event")?;
            if tme.table_name() == "s_my0b" {
                rows += w.rows(tme).count();
            }
        }
    }
    assert_eq!(rows, 1, "one WriteRows row for s_my0b after GTID set {gtid}");
    println!("S-MY0b ✅ mysql_async: GTID dump from {gtid:?} → 1 WriteRows row");
    Ok(())
}

// ================================================================================================
// Gates S-MY1…S-MY9 — each scenario runs against BOTH candidates through a normalizing adapter.

use std::collections::HashMap;

const MARIA_URL: &str = "mysql://root:probe@127.0.0.1:53307/probe";

#[derive(Debug, Clone, PartialEq)]
enum NEv {
    Gtid(String),
    Query(String),
    Map { table: String, names: Vec<String>, unsigned: Vec<Option<bool>> },
    Write { table: String, rows: Vec<Vec<String>> },
    Update { table: String, rows: Vec<(Vec<String>, Vec<String>)> },
    Delete { table: String, rows: Vec<Vec<String>> },
    Xid,
    Heartbeat,
    Payload,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Cand {
    A, // mysql-binlog-connector-rust
    B, // mysql_async
}

/// How to position the stream.
#[derive(Clone)]
enum Pos {
    MySqlGtid(String),
    FilePos(String, u32),
    /// MariaDB GTID via session variables (`@slave_connect_state`) — candidate (b) only.
    MariaGtid(String),
}

// ---- candidate (a)

fn a_val(v: &mysql_binlog_connector_rust::column::column_value::ColumnValue) -> String {
    use mysql_binlog_connector_rust::column::column_value::ColumnValue as V;
    use mysql_binlog_connector_rust::column::json::json_binary::JsonBinary;
    match v {
        V::None => "NULL".into(),
        V::Tiny(x) => x.to_string(),
        V::Short(x) => x.to_string(),
        V::Long(x) => x.to_string(),
        V::LongLong(x) => x.to_string(),
        V::Float(x) => x.to_string(),
        V::Double(x) => x.to_string(),
        V::Decimal(s) | V::Time(s) | V::Date(s) | V::DateTime(s) => s.clone(),
        V::Timestamp(x) => format!("ts:{x}"),
        V::Year(x) => x.to_string(),
        V::String(b) => String::from_utf8_lossy(b).into_owned(),
        V::Blob(b) => format!("0x{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>()),
        V::Bit(x) => format!("bit:{x}"),
        V::Set(x) => format!("set:{x}"),
        V::Enum(x) => format!("enum:{x}"),
        V::Json(b) => JsonBinary::parse_as_string(b).unwrap_or_else(|e| format!("json-err:{e:?}")),
    }
}

async fn a_stream(url: &str, pos: Pos, heartbeat: Option<Duration>) -> anyhow::Result<mysql_binlog_connector_rust::binlog_stream::BinlogStream> {
    use mysql_binlog_connector_rust::binlog_client::{BinlogClient, StartPosition};
    let sp = match pos {
        Pos::MySqlGtid(g) => StartPosition::Gtid(g),
        Pos::FilePos(f, p) => StartPosition::BinlogPosition(f, p),
        Pos::MariaGtid(_) => anyhow::bail!("candidate (a) has no MariaDB GTID positioning"),
    };
    let mut c = BinlogClient::new(url, 2001, sp).with_read_timeout(Duration::from_secs(10));
    if let Some(h) = heartbeat {
        c = c.with_master_heartbeat(h);
    }
    c.connect().await.map_err(|e| anyhow::anyhow!("(a) connect: {e:?}"))
}

fn a_norm(
    data: mysql_binlog_connector_rust::event::event_data::EventData,
    tables: &mut HashMap<u64, String>,
    out: &mut Vec<NEv>,
) {
    use mysql_binlog_connector_rust::event::event_data::EventData as E;
    let row = |r: &mysql_binlog_connector_rust::event::row_event::RowEvent| r.column_values.iter().map(a_val).collect::<Vec<_>>();
    match data {
        E::Gtid(g) => out.push(NEv::Gtid(g.gtid)),
        E::Query(q) => out.push(NEv::Query(q.query)),
        E::TableMap(t) => {
            tables.insert(t.table_id, t.table_name.clone());
            let (names, unsigned) = match &t.table_metadata {
                Some(md) => (
                    md.columns.iter().map(|c| c.column_name.clone().unwrap_or_default()).collect(),
                    // FINDING (S-MY1): despite its name, `is_signed` carries MySQL's SIGNEDNESS bit,
                    // which is 1 = UNSIGNED. Read literally it inverts every numeric column.
                    md.columns.iter().map(|c| c.is_signed).collect(),
                ),
                None => (vec![], vec![]),
            };
            out.push(NEv::Map { table: t.table_name, names, unsigned });
        }
        E::WriteRows(w) => out.push(NEv::Write { table: tables.get(&w.table_id).cloned().unwrap_or_default(), rows: w.rows.iter().map(row).collect() }),
        E::UpdateRows(u) => out.push(NEv::Update {
            table: tables.get(&u.table_id).cloned().unwrap_or_default(),
            rows: u.rows.iter().map(|(b, a)| (row(b), row(a))).collect(),
        }),
        E::DeleteRows(d) => out.push(NEv::Delete { table: tables.get(&d.table_id).cloned().unwrap_or_default(), rows: d.rows.iter().map(row).collect() }),
        E::Xid(_) => out.push(NEv::Xid),
        E::HeartBeat => out.push(NEv::Heartbeat),
        E::TransactionPayload(p) => {
            out.push(NEv::Payload);
            for (_h, inner) in p.uncompressed_events {
                a_norm(inner, tables, out);
            }
        }
        _ => {}
    }
}

// ---- candidate (b)

fn b_val(v: mysql_async::binlog::value::BinlogValue<'static>) -> String {
    use mysql_async::Value as V;
    match V::try_from(v) {
        Ok(V::NULL) => "NULL".into(),
        Ok(V::Bytes(b)) => match String::from_utf8(b.clone()) {
            Ok(s) => s,
            Err(_) => format!("0x{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>()),
        },
        Ok(V::Int(x)) => x.to_string(),
        Ok(V::UInt(x)) => x.to_string(),
        Ok(V::Float(x)) => x.to_string(),
        Ok(V::Double(x)) => x.to_string(),
        Ok(V::Date(y, mo, d, h, mi, s, us)) => format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}"),
        Ok(V::Time(neg, d, h, mi, s, us)) => format!("{}{}d {h:02}:{mi:02}:{s:02}.{us:06}", if neg { "-" } else { "" }, d),
        Err(e) => format!("err:{e}"),
    }
}

async fn b_conn(url: &str, pos: &Pos, heartbeat: Option<Duration>) -> anyhow::Result<mysql_async::BinlogStream> {
    let mut conn = mysql_async::Conn::from_url(url).await?;
    if let Some(h) = heartbeat {
        conn.query_drop(format!("SET @master_heartbeat_period = {}", h.as_nanos())).await?;
    }
    let mut req = mysql_async::BinlogStreamRequest::new(2002);
    let file;
    match pos {
        Pos::MySqlGtid(g) => {
            let sids: Vec<mysql_async::Sid<'static>> = g
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().parse().map_err(|e| anyhow::anyhow!("gtid {s}: {e:?}")))
                .collect::<anyhow::Result<_>>()?;
            req = req.with_gtid().with_gtid_set(sids);
        }
        Pos::FilePos(f, p) => {
            file = f.clone().into_bytes();
            req = req.with_filename(&file).with_pos(*p as u64);
        }
        Pos::MariaGtid(g) => {
            conn.query_drop("SET @mariadb_slave_capability = 4").await?;
            conn.query_drop(format!("SET @slave_connect_state = '{g}'")).await?;
            conn.query_drop("SET @slave_gtid_strict_mode = 1").await?;
        }
    }
    Ok(conn.get_binlog_stream(req).await?)
}

fn b_norm(ev: &mysql_async::binlog::events::Event, stream: &mysql_async::BinlogStream, out: &mut Vec<NEv>) -> anyhow::Result<()> {
    use mysql_async::binlog::events::{EventData as E, OptionalMetadataField, RowsEventData as R};
    use mysql_common::constants::ColumnType as T;
    // MariaDB GTID_EVENT (type 162) is not modelled by mysql_common — decode the fixed prefix from the
    // raw body ourselves: seq_no u64 LE, domain_id u32 LE, flags u8. server_id is in the header.
    if ev.header().event_type_raw() == 162 {
        let d = ev.data();
        anyhow::ensure!(d.len() >= 13, "short MariaDB GTID event");
        let seq = u64::from_le_bytes(d[0..8].try_into()?);
        let domain = u32::from_le_bytes(d[8..12].try_into()?);
        out.push(NEv::Gtid(format!("{domain}-{}-{seq}", ev.header().server_id())));
        return Ok(());
    }
    let Some(data) = ev.read_data()? else { return Ok(()) };
    let rows_of = |tid: u64, rows: &mut dyn FnMut(&mysql_async::binlog::events::TableMapEvent<'static>) -> anyhow::Result<NEv>| -> anyhow::Result<Option<NEv>> {
        match stream.get_tme(tid) {
            Some(t) => Ok(Some(rows(t)?)),
            None => Ok(None),
        }
    };
    let vals = |r: Option<mysql_async::binlog::row::BinlogRow>| -> Vec<String> { r.map(|r| r.unwrap().into_iter().map(b_val).collect()).unwrap_or_default() };
    match data {
        E::GtidEvent(g) => out.push(NEv::Gtid(format!("{}:{}", uuid_str(&g.sid()), g.gno()))),
        E::QueryEvent(q) => out.push(NEv::Query(q.query().into_owned())),
        E::TableMapEvent(t) => {
            let n = t.columns_count() as usize;
            let mut names = vec![String::new(); n];
            let mut unsigned = vec![None; n];
            for f in t.iter_optional_meta() {
                match f? {
                    OptionalMetadataField::ColumnName(cn) => {
                        for (i, x) in cn.iter_names().enumerate() {
                            names[i] = x?.name().into_owned();
                        }
                    }
                    OptionalMetadataField::Signedness(bits) => {
                        let mut k = 0;
                        for i in 0..n {
                            let numeric = matches!(
                                t.get_column_type(i),
                                Ok(Some(T::MYSQL_TYPE_TINY | T::MYSQL_TYPE_SHORT | T::MYSQL_TYPE_INT24 | T::MYSQL_TYPE_LONG
                                    | T::MYSQL_TYPE_LONGLONG | T::MYSQL_TYPE_FLOAT | T::MYSQL_TYPE_DOUBLE | T::MYSQL_TYPE_NEWDECIMAL))
                            );
                            if numeric {
                                unsigned[i] = bits.get(k).map(|b| *b);
                                k += 1;
                            }
                        }
                    }
                    _ => {}
                }
            }
            out.push(NEv::Map { table: t.table_name().into_owned(), names, unsigned });
        }
        E::RowsEvent(re) => {
            let tid = re.table_id();
            let ev = rows_of(tid, &mut |t| {
                let table = t.table_name().into_owned();
                let mut pairs = vec![];
                for r in re.rows(t) {
                    let (b, a) = r?;
                    pairs.push((vals(b), vals(a)));
                }
                Ok(match &re {
                    R::WriteRowsEvent(_) | R::WriteRowsEventV1(_) => NEv::Write { table, rows: pairs.into_iter().map(|p| p.1).collect() },
                    R::UpdateRowsEvent(_) | R::UpdateRowsEventV1(_) | R::PartialUpdateRowsEvent(_) => NEv::Update { table, rows: pairs },
                    R::DeleteRowsEvent(_) | R::DeleteRowsEventV1(_) => NEv::Delete { table, rows: pairs.into_iter().map(|p| p.0).collect() },
                })
            })?;
            out.extend(ev);
        }
        E::XidEvent(_) => out.push(NEv::Xid),
        E::HeartbeatEvent => out.push(NEv::Heartbeat),
        E::TransactionPayloadEvent(_) => out.push(NEv::Payload),
        _ => {}
    }
    Ok(())
}

fn uuid_str(b: &[u8; 16]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// Stream from `pos` with candidate `cand`, running `work` on a separate connection, and collect
/// normalized events until `done(events)` or `secs` elapse.
async fn run(
    cand: Cand,
    url: &str,
    pos: Pos,
    heartbeat: Option<Duration>,
    work: &[&str],
    secs: u64,
    done: impl Fn(&[NEv]) -> bool,
) -> anyhow::Result<Vec<NEv>> {
    let mut out = vec![];
    let mut w = mysql_async::Conn::from_url(url).await?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    match cand {
        Cand::A => {
            let mut s = a_stream(url, pos, heartbeat).await?;
            for q in work {
                w.query_drop(*q).await?;
            }
            let mut tables = HashMap::new();
            while !done(&out) && tokio::time::Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_secs(3), s.read()).await {
                    Ok(Ok((_h, d))) => a_norm(d, &mut tables, &mut out),
                    Ok(Err(e)) => anyhow::bail!("(a) read: {e:?}"),
                    Err(_) => {}
                }
            }
            let _ = s.close().await;
        }
        Cand::B => {
            let mut s = b_conn(url, &pos, heartbeat).await?;
            for q in work {
                w.query_drop(*q).await?;
            }
            while !done(&out) && tokio::time::Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_secs(3), s.next()).await {
                    Ok(Some(Ok(ev))) => b_norm(&ev, &s, &mut out)?,
                    Ok(Some(Err(e))) => anyhow::bail!("(b) read: {e}"),
                    Ok(None) => break,
                    Err(_) => {}
                }
            }
        }
    }
    Ok(out)
}

async fn gtid_now(url: &str) -> anyhow::Result<String> {
    let mut c = mysql_async::Conn::from_url(url).await?;
    let g: String = c.query_first("SELECT @@GLOBAL.gtid_executed").await?.unwrap_or_default();
    Ok(g.replace('\n', ""))
}

async fn exec(url: &str, sql: &[&str]) -> anyhow::Result<()> {
    let mut c = mysql_async::Conn::from_url(url).await?;
    for q in sql {
        c.query_drop(*q).await?;
    }
    Ok(())
}

fn writes(evs: &[NEv], table: &str) -> Vec<Vec<String>> {
    evs.iter().filter_map(|e| match e { NEv::Write { table: t, rows } if t == table => Some(rows.clone()), _ => None }).flatten().collect()
}

macro_rules! both {
    ($name:ident, $body:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() -> anyhow::Result<()> {
            if !enabled() {
                return Ok(println!("{} skipped", stringify!($name)));
            }
            for cand in [Cand::A, Cand::B] {
                match $body(cand).await {
                    Ok(msg) => println!("{} {:?} ✅ {msg}", stringify!($name), cand),
                    Err(e) => println!("{} {:?} ❌ {e:#}", stringify!($name), cand),
                }
            }
            Ok(())
        }
    };
}

// ---- gates

/// S-MY1 — optional TABLE_MAP metadata: column names + UNSIGNED flags (binlog_row_metadata=FULL).
async fn my1(cand: Cand) -> anyhow::Result<String> {
    exec(URL, &["DROP TABLE IF EXISTS my1", "CREATE TABLE my1 (id BIGINT UNSIGNED PRIMARY KEY, a INT, b INT UNSIGNED, c VARCHAR(10))"]).await?;
    let evs = run(cand, URL, Pos::MySqlGtid(gtid_now(URL).await?), None, &["INSERT INTO my1 VALUES (1, -1, 2, 'x')"], 15, |e| !writes(e, "my1").is_empty()).await?;
    let map = evs.iter().find_map(|e| match e { NEv::Map { table, names, unsigned } if table == "my1" => Some((names.clone(), unsigned.clone())), _ => None }).context("no TableMap")?;
    anyhow::ensure!(map.0 == ["id", "a", "b", "c"], "names {:?}", map.0);
    anyhow::ensure!(map.1 == [Some(true), Some(false), Some(true), None], "unsigned {:?}", map.1);
    Ok(format!("names {:?}, unsigned {:?}", map.0, map.1))
}
both!(s_my1_optional_metadata, my1);

/// S-MY2 — full row images: UPDATE before+after, DELETE full before image.
async fn my2(cand: Cand) -> anyhow::Result<String> {
    exec(URL, &["DROP TABLE IF EXISTS my2", "CREATE TABLE my2 (id INT PRIMARY KEY, a VARCHAR(8), b INT)", "INSERT INTO my2 VALUES (1,'a',10)"]).await?;
    let evs = run(cand, URL, Pos::MySqlGtid(gtid_now(URL).await?), None,
        &["UPDATE my2 SET a='b' WHERE id=1", "DELETE FROM my2 WHERE id=1"], 15,
        |e| e.iter().any(|x| matches!(x, NEv::Delete { .. }))).await?;
    let upd = evs.iter().find_map(|e| match e { NEv::Update { rows, .. } => Some(rows[0].clone()), _ => None }).context("no update")?;
    let del = evs.iter().find_map(|e| match e { NEv::Delete { rows, .. } => Some(rows[0].clone()), _ => None }).context("no delete")?;
    anyhow::ensure!(upd.0 == ["1", "a", "10"] && upd.1 == ["1", "b", "10"], "update {upd:?}");
    anyhow::ensure!(del == ["1", "b", "10"], "delete {del:?}");
    Ok(format!("update {:?} → {:?}; delete {:?}", upd.0, upd.1, del))
}
both!(s_my2_full_row_images, my2);

/// S-MY3 — DDL arrives as QUERY_EVENT text (DDL-classifier input).
async fn my3(cand: Cand) -> anyhow::Result<String> {
    exec(URL, &["DROP TABLE IF EXISTS my3, my3r", "CREATE TABLE my3 (id INT PRIMARY KEY)"]).await?;
    let ddl = ["ALTER TABLE my3 ADD COLUMN x INT", "TRUNCATE TABLE my3", "RENAME TABLE my3 TO my3r", "DROP TABLE my3r"];
    let evs = run(cand, URL, Pos::MySqlGtid(gtid_now(URL).await?), None, &ddl, 15,
        |e| e.iter().any(|x| matches!(x, NEv::Query(q) if q.contains("DROP TABLE")))).await?;
    let qs: Vec<String> = evs.iter().filter_map(|e| if let NEv::Query(q) = e { Some(q.clone()) } else { None }).collect();
    for d in ddl {
        let head = d.split(' ').take(2).collect::<Vec<_>>().join(" ");
        anyhow::ensure!(qs.iter().any(|q| q.starts_with(&head)), "{head} missing in {qs:?}");
    }
    Ok(format!("{} DDL QUERY_EVENTs: {:?}", qs.len(), qs))
}
both!(s_my3_ddl_query_events, my3);

/// S-MY4 — GTID resume: reconnect with the executed set → only later transactions arrive.
async fn my4(cand: Cand) -> anyhow::Result<String> {
    exec(URL, &["DROP TABLE IF EXISTS my4", "CREATE TABLE my4 (id INT PRIMARY KEY)", "INSERT INTO my4 VALUES (1)"]).await?;
    let after_1 = gtid_now(URL).await?;
    exec(URL, &["INSERT INTO my4 VALUES (2)"]).await?;
    let evs = run(cand, URL, Pos::MySqlGtid(after_1.clone()), None, &["INSERT INTO my4 VALUES (3)"], 15, |e| writes(e, "my4").len() >= 2).await?;
    let ids: Vec<String> = writes(&evs, "my4").into_iter().map(|r| r[0].clone()).collect();
    anyhow::ensure!(ids == ["2", "3"], "ids {ids:?}");
    let gtids = evs.iter().filter(|e| matches!(e, NEv::Gtid(_))).count();
    let xids = evs.iter().filter(|e| matches!(e, NEv::Xid)).count();
    Ok(format!("resumed from {after_1}: rows {ids:?} (id 1 skipped); {gtids} GTID + {xids} XID boundaries"))
}
both!(s_my4_gtid_resume, my4);

/// S-MY5 — heartbeats while idle (checksum CRC32 is the 8.4 default and parses throughout).
async fn my5(cand: Cand) -> anyhow::Result<String> {
    let evs = run(cand, URL, Pos::MySqlGtid(gtid_now(URL).await?), Some(Duration::from_secs(1)), &[], 6,
        |e| e.iter().filter(|x| matches!(x, NEv::Heartbeat)).count() >= 3).await?;
    let hb = evs.iter().filter(|x| matches!(x, NEv::Heartbeat)).count();
    anyhow::ensure!(hb >= 3, "only {hb} heartbeats in 6 s at 1 s period");
    Ok(format!("{hb} heartbeats while idle (period 1 s)"))
}
both!(s_my5_heartbeat, my5);

/// S-MY6 — binlog transaction compression (zstd TRANSACTION_PAYLOAD) decodes.
async fn my6(cand: Cand) -> anyhow::Result<String> {
    exec(URL, &["DROP TABLE IF EXISTS my6", "CREATE TABLE my6 (id INT PRIMARY KEY, pad VARCHAR(200))"]).await?;
    let gtid = gtid_now(URL).await?;
    let mut w = mysql_async::Conn::from_url(URL).await?;
    w.query_drop("SET SESSION binlog_transaction_compression = ON").await?;
    let work = ["SELECT 1"];
    let evs = {
        let fut = run(cand, URL, Pos::MySqlGtid(gtid), None, &work, 15, |e| writes(e, "my6").len() >= 100);
        tokio::pin!(fut);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let ins = format!("INSERT INTO my6 VALUES {}", (1..=100).map(|i| format!("({i}, REPEAT('z', 150))")).collect::<Vec<_>>().join(","));
        let (r, _) = tokio::join!(fut, async { w.query_drop(ins).await });
        r?
    };
    let payloads = evs.iter().filter(|e| matches!(e, NEv::Payload)).count();
    let n = writes(&evs, "my6").len();
    anyhow::ensure!(payloads >= 1 && n == 100, "payload events {payloads}, rows {n}");
    Ok(format!("{payloads} TRANSACTION_PAYLOAD event(s) → {n} rows decoded"))
}
both!(s_my6_compressed_payload, my6);

/// S-MY7 — value decode for the Type_Mapping MySQL rows.
async fn my7(cand: Cand) -> anyhow::Result<String> {
    exec(URL, &[
        "DROP TABLE IF EXISTS my7",
        "CREATE TABLE my7 (id INT PRIMARY KEY, bu BIGINT UNSIGNED, tb TINYINT(1), d DECIMAL(38,10), dt DATETIME(6), ts TIMESTAMP(6),
          tm TIME(6), y YEAR, j JSON, bt BIT(8), e ENUM('lo','hi'), s SET('r','g','b'), bl BLOB, v VARCHAR(16), dbl DOUBLE)",
    ]).await?;
    let evs = run(cand, URL, Pos::MySqlGtid(gtid_now(URL).await?), None,
        &["SET time_zone = '+03:00'",
          "INSERT INTO my7 VALUES (1, 18446744073709551615, 1, -1234567890123456789012345678.0123456789,
            '2026-09-26 12:34:56.789012', '2026-09-26 12:34:56.789012', '-12:34:56.000001', 2026,
            '{\"a\": [1, 2]}', b'10100101', 'hi', 'r,b', X'DEADBEEF', 'héllo ✓', 1.5)"],
        15, |e| !writes(e, "my7").is_empty()).await?;
    let row = writes(&evs, "my7").pop().context("no row")?;
    let cols = ["id", "bu", "tb", "d", "dt", "ts", "tm", "y", "j", "bt", "e", "s", "bl", "v", "dbl"];
    let shown: Vec<String> = cols.iter().zip(row.iter()).map(|(c, v)| format!("{c}={v}")).collect();
    // hard requirements (value-exact); representation differences are reported, not failed
    anyhow::ensure!(row[3] == "-1234567890123456789012345678.0123456789", "DECIMAL not exact: {}", row[3]);
    anyhow::ensure!(row[13] == "héllo ✓", "utf8mb4: {}", row[13]);
    anyhow::ensure!(row[8].replace(' ', "").contains("\"a\":[1,2]"), "JSON: {}", row[8]);
    Ok(shown.join(" · "))
}
both!(s_my7_type_decode, my7);

/// S-MY8 — TLS: a `REQUIRE SSL` account can stream.
async fn my8(cand: Cand) -> anyhow::Result<String> {
    // FINDING: two rustls crypto backends are linked (ring via pgwire-replication, aws-lc-rs via
    // mysql_async) — rustls cannot choose automatically; the process must install one.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    exec(URL, &[
        "DROP USER IF EXISTS 'tlsprobe'@'%'",
        "CREATE USER 'tlsprobe'@'%' IDENTIFIED BY 'probe' REQUIRE SSL",
        "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'tlsprobe'@'%'",
    ]).await?;
    let gtid = gtid_now(URL).await?;
    match cand {
        Cand::A => {
            let url = "mysql://tlsprobe:probe@127.0.0.1:53306/probe?ssl-mode=required";
            let mut s = a_stream(url, Pos::MySqlGtid(gtid), None).await?;
            let _ = tokio::time::timeout(Duration::from_secs(2), s.read()).await;
            Ok("connected as REQUIRE SSL user".into())
        }
        Cand::B => {
            let opts = mysql_async::OptsBuilder::from_opts(mysql_async::Opts::from_url("mysql://tlsprobe:probe@127.0.0.1:53306/probe")?)
                .ssl_opts(mysql_async::SslOpts::default().with_danger_accept_invalid_certs(true));
            let mut conn = mysql_async::Conn::new(opts).await?;
            let cipher: Option<(String, String)> = conn.query_first("SHOW SESSION STATUS LIKE 'Ssl_cipher'").await?;
            let sids: Vec<mysql_async::Sid<'static>> = gtid.split(',').map(|s| s.trim().parse().unwrap()).collect();
            let mut s = conn.get_binlog_stream(mysql_async::BinlogStreamRequest::new(2003).with_gtid().with_gtid_set(sids)).await?;
            exec(URL, &["DROP TABLE IF EXISTS my8", "CREATE TABLE my8 (id INT PRIMARY KEY)"]).await?;
            let got = tokio::time::timeout(Duration::from_secs(5), s.next()).await.is_ok();
            anyhow::ensure!(got, "no event over TLS");
            Ok(format!("REQUIRE SSL user streams over TLS (cipher {:?})", cipher.map(|c| c.1)))
        }
    }
}
both!(s_my8_tls, my8);

/// S-MY9 — MariaDB 11.4: GTID positioning (MariaDB dialect) and row decode.
async fn my9(cand: Cand) -> anyhow::Result<String> {
    exec(MARIA_URL, &["DROP TABLE IF EXISTS my9", "CREATE TABLE my9 (id INT PRIMARY KEY, v VARCHAR(8))", "INSERT INTO my9 VALUES (1,'a')"]).await?;
    let mut c = mysql_async::Conn::from_url(MARIA_URL).await?;
    let gtid: String = c.query_first("SELECT @@GLOBAL.gtid_binlog_pos").await?.unwrap_or_default();
    // MariaDB 11.4 has no MySQL-8.4 `SHOW BINARY LOG STATUS`
    let (file, pos): (String, u64) = c.query_first("SHOW MASTER STATUS").await?
        .map(|r: mysql_async::Row| (r.get::<String, _>(0).unwrap(), r.get::<u64, _>(1).unwrap()))
        .context("SHOW MASTER STATUS")?;
    let work = ["INSERT INTO my9 VALUES (2,'b')", "UPDATE my9 SET v='c' WHERE id=2"];
    let gtid_try = run(cand, MARIA_URL, Pos::MariaGtid(gtid.clone()), None, &work, 10,
        |e| e.iter().any(|x| matches!(x, NEv::Update { .. }))).await;
    let after: String = c.query_first("SELECT @@GLOBAL.gtid_binlog_pos").await?.unwrap_or_default();
    let gtid_res = match &gtid_try {
        Ok(evs) => {
            let ids: Vec<String> = writes(evs, "my9").iter().map(|r| r[0].clone()).collect();
            let gtids: Vec<String> = evs.iter().filter_map(|e| if let NEv::Gtid(g) = e { Some(g.clone()) } else { None }).collect();
            anyhow::ensure!(ids == ["2"], "resume from {gtid} must skip id 1: {ids:?}");
            anyhow::ensure!(gtids.last() == Some(&after), "last txn GTID {:?} must equal gtid_binlog_pos {after}", gtids.last());
            format!("MariaDB GTID resume from {gtid}: rows {ids:?} + update; per-txn GTIDs {gtids:?} (last = gtid_binlog_pos)")
        }
        Err(e) => format!("MariaDB GTID ❌ {e:#}"),
    };
    let fp_try = run(cand, MARIA_URL, Pos::FilePos(file.clone(), pos as u32), None, &["INSERT INTO my9 VALUES (3,'d')"], 10,
        |e| writes(e, "my9").iter().any(|r| r[0] == "3")).await;
    let fp_res = match &fp_try {
        Ok(evs) => format!("file/pos {file}:{pos}: rows {:?}", writes(evs, "my9").iter().map(|r| r[0].clone()).collect::<Vec<_>>()),
        Err(e) => format!("file/pos ❌ {e:#}"),
    };
    anyhow::ensure!(gtid_try.is_ok() || fp_try.is_ok(), "{gtid_res} | {fp_res}");
    Ok(format!("{gtid_res} | {fp_res}"))
}
both!(s_my9_mariadb, my9);
