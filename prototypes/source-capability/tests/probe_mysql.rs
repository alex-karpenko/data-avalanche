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
