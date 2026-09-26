//! PostgreSQL source probes (gate DA-SRC-10). Needs the `postgres` service from docker-compose.yml.
//!
//!   SRC_PROBE=1 cargo test --features pg --test probe_pg -- --nocapture --test-threads=1
//!
//! `pgwire-replication` is transport-level: it frames `START_REPLICATION`, keepalives and standby
//! status updates and decodes only Begin/Commit; every other pgoutput message arrives raw in
//! `XLogData.data`. The smoke probe checks the transport end to end and reads just the message tag.

#![cfg(feature = "pg")]

use std::time::Duration;

use anyhow::Context;
use pgwire_replication::{client::ReplicationEvent, Lsn, ReplicationClient, ReplicationConfig, TlsConfig};
use tokio_postgres::NoTls;

const HOST: &str = "127.0.0.1";
const PORT: u16 = 55432;
const USER: &str = "probe";
const PASS: &str = "probe";
const DB: &str = "probe";

fn enabled() -> bool {
    std::env::var("SRC_PROBE").is_ok_and(|v| !v.is_empty())
}

async fn control() -> anyhow::Result<tokio_postgres::Client> {
    let dsn = format!("host={HOST} port={PORT} user={USER} password={PASS} dbname={DB}");
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls).await.context("control connect")?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

/// S-PG0 — smoke: slot + publication, stream from the slot's LSN, one INSERT arrives as pgoutput
/// `R` (Relation) then `I` (Insert) inside Begin/Commit; the ack advances `confirmed_flush_lsn`.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg0_smoke_insert_roundtrip() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-PG0 skipped: set SRC_PROBE=1 (and `docker compose up -d --wait`)");
        return Ok(());
    }
    let (slot, publ) = ("probe_s_pg0", "probe_s_pg0_pub");
    let c = control().await?;
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS s_pg0; CREATE TABLE s_pg0 (id bigint PRIMARY KEY, val text);
         DROP PUBLICATION IF EXISTS {publ}; CREATE PUBLICATION {publ} FOR TABLE s_pg0;"
    ))
    .await?;
    let _ = c.execute(&format!("SELECT pg_drop_replication_slot('{slot}')"), &[]).await;
    let row = c
        .query_one(&format!("SELECT lsn::text FROM pg_create_logical_replication_slot('{slot}','pgoutput')"), &[])
        .await?;
    let start = Lsn::parse(&row.get::<_, String>(0))?;

    let cfg = ReplicationConfig::new(HOST, USER, PASS, DB, slot, publ)
        .with_port(PORT)
        .with_tls(TlsConfig::disabled())
        .with_start_lsn(start)
        .with_status_interval(Duration::from_millis(200));
    let mut repl = ReplicationClient::connect(cfg).await?;
    c.execute("INSERT INTO s_pg0 VALUES (1, 'hello')", &[]).await?;

    let mut tags = String::new();
    let mut commit_end = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while commit_end.is_none() && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(5), repl.recv()).await {
            Ok(Ok(Some(ReplicationEvent::Begin { .. }))) => tags.push('B'),
            Ok(Ok(Some(ReplicationEvent::XLogData { data, .. }))) => {
                if let Some(t) = data.first() {
                    tags.push(*t as char)
                }
            }
            Ok(Ok(Some(ReplicationEvent::Commit { end_lsn, .. }))) => {
                tags.push('C');
                commit_end = Some(end_lsn);
            }
            Ok(Ok(Some(_))) => {}
            Ok(Ok(None)) => break,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {}
        }
    }
    let end = commit_end.context("no Commit within 20 s")?;
    assert!(tags.contains("RI"), "expected Relation then Insert, got {tags:?}");
    repl.update_applied_lsn(end);
    tokio::time::sleep(Duration::from_millis(800)).await; // > status interval
    let flushed: String = c
        .query_one("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", &[&slot])
        .await?
        .get(0);
    let flushed = Lsn::parse(&flushed)?;
    assert!(flushed >= end, "ack must advance confirmed_flush_lsn: {flushed} < {end}");
    println!("S-PG0 ✅ tags {tags} · commit end {end} · confirmed_flush_lsn {flushed}");
    repl.shutdown().await?;
    c.execute(&format!("SELECT pg_drop_replication_slot('{slot}')"), &[]).await?;
    Ok(())
}
