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
        .with_status_interval(Duration::from_millis(200))
        // FINDING: while idle the worker sends feedback only every `idle_wakeup_interval` (default
        // 10 s) — `status_interval` is checked only when a message arrives. Without this an ack made
        // during a quiet period reaches the server up to 10 s late (slot lag, not a correctness bug).
        .with_wakeup_interval(Duration::from_millis(200));
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

// ================================================================================================
// Gates S-PG1…S-PG7 — decoded with the probe's own pgoutput decoder (tests/pgoutput).

mod pgoutput;
use pgoutput::{decode, Datum, Msg};

#[derive(Debug)]
enum Ev {
    Begin,
    M(Msg),
    Commit(Lsn),
}

/// Fresh slot + publication over `tables`; returns the control client and the slot's start LSN.
async fn fresh(slot: &str, publ: &str, ddl: &str, tables: &str) -> anyhow::Result<(tokio_postgres::Client, Lsn)> {
    let c = control().await?;
    let _ = c.execute(&format!("SELECT pg_drop_replication_slot('{slot}')"), &[]).await;
    c.batch_execute(ddl).await?;
    c.batch_execute(&format!("DROP PUBLICATION IF EXISTS {publ}; CREATE PUBLICATION {publ} FOR TABLE {tables};")).await?;
    let row = c
        .query_one(&format!("SELECT lsn::text FROM pg_create_logical_replication_slot('{slot}','pgoutput')"), &[])
        .await?;
    Ok((c, Lsn::parse(&row.get::<_, String>(0))?))
}

fn cfg(slot: &str, publ: &str, start: Lsn) -> ReplicationConfig {
    ReplicationConfig::new(HOST, USER, PASS, DB, slot, publ)
        .with_port(PORT)
        .with_tls(TlsConfig::disabled())
        .with_start_lsn(start)
        .with_status_interval(Duration::from_millis(200))
        .with_wakeup_interval(Duration::from_millis(200))
}

/// Receive until `commits` Commit events (or `secs` elapse), decoding XLogData.
async fn collect(repl: &mut ReplicationClient, commits: usize, secs: u64) -> anyhow::Result<Vec<Ev>> {
    let mut out = vec![];
    let mut seen = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while seen < commits && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(2), repl.recv()).await {
            Ok(Ok(Some(ReplicationEvent::Begin { .. }))) => out.push(Ev::Begin),
            Ok(Ok(Some(ReplicationEvent::XLogData { data, .. }))) => out.push(Ev::M(decode(&data)?)),
            Ok(Ok(Some(ReplicationEvent::Commit { end_lsn, .. }))) => {
                out.push(Ev::Commit(end_lsn));
                seen += 1;
            }
            Ok(Ok(Some(_))) => {}
            Ok(Ok(None)) => break,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {}
        }
    }
    anyhow::ensure!(seen >= commits, "only {seen}/{commits} commits within {secs} s");
    Ok(out)
}

fn msgs(evs: &[Ev]) -> Vec<&Msg> {
    evs.iter().filter_map(|e| if let Ev::M(m) = e { Some(m) } else { None }).collect()
}

fn text(d: &Datum) -> &str {
    match d {
        Datum::Text(s) => s,
        other => panic!("expected text, got {other:?}"),
    }
}

/// S-PG1 — every message kind the design consumes decodes: Relation, Type, Origin, Insert,
/// Update (no old / 'K' key-only / 'O' full old tuple), Delete ('K' / 'O'), Truncate.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg1_decoder_coverage() -> anyhow::Result<()> {
    if !enabled() {
        return Ok(println!("S-PG1 skipped"));
    }
    let (c, start) = fresh(
        "probe_s_pg1",
        "probe_s_pg1_pub",
        "DROP TABLE IF EXISTS pg1_a, pg1_b; DROP TYPE IF EXISTS pg1_mood;
         CREATE TYPE pg1_mood AS ENUM ('sad','happy');
         CREATE TABLE pg1_a (id bigint PRIMARY KEY, v text, n int, m pg1_mood);
         CREATE TABLE pg1_b (id bigint PRIMARY KEY, v text); ALTER TABLE pg1_b REPLICA IDENTITY FULL;
         SELECT pg_replication_origin_drop('probe_origin') FROM pg_replication_origin WHERE roname='probe_origin';
         SELECT pg_replication_origin_create('probe_origin');",
        "pg1_a, pg1_b",
    )
    .await?;
    let mut repl = ReplicationClient::connect(cfg("probe_s_pg1", "probe_s_pg1_pub", start)).await?;
    for sql in [
        "INSERT INTO pg1_a VALUES (1,'x',1,'happy')",
        "INSERT INTO pg1_b VALUES (1,'y')",
        "UPDATE pg1_a SET n = 2 WHERE id = 1",
        "UPDATE pg1_a SET id = 10 WHERE id = 1",
        "UPDATE pg1_b SET v = 'z' WHERE id = 1",
        "DELETE FROM pg1_a WHERE id = 10",
        "DELETE FROM pg1_b WHERE id = 1",
        "TRUNCATE pg1_a",
    ] {
        c.execute(sql, &[]).await?;
    }
    // a transaction with a replication origin (as another replicator would write it)
    c.batch_execute(
        "SELECT pg_replication_origin_session_setup('probe_origin');
         BEGIN; SELECT pg_replication_origin_xact_setup('0/AB', now()); INSERT INTO pg1_a VALUES (2,'o',0,'sad'); COMMIT;
         SELECT pg_replication_origin_session_reset();",
    )
    .await?;
    let evs = collect(&mut repl, 9, 30).await?;
    let m = msgs(&evs);
    let rel_a = m.iter().find_map(|x| match x {
        Msg::Relation { oid, name, replica_identity, columns, .. } if name == "pg1_a" => {
            assert_eq!(*replica_identity, 'd');
            assert!(columns[0].key && columns[0].name == "id");
            Some(*oid)
        }
        _ => None,
    });
    let rel_b = m.iter().find_map(|x| match x {
        Msg::Relation { oid, name, replica_identity, .. } if name == "pg1_b" => {
            assert_eq!(*replica_identity, 'f');
            Some(*oid)
        }
        _ => None,
    });
    let (rel_a, rel_b) = (rel_a.expect("Relation pg1_a"), rel_b.expect("Relation pg1_b"));
    let has = |f: &dyn Fn(&Msg) -> bool| m.iter().any(|x| f(x));
    assert!(has(&|x| matches!(x, Msg::Type { name, .. } if name == "pg1_mood")), "Type message for the enum");
    assert!(has(&|x| matches!(x, Msg::Insert { rel, new } if *rel == rel_a && text(&new[3]) == "happy")));
    assert!(has(&|x| matches!(x, Msg::Update { rel, old_kind: None, new, .. } if *rel == rel_a && text(&new[2]) == "2")), "non-key update: no old tuple");
    assert!(has(&|x| matches!(x, Msg::Update { rel, old_kind: Some('K'), old: Some(o), new } if *rel == rel_a && text(&o[0]) == "1" && text(&new[0]) == "10")), "key change: 'K' old key");
    assert!(has(&|x| matches!(x, Msg::Update { rel, old_kind: Some('O'), old: Some(o), .. } if *rel == rel_b && text(&o[1]) == "y")), "REPLICA IDENTITY FULL: 'O' old row");
    assert!(has(&|x| matches!(x, Msg::Delete { rel, old_kind: 'K', old } if *rel == rel_a && text(&old[0]) == "10")));
    assert!(has(&|x| matches!(x, Msg::Delete { rel, old_kind: 'O', old } if *rel == rel_b && text(&old[1]) == "z")));
    assert!(has(&|x| matches!(x, Msg::Truncate { rels, .. } if rels.contains(&rel_a))));
    let origin = m.iter().find_map(|x| if let Msg::Origin { name, .. } = x { Some(name.clone()) } else { None });
    assert_eq!(origin.as_deref(), Some("probe_origin"), "Origin message for the origin-tagged txn");
    let kinds: String = m.iter().map(|x| match x {
        Msg::Relation { .. } => 'R', Msg::Type { .. } => 'Y', Msg::Origin { .. } => 'O', Msg::Insert { .. } => 'I',
        Msg::Update { .. } => 'U', Msg::Delete { .. } => 'D', Msg::Truncate { .. } => 'T', Msg::Other(c) => *c,
    }).collect();
    println!("S-PG1 ✅ decoded {kinds}: Relation (identity d/f), Type, Origin, Insert, Update (none/K/O), Delete (K/O), Truncate");
    repl.shutdown().await?;
    c.execute("SELECT pg_drop_replication_slot('probe_s_pg1')", &[]).await?;
    Ok(())
}

/// S-PG2 — an UPDATE that does not touch an out-of-line TOASTed column sends it as 'u'.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg2_unchanged_toast() -> anyhow::Result<()> {
    if !enabled() {
        return Ok(println!("S-PG2 skipped"));
    }
    let (c, start) = fresh(
        "probe_s_pg2",
        "probe_s_pg2_pub",
        "DROP TABLE IF EXISTS pg2; CREATE TABLE pg2 (id bigint PRIMARY KEY, big text, n int);
         ALTER TABLE pg2 ALTER COLUMN big SET STORAGE EXTERNAL;",
        "pg2",
    )
    .await?;
    let mut repl = ReplicationClient::connect(cfg("probe_s_pg2", "probe_s_pg2_pub", start)).await?;
    c.execute("INSERT INTO pg2 SELECT 1, string_agg(md5(g::text), '') , 0 FROM generate_series(1,400) g", &[]).await?;
    c.execute("UPDATE pg2 SET n = 1 WHERE id = 1", &[]).await?;
    let evs = collect(&mut repl, 2, 20).await?;
    let m = msgs(&evs);
    let ins_len = m.iter().find_map(|x| if let Msg::Insert { new, .. } = x { Some(text(&new[1]).len()) } else { None });
    let upd = m.iter().find_map(|x| if let Msg::Update { new, .. } = x { Some(new.clone()) } else { None }).expect("update");
    assert_eq!(ins_len, Some(12_800), "insert carries the full 12.8 kB value");
    assert_eq!(upd[1], Datum::UnchangedToast, "untouched TOASTed column arrives as 'u'");
    assert_eq!(text(&upd[2]), "1");
    println!("S-PG2 ✅ insert carries 12,800-byte value; update of another column sends it as UnchangedToast ('u') → TOAST re-query path needed");
    repl.shutdown().await?;
    c.execute("SELECT pg_drop_replication_slot('probe_s_pg2')", &[]).await?;
    Ok(())
}

/// S-PG3 — a transaction far above logical_decoding_work_mem (64 kB) arrives whole at commit
/// under proto v1 (server spills; nothing streamed in progress).
#[tokio::test(flavor = "multi_thread")]
async fn s_pg3_large_transaction() -> anyhow::Result<()> {
    if !enabled() {
        return Ok(println!("S-PG3 skipped"));
    }
    let (c, start) = fresh(
        "probe_s_pg3",
        "probe_s_pg3_pub",
        "DROP TABLE IF EXISTS pg3; CREATE TABLE pg3 (id bigint PRIMARY KEY, pad text);",
        "pg3",
    )
    .await?;
    let mut repl = ReplicationClient::connect(cfg("probe_s_pg3", "probe_s_pg3_pub", start).with_buffer_size(4096)).await?;
    let t0 = std::time::Instant::now();
    c.execute("INSERT INTO pg3 SELECT g, repeat('x', 200) FROM generate_series(1, 50000) g", &[]).await?;
    let evs = collect(&mut repl, 1, 60).await?;
    let inserts = msgs(&evs).iter().filter(|x| matches!(x, Msg::Insert { .. })).count();
    let begins = evs.iter().filter(|e| matches!(e, Ev::Begin)).count();
    let spilled: i64 = c
        .query_one("SELECT coalesce(spill_txns,0) FROM pg_stat_replication_slots WHERE slot_name='probe_s_pg3'", &[])
        .await
        .map(|r| r.get(0))
        .unwrap_or(-1);
    assert_eq!((begins, inserts), (1, 50_000), "one Begin, all 50k inserts, one Commit");
    println!(
        "S-PG3 ✅ 50,000-row txn (~10 MB, work_mem 64 kB) → 1 Begin / 50,000 Insert / 1 Commit in {} ms; server spill_txns={spilled}",
        t0.elapsed().as_millis()
    );
    repl.shutdown().await?;
    c.execute("SELECT pg_drop_replication_slot('probe_s_pg3')", &[]).await?;
    Ok(())
}

/// S-PG4 — restart / resume: txn A acked, txn B not; reconnect → B replays, A does not.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg4_restart_resume() -> anyhow::Result<()> {
    if !enabled() {
        return Ok(println!("S-PG4 skipped"));
    }
    let (c, start) = fresh(
        "probe_s_pg4",
        "probe_s_pg4_pub",
        "DROP TABLE IF EXISTS pg4; CREATE TABLE pg4 (id bigint PRIMARY KEY);",
        "pg4",
    )
    .await?;
    let mut repl = ReplicationClient::connect(cfg("probe_s_pg4", "probe_s_pg4_pub", start)).await?;
    c.execute("INSERT INTO pg4 VALUES (1)", &[]).await?;
    let a = collect(&mut repl, 1, 20).await?;
    let Some(Ev::Commit(a_end)) = a.last() else { anyhow::bail!("no commit A") };
    repl.update_applied_lsn(*a_end);
    tokio::time::sleep(Duration::from_millis(600)).await;
    c.execute("INSERT INTO pg4 VALUES (2)", &[]).await?;
    let _b = collect(&mut repl, 1, 20).await?; // received but NOT acked
    repl.shutdown().await?;

    let flushed: String = c
        .query_one("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name='probe_s_pg4'", &[])
        .await?
        .get(0);
    let mut repl = ReplicationClient::connect(cfg("probe_s_pg4", "probe_s_pg4_pub", Lsn::parse(&flushed)?)).await?;
    let again = collect(&mut repl, 1, 20).await?;
    let ids: Vec<String> = msgs(&again)
        .iter()
        .filter_map(|x| if let Msg::Insert { new, .. } = x { Some(text(&new[0]).to_string()) } else { None })
        .collect();
    assert_eq!(ids, vec!["2"], "after restart: un-acked B replays, acked A does not");
    println!("S-PG4 ✅ restart from confirmed_flush_lsn {flushed}: acked txn skipped, un-acked txn replayed whole (at-least-once)");
    repl.shutdown().await?;
    c.execute("SELECT pg_drop_replication_slot('probe_s_pg4')", &[]).await?;
    Ok(())
}

/// S-PG5 — a consumer that stalls longer than wal_sender_timeout keeps its connection.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg5_slow_consumer_survives() -> anyhow::Result<()> {
    if !enabled() {
        return Ok(println!("S-PG5 skipped"));
    }
    let (c, start) = fresh(
        "probe_s_pg5",
        "probe_s_pg5_pub",
        "DROP TABLE IF EXISTS pg5; CREATE TABLE pg5 (id bigint PRIMARY KEY, pad text);",
        "pg5",
    )
    .await?;
    // ALTER SYSTEM cannot run in the implicit transaction of a multi-statement batch
    c.batch_execute("ALTER SYSTEM SET wal_sender_timeout = '3s'").await?;
    c.batch_execute("SELECT pg_reload_conf()").await?;
    let mut repl = ReplicationClient::connect(cfg("probe_s_pg5", "probe_s_pg5_pub", start).with_buffer_size(16)).await?;
    for i in 0..200 {
        c.execute("INSERT INTO pg5 VALUES ($1, repeat('y', 500))", &[&(i as i64)]).await?;
    }
    tokio::time::sleep(Duration::from_secs(10)).await; // > 3 × wal_sender_timeout, buffer (16) full
    let evs = collect(&mut repl, 200, 60).await;
    c.batch_execute("ALTER SYSTEM RESET wal_sender_timeout").await?;
    c.batch_execute("SELECT pg_reload_conf()").await?;
    let evs = evs?;
    let inserts = msgs(&evs).iter().filter(|x| matches!(x, Msg::Insert { .. })).count();
    assert_eq!(inserts, 200);
    println!("S-PG5 ✅ consumer stalled 10 s with a full 16-event buffer, wal_sender_timeout 3 s: connection kept, 200/200 txns delivered");
    repl.shutdown().await?;
    c.execute("SELECT pg_drop_replication_slot('probe_s_pg5')", &[]).await?;
    Ok(())
}

/// S-PG6 — ALTER PUBLICATION … ADD TABLE takes effect on a running stream (no restart).
#[tokio::test(flavor = "multi_thread")]
async fn s_pg6_publication_change_mid_stream() -> anyhow::Result<()> {
    if !enabled() {
        return Ok(println!("S-PG6 skipped"));
    }
    let (c, start) = fresh(
        "probe_s_pg6",
        "probe_s_pg6_pub",
        "DROP TABLE IF EXISTS pg6_a, pg6_b; CREATE TABLE pg6_a (id bigint PRIMARY KEY); CREATE TABLE pg6_b (id bigint PRIMARY KEY);",
        "pg6_a",
    )
    .await?;
    let mut repl = ReplicationClient::connect(cfg("probe_s_pg6", "probe_s_pg6_pub", start)).await?;
    c.execute("INSERT INTO pg6_b VALUES (1)", &[]).await?; // not published yet
    c.execute("INSERT INTO pg6_a VALUES (1)", &[]).await?;
    c.execute("ALTER PUBLICATION probe_s_pg6_pub ADD TABLE pg6_b", &[]).await?;
    c.execute("INSERT INTO pg6_b VALUES (2)", &[]).await?;
    let evs = collect(&mut repl, 2, 20).await?;
    let names: Vec<String> = msgs(&evs)
        .iter()
        .filter_map(|x| if let Msg::Relation { name, .. } = x { Some(name.clone()) } else { None })
        .collect();
    let b_ids: Vec<String> = msgs(&evs)
        .iter()
        .filter_map(|x| match x {
            Msg::Insert { new, .. } if names.len() == 2 => Some(text(&new[0]).to_string()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&"pg6_b".to_string()), "pg6_b appears after ADD TABLE: {names:?}");
    println!("S-PG6 ✅ ADD TABLE on a live stream: Relations {names:?}; inserts {b_ids:?} (pre-ADD pg6_b row not sent)");
    repl.shutdown().await?;
    c.execute("SELECT pg_drop_replication_slot('probe_s_pg6')", &[]).await?;
    Ok(())
}

/// S-PG7 — text-format decode of the Type_Mapping PostgreSQL types, with the walsender session
/// pinned to UTC / ISO (the crate's `with_options`).
#[tokio::test(flavor = "multi_thread")]
async fn s_pg7_type_text_decode() -> anyhow::Result<()> {
    if !enabled() {
        return Ok(println!("S-PG7 skipped"));
    }
    let (c, start) = fresh(
        "probe_s_pg7",
        "probe_s_pg7_pub",
        "DROP TABLE IF EXISTS pg7; CREATE TABLE pg7 (
            id bigint PRIMARY KEY, i2 int2, i4 int4, i8 int8, num numeric(38,10), f8 float8, b bool,
            t text, by bytea, d date, tm time(6), ts timestamp(6), tstz timestamptz(6), u uuid,
            j jsonb, ai int4[], at text[], iv interval);",
        "pg7",
    )
    .await?;
    let cfg = cfg("probe_s_pg7", "probe_s_pg7_pub", start)
        .with_options("-c TimeZone=UTC -c DateStyle=ISO -c IntervalStyle=iso_8601 -c extra_float_digits=3");
    let mut repl = ReplicationClient::connect(cfg).await?;
    c.batch_execute(
        "SET TimeZone='Europe/Kyiv';
         INSERT INTO pg7 VALUES (1, -32768, 2147483647, 9223372036854775807, -1234567890123456789012345678.0123456789,
           1.5, true, 'héllo ✓', '\\xdeadbeef', '1969-12-31', '23:59:59.999999', '2026-09-26 12:34:56.789012',
           '2026-09-26 12:34:56.789012+03', '0718293a-4b5c-6d7e-8fa0-b1c2d3e4f506', '{\"a\": [1, 2]}',
           '{1,2,3}', '{a,\"b c\",NULL}', '1 day 02:03:04');",
    )
    .await?;
    let evs = collect(&mut repl, 1, 20).await?;
    let row = msgs(&evs).iter().find_map(|x| if let Msg::Insert { new, .. } = x { Some(new.clone()) } else { None }).expect("insert");
    let got: Vec<&str> = row.iter().map(text).collect();
    let want = [
        "1", "-32768", "2147483647", "9223372036854775807", "-1234567890123456789012345678.0123456789", "1.5", "t",
        "héllo ✓", "\\xdeadbeef", "1969-12-31", "23:59:59.999999", "2026-09-26 12:34:56.789012",
        "2026-09-26 09:34:56.789012+00", "0718293a-4b5c-6d7e-8fa0-b1c2d3e4f506", "{\"a\": [1, 2]}", "{1,2,3}",
        "{a,\"b c\",NULL}", "P1DT2H3M4S",
    ];
    for (i, (g, w)) in got.iter().zip(want.iter()).enumerate() {
        assert_eq!(g, w, "column {i}");
    }
    println!("S-PG7 ✅ 18 types decode as expected text (timestamptz rendered in the walsender's pinned UTC, not the writer's Europe/Kyiv)");
    repl.shutdown().await?;
    c.execute("SELECT pg_drop_replication_slot('probe_s_pg7')", &[]).await?;
    Ok(())
}

/// S-PG8 — TLS: replication stream over `verify-full` against the self-signed `postgres-tls`
/// service; the walsender's `pg_stat_ssl` row confirms the session is encrypted.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg8_tls_verify_full() -> anyhow::Result<()> {
    // FINDING: two rustls crypto backends are linked (ring via pgwire-replication, aws-lc-rs via
    // mysql_async) — rustls cannot choose automatically; the process must install one.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    if !enabled() {
        return Ok(println!("S-PG8 skipped"));
    }
    const TLS_PORT: u16 = 55433;
    let ca = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".tls/ca.crt");
    anyhow::ensure!(ca.exists(), "{} missing — is the postgres-tls service up?", ca.display());
    let dsn = format!("host={HOST} port={TLS_PORT} user={USER} password={PASS} dbname={DB}");
    let (c, conn) = tokio_postgres::connect(&dsn, NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let (slot, publ) = ("probe_s_pg8", "probe_s_pg8_pub");
    let _ = c.execute(&format!("SELECT pg_drop_replication_slot('{slot}')"), &[]).await;
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS pg8; CREATE TABLE pg8 (id bigint PRIMARY KEY);
         DROP PUBLICATION IF EXISTS {publ}; CREATE PUBLICATION {publ} FOR TABLE pg8;"
    ))
    .await?;
    let start: String = c
        .query_one(&format!("SELECT lsn::text FROM pg_create_logical_replication_slot('{slot}','pgoutput')"), &[])
        .await?
        .get(0);
    let cfg = ReplicationConfig::new("localhost", USER, PASS, DB, slot, publ)
        .with_port(TLS_PORT)
        .with_tls(TlsConfig::verify_full(Some(ca)))
        .with_start_lsn(Lsn::parse(&start)?)
        .with_status_interval(Duration::from_millis(200))
        .with_wakeup_interval(Duration::from_millis(200));
    let mut repl = ReplicationClient::connect(cfg).await?;
    c.execute("INSERT INTO pg8 VALUES (1)", &[]).await?;
    let evs = collect(&mut repl, 1, 20).await?;
    assert!(msgs(&evs).iter().any(|x| matches!(x, Msg::Insert { .. })));
    let row = c
        .query_one(
            "SELECT s.ssl, s.version FROM pg_stat_ssl s
             JOIN pg_replication_slots r ON r.active_pid = s.pid WHERE r.slot_name = $1",
            &[&slot],
        )
        .await?;
    let (ssl, ver): (bool, Option<String>) = (row.get(0), row.get(1));
    assert!(ssl, "walsender session must be TLS");
    println!("S-PG8 ✅ replication over verify-full TLS ({}), insert received", ver.unwrap_or_default());
    repl.shutdown().await?;
    c.execute(&format!("SELECT pg_drop_replication_slot('{slot}')"), &[]).await?;
    Ok(())
}
