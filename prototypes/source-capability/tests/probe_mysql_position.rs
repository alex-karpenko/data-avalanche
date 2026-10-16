//! S-MY10 (file/position mode) and S-MY11 (DA-SNAP-10, a commit the binlog delivers before it is
//! visible) on the pinned `mysql_async` binlog stream. Needs the `mysql`, `mysql-nogtid`, `mariadb`,
//! `mariadb-118` and `mariadb-1011` services from docker-compose.yml. S-MY10d restarts them
//! (`docker restart`), and S-MY10e / S-MY10k purge their binary logs: run it against the probe
//! containers only.
//!
//!   SRC_PROBE=1 cargo test --features mysql --test probe_mysql_position -- --nocapture --test-threads=1

#![cfg(feature = "mysql")]

use std::time::{Duration, Instant};

use anyhow::Context;
use futures::StreamExt;
use mysql_async::binlog::events::{Event, EventData, RowsEventData};
use mysql_async::prelude::Queryable;

fn enabled() -> bool {
    std::env::var("SRC_PROBE").is_ok_and(|v| !v.is_empty())
}

#[derive(Clone, Copy, Debug)]
struct Server {
    name: &'static str,
    url: &'static str,
    container: &'static str,
    maria: bool,
}

const MYSQL: Server = Server {
    name: "MySQL 8.4 (GTID on)",
    url: "mysql://root:probe@127.0.0.1:53306/probe",
    container: "source-capability-mysql-1",
    maria: false,
};
const MYSQL_NOGTID: Server = Server {
    name: "MySQL 8.4 (GTID off)",
    url: "mysql://root:probe@127.0.0.1:53308/probe",
    container: "source-capability-mysql-nogtid-1",
    maria: false,
};
const MARIADB_114: Server = Server {
    name: "MariaDB 11.4",
    url: "mysql://root:probe@127.0.0.1:53307/probe",
    container: "source-capability-mariadb-1",
    maria: true,
};
const MARIADB_118: Server = Server {
    name: "MariaDB 11.8",
    url: "mysql://root:probe@127.0.0.1:53309/probe",
    container: "source-capability-mariadb-118-1",
    maria: true,
};
const MARIADB_1011: Server = Server {
    name: "MariaDB 10.11",
    url: "mysql://root:probe@127.0.0.1:53310/probe",
    container: "source-capability-mariadb-1011-1",
    maria: true,
};
const ALL: [Server; 5] = [MYSQL, MYSQL_NOGTID, MARIADB_1011, MARIADB_114, MARIADB_118];

async fn conn(s: Server) -> anyhow::Result<mysql_async::Conn> {
    mysql_async::Conn::from_url(s.url)
        .await
        .with_context(|| format!("connect {}", s.name))
}

/// The server's current binlog position: `SHOW BINARY LOG STATUS` (MySQL 8.4) /
/// `SHOW BINLOG STATUS` (MariaDB).
async fn status(s: Server, c: &mut mysql_async::Conn) -> anyhow::Result<(String, u64)> {
    let sql = if s.maria {
        "SHOW BINLOG STATUS"
    } else {
        "SHOW BINARY LOG STATUS"
    };
    let row: mysql_async::Row = c.query_first(sql).await?.context("no status row")?;
    Ok((
        row.get::<String, _>(0).context("file")?,
        row.get::<u64, _>(1).context("pos")?,
    ))
}

async fn binary_logs(c: &mut mysql_async::Conn) -> anyhow::Result<Vec<String>> {
    let rows: Vec<mysql_async::Row> = c.query("SHOW BINARY LOGS").await?;
    Ok(rows
        .into_iter()
        .map(|r| r.get::<String, _>(0).unwrap_or_default())
        .collect())
}

/// One normalized binlog event, with the header's `log_pos` (the end position).
#[derive(Debug, Clone, PartialEq)]
enum Ev {
    Rotate {
        next_file: String,
        next_pos: u64,
        fake: bool,
        log_pos: u32,
    },
    Format {
        log_pos: u32,
    },
    Gtid {
        log_pos: u32,
    },
    Query {
        sql: String,
        log_pos: u32,
    },
    Map {
        table: String,
        log_pos: u32,
    },
    Rows {
        table: String,
        kind: &'static str,
        keys: Vec<String>,
        log_pos: u32,
    },
    Xid {
        file: String,
        log_pos: u32,
    },
    Payload {
        log_pos: u32,
    },
    Heartbeat {
        raw: u8,
        log_pos: u32,
        body: String,
    },
    Other {
        raw: u8,
        log_pos: u32,
    },
}

struct Reader {
    stream: mysql_async::BinlogStream,
    /// The binlog file the stream is in (from the fake rotate at the start, then each rotate).
    file: String,
    /// Events the stream yielded, as read.
    seen: Vec<Ev>,
    /// The end offset of the last event in `file`, computed: the start position, then each event's
    /// size. Inside a compressed payload, the payload's end (`inner` counts its bytes down).
    offset: u64,
    inner: u64,
    /// The offset came from a header, not from a dump's start (a GTID dump starts anywhere).
    anchored: bool,
    /// A GTID-positioned dump.
    gtid: bool,
    /// The offset was computed from sizes since the last set position (or the dump is
    /// file-positioned, where nothing is skipped).
    chained: bool,
    /// Runs of computed offsets short of the next set position (left-out events), for the report.
    gaps: Vec<String>,
    /// Events whose header `log_pos` was set and differed from `offset` (must stay empty).
    mismatches: Vec<String>,
    /// (header `log_pos`, computed end) per event, for the report.
    offsets: Vec<(u32, u64, String)>,
}

async fn open(
    s: Server,
    file: &str,
    pos: u64,
    replica_id: u32,
    heartbeat: Option<Duration>,
) -> anyhow::Result<Reader> {
    let mut c = conn(s).await?;
    if let Some(h) = heartbeat {
        c.query_drop(format!("SET @master_heartbeat_period = {}", h.as_nanos()))
            .await?;
    }
    let name = file.as_bytes().to_vec();
    let req = mysql_async::BinlogStreamRequest::new(replica_id)
        .with_filename(&name)
        .with_pos(pos);
    let stream = c.get_binlog_stream(req).await?;
    Ok(Reader {
        stream,
        file: file.to_owned(),
        seen: vec![],
        offset: pos,
        inner: 0,
        anchored: true,
        gtid: false,
        chained: true,
        gaps: vec![],
        mismatches: vec![],
        offsets: vec![],
    })
}

impl Reader {
    /// The next event, or `None` when nothing arrives within `wait`.
    async fn next(&mut self, wait: Duration) -> anyhow::Result<Option<Ev>> {
        let Ok(item) = tokio::time::timeout(wait, self.stream.next()).await else {
            return Ok(None);
        };
        let event = item.context("the stream ended")??;
        let ev = self.normalize(&event)?;
        self.track(&event, &ev)?;
        self.seen.push(ev.clone());
        Ok(Some(ev))
    }

    fn normalize(&mut self, e: &Event) -> anyhow::Result<Ev> {
        let h = e.header();
        let (raw, log_pos) = (h.event_type_raw(), h.log_pos());
        if raw == 27 || raw == 41 {
            return Ok(Ev::Heartbeat {
                raw,
                log_pos,
                body: String::from_utf8_lossy(e.data()).into_owned(),
            });
        }
        if raw == 162 {
            return Ok(Ev::Gtid { log_pos });
        }
        let data = match e.read_data() {
            Ok(Some(d)) => d,
            Ok(None) | Err(_) => return Ok(Ev::Other { raw, log_pos }),
        };
        Ok(match data {
            EventData::RotateEvent(r) => {
                // FINDING (S-MY10b): MariaDB's fake ROTATE at the start of a dump comes before the
                // format description, so `mysql_common` doesn't know the checksum is on and leaves
                // the 4 CRC32 bytes on the file name. Strip them when the name only parses without.
                let raw_name = r.name_raw().to_vec();
                let parses = |n: &[u8]| {
                    n.len() > 7
                        && n[n.len() - 7] == b'.'
                        && n[n.len() - 6..].iter().all(u8::is_ascii_digit)
                };
                let name = if !parses(&raw_name)
                    && raw_name.len() > 4
                    && parses(&raw_name[..raw_name.len() - 4])
                {
                    println!(
                        "  (a ROTATE name with 4 trailing bytes: {:?})",
                        String::from_utf8_lossy(&raw_name)
                    );
                    &raw_name[..raw_name.len() - 4]
                } else {
                    &raw_name[..]
                };
                let next_file = String::from_utf8_lossy(name).into_owned();
                self.file = next_file.clone();
                Ev::Rotate {
                    next_file,
                    next_pos: r.position(),
                    fake: log_pos == 0,
                    log_pos,
                }
            }
            EventData::FormatDescriptionEvent(_) => Ev::Format { log_pos },
            EventData::GtidEvent(_) => Ev::Gtid { log_pos },
            EventData::QueryEvent(q) => Ev::Query {
                sql: q.query().into_owned(),
                log_pos,
            },
            EventData::TableMapEvent(t) => Ev::Map {
                table: t.table_name().into_owned(),
                log_pos,
            },
            EventData::XidEvent(_) => Ev::Xid {
                file: self.file.clone(),
                log_pos,
            },
            EventData::TransactionPayloadEvent(_) => Ev::Payload { log_pos },
            EventData::RowsEvent(re) => {
                let tme = self
                    .stream
                    .get_tme(re.table_id())
                    .context("TableMap for a rows event")?;
                let table = tme.table_name().into_owned();
                let kind = match &re {
                    RowsEventData::WriteRowsEvent(_) | RowsEventData::WriteRowsEventV1(_) => {
                        "insert"
                    }
                    RowsEventData::UpdateRowsEvent(_)
                    | RowsEventData::UpdateRowsEventV1(_)
                    | RowsEventData::PartialUpdateRowsEvent(_) => "update",
                    RowsEventData::DeleteRowsEvent(_) | RowsEventData::DeleteRowsEventV1(_) => {
                        "delete"
                    }
                };
                let mut keys = vec![];
                for row in re.rows(tme) {
                    let (before, after) = row?;
                    let first = |r: Option<mysql_async::binlog::row::BinlogRow>| {
                        r.and_then(|r| r.unwrap().into_iter().next()).map(|v| {
                            match mysql_async::Value::try_from(v) {
                                Ok(v) => v.as_sql(true),
                                Err(e) => format!("err:{e}"),
                            }
                        })
                    };
                    keys.push(match (first(before), first(after)) {
                        (Some(b), Some(a)) => format!("{b}->{a}"),
                        (Some(k), None) | (None, Some(k)) => k,
                        (None, None) => "?".into(),
                    });
                }
                Ev::Rows {
                    table,
                    kind,
                    keys,
                    log_pos,
                }
            }
            _ => Ev::Other { raw, log_pos },
        })
    }

    /// Advances the computed offset past `e` and checks it against the header's `log_pos`.
    fn track(&mut self, e: &Event, ev: &Ev) -> anyhow::Result<()> {
        let h = e.header();
        let size = u64::from(h.event_size());
        match ev {
            Ev::Rotate { next_pos, .. } => {
                // The real rotate is the old file's last event; either way the next file starts here.
                self.offset = *next_pos;
                self.inner = 0;
                // A GTID dump's fake rotate names the first file it reads, not where it starts.
                self.anchored = !self.gtid;
                self.offsets
                    .push((h.log_pos(), self.offset, self.file.clone()));
                return Ok(());
            }
            Ev::Heartbeat { .. } | Ev::Format { log_pos: 0 } => {
                // Not in the file here (a heartbeat; the format description resent at a dump's
                // start): the offset stays.
                self.offsets
                    .push((h.log_pos(), self.offset, self.file.clone()));
                return Ok(());
            }
            _ => {}
        }
        if self.inner > 0 {
            // An event from inside a compressed payload: its bytes are the payload's.
            self.inner = self.inner.saturating_sub(size);
            self.offsets
                .push((h.log_pos(), self.offset, self.file.clone()));
            return Ok(());
        }
        // The hybrid rule: the header's end position when the server set it (a GTID dump skips
        // transactions, so only a set position re-anchors), else the last end plus this size.
        let computed = self.offset + size;
        if h.log_pos() != 0 {
            // Within a run of unset positions the sizes must add up to the next set one; between
            // two set positions a GTID dump may have skipped transactions.
            if self.anchored && self.chained && u64::from(h.log_pos()) != computed {
                let note = format!("{ev:?}: header {} computed {computed}", h.log_pos());
                if u64::from(h.log_pos()) > computed {
                    // The server left out events inside the transaction (MariaDB with the GTID
                    // capability: the Annotate_rows events a replica didn't ask for): the computed
                    // keys are short of the file's bytes but still below this position.
                    self.gaps.push(note);
                } else {
                    self.mismatches.push(note);
                }
            }
            self.offset = u64::from(h.log_pos());
            self.anchored = true;
            self.chained = !self.gtid;
        } else {
            self.offset = computed;
            self.chained = true;
        }
        if let Ev::Payload { .. } = ev {
            let tpe = e.read_event::<mysql_async::binlog::events::TransactionPayloadEvent<'_>>()?;
            self.inner = tpe.danger_decompress().len() as u64;
        }
        self.offsets
            .push((h.log_pos(), self.offset, self.file.clone()));
        Ok(())
    }

    /// Reads until `n` XIDs arrived or `wait` passed with nothing new.
    async fn until_xids(&mut self, n: usize, wait: Duration) -> anyhow::Result<Vec<(String, u32)>> {
        let mut xids = vec![];
        while xids.len() < n {
            match self.next(wait).await? {
                Some(Ev::Xid { file, log_pos }) => xids.push((file, log_pos)),
                Some(_) => {}
                None => break,
            }
        }
        Ok(xids)
    }

    /// The keys of the rows events for `table` the stream yielded so far.
    fn keys(&self, table: &str) -> Vec<String> {
        self.seen
            .iter()
            .filter_map(|e| match e {
                Ev::Rows { table: t, keys, .. } if t == table => Some(keys.clone()),
                _ => None,
            })
            .flatten()
            .collect()
    }
}

async fn fresh_table(c: &mut mysql_async::Conn, t: &str) -> anyhow::Result<()> {
    c.query_drop(format!("DROP TABLE IF EXISTS {t}")).await?;
    c.query_drop(format!("CREATE TABLE {t} (id BIGINT PRIMARY KEY, v INT)"))
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10a — the watermark statement, the identity variables and the flavour.

#[tokio::test(flavor = "multi_thread")]
async fn s_my10a_watermark_identity_flavour() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10a skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in ALL {
        let mut c = conn(s).await?;
        let version: String = c
            .query_first("SELECT version()")
            .await?
            .context("version")?;
        let (file, pos) = status(s, &mut c).await?;
        let other = if s.maria {
            "SHOW BINARY LOG STATUS"
        } else {
            "SHOW BINLOG STATUS"
        };
        let other_ok = c.query_drop(other).await.is_ok();
        let master_ok = c.query_drop("SHOW MASTER STATUS").await.is_ok();
        let uuid: Result<Option<String>, _> = c.query_first("SELECT @@server_uuid").await;
        let server_id: u32 = c
            .query_first("SELECT @@server_id")
            .await?
            .context("server_id")?;
        println!(
            "S-MY10a {}: version {version:?} (MariaDB in it: {}); status {file}:{pos}; `{other}` {}; `SHOW MASTER STATUS` {}; @@server_uuid {}; @@server_id {server_id}",
            s.name,
            version.contains("MariaDB"),
            if other_ok { "works" } else { "fails" },
            if master_ok { "works" } else { "fails" },
            match uuid {
                Ok(u) => format!("{u:?}"),
                Err(e) => format!("fails ({e})"),
            },
        );
        assert_eq!(
            version.contains("MariaDB"),
            s.maria,
            "the flavour from version()"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10b — resume at (file, end_log_pos of an XID): exactly the transactions after it.

#[tokio::test(flavor = "multi_thread")]
async fn s_my10b_resume_after_an_xid() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10b skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in ALL {
        let mut c = conn(s).await?;
        fresh_table(&mut c, "my10b").await?;
        let (file, pos) = status(s, &mut c).await?;
        let mut r = open(s, &file, pos, 9101, None).await?;
        for i in 1..=5 {
            c.query_drop(format!("INSERT INTO my10b VALUES ({i}, 0)"))
                .await?;
        }
        let xids = r.until_xids(5, Duration::from_secs(5)).await?;
        assert_eq!(xids.len(), 5, "{}: five XIDs", s.name);
        assert_eq!(r.keys("my10b"), ["1", "2", "3", "4", "5"], "{}", s.name);
        let head: Vec<&Ev> = r.seen.iter().take(3).collect();
        println!(
            "S-MY10b {}: started at {file}:{pos}; first events {head:?}; XIDs at {xids:?}",
            s.name
        );
        drop(r);

        let (f3, p3) = xids[2].clone();
        let mut r = open(s, &f3, u64::from(p3), 9101, None).await?;
        let more = r.until_xids(2, Duration::from_secs(5)).await?;
        assert_eq!(
            r.keys("my10b"),
            ["4", "5"],
            "{}: resumed after the third XID",
            s.name
        );
        assert_eq!(
            more,
            xids[3..].to_vec(),
            "{}: the same positions again",
            s.name
        );
        assert_eq!(
            r.until_xids(1, Duration::from_secs(2)).await?,
            vec![],
            "{}: nothing else",
            s.name
        );
        c.query_drop("INSERT INTO my10b VALUES (6, 0)").await?;
        r.until_xids(1, Duration::from_secs(5)).await?;
        assert_eq!(r.keys("my10b"), ["4", "5", "6"], "{}: and new ones", s.name);
        assert!(
            r.mismatches.is_empty(),
            "{}: computed offsets {:?}",
            s.name,
            r.mismatches
        );
        println!("S-MY10b ✅ {}: resumed at {f3}:{p3} → 4, 5, then 6; no duplicate; computed offsets agree", s.name);
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10c — rotation: a FLUSH BINARY LOGS mid-stream; resume across it.

#[tokio::test(flavor = "multi_thread")]
async fn s_my10c_rotation() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10c skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in ALL {
        let mut c = conn(s).await?;
        fresh_table(&mut c, "my10c").await?;
        let (file, pos) = status(s, &mut c).await?;
        let mut r = open(s, &file, pos, 9102, None).await?;
        c.query_drop("INSERT INTO my10c VALUES (1, 0)").await?;
        c.query_drop("FLUSH BINARY LOGS").await?;
        c.query_drop("INSERT INTO my10c VALUES (2, 0)").await?;
        let xids = r.until_xids(2, Duration::from_secs(5)).await?;
        assert_eq!(xids.len(), 2, "{}", s.name);
        assert_ne!(
            xids[0].0, xids[1].0,
            "{}: the second transaction is in the next file",
            s.name
        );
        let rotates: Vec<&Ev> = r
            .seen
            .iter()
            .filter(|e| matches!(e, Ev::Rotate { .. }))
            .collect();
        println!("S-MY10c {}: XIDs {xids:?}; rotates {rotates:?}", s.name);
        drop(r);

        let (f1, p1) = xids[0].clone();
        let mut r = open(s, &f1, u64::from(p1), 9102, None).await?;
        let after = r.until_xids(1, Duration::from_secs(5)).await?;
        assert_eq!(
            r.keys("my10c"),
            ["2"],
            "{}: resumed before the rotation",
            s.name
        );
        assert_eq!(after, xids[1..].to_vec(), "{}", s.name);
        let seen: Vec<&Ev> = r.seen.iter().collect();
        assert!(
            r.mismatches.is_empty(),
            "{}: computed offsets {:?}",
            s.name,
            r.mismatches
        );
        println!("S-MY10c ✅ {}: resumed at {f1}:{p1} across the rotation → 2; events {seen:?}; (header, computed) {:?}", s.name, r.offsets);
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10d — a server restart: the identity survives, the server starts a new file, and an old
// position resumes into it.

fn docker(args: &[&str]) -> anyhow::Result<String> {
    let out = std::process::Command::new("docker").args(args).output()?;
    anyhow::ensure!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

async fn reconnect(s: Server) -> anyhow::Result<mysql_async::Conn> {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match conn(s).await {
            Ok(c) => return Ok(c),
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn identity(s: Server, c: &mut mysql_async::Conn) -> anyhow::Result<String> {
    Ok(if s.maria {
        let id: u32 = c.query_first("SELECT @@server_id").await?.context("id")?;
        id.to_string()
    } else {
        c.query_first("SELECT @@server_uuid")
            .await?
            .context("uuid")?
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn s_my10d_server_restart() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10d skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in [MYSQL_NOGTID, MARIADB_118] {
        let mut c = conn(s).await?;
        fresh_table(&mut c, "my10d").await?;
        let (file, pos) = status(s, &mut c).await?;
        let mut r = open(s, &file, pos, 9103, None).await?;
        c.query_drop("INSERT INTO my10d VALUES (1, 0)").await?;
        let xids = r.until_xids(1, Duration::from_secs(5)).await?;
        let (f1, p1) = xids[0].clone();
        let id_before = identity(s, &mut c).await?;
        let logs_before = binary_logs(&mut c).await?;
        drop(r);
        drop(c);

        docker(&["restart", s.container])?;
        let mut c = reconnect(s).await?;
        c.query_drop("INSERT INTO my10d VALUES (2, 0)").await?;
        let id_after = identity(s, &mut c).await?;
        let logs_after = binary_logs(&mut c).await?;
        let mut r = open(s, &f1, u64::from(p1), 9103, None).await?;
        r.until_xids(1, Duration::from_secs(5)).await?;
        assert_eq!(
            r.keys("my10d"),
            ["2"],
            "{}: resumed across the restart",
            s.name
        );
        assert_eq!(
            id_before, id_after,
            "{}: the identity survives a restart",
            s.name
        );
        let kinds: Vec<&Ev> = r.seen.iter().collect();
        println!(
            "S-MY10d ✅ {}: identity {id_before} before and after; binary logs {logs_before:?} → {logs_after:?}; resumed at {f1}:{p1} → 2; events {kinds:?}",
            s.name
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10e — a purged file: what the dump answers, and what SHOW BINARY LOGS lists.

#[tokio::test(flavor = "multi_thread")]
async fn s_my10e_purged_file() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10e skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in ALL {
        let mut c = conn(s).await?;
        fresh_table(&mut c, "my10e").await?;
        c.query_drop("INSERT INTO my10e VALUES (1, 0)").await?;
        let (old, pos) = status(s, &mut c).await?;
        c.query_drop("FLUSH BINARY LOGS").await?;
        let (current, _) = status(s, &mut c).await?;
        // MariaDB keeps a file until its binlog checkpoint is written ("may be needed for crash
        // recovery"): retry the purge for a while.
        let deadline = Instant::now() + Duration::from_secs(10);
        let (mut purge, mut warnings, mut listed);
        loop {
            purge = c
                .query_drop(format!("PURGE BINARY LOGS TO '{current}'"))
                .await;
            let w: Vec<mysql_async::Row> = c.query("SHOW WARNINGS").await?;
            warnings = w
                .into_iter()
                .map(|w| w.get::<String, _>(2).unwrap_or_default())
                .collect::<Vec<String>>();
            listed = binary_logs(&mut c).await?;
            if !listed.contains(&old) || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        if listed.contains(&old) {
            println!("S-MY10e {}: PURGE BINARY LOGS TO '{current}' → {purge:?}, warnings {warnings:?}; {old} still listed {listed:?}", s.name);
            continue;
        }
        let answer = match open(s, &old, pos, 9104, None).await {
            Err(e) => format!("open fails: {e:?}"),
            Ok(mut r) => match r.next(Duration::from_secs(5)).await {
                Ok(ev) => format!("first item {ev:?}"),
                Err(e) => match e.downcast_ref::<mysql_async::Error>() {
                    Some(mysql_async::Error::Server(se)) => {
                        format!("server error {} ({}): {}", se.code, se.state, se.message)
                    }
                    _ => format!("error {e:?}"),
                },
            },
        };
        println!(
            "S-MY10e {}: resume at purged {old}:{pos} (listed now {listed:?}) → {answer}",
            s.name
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10f — the per-change order key: each rows event's end_log_pos, inside a multi-row event,
// and inside a compressed transaction payload (MySQL).

#[tokio::test(flavor = "multi_thread")]
async fn s_my10f_positions_inside_a_transaction() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10f skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for (s, compressed) in [
        (MYSQL, false),
        (MYSQL, true),
        (MARIADB_1011, false),
        (MARIADB_114, false),
        (MARIADB_118, false),
    ] {
        let mut c = conn(s).await?;
        fresh_table(&mut c, "my10f").await?;
        c.query_drop("INSERT INTO my10f VALUES (1, 0), (2, 0)")
            .await?;
        if compressed {
            c.query_drop("SET SESSION binlog_transaction_compression = ON")
                .await?;
        }
        let (file, pos) = status(s, &mut c).await?;
        let mut r = open(s, &file, pos, 9105, None).await?;
        c.query_drop(
            "START TRANSACTION; INSERT INTO my10f VALUES (3, 0); UPDATE my10f SET v = 1 WHERE id = 3; \
             INSERT INTO my10f VALUES (4, 0), (5, 0); UPDATE my10f SET id = id + 10 ORDER BY id DESC; COMMIT",
        )
        .await?;
        r.until_xids(1, Duration::from_secs(5)).await?;
        let rows: Vec<&Ev> = r
            .seen
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Ev::Rows { .. } | Ev::Payload { .. } | Ev::Xid { .. } | Ev::Map { .. }
                )
            })
            .collect();
        println!("S-MY10f {} (compressed {compressed}): {rows:?}; (header, computed) {:?}; mismatches {:?}", s.name, r.offsets, r.mismatches);
        assert!(r.mismatches.is_empty(), "{}", s.name);
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10g — heartbeats on an idle file/position stream: the raw type, the header position and
// the body.

#[tokio::test(flavor = "multi_thread")]
async fn s_my10g_heartbeats() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10g skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in ALL {
        let mut c = conn(s).await?;
        let (file, pos) = status(s, &mut c).await?;
        let mut r = open(s, &file, pos, 9106, Some(Duration::from_secs(1))).await?;
        let deadline = Instant::now() + Duration::from_millis(3500);
        while Instant::now() < deadline {
            if r.next(Duration::from_millis(500)).await?.is_none() {
                continue;
            }
        }
        let beats: Vec<&Ev> = r
            .seen
            .iter()
            .filter(|e| matches!(e, Ev::Heartbeat { .. }))
            .collect();
        println!(
            "S-MY10g {}: idle at {file}:{pos} for 3.5 s → {} heartbeats {beats:?}; all events {:?}",
            s.name,
            beats.len(),
            r.seen
        );
        assert!(beats.len() >= 2, "{}: heartbeats while idle", s.name);
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10h — two dumps with the same replica server id: does the server end the older one?

#[tokio::test(flavor = "multi_thread")]
async fn s_my10h_same_replica_id() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10h skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in ALL {
        let mut c = conn(s).await?;
        fresh_table(&mut c, "my10h").await?;
        let (file, pos) = status(s, &mut c).await?;
        let mut first = open(s, &file, pos, 9107, Some(Duration::from_secs(1))).await?;
        first.next(Duration::from_secs(2)).await?;
        let mut second = open(s, &file, pos, 9107, Some(Duration::from_secs(1))).await?;
        second.next(Duration::from_secs(2)).await?;
        c.query_drop("INSERT INTO my10h VALUES (1, 0)").await?;
        let deadline = Instant::now() + Duration::from_secs(4);
        let mut first_end = String::from("still open");
        while Instant::now() < deadline {
            match first.next(Duration::from_millis(500)).await {
                Ok(_) => {}
                Err(e) => {
                    first_end = format!("ended: {e:#}");
                    break;
                }
            }
        }
        second.until_xids(1, Duration::from_secs(3)).await?;
        println!(
            "S-MY10h {}: second dump with the same replica id; the first {first_end}, rows {:?}; the second rows {:?}",
            s.name,
            first.keys("my10h"),
            second.keys("my10h")
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY11 — DA-SNAP-10: a semi-sync AFTER_SYNC commit stalled on the replica ack. Does the binlog
// stream deliver it while a read can't see it, does the start position already include it, and
// does a locking read (`LOCK IN SHARE MODE`) wait for it?

#[tokio::test(flavor = "multi_thread")]
async fn s_my11_stalled_commit() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY11 skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in [MYSQL, MARIADB_1011, MARIADB_118] {
        let var = if s.maria { "master" } else { "source" };
        let mut c = conn(s).await?;
        if !s.maria {
            let installed: Option<String> = c
                .query_first("SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'rpl_semi_sync_source'")
                .await?;
            if installed.is_none() {
                c.query_drop("INSTALL PLUGIN rpl_semi_sync_source SONAME 'semisync_source.so'")
                    .await?;
            }
        }
        fresh_table(&mut c, "my11").await?;
        c.query_drop("INSERT INTO my11 VALUES (1, 0)").await?;
        let (file, pos) = status(s, &mut c).await?;
        let gtid_sql = if s.maria {
            "SELECT @@global.gtid_binlog_pos"
        } else {
            "SELECT @@global.gtid_executed"
        };
        let gtid_before: String = c.query_first(gtid_sql).await?.unwrap_or_default();
        let mut r = open(s, &file, pos, 9108, None).await?;
        c.query_drop(format!(
            "SET GLOBAL rpl_semi_sync_{var}_timeout = 6000; SET GLOBAL rpl_semi_sync_{var}_wait_point = 'AFTER_SYNC'; SET GLOBAL rpl_semi_sync_{var}_enabled = 1"
        ))
        .await?;
        let t0 = Instant::now();
        let writer = tokio::spawn(async move {
            let mut w = conn(s).await?;
            w.query_drop("INSERT INTO my11 VALUES (2, 0)").await?;
            anyhow::Ok(t0.elapsed())
        });
        // The stalled transaction on the stream, and when.
        let mut delivered = None;
        while t0.elapsed() < Duration::from_secs(5) {
            if let Some(Ev::Xid { .. }) = r.next(Duration::from_millis(200)).await? {
                delivered = Some(t0.elapsed());
                break;
            }
        }
        let plain: Vec<i64> = c.query("SELECT id FROM my11 ORDER BY id").await?;
        let (file_now, pos_now) = status(s, &mut c).await?;
        let gtid_now: String = c.query_first(gtid_sql).await?.unwrap_or_default();
        let lr = Instant::now();
        let mut l = conn(s).await?;
        l.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .await?;
        l.query_drop("START TRANSACTION").await?;
        let locked: Vec<i64> = l
            .query("SELECT id FROM my11 ORDER BY id LOCK IN SHARE MODE")
            .await?;
        l.query_drop("COMMIT").await?;
        let lock_wait = lr.elapsed();
        let committed = writer.await??;
        c.query_drop(format!("SET GLOBAL rpl_semi_sync_{var}_enabled = 0"))
            .await?;
        println!(
            "S-MY11 {}: stream delivered the stalled transaction after {delivered:?} (the commit returned after {committed:?}); \
             a plain read then saw {plain:?}; status moved {file}:{pos} → {file_now}:{pos_now}; GTID {gtid_before:?} → {gtid_now:?}; \
             a locking read waited {lock_wait:?} and saw {locked:?}; rows {:?}",
            s.name,
            r.keys("my11")
        );
        assert!(
            delivered.is_some(),
            "{}: the stream delivers the stalled commit",
            s.name
        );
        assert_eq!(plain, [1], "{}: a plain read doesn't see it yet", s.name);
        assert_eq!(
            locked,
            [1, 2],
            "{}: a locking read waits for it and sees it",
            s.name
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10i — the per-change order key in GTID dumps (the server skips executed transactions) and
// in a MariaDB file/position dump that declares the GTID capability: the hybrid rule above.

async fn open_gtid(s: Server, gtid: &str, replica_id: u32) -> anyhow::Result<Reader> {
    let mut c = conn(s).await?;
    let mut req = mysql_async::BinlogStreamRequest::new(replica_id);
    if s.maria {
        c.query_drop("SET @mariadb_slave_capability = 4").await?;
        c.query_drop(format!("SET @slave_connect_state = '{gtid}'"))
            .await?;
        c.query_drop("SET @slave_gtid_strict_mode = 1").await?;
    } else {
        let sids: Vec<mysql_async::Sid<'static>> = gtid
            .split(',')
            .filter(|x| !x.trim().is_empty())
            .map(|x| {
                x.trim()
                    .parse()
                    .map_err(|e| anyhow::anyhow!("gtid {x}: {e:?}"))
            })
            .collect::<anyhow::Result<_>>()?;
        req = req.with_gtid().with_gtid_set(sids);
    }
    let stream = c.get_binlog_stream(req).await?;
    Ok(Reader {
        stream,
        file: String::new(),
        seen: vec![],
        offset: 0,
        inner: 0,
        anchored: false,
        gtid: true,
        chained: false,
        gaps: vec![],
        mismatches: vec![],
        offsets: vec![],
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn s_my10i_order_key_in_gtid_dumps() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10i skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in [MYSQL, MARIADB_1011, MARIADB_118] {
        let mut c = conn(s).await?;
        fresh_table(&mut c, "my10i").await?;
        c.query_drop("INSERT INTO my10i VALUES (1, 0)").await?;
        let gtid_sql = if s.maria {
            "SELECT @@global.gtid_binlog_pos"
        } else {
            "SELECT @@global.gtid_executed"
        };
        let gtid: String = c.query_first(gtid_sql).await?.unwrap_or_default();
        c.query_drop("INSERT INTO my10i VALUES (2, 0)").await?;
        c.query_drop("FLUSH BINARY LOGS").await?;
        c.query_drop("START TRANSACTION; INSERT INTO my10i VALUES (3, 0), (4, 0); UPDATE my10i SET v = 1 WHERE id = 3; COMMIT").await?;
        let mut r = open_gtid(s, &gtid.replace('\n', ""), 9109).await?;
        r.until_xids(2, Duration::from_secs(5)).await?;
        let rows: Vec<(String, u32, (String, u64))> = r
            .seen
            .iter()
            .zip(r.offsets.iter())
            .filter_map(|(e, (h, o, f))| match e {
                Ev::Rows { kind, keys, .. } => {
                    Some((format!("{kind} {keys:?}"), *h, (f.clone(), *o)))
                }
                _ => None,
            })
            .collect();
        println!("S-MY10i {} GTID dump after {gtid:?}: rows (event, header, order key) {rows:?}; gaps {:?}; mismatches {:?}", s.name, r.gaps, r.mismatches);
        assert_eq!(r.keys("my10i"), ["2", "3", "4", "3->3"], "{}", s.name);
        assert!(r.mismatches.is_empty(), "{}", s.name);
        let keys: Vec<(String, u64)> = rows.iter().map(|r| r.2.clone()).collect();
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "{}: strictly increasing",
            s.name
        );
    }
    // MariaDB file/position with the GTID capability: real GTID events, positions unchanged.
    let s = MARIADB_118;
    let mut c = conn(s).await?;
    fresh_table(&mut c, "my10j").await?;
    let (file, pos) = status(s, &mut c).await?;
    let mut cc = conn(s).await?;
    cc.query_drop("SET @mariadb_slave_capability = 4").await?;
    let name = file.as_bytes().to_vec();
    let req = mysql_async::BinlogStreamRequest::new(9110)
        .with_filename(&name)
        .with_pos(pos);
    let stream = cc.get_binlog_stream(req).await?;
    let mut r = Reader {
        stream,
        file: file.clone(),
        seen: vec![],
        offset: pos,
        inner: 0,
        anchored: true,
        gtid: false,
        chained: true,
        gaps: vec![],
        mismatches: vec![],
        offsets: vec![],
    };
    c.query_drop("START TRANSACTION; INSERT INTO my10j VALUES (1, 0), (2, 0); UPDATE my10j SET v = 1 WHERE id = 1; COMMIT").await?;
    r.until_xids(1, Duration::from_secs(5)).await?;
    println!("S-MY10i MariaDB 11.8 file/position with capability 4: {:?}; (header, key) {:?}; gaps {:?}; mismatches {:?}", r.seen, r.offsets, r.gaps, r.mismatches);
    assert!(r.mismatches.is_empty());
    assert!(
        r.seen.iter().any(|e| matches!(e, Ev::Gtid { .. })),
        "a real GTID event"
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// S-MY10k — GTID mode after a purge: what the dump answers when the transactions the reader
// still needs are gone, and what the server's variables show beforehand.

#[tokio::test(flavor = "multi_thread")]
async fn s_my10k_gtid_purged() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY10k skipped: set SRC_PROBE=1");
        return Ok(());
    }
    for s in [MYSQL, MARIADB_1011, MARIADB_118] {
        let mut c = conn(s).await?;
        fresh_table(&mut c, "my10k").await?;
        let gtid_sql = if s.maria {
            "SELECT @@global.gtid_binlog_pos"
        } else {
            "SELECT @@global.gtid_executed"
        };
        let before: String = c.query_first(gtid_sql).await?.unwrap_or_default();
        c.query_drop("INSERT INTO my10k VALUES (1, 0)").await?;
        c.query_drop("FLUSH BINARY LOGS").await?;
        c.query_drop("INSERT INTO my10k VALUES (2, 0)").await?;
        let (current, _) = status(s, &mut c).await?;
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut listed;
        loop {
            c.query_drop(format!("PURGE BINARY LOGS TO '{current}'"))
                .await?;
            listed = binary_logs(&mut c).await?;
            if listed.len() == 1 || Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let purged_var: String = if s.maria {
            let first: String = c
                .query_first(format!("SELECT BINLOG_GTID_POS('{current}', 4)"))
                .await?
                .unwrap_or_default();
            format!("BINLOG_GTID_POS('{current}', 4) = {first:?}")
        } else {
            let p: String = c
                .query_first("SELECT @@global.gtid_purged")
                .await?
                .unwrap_or_default();
            format!("@@gtid_purged = {p:?}")
        };
        let answer = match open_gtid(s, &before.replace('\n', ""), 9111).await {
            Err(e) => format!("open fails: {e:#}"),
            Ok(mut r) => match r.next(Duration::from_secs(5)).await {
                Ok(_) => match r.until_xids(1, Duration::from_secs(3)).await {
                    Ok(x) => format!("streams ({} XIDs, rows {:?})", x.len(), r.keys("my10k")),
                    Err(e) => format!("error after the first event: {e:#}"),
                },
                Err(e) => format!("{e:#}"),
            },
        };
        println!("S-MY10k {}: needs after {before:?}; logs now {listed:?}; {purged_var}; the GTID dump → {answer}", s.name);
    }
    Ok(())
}

/// S-MY11b — what the DA-SNAP-10 locking chunk read costs: the privilege it needs, how long it
/// waits for an open writer of its rows, what bounds that wait, and whom it blocks.
#[tokio::test]
async fn s_my11b_locking_read_cost() -> anyhow::Result<()> {
    if !enabled() {
        println!("S-MY11b skipped: set SRC_PROBE=1");
        return Ok(());
    }
    const CHUNK: &str =
        "SELECT id, v FROM my11b WHERE id > 0 ORDER BY id LIMIT 2 LOCK IN SHARE MODE";
    for s in [MYSQL, MARIADB_1011, MARIADB_118] {
        let mut c = conn(s).await?;
        fresh_table(&mut c, "my11b").await?;
        c.query_drop("INSERT INTO my11b VALUES (1, 1), (2, 2), (3, 3)")
            .await?;
        // A replication user with no write privilege at all.
        c.query_drop("DROP USER IF EXISTS repl_ro").await?;
        c.query_drop("CREATE USER repl_ro IDENTIFIED BY 'ro'")
            .await?;
        c.query_drop("GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO repl_ro")
            .await?;
        let ro_url = s.url.replace("root:probe", "repl_ro:ro");
        let reader = || async {
            let mut r = mysql_async::Conn::from_url(ro_url.as_str()).await?;
            r.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                .await?;
            anyhow::Ok(r)
        };

        // 1. The privilege: SELECT is enough.
        let mut r = reader().await?;
        r.query_drop("START TRANSACTION").await?;
        let rows: Vec<(i64, i64)> = r.query(CHUNK).await?;
        r.query_drop("COMMIT").await?;
        assert_eq!(rows, [(1, 1), (2, 2)], "{}: SELECT-only user", s.name);

        // 2. An open writer of a chunk row: the read waits for its commit, then sees its value.
        let mut w = conn(s).await?;
        w.query_drop("START TRANSACTION").await?;
        w.query_drop("UPDATE my11b SET v = 20 WHERE id = 2").await?;
        let t0 = Instant::now();
        let read = tokio::spawn(async move {
            let mut r = r;
            r.query_drop("START TRANSACTION").await?;
            let rows: Vec<(i64, i64)> = r.query(CHUNK).await?;
            r.query_drop("COMMIT").await?;
            anyhow::Ok((rows, t0.elapsed()))
        });
        tokio::time::sleep(Duration::from_secs(2)).await;
        w.query_drop("COMMIT").await?;
        let (seen, waited) = read.await??;
        assert_eq!(
            seen,
            [(1, 1), (2, 20)],
            "{}: waits, then sees the commit",
            s.name
        );
        assert!(
            waited >= Duration::from_secs(2),
            "{}: waited {waited:?}",
            s.name
        );

        // 3. The wait is bounded by innodb_lock_wait_timeout: error 1205.
        w.query_drop("START TRANSACTION").await?;
        w.query_drop("UPDATE my11b SET v = 21 WHERE id = 2").await?;
        let mut r = reader().await?;
        r.query_drop("SET SESSION innodb_lock_wait_timeout = 1")
            .await?;
        r.query_drop("START TRANSACTION").await?;
        let t1 = Instant::now();
        let timed_out = r.query::<(i64, i64), _>(CHUNK).await;
        let after = t1.elapsed();
        r.query_drop("ROLLBACK").await?;
        w.query_drop("COMMIT").await?;
        let code = match &timed_out {
            Err(mysql_async::Error::Server(e)) => e.code,
            other => anyhow::bail!("{}: expected a lock wait timeout, got {other:?}", s.name),
        };
        assert_eq!(code, 1205, "{}: lock wait timeout", s.name);

        // 4. Whom a chunk read blocks while its transaction is open: only writers of its rows
        //    (READ COMMITTED takes no gap locks).
        r.query_drop("START TRANSACTION").await?;
        let _held: Vec<(i64, i64)> = r.query(CHUNK).await?;
        let mut o = conn(s).await?;
        o.query_drop("SET SESSION innodb_lock_wait_timeout = 1")
            .await?;
        let insert = o.query_drop("INSERT INTO my11b VALUES (10, 10)").await;
        let outside = o.query_drop("UPDATE my11b SET v = 30 WHERE id = 3").await;
        let inside = o.query_drop("UPDATE my11b SET v = 11 WHERE id = 1").await;
        r.query_drop("COMMIT").await?;
        c.query_drop("DROP USER repl_ro").await?;
        println!(
            "S-MY11b {}: SELECT-only user reads {rows:?}; an open writer of row 2 held it {waited:?}, then the read saw {seen:?}; \
             innodb_lock_wait_timeout=1 ended the wait after {after:?} with {code}; while a chunk transaction holds rows 1-2: \
             insert of id 10 {insert:?}, update of id 3 {outside:?}, update of id 1 {}",
            s.name,
            match &inside {
                Err(mysql_async::Error::Server(e)) => format!("error {}", e.code),
                other => format!("{other:?}"),
            }
        );
        assert!(
            insert.is_ok() && outside.is_ok(),
            "{}: rows outside the chunk aren't blocked",
            s.name
        );
        assert!(
            matches!(&inside, Err(mysql_async::Error::Server(e)) if e.code == 1205),
            "{}: a writer of a chunk row waits for the chunk transaction",
            s.name
        );
    }
    Ok(())
}
