//! Stream positions on the server (S-MY10): the dialect a pipeline's positions take, the server's
//! write position now, its identity, the binlogs it still holds, and whether a checkpoint can
//! still be served. The decisions are pure functions over what the server reported.

use data_avalanche_config::ReplicationType;
use data_avalanche_core::position::{BinlogFilePos, GtidSet, MariaGtidState};
use data_avalanche_core::{ServerIdentity, SourceError, SourcePosition};
use mysql_async::prelude::Queryable;

use crate::connector::MySqlConnector;
use crate::server::Flavour;

/// The dialect of a pipeline's positions: a MySQL GTID set, a MariaDB GTID state, or a binlog
/// file/position (either server, `replication_type: binlog`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    MysqlGtid,
    MariadbGtid,
    File,
}

impl Mode {
    pub fn of(flavour: Flavour, replication: ReplicationType) -> Self {
        match (replication, flavour) {
            (ReplicationType::Binlog, _) => Self::File,
            (ReplicationType::Gtid, Flavour::Mysql) => Self::MysqlGtid,
            (ReplicationType::Gtid, Flavour::Mariadb) => Self::MariadbGtid,
        }
    }
}

/// One row of `SHOW BINARY LOGS`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryLog {
    pub name: String,
    pub size: u64,
}

/// The binlog bytes the server holds after `at`: the rest of its file and every later file. A
/// file not in the list (purged, or another series) counts nothing.
pub fn bytes_after(logs: &[BinaryLog], at: &BinlogFilePos) -> u64 {
    let Some(i) = logs.iter().position(|l| l.name == at.file()) else {
        return 0;
    };
    let rest = logs[i].size.saturating_sub(at.pos());
    rest + logs[i + 1..].iter().map(|l| l.size).sum::<u64>()
}

/// What the server reports about the binlogs it no longer has, in the checkpoint's dialect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retention {
    /// `SHOW BINARY LOGS`: the files it still has.
    Files(Vec<BinaryLog>),
    /// MySQL `@@GLOBAL.gtid_purged`.
    MysqlPurged(GtidSet),
    /// MariaDB `BINLOG_GTID_POS(<oldest file>, 4)`: the state where the oldest binlog starts.
    MariadbOldest(MariaGtidState),
}

/// Why the server can't serve `checkpoint` any more, or `None` when it can: its file is gone, or
/// it lacks transactions the server purged (MySQL), or the oldest binlog starts after it
/// (MariaDB). The server's own refusal (error 1236) is the fallback for a purge after this check.
pub fn purged(checkpoint: &SourcePosition, retention: &Retention) -> Option<String> {
    match (checkpoint, retention) {
        (SourcePosition::BinlogFile(at), Retention::Files(logs)) => {
            (!logs.iter().any(|l| l.name == at.file())).then(|| {
                let first = logs.first().map_or("none", |l| l.name.as_str());
                format!(
                    "binlog {} is no longer on the server (oldest: {first})",
                    at.file()
                )
            })
        }
        (SourcePosition::MySqlGtid(set), Retention::MysqlPurged(purged)) => (!set.covers(purged))
            .then(|| format!("the server purged GTIDs {purged} the position {set} hasn't read")),
        (SourcePosition::MariaDbGtid(state), Retention::MariadbOldest(oldest)) => (!state
            .covers(oldest))
        .then(|| format!("the oldest binlog starts at {oldest}, after the position {state}")),
        (p, r) => Some(format!("position {p} can't be checked against {r:?}")),
    }
}

impl MySqlConnector {
    /// The dialect this pipeline's positions take on this server.
    pub async fn mode(&self) -> Result<Mode, SourceError> {
        Ok(Mode::of(self.flavour().await?, self.mysql.replication_type))
    }

    /// The server's write position now, in the pipeline's dialect: MySQL `@@GLOBAL.gtid_executed`,
    /// MariaDB `@@GLOBAL.gtid_binlog_pos`, or the binlog status statement's file and position.
    pub async fn position_now(&self) -> Result<SourcePosition, SourceError> {
        let mode = self.mode().await?;
        let status = self.flavour().await?.status_statement();
        let text = self
            .conn
            .command("read the binlog position", move |c| {
                Box::pin(async move {
                    match mode {
                        Mode::MysqlGtid => c.query_first("SELECT @@GLOBAL.gtid_executed").await,
                        Mode::MariadbGtid => c.query_first("SELECT @@GLOBAL.gtid_binlog_pos").await,
                        Mode::File => {
                            let row: Option<mysql_async::Row> = c.query_first(status).await?;
                            Ok(row.map(|r| {
                                let file: String = r.get(0).unwrap_or_default();
                                let pos: u64 = r.get(1).unwrap_or_default();
                                format!("{file}:{pos}")
                            }))
                        }
                    }
                })
            })
            .await?
            .ok_or_else(|| {
                SourceError::Permanent("the server reports no binlog position".into())
            })?;
        parse(mode, &text)
    }

    /// The server a file/position belongs to: MySQL `@@server_uuid`, MariaDB `@@server_id`
    /// (MariaDB has no server UUID, S-MY10a).
    pub async fn server_identity(&self) -> Result<ServerIdentity, SourceError> {
        let flavour = self.flavour().await?;
        self.conn
            .command("read the server identity", move |c| {
                Box::pin(async move {
                    Ok(match flavour {
                        Flavour::Mysql => ServerIdentity::Mysql {
                            server_uuid: c
                                .query_first("SELECT @@server_uuid")
                                .await?
                                .unwrap_or_default(),
                        },
                        Flavour::Mariadb => ServerIdentity::Mariadb {
                            server_id: c
                                .query_first("SELECT @@server_id")
                                .await?
                                .unwrap_or_default(),
                        },
                    })
                })
            })
            .await
    }

    /// `SHOW BINARY LOGS`, oldest first.
    pub async fn binary_logs(&self) -> Result<Vec<BinaryLog>, SourceError> {
        self.conn
            .command("list the binlogs", |c| {
                Box::pin(async move {
                    let rows: Vec<mysql_async::Row> = c.query("SHOW BINARY LOGS").await?;
                    Ok(rows
                        .into_iter()
                        .map(|r| BinaryLog {
                            name: r.get(0).unwrap_or_default(),
                            size: r.get(1).unwrap_or_default(),
                        })
                        .collect())
                })
            })
            .await
    }

    /// What the server reports about purged binlogs, in `checkpoint`'s dialect.
    pub async fn retention(&self, checkpoint: &SourcePosition) -> Result<Retention, SourceError> {
        match checkpoint {
            SourcePosition::BinlogFile(_) => Ok(Retention::Files(self.binary_logs().await?)),
            SourcePosition::MySqlGtid(_) => {
                let text: String = self
                    .conn
                    .command("read gtid_purged", |c| {
                        Box::pin(async move {
                            Ok(c.query_first("SELECT @@GLOBAL.gtid_purged")
                                .await?
                                .unwrap_or_default())
                        })
                    })
                    .await?;
                match parse(Mode::MysqlGtid, &text)? {
                    SourcePosition::MySqlGtid(set) => Ok(Retention::MysqlPurged(set)),
                    _ => unreachable!("parsed in the MySQL GTID dialect"),
                }
            }
            SourcePosition::MariaDbGtid(_) => {
                let logs = self.binary_logs().await?;
                let Some(oldest) = logs.first().map(|l| l.name.clone()) else {
                    return Ok(Retention::MariadbOldest(MariaGtidState::default()));
                };
                let text: Option<String> = self
                    .conn
                    .command("read the oldest binlog's GTID state", move |c| {
                        Box::pin(async move {
                            c.exec_first("SELECT BINLOG_GTID_POS(?, 4)", (oldest,))
                                .await
                        })
                    })
                    .await?;
                match parse(Mode::MariadbGtid, text.as_deref().unwrap_or_default())? {
                    SourcePosition::MariaDbGtid(s) => Ok(Retention::MariadbOldest(s)),
                    _ => unreachable!("parsed in the MariaDB GTID dialect"),
                }
            }
            SourcePosition::Pg(_) => Err(SourceError::Permanent(format!(
                "position {checkpoint} isn't a MySQL position"
            ))),
        }
    }
}

/// A position the server printed, in `mode`'s dialect.
pub(crate) fn parse(mode: Mode, text: &str) -> Result<SourcePosition, SourceError> {
    let tag = match mode {
        Mode::MysqlGtid => "mysql-gtid",
        Mode::MariadbGtid => "mariadb-gtid",
        Mode::File => "binlog",
    };
    SourcePosition::parse_tagged(&format!("{tag}:{text}"))
        .map_err(|e| SourceError::Permanent(format!("the server's position: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logs(sizes: &[(&str, u64)]) -> Vec<BinaryLog> {
        sizes
            .iter()
            .map(|(n, s)| BinaryLog {
                name: n.to_string(),
                size: *s,
            })
            .collect()
    }

    fn pos(s: &str) -> SourcePosition {
        SourcePosition::parse_tagged(s).unwrap()
    }

    #[test]
    fn the_mode_follows_the_flavour_and_replication_type() {
        use Flavour::*;
        use ReplicationType::*;
        assert_eq!(Mode::of(Mysql, Gtid), Mode::MysqlGtid);
        assert_eq!(Mode::of(Mariadb, Gtid), Mode::MariadbGtid);
        assert_eq!(Mode::of(Mysql, Binlog), Mode::File);
        assert_eq!(Mode::of(Mariadb, Binlog), Mode::File);
    }

    #[test]
    fn server_positions_parse_in_their_dialect() {
        // gtid_executed wraps long sets with a newline after each comma.
        let set =
            "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,\n3e11fa47-71ca-11e1-9e33-c80aa9429563:1";
        assert!(matches!(
            parse(Mode::MysqlGtid, set),
            Ok(SourcePosition::MySqlGtid(_))
        ));
        assert_eq!(
            parse(Mode::MysqlGtid, "").unwrap(),
            SourcePosition::MySqlGtid(GtidSet::default())
        );
        assert_eq!(
            parse(Mode::MariadbGtid, "0-1-42").unwrap(),
            pos("mariadb-gtid:0-1-42")
        );
        assert_eq!(
            parse(Mode::File, "mysql-bin.000003:157").unwrap(),
            pos("binlog:mysql-bin.000003:157")
        );
        assert!(parse(Mode::File, ":0").is_err());
    }

    #[test]
    fn retained_bytes_count_the_rest_of_the_file_and_later_files() {
        let l = logs(&[("b.000001", 1000), ("b.000002", 500), ("b.000003", 200)]);
        let at = |s: &str| s.parse::<BinlogFilePos>().unwrap();
        assert_eq!(bytes_after(&l, &at("b.000002:100")), 400 + 200);
        assert_eq!(bytes_after(&l, &at("b.000003:200")), 0);
        assert_eq!(bytes_after(&l, &at("b.000001:4")), 996 + 700);
        assert_eq!(bytes_after(&l, &at("b.000009:4")), 0, "not on the server");
    }

    #[test]
    fn a_file_gone_from_the_server_is_purged() {
        let l = Retention::Files(logs(&[("b.000002", 500), ("b.000003", 200)]));
        assert_eq!(purged(&pos("binlog:b.000002:4"), &l), None);
        assert_eq!(
            purged(&pos("binlog:b.000001:1193"), &l).unwrap(),
            "binlog b.000001 is no longer on the server (oldest: b.000002)"
        );
    }

    #[test]
    fn purged_gtids_the_checkpoint_lacks_are_purged() {
        let u = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let purged_set = |s: &str| Retention::MysqlPurged(s.parse().unwrap());
        let cp = pos(&format!("mysql-gtid:{u}:1-10"));
        assert_eq!(purged(&cp, &purged_set("")), None);
        assert_eq!(purged(&cp, &purged_set(&format!("{u}:1-10"))), None);
        let p = purged(&cp, &purged_set(&format!("{u}:1-12"))).unwrap();
        assert!(p.contains(&format!("{u}:1-12")), "{p}");
    }

    #[test]
    fn a_mariadb_checkpoint_before_the_oldest_binlog_is_purged() {
        let oldest = |s: &str| Retention::MariadbOldest(s.parse().unwrap());
        let cp = pos("mariadb-gtid:0-1-42");
        assert_eq!(purged(&cp, &oldest("")), None, "no purge yet");
        assert_eq!(purged(&cp, &oldest("0-1-42")), None);
        assert!(purged(&cp, &oldest("0-1-43")).is_some());
        // A domain the checkpoint never saw whose start was purged.
        assert!(purged(&cp, &oldest("0-1-40,1-1-5")).is_some());
    }
}
