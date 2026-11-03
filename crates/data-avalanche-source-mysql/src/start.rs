//! A start's stream position (Data-Flow Phase 2b, G-26, Q-M7-2, Q-M7a-6), the heartbeat's watch on
//! the dump.
//!
//! A first start snapshots from the server's write position and records the server. A checkpoint
//! in another dialect than the pipeline reads now (`replication_type` changed, or the server is
//! another flavour) and a checkpoint the server can no longer serve both follow
//! `binlog_purge_action`: at a start, `re_snapshot` plans a snapshot from the server's position
//! now (`PositionLost`, with a warning) and `error` halts. A purge the stream meets later (the
//! dump's error 1236) follows it through [`purge_action`]. In file/position mode a checkpoint
//! recorded on another server is lost too (DA-SRC-29): a position in one server's files means
//! nothing in another's, so it is never resumed. `error` halts the pipeline
//! (`BINLOG_SOURCE_CHANGED`); `re_snapshot` plans a snapshot from the server's position now
//! (`SourceChanged`: the pipeline resets every table's marker to it). MariaDB names a server by
//! `@@server_id` alone, which can repeat, so there a position that isn't an event's boundary on
//! the server (`BINLOG_GTID_POS` NULL, DA-SRC-21) is another server's too.

use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use data_avalanche_config::BinlogPurgeAction;
use data_avalanche_core::position::BinlogFilePos;
use data_avalanche_core::{
    Checkpoint, Prepared, RetainedLog, ServerIdentity, SnapshotReason, SourceError, SourcePosition,
    StartPlan,
};
use mysql_async::prelude::Queryable;

use crate::connector::MySqlConnector;
use crate::position::{BinaryLog, Mode, purged};
use crate::server::Flavour;

/// Where the binlog stream has read to, for the heartbeat's lag
/// (`data_avalanche_binlog_lag_seconds`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadAt {
    /// Where the stream opened, then each transaction's end.
    pub position: SourcePosition,
    /// The last transaction's commit time (microseconds since the Unix epoch); `None` before the
    /// stream read one.
    pub commit_micros: Option<i64>,
    /// A heartbeat came after it: the dump had nothing more to send. A commit clears it.
    pub caught_up: Option<CaughtUp>,
}

/// The stream caught up with the server at a heartbeat. The server's write position isn't known
/// then, so the first check after it takes the position as the baseline: a write the server makes
/// between the heartbeat and that check becomes the baseline, and a stall then shows only from
/// the server's next write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CaughtUp {
    /// The heartbeat's wall time (microseconds since the Unix epoch).
    pub at_micros: i64,
    /// The server's write position at the first check after the heartbeat.
    pub head: Option<SourcePosition>,
}

/// How far the stream that read to `read` is behind the server's write position `head` at
/// `now_micros`, and the baseline to record for a stream that just caught up (even one at `head`:
/// the baseline is where the server stood then). Zero when the stream's position covers `head`, or
/// it caught up at a heartbeat and `head` hasn't moved past the baseline since; once `head` moved past it, the time since the heartbeat; else the age of
/// the last transaction it read. Never negative. `None` when `head` is unknown, or the stream is
/// behind without having read a transaction.
pub(crate) fn behind(
    read: &ReadAt,
    head: Option<&SourcePosition>,
    now_micros: i64,
) -> (Option<Duration>, Option<SourcePosition>) {
    let Some(head) = head else {
        return (None, None);
    };
    let since = |micros: i64| {
        let age = now_micros.saturating_sub(micros);
        Some(Duration::from_micros(u64::try_from(age).unwrap_or(0)))
    };
    let covered = read.position.covers(head).unwrap_or(false);
    match &read.caught_up {
        Some(CaughtUp { head: None, .. }) => (Some(Duration::ZERO), Some(head.clone())),
        _ if covered => (Some(Duration::ZERO), None),
        Some(CaughtUp {
            at_micros,
            head: Some(baseline),
        }) => (
            if baseline == head {
                Some(Duration::ZERO)
            } else {
                since(*at_micros)
            },
            None,
        ),
        None => (read.commit_micros.and_then(since), None),
    }
}

pub(crate) fn now_micros() -> i64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
}

/// Why `checkpoint` can't be read in `mode`, if it can't.
pub fn dialect_change(mode: Mode, checkpoint: &SourcePosition) -> Option<String> {
    (checkpoint.dialect() != mode.dialect()).then(|| {
        format!(
            "the checkpoint {} is a {} position, and the pipeline now reads {} positions \
             (replication_type changed, or the server is another flavour)",
            checkpoint.to_tagged(),
            checkpoint.dialect(),
            mode.dialect()
        )
    })
}

/// MariaDB's GTID state at a binlog position: NULL unless the position is an event's boundary
/// there ([`at_boundary`]).
pub(crate) const BOUNDARY_QUERY: &str = "SELECT BINLOG_GTID_POS(?, ?)";

/// File/position mode: why the checkpoint isn't from the connected server, or `None` when it is.
pub(crate) fn server_changed(checkpoint: &Checkpoint, now: &ServerIdentity) -> Option<String> {
    let at = checkpoint.position.untagged();
    let recorded = match &checkpoint.server {
        Some(s) if s == now => return None,
        Some(s) => s.to_string(),
        None => "an unrecorded server".to_owned(),
    };
    Some(format!(
        "binlog position {at} was recorded on {recorded}, now connected to {now}"
    ))
}

/// File/position mode: the checkpoint must be from the connected server (Q-M7-2). The halt
/// `binlog_purge_action: error` gives otherwise ([`halt`]).
pub fn same_server(checkpoint: &Checkpoint, now: &ServerIdentity) -> Result<(), SourceError> {
    server_changed(checkpoint, now).map_or(Ok(()), |why| Err(halt(why)))
}

/// A binlog file name split into its series (the base name) and index: `binlog.000003` is
/// (`binlog`, 3), from the server's list.
fn series(file: &str) -> Option<(&str, u64)> {
    let (base, index) = file.rsplit_once('.')?;
    Some((base, index.parse().ok()?))
}

/// Whether the connected server, whose binlogs are `logs` (oldest first), could never have held
/// the checkpoint's file, so the checkpoint is another server's rather than a purge: no log has
/// its base name, or its index is past the newest log's in its series. A file before the oldest
/// of its series is a purge ([`crate::purged`]), a file the server holds is its own; an empty
/// list is left to the purge's checks.
pub fn never_held(at: &BinlogFilePos, logs: &[BinaryLog]) -> bool {
    let (base, index) = (at.base(), at.file_index());
    if logs.is_empty() || logs.iter().any(|l| l.name == at.file()) {
        return false;
    }
    let newest = logs
        .iter()
        .filter_map(|l| series(&l.name))
        .filter(|(b, _)| *b == base)
        .map(|(_, i)| i)
        .max();
    newest.is_none_or(|newest| index > newest)
}

/// MariaDB file/position mode: whether `at` is an event's boundary in a binlog of the connected
/// server, from its answer to `SELECT BINLOG_GTID_POS(<file>, <pos>)`: NULL for a file it doesn't
/// hold, an offset inside an event or past the end (measured on MariaDB 11.8). A checkpoint is
/// always one: a transaction's end, a file's start. A NULL for a file the server no longer holds
/// (`held` false) is a purge since, which the purge's own checks report. The halt
/// `binlog_purge_action: error` gives otherwise ([`halt`]).
pub fn at_boundary(
    at: &SourcePosition,
    now: &ServerIdentity,
    answer: Option<Option<String>>,
    held: bool,
) -> Result<(), SourceError> {
    off_boundary(at, now, answer, held).map_or(Ok(()), |why| Err(halt(why)))
}

fn off_boundary(
    at: &SourcePosition,
    now: &ServerIdentity,
    answer: Option<Option<String>>,
    held: bool,
) -> Option<String> {
    match answer {
        Some(Some(_)) => None,
        _ if !held => None,
        _ => Some(format!(
            "binlog position {} isn't an event's boundary on {now}, so it was recorded on another \
             server with the same @@server_id",
            at.untagged()
        )),
    }
}

/// Why the checkpoint's file position isn't the connected server's, from one `SHOW BINARY LOGS`
/// on `c` (and on MariaDB, `BINLOG_GTID_POS`): the file was never on the server ([`never_held`]),
/// or the position isn't an event's boundary in it ([`at_boundary`]). `None` for a position the
/// server holds or purged since. One sequence for the start's `prepare` and the dump's open.
pub(crate) async fn foreign_position(
    c: &mut mysql_async::Conn,
    flavour: Flavour,
    at: &BinlogFilePos,
    now: &ServerIdentity,
) -> Result<Option<String>, mysql_async::Error> {
    let rows: Vec<mysql_async::Row> = c.query("SHOW BINARY LOGS").await?;
    let logs: Vec<BinaryLog> = rows
        .into_iter()
        .map(|r| BinaryLog {
            name: r.get(0).unwrap_or_default(),
            size: r.get(1).unwrap_or_default(),
        })
        .collect();
    if never_held(at, &logs) {
        let (oldest, newest) = (&logs[0].name, &logs[logs.len() - 1].name);
        return Ok(Some(format!(
            "binlog {} was never on {now}: its binlogs run {oldest} … {newest}",
            at.file()
        )));
    }
    let held = logs.iter().any(|l| l.name == at.file());
    if flavour != Flavour::Mariadb || !held {
        return Ok(None);
    }
    let answer: Option<Option<String>> =
        c.exec_first(BOUNDARY_QUERY, (at.file(), at.pos())).await?;
    Ok(off_boundary(
        &SourcePosition::BinlogFile(at.clone()),
        now,
        answer,
        held,
    ))
}

/// A file/position checkpoint the connected server didn't write, as the pipeline's halt
/// (`binlog_purge_action: error`).
fn halt(why: String) -> SourceError {
    SourceError::Permanent(format!(
        "BINLOG_SOURCE_CHANGED: {why}; file/position cannot be carried across servers — set \
         binlog_purge_action: re_snapshot to re-snapshot from this server, or switch to \
         replication_type: gtid"
    ))
}

/// A server change the stream's open found, as `binlog_purge_action` says: `error` halts
/// ([`halt`]); `re_snapshot` is [`SourceError::SourceChanged`], which reruns Phase 0 so that its
/// `prepare` plans the recovery from this server's position.
pub(crate) fn changed_by(action: BinlogPurgeAction, why: String) -> SourceError {
    match action {
        BinlogPurgeAction::Error => halt(why),
        BinlogPurgeAction::ReSnapshot => SourceError::SourceChanged(format!(
            "BINLOG_SOURCE_CHANGED: {why}; binlog_purge_action is re_snapshot, so the pipeline \
             starts again to recover from this server's position"
        )),
    }
}

/// A position the stream can't read on, as `binlog_purge_action` says (G-26): `re_snapshot` keeps
/// it [`SourceError::PositionUnreachable`], which the pipeline's session answers with a
/// re-snapshot from a new position; `error` halts the pipeline for the operator. The dump's open
/// and the stream (error 1236, a purge after `prepare`) decide by it; `prepare` re-snapshots
/// under `re_snapshot` itself, and halts by it under `error`.
pub fn purge_action(action: BinlogPurgeAction, e: SourceError) -> SourceError {
    match (action, e) {
        (BinlogPurgeAction::Error, SourceError::PositionUnreachable { position, detail }) => {
            SourceError::Permanent(format!(
                "the source can't serve position {position}: {detail}; binlog_purge_action is \
                 error, so the pipeline halts: set binlog_purge_action: re_snapshot to re-snapshot \
                 from the server's position now"
            ))
        }
        (_, e) => e,
    }
}

impl MySqlConnector {
    /// The stream position for a start. The binlog carries every table, so a resume adds none.
    pub async fn prepare(&self, checkpoint: Option<&Checkpoint>) -> Result<Prepared, SourceError> {
        let mode = self.mode().await?;
        let server = self.server_identity().await?;
        let Some(cp) = checkpoint else {
            return Ok(Prepared {
                plan: StartPlan::Snapshot {
                    from: self.position_now().await?,
                    reason: SnapshotReason::FirstStart,
                },
                added: vec![],
                warnings: vec![],
                server: Some(server),
            });
        };
        let lost = match dialect_change(mode, &cp.position) {
            Some(why) => Some(why),
            None => {
                if mode == Mode::File
                    && let Some(why) = self.foreign(cp, &server).await?
                {
                    return self.changed(why, server).await;
                }
                let retention = self.retention(&cp.position).await?;
                purged(&cp.position, &retention)
            }
        };
        if let Some(why) = lost {
            let position = cp.position.to_tagged();
            if self.live().binlog_purge_action == BinlogPurgeAction::ReSnapshot {
                return Ok(Prepared {
                    plan: StartPlan::Snapshot {
                        from: self.position_now().await?,
                        reason: SnapshotReason::PositionLost,
                    },
                    added: vec![],
                    warnings: vec![format!(
                        "the source can't serve the checkpoint {position}: {why}; \
                         binlog_purge_action is re_snapshot, so the pipeline re-snapshots from \
                         the server's position now"
                    )],
                    server: Some(server),
                });
            }
            return Err(purge_action(
                self.live().binlog_purge_action,
                SourceError::PositionUnreachable {
                    position,
                    detail: why,
                },
            ));
        }
        Ok(Prepared {
            plan: StartPlan::Resume,
            added: vec![],
            warnings: vec![],
            server: Some(server),
        })
    }

    /// A file/position checkpoint from another server (`why`), as `binlog_purge_action` says:
    /// `error` halts; `re_snapshot` snapshots from the server's position now.
    async fn changed(&self, why: String, server: ServerIdentity) -> Result<Prepared, SourceError> {
        if self.live().binlog_purge_action != BinlogPurgeAction::ReSnapshot {
            return Err(halt(why));
        }
        Ok(Prepared {
            plan: StartPlan::Snapshot {
                from: self.position_now().await?,
                reason: SnapshotReason::SourceChanged,
            },
            added: vec![],
            warnings: vec![format!(
                "BINLOG_SOURCE_CHANGED: {why}; binlog_purge_action is re_snapshot, so the \
                 pipeline re-snapshots from the server's position now"
            )],
            server: Some(server),
        })
    }

    /// File/position mode: why the checkpoint isn't the connected server's, beyond its
    /// identity ([`foreign_position`]).
    async fn foreign(
        &self,
        cp: &Checkpoint,
        server: &ServerIdentity,
    ) -> Result<Option<String>, SourceError> {
        if let Some(why) = server_changed(cp, server) {
            return Ok(Some(why));
        }
        let SourcePosition::BinlogFile(p) = &cp.position else {
            return Ok(None);
        };
        let flavour = self.flavour().await?;
        let (p, server) = (p.clone(), server.clone());
        self.conn
            .command("check the position on the server", move |c| {
                let (p, server) = (p.clone(), server.clone());
                Box::pin(async move { foreign_position(c, flavour, &p, &server).await })
            })
            .await
    }

    /// The connection id of the open binlog dump, `None` without one: the server's
    /// `information_schema.PROCESSLIST` id of the stream's connection, for status and logs.
    pub fn dump_id(&self) -> Option<u32> {
        match self.shared.dump.load(Ordering::Relaxed) {
            0 => None,
            id => Some(id),
        }
    }

    /// The heartbeat's watch on the dump (DA-SRC-16, V-121): whether the server still runs the
    /// stream's binlog dump, found by its connection id, and how far the stream reads behind the
    /// server's write position ([`behind`]). The server keeps no log for the pipeline (its
    /// binlogs expire by its own policy), so `bytes` is `None`.
    pub async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
        let id = self.shared.dump.load(Ordering::Relaxed);
        let streamed = match id {
            0 => false,
            id => {
                self.conn
                    .command("find the binlog dump", move |c| {
                        Box::pin(async move {
                            let n: Option<i64> = c
                                .exec_first(
                                    "SELECT COUNT(*) FROM information_schema.PROCESSLIST
                                  WHERE ID = ? AND COMMAND LIKE 'Binlog Dump%'",
                                    (id,),
                                )
                                .await?;
                            Ok(n.unwrap_or(0) > 0)
                        })
                    })
                    .await?
            }
        };
        let behind = match self.shared.read() {
            None => None,
            Some(read) => {
                let head = self.position_now().await.ok();
                let (lag, baseline) = behind(&read, head.as_ref(), now_micros());
                if let (Some(baseline), Some(c)) = (baseline, &read.caught_up) {
                    self.shared.caught_up_head(c.at_micros, &baseline);
                }
                lag
            }
        };
        Ok(Some(RetainedLog {
            bytes: None,
            streamed,
            behind,
        }))
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::position::BinlogFilePos;

    use super::*;

    fn file() -> SourcePosition {
        SourcePosition::BinlogFile(BinlogFilePos::new("binlog.000003", 154).unwrap())
    }

    fn at(pos: u64, commit_micros: Option<i64>, caught_up: Option<CaughtUp>) -> ReadAt {
        ReadAt {
            position: SourcePosition::BinlogFile(BinlogFilePos::new("binlog.000003", pos).unwrap()),
            commit_micros,
            caught_up,
        }
    }

    fn head(pos: u64) -> SourcePosition {
        SourcePosition::BinlogFile(BinlogFilePos::new("binlog.000003", pos).unwrap())
    }

    fn heartbeat(at_micros: i64, baseline: Option<u64>) -> Option<CaughtUp> {
        Some(CaughtUp {
            at_micros,
            head: baseline.map(head),
        })
    }

    /// At the server's write position the stream is 0 behind; short of it, the last transaction
    /// it read is as old as the lag. Behind with nothing read, or without the server's position,
    /// the lag is unknown.
    #[test]
    fn the_binlog_lag_is_zero_at_the_head_else_the_last_transaction_s_age() {
        let h = head(900);
        let now = 10_000_000;
        let lag = |r: ReadAt, h: Option<&SourcePosition>| behind(&r, h, now).0;
        assert_eq!(
            lag(at(900, Some(1_000_000), None), Some(&h)),
            Some(Duration::ZERO)
        );
        assert_eq!(
            lag(at(500, Some(1_000_000), None), Some(&h)),
            Some(Duration::from_secs(9))
        );
        assert_eq!(lag(at(500, None, None), Some(&h)), None);
        assert_eq!(lag(at(500, Some(1_000_000), None), None), None);
        assert_eq!(
            lag(at(500, Some(now + 5), None), Some(&h)),
            Some(Duration::ZERO),
            "a clock behind the server's reads no lag"
        );
    }

    /// A stream that caught up at a heartbeat is 0 behind while the server writes nothing more;
    /// once it writes past what the first check saw, the lag counts from the heartbeat.
    #[test]
    fn a_caught_up_stream_lags_from_its_heartbeat_once_the_server_writes_on() {
        let now = 10_000_000;
        let (lag, baseline) = behind(
            &at(500, None, heartbeat(4_000_000, None)),
            Some(&head(900)),
            now,
        );
        assert_eq!(lag, Some(Duration::ZERO));
        assert_eq!(
            baseline,
            Some(head(900)),
            "the first check's head is recorded"
        );
        let (lag, baseline) = behind(
            &at(500, None, heartbeat(4_000_000, Some(900))),
            Some(&head(900)),
            now,
        );
        assert_eq!((lag, baseline), (Some(Duration::ZERO), None));
        let (lag, baseline) = behind(
            &at(500, Some(1_000_000), heartbeat(4_000_000, Some(900))),
            Some(&head(950)),
            now,
        );
        assert_eq!((lag, baseline), (Some(Duration::from_secs(6)), None));
        let (lag, _) = behind(
            &at(500, None, heartbeat(now + 5, Some(900))),
            Some(&head(950)),
            now,
        );
        assert_eq!(lag, Some(Duration::ZERO), "a skewed clock reads no lag");
        let (lag, _) = behind(&at(500, None, heartbeat(4_000_000, None)), None, now);
        assert_eq!(lag, None, "no head, no lag");
        let (lag, _) = behind(
            &at(950, None, heartbeat(4_000_000, Some(900))),
            Some(&head(950)),
            now,
        );
        assert_eq!(lag, Some(Duration::ZERO), "the stream covers the head");
        let (lag, baseline) = behind(
            &at(900, None, heartbeat(4_000_000, None)),
            Some(&head(900)),
            now,
        );
        assert_eq!(
            (lag, baseline),
            (Some(Duration::ZERO), Some(head(900))),
            "at the head, the baseline is recorded all the same"
        );
    }

    #[test]
    fn a_checkpoint_in_another_dialect_is_named() {
        assert_eq!(dialect_change(Mode::File, &file()), None);
        let why = dialect_change(Mode::MysqlGtid, &file()).unwrap();
        assert!(
            why.contains("binlog position") && why.contains("mysql-gtid"),
            "{why}"
        );
    }

    #[test]
    fn a_file_position_from_another_server_halts() {
        let a = ServerIdentity::Mariadb { server_id: 1 };
        let b = ServerIdentity::Mariadb { server_id: 2 };
        let cp = |server| Checkpoint {
            position: file(),
            server,
        };
        assert!(same_server(&cp(Some(a.clone())), &a).is_ok());
        for c in [cp(Some(b)), cp(None)] {
            let e = same_server(&c, &a).unwrap_err();
            assert!(
                matches!(&e, SourceError::Permanent(m) if m.starts_with("BINLOG_SOURCE_CHANGED: binlog position binlog.000003:154")
                    && m.contains("set binlog_purge_action: re_snapshot")),
                "{e}"
            );
        }
    }

    fn logs(names: &[&str]) -> Vec<BinaryLog> {
        names
            .iter()
            .map(|n| BinaryLog {
                name: (*n).to_owned(),
                size: 100,
            })
            .collect()
    }

    #[test]
    fn a_file_the_server_could_not_have_purged_was_never_its() {
        let held = logs(&["bin.000004", "bin.000005", "bin.000006"]);
        let at = |f: &str| BinlogFilePos::new(f, 4).unwrap();
        assert!(never_held(&at("bin.000007"), &held), "after the newest");
        assert!(
            never_held(&at("bin.000011"), &held),
            "well after the newest"
        );
        assert!(never_held(&at("other.000001"), &held), "another base name");
        assert!(
            !never_held(&at("bin.000002"), &held),
            "before the oldest: purged"
        );
        assert!(!never_held(&at("bin.000005"), &held), "held");
        assert!(!never_held(&at("bin.000007"), &[]), "no list: the purge's");
        // A gap inside the series is a purge's too.
        let gap = logs(&["bin.000002", "bin.000006"]);
        assert!(!never_held(&at("bin.000004"), &gap));
    }

    #[test]
    fn a_position_off_an_event_boundary_is_another_server_s() {
        let a = ServerIdentity::Mariadb { server_id: 1 };
        assert!(at_boundary(&file(), &a, Some(Some("0-1-20505".into())), true).is_ok());
        assert!(
            at_boundary(&file(), &a, Some(Some(String::new())), true).is_ok(),
            "no GTIDs yet"
        );
        assert!(
            at_boundary(&file(), &a, Some(None), false).is_ok(),
            "a file purged since is the purge's to report"
        );
        for answer in [Some(None), None] {
            let e = at_boundary(&file(), &a, answer, true).unwrap_err();
            assert!(
                matches!(&e, SourceError::Permanent(m) if m.starts_with("BINLOG_SOURCE_CHANGED: binlog position binlog.000003:154 isn't an event's boundary on MariaDB server_id 1")),
                "{e}"
            );
        }
    }

    #[test]
    fn the_purge_action_decides() {
        let lost = || SourceError::PositionUnreachable {
            position: "binlog:binlog.000003:154".into(),
            detail: "gone".into(),
        };
        assert_eq!(
            purge_action(BinlogPurgeAction::ReSnapshot, lost()).to_string(),
            lost().to_string()
        );
        assert!(matches!(
            purge_action(BinlogPurgeAction::Error, lost()),
            SourceError::Permanent(m) if m.starts_with("the source can't serve position binlog:binlog.000003:154: gone; binlog_purge_action is error")
        ));
        // Other errors pass through.
        assert!(matches!(
            purge_action(BinlogPurgeAction::Error, SourceError::Transient("x".into())),
            SourceError::Transient(_)
        ));
    }
}
