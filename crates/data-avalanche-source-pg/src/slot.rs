//! The slot and publication lifecycle (`Postgres_Slot_and_Publication_Lifecycle.md`): the slot
//! state machine (D-15.1–15.3), publication reconciliation (D-15.4), teardown and the WAL the
//! slot retains.

use std::collections::BTreeSet;

use data_avalanche_config::{SlotManagement, SlotOnOrphan};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    Checkpoint, Prepared, RetainedLog, SnapshotReason, SourceError, SourcePosition, StartPlan,
    TableRef,
};
use tokio_postgres::Client;

use crate::catalog::table_key;
use crate::connect::{SERVER_FIRST, server_millis};
use crate::connector::PgConnector;
use crate::position::{lsn_of, parse_lsn};

/// What [`PgConnector::reconcile_publication`] would change, and what it noticed. Nothing is
/// applied until `prepare` has decided about the slot.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Reconciled {
    /// The statements that bring the publication in line (`managed` only), in order.
    pub sql: Vec<String>,
    /// They create the publication, or change what it publishes for tracked tables (its
    /// operations; `publish_via_partition_root` while a tracked table is a partition). The WAL
    /// before now was decoded under the old definition.
    pub rewritten: bool,
    /// Tracked tables they add to the publication.
    pub added: Vec<TableRef>,
    pub warnings: Vec<String>,
}

/// The server's row for the pipeline's slot (`pg_replication_slots`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotState {
    pub plugin: Option<String>,
    pub database: Option<String>,
    pub confirmed_flush: Option<Lsn>,
    /// `wal_status = 'lost'`, a PostgreSQL 16 `conflicting` logical slot, or a PostgreSQL 17+
    /// `invalidation_reason`: the slot can't stream any more.
    pub invalidated: bool,
}

/// What [`decide`] says to do with the slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotAction {
    /// Stream from the checkpoint.
    Resume,
    /// Create the slot (none exists), then snapshot.
    Create(SnapshotReason),
    /// Drop the unusable slot and create it again, then snapshot.
    Recreate(SnapshotReason),
    /// Move the orphan slot to the current WAL position (`pg_replication_slot_advance`), then
    /// snapshot: `on_orphan: adopt_resnapshot`. Advancing works under `require_existing` too.
    Adopt,
    /// Refuse to start: the operator decides.
    Refuse(String),
}

/// The slot state machine (`Postgres_Slot…` §1–§3), one cell per combination of pipeline state
/// (`checkpoint`) and server slot (`slot`).
pub fn decide(
    checkpoint: Option<Lsn>,
    slot: Option<&SlotState>,
    db: &str,
    name: &str,
    management: SlotManagement,
    orphan: SlotOnOrphan,
) -> SlotAction {
    let managed = management == SlotManagement::Managed;
    let not_ours = |s: &SlotState| {
        if s.plugin.as_deref() != Some("pgoutput") {
            Some(format!(
                "replication slot {name} uses the {} plugin, not pgoutput",
                s.plugin.as_deref().unwrap_or("physical")
            ))
        } else if s.database.as_deref() != Some(db) {
            Some(format!(
                "replication slot {name} belongs to database {}, not {db}",
                s.database.as_deref().unwrap_or("-")
            ))
        } else {
            None
        }
    };
    let lost = |why: &str| {
        if managed {
            SlotAction::Recreate(SnapshotReason::PositionLost)
        } else {
            SlotAction::Refuse(format!(
                "replication slot {name} {why}; slot_management is require_existing, so recreate it and restart: the pipeline re-snapshots"
            ))
        }
    };
    match (checkpoint, slot) {
        (Some(at), Some(s)) => {
            if let Some(m) = not_ours(s) {
                return SlotAction::Refuse(m);
            }
            if s.invalidated {
                return lost("was invalidated");
            }
            match s.confirmed_flush {
                Some(flushed) if flushed > at => lost(&format!(
                    "confirmed {flushed}, past the pipeline's checkpoint {at}"
                )),
                _ => SlotAction::Resume,
            }
        }
        (Some(_), None) if managed => SlotAction::Create(SnapshotReason::PositionLost),
        (Some(_), None) => SlotAction::Refuse(format!(
            "replication slot {name} doesn't exist and slot_management is require_existing"
        )),
        (None, Some(s)) => match orphan {
            SlotOnOrphan::Halt => SlotAction::Refuse(format!(
                "replication slot {name} exists but the pipeline has no state (an orphan): drop the slot for a first start, restore the pipeline state, or set slot.on_orphan: adopt_resnapshot"
            )),
            SlotOnOrphan::AdoptResnapshot => match not_ours(s) {
                Some(m) => SlotAction::Refuse(m),
                None if s.invalidated && managed => {
                    SlotAction::Recreate(SnapshotReason::AdoptedOrphan)
                }
                None if s.invalidated => lost("was invalidated"),
                None => SlotAction::Adopt,
            },
        },
        (None, None) if managed => SlotAction::Create(SnapshotReason::FirstStart),
        (None, None) => SlotAction::Refuse(format!(
            "replication slot {name} doesn't exist and slot_management is require_existing"
        )),
    }
}

/// A SQL identifier, quoted.
pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `schema.table`, quoted.
pub(crate) fn qualified(t: &TableRef) -> String {
    format!("{}.{}", ident(&t.schema), ident(&t.name))
}

/// The tables among `tables` that are partitions (`relispartition`).
async fn partitions(
    client: &Client,
    tables: &[TableRef],
) -> Result<Vec<TableRef>, tokio_postgres::Error> {
    let schemas: Vec<&str> = tables.iter().map(|t| t.schema.as_str()).collect();
    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    let rows = client
        .query(
            "SELECT n.nspname::text, c.relname::text
               FROM pg_catalog.pg_class c
               JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               JOIN unnest($1::text[], $2::text[]) AS w(s, t) ON w.s = n.nspname AND w.t = c.relname
              WHERE c.relispartition
              ORDER BY 1, 2",
            &[&schemas, &names],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| TableRef::new(r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect())
}

pub(crate) async fn slot_state(
    client: &Client,
    name: &str,
) -> Result<Option<SlotState>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "SELECT plugin::text, database::text, confirmed_flush_lsn::text,
                    coalesce(wal_status = 'lost', false)
                      OR coalesce((to_jsonb(s) ->> 'conflicting')::bool, false)
                      OR (to_jsonb(s) ->> 'invalidation_reason') IS NOT NULL
               FROM pg_catalog.pg_replication_slots s
              WHERE slot_name = $1",
            &[&name],
        )
        .await?;
    Ok(row.map(|r| SlotState {
        plugin: r.get(0),
        database: r.get(1),
        confirmed_flush: r.get::<_, Option<String>>(2).and_then(|t| t.parse().ok()),
        invalidated: r.get(3),
    }))
}

impl PgConnector {
    fn slot_name(&self) -> &str {
        &self.pg.slot
    }

    async fn create_slot(&self, client: &Client) -> Result<Lsn, SourceError> {
        // Creating a slot waits for the transactions running at that moment: the snapshot timeout
        // bounds it, not the command timeout.
        let row = self
            .conn
            .bounded(
                "create the replication slot",
                self.conn.timeouts.snapshot,
                client.query_one(
                    "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
                    &[&self.slot_name()],
                ),
            )
            .await?;
        parse_lsn(row.get(0))
    }

    async fn drop_slot(&self, client: &Client) -> Result<(), SourceError> {
        self.conn
            .command(
                "drop the replication slot",
                client.execute(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot_name()],
                ),
            )
            .await
            .map(drop)
    }

    /// D-15.4: under `managed`, the publication publishes exactly `tables` and every operation
    /// (created when absent, then `ALTER PUBLICATION … ADD/DROP TABLE` and `SET`); under
    /// `require_existing`, it must exist and a tracked table it doesn't publish warns. A
    /// publication this can't change (`require_existing`, `FOR ALL TABLES`) that publishes via the
    /// partition root while a tracked table is a partition is refused. Reads only: the statements
    /// come back in [`Reconciled::sql`].
    pub(crate) async fn reconcile_publication(
        &self,
        client: &Client,
        tables: &[TableRef],
    ) -> Result<Reconciled, SourceError> {
        let name = &self.pg.publication;
        let managed = self.pg.slot_management == SlotManagement::Managed;
        let row = self
            .conn
            .command(
                "read the publication",
                client.query_opt(
                    "SELECT puballtables, pubinsert AND pubupdate AND pubdelete AND pubtruncate,
                            pubviaroot
                       FROM pg_catalog.pg_publication WHERE pubname = $1",
                    &[name],
                ),
            )
            .await?;
        let wanted: BTreeSet<&TableRef> = tables.iter().collect();
        let Some(row) = row else {
            if !managed {
                return Err(SourceError::Permanent(format!(
                    "publication {name} doesn't exist and slot_management is require_existing"
                )));
            }
            let list: Vec<String> = tables.iter().map(qualified).collect();
            let sql = if list.is_empty() {
                format!("CREATE PUBLICATION {}", ident(name))
            } else {
                format!(
                    "CREATE PUBLICATION {} FOR TABLE {}",
                    ident(name),
                    list.join(", ")
                )
            };
            return Ok(Reconciled {
                sql: vec![sql],
                rewritten: true,
                ..Reconciled::default()
            });
        };
        let (all_tables, every_op, via_root): (bool, bool, bool) =
            (row.get(0), row.get(1), row.get(2));
        let published: BTreeSet<TableRef> = self
            .conn
            .command(
                "read the publication's tables",
                client.query(
                    "SELECT schemaname::text, tablename::text FROM pg_catalog.pg_publication_tables WHERE pubname = $1",
                    &[name],
                ),
            )
            .await?
            .iter()
            .map(|r| TableRef::new(r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect();
        let missing: Vec<&TableRef> = wanted
            .iter()
            .copied()
            .filter(|t| !published.contains(*t))
            .collect();
        let mut warnings = vec![];
        if !managed || all_tables {
            // Q-M5-5: a partition's changes would arrive under its parent's name, which isn't
            // tracked, so the stream would drop them. Refused, not warned: it would lose data.
            if via_root {
                let leaves = self
                    .conn
                    .command("find partitions", partitions(client, tables))
                    .await?;
                if !leaves.is_empty() {
                    let list: Vec<String> = leaves.iter().map(table_key).collect();
                    return Err(SourceError::Permanent(format!(
                        "publication {name} publishes via the partition root, so the changes of the partitions {} would arrive under their parent's name and be lost; run ALTER PUBLICATION {} SET (publish_via_partition_root = false)",
                        list.join(", "),
                        ident(name)
                    )));
                }
            }
            for t in missing {
                warnings.push(format!(
                    "publication {name} doesn't publish {t}: its changes won't arrive"
                ));
            }
            if !every_op {
                warnings.push(format!(
                    "publication {name} doesn't publish every operation; set publish = 'insert, update, delete, truncate'"
                ));
            }
            return Ok(Reconciled {
                warnings,
                ..Reconciled::default()
            });
        }
        let mut sql = vec![];
        if !missing.is_empty() {
            let list: Vec<String> = missing.iter().map(|t| qualified(t)).collect();
            sql.push(format!(
                "ALTER PUBLICATION {} ADD TABLE {}",
                ident(name),
                list.join(", ")
            ));
        }
        let extra: Vec<String> = published
            .iter()
            .filter(|t| !wanted.contains(t))
            .map(qualified)
            .collect();
        if !extra.is_empty() {
            sql.push(format!(
                "ALTER PUBLICATION {} DROP TABLE {}",
                ident(name),
                extra.join(", ")
            ));
        }
        if !every_op || via_root {
            sql.push(format!(
                "ALTER PUBLICATION {} SET (publish = 'insert, update, delete, truncate', publish_via_partition_root = false)",
                ident(name)
            ));
        }
        // Publishing via the root mattered only for tracked partitions: their changes arrived
        // under the parent's name.
        let partitioned = via_root
            && !self
                .conn
                .command("find partitions", partitions(client, tables))
                .await?
                .is_empty();
        Ok(Reconciled {
            sql,
            rewritten: !every_op || partitioned,
            added: missing.into_iter().cloned().collect(),
            warnings,
        })
    }

    /// Runs [`Reconciled::sql`] as one implicit transaction: all of it or none. The server bounds
    /// it (`lock_timeout`, `statement_timeout`), so a change that waits on a table's lock is
    /// cancelled, not left to commit after its reply was lost: the retry then reports the table as
    /// added again.
    async fn apply_publication(&self, client: &Client, sql: &[String]) -> Result<(), SourceError> {
        if sql.is_empty() {
            return Ok(());
        }
        let command = self.conn.timeouts.command;
        let ms = server_millis(command);
        let batch = format!(
            "SET LOCAL lock_timeout = {ms}; SET LOCAL statement_timeout = {ms}; {}",
            sql.join("; ")
        );
        // The server bounds each statement and each lock wait by `command`.
        let budget = command.saturating_mul(sql.len() as u32) + SERVER_FIRST;
        self.conn
            .bounded(
                "reconcile the publication",
                budget,
                client.batch_execute(&batch),
            )
            .await
    }

    /// The slot state machine, with the publication's repair (its changes are decoded with the
    /// catalog as of their WAL position, so it must be in place before a new slot's start). The
    /// order survives a retry: a refusal changes nothing; a repair that rewrites the publication
    /// drops the slot before it changes the publication, so a retry finds no slot and re-snapshots
    /// rather than resuming over WAL the old publication didn't publish.
    pub async fn prepare(
        &self,
        checkpoint: Option<&Checkpoint>,
        tables: &[TableRef],
    ) -> Result<Prepared, SourceError> {
        let checkpoint = checkpoint.map(|c| lsn_of(&c.position)).transpose()?;
        let client = self.conn.client().await?;
        let Reconciled {
            sql,
            rewritten,
            added,
            mut warnings,
        } = self.reconcile_publication(&client, tables).await?;
        let slot = self
            .conn
            .command(
                "read the replication slot",
                slot_state(&client, self.slot_name()),
            )
            .await?;
        let action = match decide(
            checkpoint,
            slot.as_ref(),
            &self.pipeline.source.db,
            self.slot_name(),
            self.pg.slot_management,
            self.live().slot_on_orphan,
        ) {
            // pgoutput reads the publication as of each change's WAL position: the WAL from the
            // checkpoint up to now was decoded under the old publication, and what it didn't
            // publish never reaches the stream. Only `managed` rewrites the publication, so the
            // slot is ours to recreate.
            SlotAction::Resume if rewritten => {
                warnings.push(format!(
                    "publication {} was created or changed what it publishes: changes since the checkpoint may be missing, so the pipeline re-snapshots",
                    self.pg.publication
                ));
                SlotAction::Recreate(SnapshotReason::PositionLost)
            }
            a => a,
        };
        let snapshot = |from: Lsn, reason| StartPlan::Snapshot {
            from: SourcePosition::Pg(from),
            reason,
        };
        if let SlotAction::Refuse(m) = action {
            return Err(SourceError::Permanent(m));
        }
        if let SlotAction::Recreate(_) = action {
            self.drop_slot(&client).await?;
        }
        self.apply_publication(&client, &sql).await?;
        let plan = match action {
            SlotAction::Resume => StartPlan::Resume,
            SlotAction::Create(reason) | SlotAction::Recreate(reason) => {
                snapshot(self.create_slot(&client).await?, reason)
            }
            SlotAction::Adopt => {
                // Advancing decodes the orphan's whole backlog: the snapshot timeout bounds it.
                let row = self.conn.bounded(
                    "advance the orphan slot",
                    self.conn.timeouts.snapshot,
                    client.query_one(
                        "SELECT end_lsn::text FROM pg_replication_slot_advance($1, pg_current_wal_lsn())",
                        &[&self.slot_name()],
                    ),
                )
                .await?;
                snapshot(parse_lsn(row.get(0))?, SnapshotReason::AdoptedOrphan)
            }
            SlotAction::Refuse(_) => unreachable!("refused above"),
        };
        let added = match plan {
            StartPlan::Resume => added,
            StartPlan::Snapshot { .. } => vec![],
        };
        Ok(Prepared {
            plan,
            added,
            warnings,
            server: None,
        })
    }

    /// D-15.3 after [`SourceError::PositionUnreachable`]: drop the slot and create it again;
    /// stream from the new position after a re-snapshot. Under `require_existing` the slot isn't
    /// ours to recreate.
    pub async fn reset_position(&self) -> Result<SourcePosition, SourceError> {
        if self.pg.slot_management == SlotManagement::RequireExisting {
            return Err(SourceError::Permanent(format!(
                "replication slot {} lost its position and slot_management is require_existing: recreate it and restart",
                self.slot_name()
            )));
        }
        let client = self.conn.client().await?;
        self.drop_slot(&client).await?;
        Ok(SourcePosition::Pg(self.create_slot(&client).await?))
    }

    /// `auto_cleanup`: drops the slot and the publication when the pipeline is deleted. Nothing
    /// otherwise (`require_existing` forces `auto_cleanup: false`, V-103). A slot still streaming
    /// is an error.
    pub async fn teardown(&self) -> Result<(), SourceError> {
        if !self.live().auto_cleanup || self.pg.slot_management == SlotManagement::RequireExisting {
            return Ok(());
        }
        let client = self.conn.client().await?;
        self.drop_slot(&client).await?;
        self.conn
            .command(
                "drop the publication",
                client.batch_execute(&format!(
                    "DROP PUBLICATION IF EXISTS {}",
                    ident(&self.pg.publication)
                )),
            )
            .await
    }

    /// Ends the walsender that holds the slot when it runs as the pipeline's own role, and waits up
    /// to 5 s for it to exit (`pg_terminate_backend(pid, timeout)`, PostgreSQL 14+): a role may
    /// end its own sessions without a grant. `false` when no session of the role holds the slot,
    /// or it didn't exit in time (DA-SRC-28).
    pub async fn end_other_reader(&self) -> Result<bool, SourceError> {
        let client = self.conn.client().await?;
        let row = self
            .conn
            .command(
                "end the slot's other reader",
                client.query_opt(
                    "SELECT pg_catalog.pg_terminate_backend(a.pid, 5000)
                       FROM pg_catalog.pg_replication_slots s
                       JOIN pg_catalog.pg_stat_activity a ON a.pid = s.active_pid
                      WHERE s.slot_name = $1 AND a.usename = current_user",
                    &[&self.slot_name()],
                ),
            )
            .await?;
        Ok(row.is_some_and(|r| r.get(0)))
    }

    /// The WAL the slot keeps on the source's disk: `pg_current_wal_lsn() - restart_lsn`, the
    /// `max_wal_lag_bytes` guard's measure, and whether a walsender reads the slot (`active`).
    /// `restart_lsn` trails `confirmed_flush_lsn` while a transaction older than the confirmed
    /// position is still open. `None` without a slot, or with a slot that lost its WAL.
    pub async fn retained(&self) -> Result<Option<RetainedLog>, SourceError> {
        let client = self.conn.client().await?;
        let row = self
            .conn
            .command(
                "measure the slot's WAL",
                client.query_opt(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, active
                       FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&self.slot_name()],
                ),
            )
            .await?;
        Ok(row.and_then(|r| {
            let bytes = r.get::<_, Option<i64>>(0)?;
            Some(RetainedLog {
                bytes: Some(u64::try_from(bytes).unwrap_or(0)),
                streamed: r.get(1),
                behind: None,
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SlotAction as A;
    use SnapshotReason as R;

    const M: SlotManagement = SlotManagement::Managed;
    const X: SlotManagement = SlotManagement::RequireExisting;
    const HALT: SlotOnOrphan = SlotOnOrphan::Halt;
    const ADOPT: SlotOnOrphan = SlotOnOrphan::AdoptResnapshot;

    fn slot(flushed: u64) -> SlotState {
        SlotState {
            plugin: Some("pgoutput".into()),
            database: Some("shop".into()),
            confirmed_flush: Some(Lsn(flushed)),
            invalidated: false,
        }
    }

    fn d(
        checkpoint: Option<u64>,
        s: Option<SlotState>,
        m: SlotManagement,
        o: SlotOnOrphan,
    ) -> SlotAction {
        decide(checkpoint.map(Lsn), s.as_ref(), "shop", "s", m, o)
    }

    fn refused(a: SlotAction, needle: &str) {
        match a {
            A::Refuse(m) => assert!(m.contains(needle), "{m:?} should mention {needle:?}"),
            other => panic!("refused expected, got {other:?}"),
        }
    }

    #[test]
    fn state_and_slot_resume() {
        assert_eq!(d(Some(0x20), Some(slot(0x20)), M, HALT), A::Resume);
        assert_eq!(
            d(Some(0x20), Some(slot(0x10)), X, HALT),
            A::Resume,
            "behind the checkpoint is fine"
        );
    }

    #[test]
    fn state_without_slot_recreates_under_managed() {
        assert_eq!(d(Some(0x20), None, M, HALT), A::Create(R::PositionLost));
        refused(d(Some(0x20), None, X, HALT), "doesn't exist");
    }

    #[test]
    fn a_slot_past_the_checkpoint_or_invalidated_lost_the_position() {
        assert_eq!(
            d(Some(0x10), Some(slot(0x20)), M, HALT),
            A::Recreate(R::PositionLost)
        );
        refused(
            d(Some(0x10), Some(slot(0x20)), X, HALT),
            "past the pipeline's checkpoint",
        );
        let mut gone = slot(0x10);
        gone.invalidated = true;
        assert_eq!(
            d(Some(0x20), Some(gone.clone()), M, HALT),
            A::Recreate(R::PositionLost)
        );
        refused(d(Some(0x20), Some(gone), X, HALT), "invalidated");
    }

    #[test]
    fn an_orphan_halts_or_is_adopted() {
        refused(d(None, Some(slot(0x10)), M, HALT), "orphan");
        refused(d(None, Some(slot(0x10)), X, HALT), "orphan");
        assert_eq!(d(None, Some(slot(0x10)), M, ADOPT), A::Adopt);
        assert_eq!(
            d(None, Some(slot(0x10)), X, ADOPT),
            A::Adopt,
            "advancing isn't recreating"
        );
        let mut gone = slot(0x10);
        gone.invalidated = true;
        assert_eq!(
            d(None, Some(gone.clone()), M, ADOPT),
            A::Recreate(R::AdoptedOrphan)
        );
        refused(d(None, Some(gone), X, ADOPT), "invalidated");
    }

    #[test]
    fn a_first_start_creates_under_managed() {
        assert_eq!(d(None, None, M, HALT), A::Create(R::FirstStart));
        refused(d(None, None, X, ADOPT), "doesn't exist");
    }

    #[test]
    fn someone_elses_slot_is_refused() {
        let mut physical = slot(0x10);
        physical.plugin = None;
        refused(
            d(Some(0x10), Some(physical.clone()), M, ADOPT),
            "physical plugin",
        );
        refused(d(None, Some(physical), M, ADOPT), "physical plugin");
        let mut wal2json = slot(0x10);
        wal2json.plugin = Some("wal2json".into());
        refused(d(Some(0x10), Some(wal2json), M, HALT), "wal2json");
        let mut elsewhere = slot(0x10);
        elsewhere.database = Some("other".into());
        refused(d(Some(0x10), Some(elsewhere), M, HALT), "database other");
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(
            qualified(&TableRef::new("My Schema", "t")),
            "\"My Schema\".\"t\""
        );
    }
}
