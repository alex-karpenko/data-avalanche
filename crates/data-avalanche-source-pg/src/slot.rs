//! The slot and publication lifecycle (`Postgres_Slot_and_Publication_Lifecycle.md`): the slot
//! state machine (D-15.1–15.3), publication reconciliation (D-15.4), teardown and the WAL the
//! slot retains.

use std::collections::BTreeSet;

use data_avalanche_config::{SlotManagement, SlotOnOrphan, SourceChangeAction};
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    Checkpoint, HaltReason, Prepared, ResolvedTables, RetainedLog, ServerIdentity, SnapshotReason,
    SourceError, SourcePosition, StartPlan, TableRef,
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
    /// operations, a row filter, a column list). The WAL before now was decoded under the old
    /// definition. Not `publish_via_partition_root`: `pg_publication_tables` lists only the root
    /// under it, even for a partition the publication names too, so the tracked partitions are
    /// among `added` and are copied again, the slot and the other tables' stream staying.
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
    /// Move the slot to the current WAL position (`pg_replication_slot_advance`), then snapshot:
    /// an orphan under `slot_on_orphan: adopt_resnapshot`, under `require_existing` also a slot made
    /// again past the checkpoint, or the slot of a source change. Advancing works under
    /// `require_existing` too.
    Adopt(SnapshotReason),
    /// Refuse to start: the operator decides.
    Refuse(String),
}

/// A slot of the pipeline's name that isn't a pgoutput slot of the pipeline's database: never
/// streamed, adopted, dropped or advanced (refused, naming the plugin or the database).
fn not_ours(s: &SlotState, db: &str, name: &str) -> Option<String> {
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
}

/// The slot state machine (`Postgres_Slot…` §1–§3), one cell per combination of pipeline state
/// (`checkpoint`) and server slot (`slot`). Under `require_existing` the pipeline never drops or
/// creates a slot: one the DBA makes again after the pipeline's was lost starts past the
/// checkpoint, and is an orphan, adopted under `slot_on_orphan: adopt_resnapshot` (DA-SRC-30).
pub fn decide(
    checkpoint: Option<Lsn>,
    slot: Option<&SlotState>,
    db: &str,
    name: &str,
    management: SlotManagement,
    orphan: SlotOnOrphan,
) -> SlotAction {
    let managed = management == SlotManagement::Managed;
    let not_ours = |s: &SlotState| not_ours(s, db, name);
    // What a DBA does under `require_existing`: make the slot (again), and let the pipeline
    // adopt it.
    let remake = "slot_management is require_existing, so create it again, with slot_on_orphan: adopt_resnapshot for the pipeline to adopt it and re-snapshot";
    let lost = |why: &str| {
        if managed {
            SlotAction::Recreate(SnapshotReason::PositionLost)
        } else {
            SlotAction::Refuse(format!("replication slot {name} {why}; drop it: {remake}"))
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
                Some(flushed) if flushed > at => match (managed, orphan) {
                    (true, _) => SlotAction::Recreate(SnapshotReason::PositionLost),
                    (false, SlotOnOrphan::AdoptResnapshot) => {
                        SlotAction::Adopt(SnapshotReason::PositionLost)
                    }
                    (false, SlotOnOrphan::Halt) => SlotAction::Refuse(format!(
                        "replication slot {name} confirmed {flushed}, past the pipeline's checkpoint {at}: the changes between are lost (a slot made again, or moved by something else); slot_management is require_existing, so set slot_on_orphan: adopt_resnapshot for the pipeline to adopt it and re-snapshot"
                    )),
                },
                _ => SlotAction::Resume,
            }
        }
        (Some(_), None) if managed => SlotAction::Create(SnapshotReason::PositionLost),
        (Some(_), None) => {
            SlotAction::Refuse(format!("replication slot {name} doesn't exist; {remake}"))
        }
        (None, Some(s)) => match orphan {
            SlotOnOrphan::Halt if managed => SlotAction::Refuse(format!(
                "replication slot {name} exists but the pipeline has no state (an orphan): drop the slot for a first start, restore the pipeline state, or set slot_on_orphan: adopt_resnapshot"
            )),
            // Under `require_existing` a dropped slot isn't made again: a first start refuses.
            SlotOnOrphan::Halt => SlotAction::Refuse(format!(
                "replication slot {name} exists but the pipeline has no state (an orphan): restore the pipeline state, or set slot_on_orphan: adopt_resnapshot for the pipeline to adopt it and re-snapshot"
            )),
            SlotOnOrphan::AdoptResnapshot => match not_ours(s) {
                Some(m) => SlotAction::Refuse(m),
                None if s.invalidated && managed => {
                    SlotAction::Recreate(SnapshotReason::AdoptedOrphan)
                }
                None if s.invalidated => lost("was invalidated"),
                None => SlotAction::Adopt(SnapshotReason::AdoptedOrphan),
            },
        },
        (None, None) if managed => SlotAction::Create(SnapshotReason::FirstStart),
        (None, None) => SlotAction::Refuse(format!(
            "replication slot {name} doesn't exist and slot_management is require_existing: have it created (pg_create_logical_replication_slot('{name}', 'pgoutput') in database {db}), then restart the pipeline"
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

/// What a publication publishes beyond its table list (DA-SRC-13), by server version.
#[derive(Debug, Default)]
struct Shape {
    /// Its tables by name (`pg_publication_rel`): what `DROP TABLE` can take out.
    members: Vec<TableRef>,
    /// PostgreSQL 15+: its member tables with a row filter or a column list.
    filtered: Vec<TableRef>,
    /// PostgreSQL 15+: the schemas it publishes (`FOR TABLES IN SCHEMA`).
    schemas: Vec<String>,
    /// PostgreSQL 18+: `publish_generated_columns = stored`.
    generated_columns: bool,
    /// Of `tables`, those with a stored generated column (read when it publishes them).
    generated: Vec<TableRef>,
}

async fn publication_shape(
    client: &Client,
    name: &str,
    version: i32,
    tables: &[TableRef],
) -> Result<Shape, tokio_postgres::Error> {
    let table =
        |r: &tokio_postgres::Row| TableRef::new(r.get::<_, String>(0), r.get::<_, String>(1));
    let rows = if version >= 150_000 {
        client
            .query(
                "SELECT n.nspname::text, c.relname::text,
                        r.prqual IS NOT NULL OR r.prattrs IS NOT NULL
                   FROM pg_catalog.pg_publication_rel r
                   JOIN pg_catalog.pg_publication p ON p.oid = r.prpubid
                   JOIN pg_catalog.pg_class c ON c.oid = r.prrelid
                   JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                  WHERE p.pubname = $1",
                &[&name],
            )
            .await?
    } else {
        client
            .query(
                "SELECT n.nspname::text, c.relname::text, false
                   FROM pg_catalog.pg_publication_rel r
                   JOIN pg_catalog.pg_publication p ON p.oid = r.prpubid
                   JOIN pg_catalog.pg_class c ON c.oid = r.prrelid
                   JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                  WHERE p.pubname = $1",
                &[&name],
            )
            .await?
    };
    let mut shape = Shape {
        members: rows.iter().map(table).collect(),
        filtered: rows.iter().filter(|r| r.get(2)).map(table).collect(),
        ..Shape::default()
    };
    if version >= 150_000 {
        shape.schemas = client
            .query(
                "SELECT n.nspname::text
                   FROM pg_catalog.pg_publication_namespace pn
                   JOIN pg_catalog.pg_publication p ON p.oid = pn.pnpubid
                   JOIN pg_catalog.pg_namespace n ON n.oid = pn.pnnspid
                  WHERE p.pubname = $1 ORDER BY 1",
                &[&name],
            )
            .await?
            .iter()
            .map(|r| r.get(0))
            .collect();
    }
    if version >= 180_000 {
        shape.generated_columns = client
            .query_one(
                "SELECT pubgencols = 's' FROM pg_catalog.pg_publication WHERE pubname = $1",
                &[&name],
            )
            .await?
            .get(0);
    }
    if shape.generated_columns {
        let schemas: Vec<&str> = tables.iter().map(|t| t.schema.as_str()).collect();
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        shape.generated = client
            .query(
                "SELECT DISTINCT n.nspname::text, c.relname::text
                   FROM pg_catalog.pg_class c
                   JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                   JOIN unnest($1::text[], $2::text[]) AS w(s, t)
                     ON w.s = n.nspname AND w.t = c.relname
                   JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                  WHERE a.attgenerated = 's' AND NOT a.attisdropped
                  ORDER BY 1, 2",
                &[&schemas, &names],
            )
            .await?
            .iter()
            .map(table)
            .collect();
    }
    Ok(shape)
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

/// The server the stream reads: its system identifier, and the timeline WAL is written on now
/// (the first eight hex digits of the current WAL file's name). A promotion keeps the identifier
/// and starts a new timeline at once (measured on PostgreSQL 18, `pg_ctl promote -w`); a standby,
/// which writes no WAL, reports its last checkpoint's timeline. Every function is open to any
/// role.
pub(crate) async fn server_identity(
    client: &Client,
) -> Result<(String, String), tokio_postgres::Error> {
    let row = client
        .query_one(
            "SELECT system_identifier::text,
                    CASE WHEN pg_is_in_recovery()
                         THEN lpad(to_hex((SELECT timeline_id FROM pg_control_checkpoint())), 8, '0')
                         ELSE substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8) END
               FROM pg_control_system()",
            &[],
        )
        .await?;
    Ok((row.get(0), row.get(1)))
}

/// [`server_identity`]'s row: the identifier in decimal, the timeline in hex.
pub fn parse_identity(id: &str, timeline: &str) -> Result<ServerIdentity, SourceError> {
    let bad = || SourceError::Permanent(format!("the server's identity reads {id}, {timeline}"));
    // `system_identifier` is a `bigint`: one at or past 2^63 prints negative.
    let system_identifier = id
        .parse::<u64>()
        .or_else(|_| id.parse::<i64>().map(|v| v as u64))
        .map_err(|_| bad())?;
    Ok(ServerIdentity::Postgres {
        system_identifier,
        timeline: u32::from_str_radix(timeline, 16).map_err(|_| bad())?,
    })
}

/// Whether the checkpoint was read on another server than `now` (DA-SRC-20): `Ok(true)` asks a
/// source-change start (`source_change_action: re_snapshot`), `error` halts. A checkpoint that
/// records no server (written before it was recorded) is taken as this server's: the start
/// records it.
pub fn source_change(
    checkpoint: Option<&Checkpoint>,
    now: &ServerIdentity,
    action: SourceChangeAction,
) -> Result<bool, SourceError> {
    let Some((at, was)) = checkpoint.and_then(|c| c.server.as_ref().map(|s| (&c.position, s)))
    else {
        return Ok(false);
    };
    if was == now {
        return Ok(false);
    }
    match action {
        SourceChangeAction::ReSnapshot => Ok(true),
        SourceChangeAction::Error => Err(SourceError::Permanent(format!(
            "WAL_SOURCE_CHANGED: the checkpoint {} was read on {was}, and the pipeline now \
             reads {now}: a promoted replica's LSNs may repeat ones the lake has marked. Set \
             source_change_action: re_snapshot to start from this server's position, \
             snapshotting as snapshots.mode says, or point the pipeline back at the old server",
            at.untagged()
        ))),
    }
}

/// A stream reconnect's check of the server (Data-Flow Phase 2a, Source Disconnect): the stream's
/// open and the reset of a lost position read the server first, and a `lost` checkpoint read on
/// another system or timeline halts as a start would (`source_change_action: error`, the same
/// `WAL_SOURCE_CHANGED`), or asks a rerun of Phase 0 (`re_snapshot`:
/// [`SourceError::SourceChanged`]), whose `prepare` plans the recovery. Nothing on the server has
/// changed by then.
pub fn reconnect_check(
    lost: &Checkpoint,
    now: &ServerIdentity,
    action: SourceChangeAction,
) -> Result<(), SourceError> {
    if !source_change(Some(lost), now, action)? {
        return Ok(());
    }
    let was = lost
        .server
        .as_ref()
        .map_or_else(String::new, ToString::to_string);
    Err(SourceError::SourceChanged(format!(
        "WAL_SOURCE_CHANGED: the checkpoint {} was read on {was}, and the stream's reconnect \
         reaches {now}: source_change_action is re_snapshot, so the pipeline starts again to \
         recover from this server's position",
        lost.position.untagged()
    )))
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

/// The slot at a source-change start (`source_change_action: re_snapshot`): the stream starts
/// from the new server's position. `managed` creates the slot there (dropping one that came
/// over with the failover); `require_existing` takes the DBA's slot, which must exist. A slot of
/// the name that isn't the pipeline's kind ([`not_ours`]) is refused under either.
pub fn on_source_change(
    slot: Option<&SlotState>,
    db: &str,
    name: &str,
    management: SlotManagement,
) -> SlotAction {
    if let Some(m) = slot.and_then(|s| not_ours(s, db, name)) {
        return SlotAction::Refuse(m);
    }
    match (management, slot) {
        (SlotManagement::Managed, None) => SlotAction::Create(SnapshotReason::SourceChanged),
        (SlotManagement::Managed, Some(_)) => SlotAction::Recreate(SnapshotReason::SourceChanged),
        (SlotManagement::RequireExisting, Some(s)) if !s.invalidated => {
            SlotAction::Adopt(SnapshotReason::SourceChanged)
        }
        (SlotManagement::RequireExisting, _) => SlotAction::Refuse(format!(
            "WAL_SOURCE_CHANGED: the pipeline starts from the new server's position, and \
             slot_management is require_existing, so replication slot {name} must exist there \
             and stream: have it created, then restart the pipeline"
        )),
    }
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

    /// DA-SRC-13: under `require_existing` or `FOR ALL TABLES`, a tracked table the publication
    /// publishes with a row filter or a column list (PostgreSQL 15+) is moved from `out.tables`
    /// to `out.halted`: some of its changes never reach the stream, so it halts itself and its
    /// recovery is a re-snapshot. Under a `managed` publication of named tables the repair
    /// publishes it whole instead.
    pub(crate) async fn halt_filtered_tables(
        &self,
        out: &mut ResolvedTables,
    ) -> Result<(), SourceError> {
        let name = &self.pg.publication;
        let client = self.conn.client().await?;
        let row = self
            .conn
            .command(
                "read the publication",
                client.query_opt(
                    "SELECT puballtables, current_setting('server_version_num')::int
                       FROM pg_catalog.pg_publication WHERE pubname = $1",
                    &[name],
                ),
            )
            .await?;
        let Some(row) = row else { return Ok(()) };
        let (all_tables, version): (bool, i32) = (row.get(0), row.get(1));
        let managed = self.pg.slot_management == SlotManagement::Managed;
        if (managed && !all_tables) || version < 150_000 || out.tables.is_empty() {
            return Ok(());
        }
        let tracked: Vec<TableRef> = out.tables.iter().map(|s| s.table().clone()).collect();
        let shape = self
            .conn
            .command(
                "read what the publication publishes",
                publication_shape(&client, name, version, &tracked),
            )
            .await?;
        for table in shape.filtered.iter().filter(|t| tracked.contains(t)) {
            out.tables.retain(|s| s.table() != table);
            let publication = ident(name);
            let target = qualified(table);
            out.halted.push((
                table.clone(),
                HaltReason::NotReplicable {
                    reason: format!(
                        "publication {name} publishes it with a row filter or a column list, so \
                         some of its changes never reach the stream; publish it whole in one \
                         transaction (BEGIN; ALTER PUBLICATION {publication} DROP TABLE {target}; \
                         ALTER PUBLICATION {publication} ADD TABLE {target}; COMMIT;), then \
                         re-snapshot the table"
                    ),
                },
            ));
        }
        Ok(())
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
                            pubviaroot, current_setting('server_version_num')::int
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
        let (all_tables, every_op, via_root, version): (bool, bool, bool, i32) =
            (row.get(0), row.get(1), row.get(2), row.get(3));
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
        let shape = self
            .conn
            .command(
                "read what the publication publishes",
                publication_shape(client, name, version, tables),
            )
            .await?;
        let filtered: Vec<String> = shape
            .filtered
            .iter()
            .filter(|t| wanted.contains(t))
            .map(table_key)
            .collect();
        let mut warnings = vec![];
        if !managed || all_tables {
            // A tracked table's row filter or column list (PostgreSQL 15+) keeps some of its rows
            // or columns from the stream (DA-SRC-13). `resolve_tables` halts such a table, so it
            // doesn't reach here; this stays as a fail-closed guard for a caller that passes one.
            if !filtered.is_empty() {
                return Err(SourceError::Permanent(format!(
                    "publication {name} publishes {} with a row filter or a column list, so some \
                     of the changes the pipeline replicates wouldn't arrive; publish the tables \
                     whole (ALTER PUBLICATION … DROP TABLE, then ADD TABLE without WHERE or a \
                     column list)",
                    filtered.join(", ")
                )));
            }
            // PostgreSQL 18 publishes stored generated columns when asked, which the pipeline
            // doesn't replicate.
            if shape.generated_columns && !shape.generated.is_empty() {
                let list: Vec<String> = shape.generated.iter().map(table_key).collect();
                return Err(SourceError::Permanent(format!(
                    "publication {name} publishes stored generated columns (of {}), which the \
                     pipeline doesn't replicate; run ALTER PUBLICATION {} SET \
                     (publish_generated_columns = none)",
                    list.join(", "),
                    ident(name)
                )));
            }
            // Q-M5-5: a partition's changes would arrive under its parent's name, which isn't
            // tracked, so the stream would drop them. Refused, not warned: it would lose data. A
            // partition the publication names itself arrives under its own.
            if via_root {
                let leaves: Vec<TableRef> = self
                    .conn
                    .command("find partitions", partitions(client, tables))
                    .await?
                    .into_iter()
                    .filter(|l| !published.contains(l))
                    .collect();
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
            // The server decodes the other tables' changes for the slot, and the stream drops
            // them (D-M5.14).
            let others: Vec<String> = published
                .iter()
                .filter(|t| !wanted.contains(t))
                .map(table_key)
                .collect();
            if !others.is_empty() {
                let shown = others
                    .iter()
                    .take(5)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ");
                let more = match others.len() {
                    n if n > 5 => format!(" and {} more", n - 5),
                    _ => String::new(),
                };
                warnings.push(format!(
                    "publication {name} also publishes tables the pipeline doesn't replicate \
                     ({shown}{more}): the server decodes their changes for the slot, and the \
                     pipeline drops them"
                ));
            }
            return Ok(Reconciled {
                warnings,
                ..Reconciled::default()
            });
        }
        let mut sql = vec![];
        // A row filter, a column list or a schema the publication publishes can't be taken out
        // table by table (a schema's table isn't a member to drop, 42704): the managed
        // publication is set to the tracked tables whole.
        let reset = !filtered.is_empty() || !shape.schemas.is_empty();
        let list: Vec<String> = tables.iter().map(qualified).collect();
        if reset && !list.is_empty() {
            sql.push(format!(
                "ALTER PUBLICATION {} SET TABLE {}",
                ident(name),
                list.join(", ")
            ));
        } else {
            if !shape.schemas.is_empty() {
                let schemas: Vec<String> = shape.schemas.iter().map(|s| ident(s)).collect();
                sql.push(format!(
                    "ALTER PUBLICATION {} DROP TABLES IN SCHEMA {}",
                    ident(name),
                    schemas.join(", ")
                ));
            }
            // The publication's direct members become exactly the tracked set: a tracked table
            // it publishes only through a partitioned root or a schema is added by name (it was
            // published before, so it isn't in `added` and isn't copied again).
            let members: BTreeSet<&TableRef> = shape.members.iter().collect();
            let to_add: Vec<String> = tables
                .iter()
                .filter(|t| !members.contains(t))
                .map(qualified)
                .collect();
            if !to_add.is_empty() {
                sql.push(format!(
                    "ALTER PUBLICATION {} ADD TABLE {}",
                    ident(name),
                    to_add.join(", ")
                ));
            }
            let extra: Vec<String> = shape
                .members
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
        }
        if !every_op || via_root || shape.generated_columns {
            let generated = match shape.generated_columns {
                true => ", publish_generated_columns = none",
                false => "",
            };
            sql.push(format!(
                "ALTER PUBLICATION {} SET (publish = 'insert, update, delete, truncate', publish_via_partition_root = false{generated})",
                ident(name)
            ));
        }
        Ok(Reconciled {
            sql,
            rewritten: !every_op
                || !filtered.is_empty()
                || (shape.generated_columns && !shape.generated.is_empty()),
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
        let recorded = checkpoint;
        let checkpoint = checkpoint.map(|c| lsn_of(&c.position)).transpose()?;
        let client = self.conn.client().await?;
        let (id, timeline) = self
            .conn
            .command("read the server's identity", server_identity(&client))
            .await?;
        let server = parse_identity(&id, &timeline)?;
        let changed = source_change(recorded, &server, self.live().source_change_action)?;
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
        let decided = if changed {
            if let Some(was) = recorded.and_then(|c| c.server.as_ref()) {
                warnings.push(format!(
                    "the checkpoint was read on {was}, and the pipeline now reads {server}: \
                     source_change_action is re_snapshot, so the pipeline re-snapshots from this \
                     server's position"
                ));
            }
            on_source_change(
                slot.as_ref(),
                &self.pipeline.source.db,
                self.slot_name(),
                self.pg.slot_management,
            )
        } else {
            decide(
                checkpoint,
                slot.as_ref(),
                &self.pipeline.source.db,
                self.slot_name(),
                self.pg.slot_management,
                self.live().slot_on_orphan,
            )
        };
        let action = match decided {
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
            SlotAction::Adopt(reason) => {
                // Advancing decodes the orphan's whole backlog: the snapshot timeout bounds it.
                let row = self.conn.bounded(
                    "advance the slot",
                    self.conn.timeouts.snapshot,
                    client.query_one(
                        "SELECT end_lsn::text FROM pg_replication_slot_advance($1, pg_current_wal_lsn())",
                        &[&self.slot_name()],
                    ),
                )
                .await?;
                snapshot(parse_lsn(row.get(0))?, reason)
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
            server: Some(server),
        })
    }

    /// D-15.3 after [`SourceError::PositionUnreachable`]: drop the slot and create it again;
    /// stream from the new position after a re-snapshot. The checkpoint names the server it was
    /// made on. First, a `lost` checkpoint read on another system or timeline (a failover whose
    /// new server can't serve the slot's position) halts or asks a rerun of Phase 0
    /// ([`reconnect_check`]): the slot isn't dropped or made there. Under `require_existing` the
    /// slot isn't ours to recreate.
    pub async fn reset_position(&self, lost: &Checkpoint) -> Result<Checkpoint, SourceError> {
        let client = self.conn.client().await?;
        let (id, timeline) = self
            .conn
            .command("read the server's identity", server_identity(&client))
            .await?;
        let server = parse_identity(&id, &timeline)?;
        reconnect_check(lost, &server, self.live().source_change_action)?;
        if self.pg.slot_management == SlotManagement::RequireExisting {
            return Err(SourceError::Permanent(format!(
                "replication slot {} lost its position; drop it: slot_management is require_existing, so create it again, with slot_on_orphan: adopt_resnapshot for the pipeline to adopt it and re-snapshot",
                self.slot_name()
            )));
        }
        self.drop_slot(&client).await?;
        let at = self.create_slot(&client).await?;
        Ok(Checkpoint {
            position: SourcePosition::Pg(at),
            server: Some(server),
        })
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

    fn read_on(lsn: u64, system_identifier: u64, timeline: u32) -> Checkpoint {
        Checkpoint {
            position: SourcePosition::Pg(Lsn(lsn)),
            server: Some(ServerIdentity::Postgres {
                system_identifier,
                timeline,
            }),
        }
    }

    #[test]
    fn the_identity_is_the_system_and_the_timeline() {
        assert_eq!(
            parse_identity("7692239610332024875", "00000002").unwrap(),
            ServerIdentity::Postgres {
                system_identifier: 7692239610332024875,
                timeline: 2
            }
        );
        assert_eq!(
            parse_identity("-1", "00000001").unwrap(),
            ServerIdentity::Postgres {
                system_identifier: u64::MAX,
                timeline: 1
            },
            "past 2^63 the bigint prints negative"
        );
        assert_eq!(
            parse_identity("-9223372036854775808", "00000001").unwrap(),
            ServerIdentity::Postgres {
                system_identifier: 1 << 63,
                timeline: 1
            }
        );
        assert!(parse_identity("x", "00000002").is_err());
        assert!(parse_identity("1", "zz").is_err());
    }

    /// A promoted replica keeps the system identifier and starts a new timeline (DA-SRC-20).
    #[test]
    fn another_timeline_or_system_is_a_source_change() {
        let now = ServerIdentity::Postgres {
            system_identifier: 7,
            timeline: 2,
        };
        let promoted = read_on(0x20, 7, 1);
        let e = source_change(Some(&promoted), &now, SourceChangeAction::Error).unwrap_err();
        assert!(
            e.to_string().starts_with(
                "WAL_SOURCE_CHANGED: the checkpoint 0/20 was read on PostgreSQL system 7 on \
                 timeline 1, and the pipeline now reads PostgreSQL system 7 on timeline 2"
            ),
            "{e}"
        );
        assert!(source_change(Some(&promoted), &now, SourceChangeAction::ReSnapshot).unwrap());
        let another = read_on(0x20, 8, 2);
        assert!(source_change(Some(&another), &now, SourceChangeAction::ReSnapshot).unwrap());
        let same = read_on(0x20, 7, 2);
        assert!(!source_change(Some(&same), &now, SourceChangeAction::Error).unwrap());
        let unrecorded: Checkpoint = SourcePosition::Pg(Lsn(0x20)).into();
        assert!(
            !source_change(Some(&unrecorded), &now, SourceChangeAction::Error).unwrap(),
            "a checkpoint written before the identity was recorded is taken as this server's"
        );
        assert!(!source_change(None, &now, SourceChangeAction::Error).unwrap());
    }

    /// A stream reconnect on another system or timeline (I-1): the start's halt under `error`, a
    /// rerun of Phase 0 under `re_snapshot`; the same server, or a checkpoint naming none, opens.
    #[test]
    fn a_reconnect_to_another_server_halts_or_asks_a_rerun() {
        let now = ServerIdentity::Postgres {
            system_identifier: 7,
            timeline: 2,
        };
        let lost = read_on(0x20, 7, 1);
        let e = reconnect_check(&lost, &now, SourceChangeAction::Error).unwrap_err();
        assert_eq!(e.halt_code(), Some("WAL_SOURCE_CHANGED"), "{e}");
        assert_eq!(
            e.to_string(),
            source_change(Some(&lost), &now, SourceChangeAction::Error)
                .unwrap_err()
                .to_string(),
            "the start's halt"
        );
        match reconnect_check(&lost, &now, SourceChangeAction::ReSnapshot) {
            Err(SourceError::SourceChanged(m)) => assert!(
                m.starts_with(
                    "WAL_SOURCE_CHANGED: the checkpoint 0/20 was read on PostgreSQL system 7 on \
                     timeline 1, and the stream's reconnect reaches PostgreSQL system 7 on \
                     timeline 2"
                ),
                "{m}"
            ),
            other => panic!("SourceChanged expected, got {other:?}"),
        }
        for action in [SourceChangeAction::Error, SourceChangeAction::ReSnapshot] {
            assert!(reconnect_check(&read_on(0x20, 7, 2), &now, action).is_ok());
            let unrecorded: Checkpoint = SourcePosition::Pg(Lsn(0x20)).into();
            assert!(reconnect_check(&unrecorded, &now, action).is_ok());
        }
    }

    #[test]
    fn a_source_change_starts_from_the_new_server() {
        let c = |s: Option<&SlotState>, m| on_source_change(s, "shop", "s", m);
        assert_eq!(c(None, M), A::Create(R::SourceChanged));
        assert_eq!(
            c(Some(&slot(0x10)), M),
            A::Recreate(R::SourceChanged),
            "a slot that came over with the failover is dropped"
        );
        assert_eq!(c(Some(&slot(0x10)), X), A::Adopt(R::SourceChanged));
        refused(c(None, X), "must exist there");
        let lost = SlotState {
            invalidated: true,
            ..slot(0x10)
        };
        refused(c(Some(&lost), X), "must exist there");
    }

    /// A same-named slot that isn't a pgoutput slot of the pipeline's database is neither
    /// dropped (`managed`) nor advanced (`require_existing`) at a source change (M-3).
    #[test]
    fn someone_elses_slot_is_refused_at_a_source_change() {
        let mut physical = slot(0x10);
        physical.plugin = None;
        let mut wal2json = slot(0x10);
        wal2json.plugin = Some("wal2json".into());
        let mut elsewhere = slot(0x10);
        elsewhere.database = Some("other".into());
        for m in [M, X] {
            let c = |s: &SlotState| on_source_change(Some(s), "shop", "s", m);
            refused(c(&physical), "uses the physical plugin, not pgoutput");
            refused(c(&wal2json), "uses the wal2json plugin, not pgoutput");
            refused(c(&elsewhere), "belongs to database other, not shop");
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
        refused(
            d(Some(0x20), None, X, HALT),
            "doesn't exist; slot_management is require_existing, so create it again",
        );
    }

    #[test]
    fn a_slot_past_the_checkpoint_or_invalidated_lost_the_position() {
        assert_eq!(
            d(Some(0x10), Some(slot(0x20)), M, HALT),
            A::Recreate(R::PositionLost)
        );
        let mut gone = slot(0x10);
        gone.invalidated = true;
        assert_eq!(
            d(Some(0x20), Some(gone.clone()), M, HALT),
            A::Recreate(R::PositionLost)
        );
        refused(
            d(Some(0x20), Some(gone), X, HALT),
            "was invalidated; drop it: slot_management is require_existing, so create it again, with slot_on_orphan: adopt_resnapshot",
        );
    }

    /// Under `require_existing` the DBA makes a lost slot again, past the checkpoint: adopted on
    /// request, like an orphan (DA-SRC-30).
    #[test]
    fn a_slot_made_again_under_require_existing_is_adopted_on_request() {
        refused(
            d(Some(0x10), Some(slot(0x20)), X, HALT),
            "confirmed 0/20, past the pipeline's checkpoint 0/10: the changes between are lost (a slot made again, or moved by something else); slot_management is require_existing, so set slot_on_orphan: adopt_resnapshot",
        );
        assert_eq!(
            d(Some(0x10), Some(slot(0x20)), X, ADOPT),
            A::Adopt(R::PositionLost)
        );
        assert_eq!(
            d(Some(0x20), Some(slot(0x20)), X, ADOPT),
            A::Resume,
            "at the checkpoint it is the pipeline's own"
        );
        assert_eq!(
            d(Some(0x20), Some(slot(0x10)), X, ADOPT),
            A::Resume,
            "behind the checkpoint it is the pipeline's own"
        );
        let mut gone = slot(0x20);
        gone.invalidated = true;
        refused(
            d(Some(0x20), Some(gone), X, ADOPT),
            "was invalidated; drop it",
        );
        assert_eq!(
            d(Some(0x10), Some(slot(0x20)), M, ADOPT),
            d(Some(0x10), Some(slot(0x20)), M, HALT),
            "managed recreates whatever slot_on_orphan says"
        );
        let mut other = slot(0x20);
        other.plugin = Some("wal2json".into());
        refused(d(Some(0x10), Some(other), X, ADOPT), "wal2json");
    }

    #[test]
    fn an_orphan_halts_or_is_adopted() {
        refused(
            d(None, Some(slot(0x10)), M, HALT),
            "(an orphan): drop the slot for a first start, restore the pipeline state, or set slot_on_orphan: adopt_resnapshot",
        );
        let a = d(None, Some(slot(0x10)), X, HALT);
        refused(
            a.clone(),
            "(an orphan): restore the pipeline state, or set slot_on_orphan: adopt_resnapshot for the pipeline to adopt it and re-snapshot",
        );
        let A::Refuse(m) = a else { unreachable!() };
        assert!(
            !m.contains("drop the slot"),
            "a dropped slot isn't made again under require_existing: {m}"
        );
        assert_eq!(
            d(None, Some(slot(0x10)), M, ADOPT),
            A::Adopt(R::AdoptedOrphan)
        );
        assert_eq!(
            d(None, Some(slot(0x10)), X, ADOPT),
            A::Adopt(R::AdoptedOrphan),
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
        refused(
            d(None, None, X, ADOPT),
            "doesn't exist and slot_management is require_existing: have it created (pg_create_logical_replication_slot('s', 'pgoutput') in database shop), then restart the pipeline",
        );
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
