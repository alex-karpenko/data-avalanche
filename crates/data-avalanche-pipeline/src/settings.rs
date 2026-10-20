//! The worker's settings, read once from the resolved pipeline (config §9).

use std::collections::BTreeMap;
use std::time::Duration;

use data_avalanche_config::{
    PartitionTransform as ConfigTransform, Pipeline, SinkType, SnapshotMode, SourceType,
    ToastStrategy, WriteMode as ConfigWriteMode,
};
use data_avalanche_core::{
    PartitionField, PartitionTransform, SinkKind, TableRef, TableSchema, WriteMode,
};

use crate::checkpoint::Store;
use crate::{FlushPolicy, Lineage, TxnLimits};

/// Status interval and stall detection when the source has no heartbeat setting.
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub pipeline_id: String,
    pub sink_kind: SinkKind,
    pub lineage: Lineage,
    pub flush: FlushPolicy,
    pub limits: TxnLimits,
    /// Rows handed to the sink writer and not yet durable, at most (`buffer_capacity_rows`).
    pub buffer_rows: u64,
    pub write_mode: WriteMode,
    pub parallelism: usize,
    pub chunk_size: usize,
    /// `source.timeouts.snapshot`: one table's snapshot within one run.
    pub snapshot_timeout: Duration,
    /// `source.postgres.heartbeat` or `source.mysql.heartbeat`: the source's log is read at this
    /// interval (PostgreSQL: the WAL-lag check; MySQL: whether the dump still runs); a stream
    /// with no item for three of them is stalled.
    pub heartbeat: Duration,
    pub max_wal_lag_bytes: Option<u64>,
    /// Channel D; `None` when `rescan_interval` is 0.
    pub rescan_interval: Option<Duration>,
    pub evolution: bool,
    /// `toast_strategy: re_query`.
    pub re_query: bool,
    /// The document holding the checkpoint.
    pub checkpoint: Store,
    mode: SnapshotMode,
    overrides: BTreeMap<String, SnapshotMode>,
    partitioning: BTreeMap<String, Vec<PartitionField>>,
    source: SourceType,
}

fn transform(t: ConfigTransform) -> PartitionTransform {
    match t {
        ConfigTransform::Identity => PartitionTransform::Identity,
        ConfigTransform::Bucket { n } => PartitionTransform::Bucket { n },
        ConfigTransform::Truncate { width } => PartitionTransform::Truncate { width },
        ConfigTransform::Year => PartitionTransform::Year,
        ConfigTransform::Month => PartitionTransform::Month,
        ConfigTransform::Day => PartitionTransform::Day,
        ConfigTransform::Hour => PartitionTransform::Hour,
    }
}

impl Settings {
    pub fn new(p: &Pipeline) -> Self {
        let pg = p.source.postgres.as_ref();
        Self {
            pipeline_id: p.id.clone(),
            sink_kind: match p.sink.kind {
                SinkType::Delta => SinkKind::Delta,
                SinkType::Iceberg => SinkKind::Iceberg,
            },
            lineage: Lineage {
                source_db: format!("{}/{}", p.source.host, p.source.db),
                pipeline: p.name.clone(),
            },
            flush: FlushPolicy {
                seconds: p.sink.flush.seconds,
                rows: p.sink.flush.rows,
                bytes: p.sink.flush.bytes,
            },
            limits: TxnLimits {
                batch_rows: p.sink.max_transaction_batch_rows,
                buffer_bytes: p.sink.max_transaction_buffer_bytes,
            },
            buffer_rows: p.sink.buffer_capacity_rows,
            write_mode: match p.sink.write_mode {
                ConfigWriteMode::Mirror => WriteMode::Mirror,
                ConfigWriteMode::Changelog => WriteMode::Changelog,
            },
            parallelism: p.tables.snapshots.parallelism.max(1) as usize,
            chunk_size: usize::try_from(p.tables.snapshots.chunk_size)
                .unwrap_or(usize::MAX)
                .max(1),
            snapshot_timeout: p.source.timeouts.snapshot,
            heartbeat: pg
                .map(|pg| pg.heartbeat)
                .or(p.source.mysql.as_ref().map(|m| m.heartbeat))
                .unwrap_or(DEFAULT_HEARTBEAT),
            max_wal_lag_bytes: pg.and_then(|pg| pg.max_wal_lag_bytes),
            rescan_interval: (!p.schema_evolution.rescan_interval.is_zero())
                .then_some(p.schema_evolution.rescan_interval),
            evolution: p.schema_evolution.enabled,
            re_query: p.source.toast_strategy == ToastStrategy::ReQuery,
            checkpoint: match pg {
                Some(pg) => Store::Slot {
                    name: pg.slot.clone(),
                    intent: (pg.slot_management == data_avalanche_config::SlotManagement::Managed)
                        .then(|| pg.slot.clone()),
                },
                None => Store::Position,
            },
            mode: p.tables.snapshots.mode,
            overrides: p.tables.snapshots.overrides.clone(),
            partitioning: p
                .tables
                .partitioning
                .iter()
                .map(|(k, fields)| {
                    let fields = fields
                        .iter()
                        .map(|f| PartitionField {
                            column: f.column.clone(),
                            transform: transform(f.transform),
                        })
                        .collect();
                    (k.clone(), fields)
                })
                .collect(),
            source: p.source.kind,
        }
    }

    /// The table's key in the `tables` maps: `schema.table` for PostgreSQL, the bare name for
    /// MySQL (V-064).
    fn key(&self, t: &TableRef) -> String {
        match self.source {
            SourceType::Postgres => format!("{}.{}", t.schema, t.name),
            SourceType::Mysql => t.name.clone(),
        }
    }

    /// `snapshots.mode`, or the table's override.
    pub fn mode(&self, t: &TableRef) -> SnapshotMode {
        self.overrides
            .get(&self.key(t))
            .copied()
            .unwrap_or(self.mode)
    }

    /// A `dump_*` table: it stops once its snapshot is done.
    pub fn is_dump(&self, t: &TableRef) -> bool {
        matches!(
            self.mode(t),
            SnapshotMode::DumpAlways | SnapshotMode::DumpInitial
        )
    }

    pub fn partition_by(&self, t: &TableRef) -> Vec<PartitionField> {
        self.partitioning
            .get(&self.key(t))
            .cloned()
            .unwrap_or_default()
    }

    /// The columns a delete carries besides the identifier: the table's partition columns that
    /// are source columns (a lineage column such as `_ts` isn't in the row).
    pub fn delete_extra(&self, schema: &TableSchema) -> Vec<String> {
        self.partition_by(schema.table())
            .into_iter()
            .map(|p| p.column)
            .filter(|c| schema.column(c).is_some())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_core::{ColumnSchema, ColumnValueType as V};

    use super::*;
    use crate::test_support::{Yaml, pipeline};

    #[test]
    fn settings_follow_the_pipeline() {
        let p = pipeline(
            r#"
      snapshots: { mode: initial, parallelism: 3, chunk_size: 500, overrides: { public.big: never } }
      partitioning: { public.orders: [ { column: region, transform: identity }, { column: _ts, transform: identity } ] }"#,
            "max_transaction_batch_rows: 1000, flush: { seconds: 0 }",
            "heartbeat: 5s, max_wal_lag_bytes: 1GiB",
        );
        let s = Settings::new(&p);
        assert_eq!(s.pipeline_id, "p");
        assert_eq!(s.sink_kind, SinkKind::Delta);
        assert_eq!(s.lineage.source_db, "127.0.0.1/avalanche");
        assert_eq!(s.flush.seconds, Some(0));
        assert_eq!(s.limits.batch_rows, Some(1000));
        assert_eq!((s.parallelism, s.chunk_size), (3, 500));
        assert_eq!(s.heartbeat, Duration::from_secs(5));
        assert_eq!(s.max_wal_lag_bytes, Some(1 << 30));
        assert_eq!(s.rescan_interval, Some(Duration::from_secs(3600)));
        assert_eq!(
            s.checkpoint,
            Store::Slot {
                name: "p_slot".into(),
                intent: Some("p_slot".into())
            }
        );
        let orders = TableRef::new("public", "orders");
        assert_eq!(s.mode(&orders), SnapshotMode::Initial);
        assert_eq!(s.mode(&TableRef::new("public", "big")), SnapshotMode::Never);
        assert_eq!(s.partition_by(&orders).len(), 2);
        let schema = TableSchema::new(
            orders,
            vec![
                ColumnSchema::new("id", V::Int64, "bigint").not_null(),
                ColumnSchema::new("region", V::Text, "text"),
            ],
            vec!["id".into()],
        )
        .unwrap();
        assert_eq!(
            s.delete_extra(&schema),
            ["region"],
            "_ts isn't a row column"
        );
    }

    #[test]
    fn require_existing_records_no_slot_intent() {
        let p = pipeline(
            "",
            "",
            "slot_management: require_existing, auto_cleanup: false",
        );
        assert_eq!(
            Settings::new(&p).checkpoint,
            Store::Slot {
                name: "p_slot".into(),
                intent: None
            }
        );
    }

    /// A MySQL pipeline keeps its checkpoint in the position document, reads the dump at
    /// `mysql.heartbeat`, has no WAL to guard, and keys its tables by bare name (V-064).
    #[test]
    fn a_mysql_pipeline_keeps_a_position_document() {
        let p = Yaml {
            mysql: Some("heartbeat: 7s"),
            tables: "\n      snapshots: { overrides: { orders: initial } }",
            ..Yaml::default()
        }
        .build();
        let s = Settings::new(&p);
        assert_eq!(s.checkpoint, Store::Position);
        assert_eq!(s.heartbeat, Duration::from_secs(7));
        assert_eq!(s.max_wal_lag_bytes, None);
        assert_eq!(
            s.mode(&TableRef::new("public", "orders")),
            SnapshotMode::Initial
        );
    }
}
