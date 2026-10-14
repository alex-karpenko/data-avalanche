//! One Delta table as the sink sees it: open and reload it, create it, and read its Hop 2 schema,
//! its marker and its rows.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_row::{RowConverter, SortField};
use arrow_schema::SchemaRef;
use data_avalanche_core::{PartitionTransform, SinkError, SourcePosition, TableRef, TableSpec};
use deltalake::datafusion::prelude::SessionContext;
use deltalake::logstore::object_store::path::Path;
use deltalake::logstore::object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use deltalake::protocol::SaveMode;
use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use futures_util::TryStreamExt;
use url::Url;

use crate::TableDefaults;
use crate::convert::from_delta;
use crate::error::{permanent, sink_error};
use crate::marker::{WalkBack, app_id, walk_back};
use crate::types::{column_logical, delta_field, hop2_schema, physical_type};

/// Table property holding the identifier columns (a JSON array), so the table can be read in key
/// order without its `TableSchema`.
pub const IDENTIFIER_KEY: &str = "data-avalanche.identifier";

pub(crate) struct TableHandle {
    /// The source table this Delta table mirrors.
    name: TableRef,
    table: DeltaTable,
    /// The storage options the table was opened with, to re-open it from scratch.
    options: HashMap<String, String>,
    /// The newest commit of this pipeline the handle knows. The walk-back stops there, so a
    /// steady stream of own commits costs no reads.
    known: Option<Known>,
    /// A commit file of the log the handle loaded, as the store described it then. A table
    /// dropped and re-created behind the handle holds another file there, or none (Q-M6b-8).
    seen: Option<Seen>,
}

/// One commit file's identity in the store: its e-tag, size and modification time.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    version: u64,
    meta: ObjectMeta,
}

/// An own commit the handle knows: the table it went to (`metaData.id`, new for a table dropped
/// and re-created under the same root), its version and its position.
#[derive(Debug, Clone)]
struct Known {
    table_id: String,
    version: u64,
    position: SourcePosition,
}

impl std::fmt::Debug for TableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableHandle")
            .field("name", &self.name)
            .field("url", self.table.table_url())
            .field("version", &self.table.version())
            .field("known", &self.known)
            .finish()
    }
}

impl TableHandle {
    /// A handle on `url`, where `name` is mirrored; the table is loaded when it exists. A local
    /// root is created first: delta-rs refuses a `file://` location that doesn't exist.
    ///
    /// Loaded only when the location holds a log (one LIST): delta-rs's own "load, else accept
    /// an absent table" makes the kernel log an ERROR ("No files in log segment") for every
    /// table a first start creates.
    pub async fn open(
        name: TableRef,
        url: Url,
        options: HashMap<String, String>,
    ) -> Result<Self, SinkError> {
        if let Ok(path) = url.to_file_path() {
            std::fs::create_dir_all(&path)
                .map_err(|e| permanent(format!("{}: {e}", path.display())))?;
        }
        let mut table = DeltaTableBuilder::from_url(url)
            .and_then(|b| b.with_storage_options(options.clone()).build())
            .map_err(sink_error)?;
        if table
            .log_store()
            .is_delta_table_location()
            .await
            .map_err(sink_error)?
        {
            match table.load().await {
                // Dropped since the LIST: absent, as if the LIST had found nothing.
                Ok(()) | Err(DeltaTableError::NotATable(_)) => {}
                Err(e) => return Err(sink_error(e)),
            }
        }
        let mut handle = Self {
            name,
            table,
            options,
            known: None,
            seen: None,
        };
        handle.see().await?;
        Ok(handle)
    }

    /// The store's description of the commit file at `version`; `None` when it's gone.
    async fn describe(&self, version: u64) -> Result<Option<Seen>, SinkError> {
        let store = self.table.log_store().object_store(None);
        let path = Path::from(format!("_delta_log/{version:020}.json"));
        match store.head(&path).await {
            Ok(meta) => Ok(Some(Seen { version, meta })),
            Err(deltalake::logstore::object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(sink_error(e.into())),
        }
    }

    /// Records the loaded version's commit file, when nothing is recorded yet.
    async fn see(&mut self) -> Result<(), SinkError> {
        if self.seen.is_none()
            && let Some(version) = self.table.version()
        {
            self.seen = self.describe(version).await?;
        }
        Ok(())
    }

    /// True while the recorded commit file is still the one the handle loaded: the log is the
    /// same log. One HEAD per call.
    async fn same_log(&self) -> Result<bool, SinkError> {
        match &self.seen {
            None => Ok(true),
            Some(seen) => Ok(self.describe(seen.version).await?.as_ref() == Some(seen)),
        }
    }

    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Replaces the table after a write returned its new state.
    pub fn set_table(&mut self, table: DeltaTable) {
        self.table = table;
    }

    /// Loads the newest version; `false` when the location holds no Delta table.
    ///
    /// A table dropped (and maybe re-created) behind the handle is another table: what the handle
    /// knew of the old one is forgotten, and it is re-opened from scratch. That shows as a
    /// recorded commit file the store now describes otherwise, or as a loaded snapshot that
    /// can't be brought forward (its log is gone). Brought forward over a re-created table's
    /// longer log, the old snapshot would hold the old table's files and miss the new one's
    /// (Q-M6b-8).
    pub async fn refresh(&mut self) -> Result<bool, SinkError> {
        let log = self.table.log_store();
        if !log.is_delta_table_location().await.map_err(sink_error)? {
            self.known = None;
            self.seen = None;
            return Ok(false);
        }
        if self.same_log().await? && self.table.update_incremental(None).await.is_ok() {
            self.see().await?;
            return Ok(true);
        }
        self.reopen().await
    }

    /// Re-opens the table from scratch (a fresh handle on the same URL and options, loaded) and
    /// forgets the known commit; `false` when the location holds no Delta table any more.
    async fn reopen(&mut self) -> Result<bool, SinkError> {
        let url = self.table.table_url().clone();
        *self = Self::open(self.name.clone(), url, self.options.clone()).await?;
        Ok(self.table.version().is_some())
    }

    /// The loaded version (0 before the first load).
    pub fn version(&self) -> u64 {
        self.table.version().unwrap_or(0)
    }

    /// An Iceberg table's metadata under this root (`metadata/…metadata.json`, or `meta/` as
    /// config §10 names it): the root holds another format, which the sink never mixes with.
    pub async fn foreign_format(&self) -> Result<Option<String>, SinkError> {
        let store = self.table.log_store().object_store(None);
        for dir in ["metadata", "meta"] {
            let found = store
                .list(Some(&Path::from(dir)))
                .try_filter(|m| std::future::ready(m.location.as_ref().ends_with(".metadata.json")))
                .try_next()
                .await
                .map_err(|e| sink_error(e.into()))?;
            if let Some(m) = found {
                return Ok(Some(format!(
                    "{} holds Iceberg metadata ({})",
                    self.table.table_url(),
                    m.location
                )));
            }
        }
        Ok(None)
    }

    /// Creates the table (Data-Flow §Delta Lake realization, item 5): column mapping off,
    /// deletion vectors off, the checkpoint interval and log retention from `defaults`, the
    /// passthrough properties, identity partitions. A table another writer created first is kept.
    pub async fn create(
        &mut self,
        spec: &TableSpec,
        defaults: &TableDefaults,
    ) -> Result<(), SinkError> {
        let columns = spec
            .schema
            .fields()
            .iter()
            .map(|f| delta_field(f, defaults.timestamp_ntz_as))
            .collect::<Result<Vec<_>, _>>()
            .map_err(permanent)?;
        let mut partitions = Vec::new();
        for p in &spec.partition_by {
            if p.transform != PartitionTransform::Identity {
                return Err(permanent(format!(
                    "Delta partitions by identity only, not {:?} (column {:?})",
                    p.transform, p.column
                )));
            }
            partitions.push(p.column.clone());
        }
        let mut config: Vec<(String, String)> = defaults
            .table_properties
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        config.extend([
            (
                "delta.checkpointInterval".to_string(),
                defaults.checkpoint_interval.to_string(),
            ),
            (
                "delta.logRetentionDuration".to_string(),
                format!("interval {} seconds", defaults.log_retention.as_secs()),
            ),
            (
                "delta.enableDeletionVectors".to_string(),
                "false".to_string(),
            ),
            (
                IDENTIFIER_KEY.to_string(),
                serde_json::to_string(&spec.identifier).expect("strings serialize"),
            ),
        ]);
        let created = self
            .table
            .clone()
            .create()
            .with_columns(columns)
            .with_partition_columns(partitions)
            .with_configuration(config.into_iter().map(|(k, v)| (k, Some(v))))
            .with_raise_if_key_not_exists(false)
            .with_table_name(format!("{}.{}", spec.table.schema, spec.table.name))
            .with_save_mode(SaveMode::Ignore)
            .await;
        match created {
            Ok(table) => {
                self.table = table;
                // A new table holds no own commit.
                self.known = None;
                self.seen = None;
                self.see().await
            }
            // Another writer created it between the check and the commit.
            Err(e) => match self.refresh().await {
                Ok(true) => {
                    self.known = None;
                    Ok(())
                }
                _ => Err(sink_error(e)),
            },
        }
    }

    /// The table's columns in Hop 2 terms (soft-dropped columns included).
    pub fn schema(&self) -> Result<SchemaRef, SinkError> {
        let snapshot = self.table.snapshot().map_err(sink_error)?;
        hop2_schema(snapshot.schema().as_ref()).map_err(permanent)
    }

    /// The identifier columns recorded at creation, empty for a table this sink didn't create.
    pub fn identifier(&self) -> Result<Vec<String>, SinkError> {
        let snapshot = self.table.snapshot().map_err(sink_error)?;
        match snapshot.metadata().configuration().get(IDENTIFIER_KEY) {
            Some(text) => serde_json::from_str(text)
                .map_err(|e| permanent(format!("table property {IDENTIFIER_KEY}: {e}"))),
            None => Ok(Vec::new()),
        }
    }

    /// The fence: the owner epoch of the latest commit of `pipeline_id` (its `txn` version).
    ///
    /// The read replays the loaded snapshot's log. When that fails, the table is re-opened and read
    /// once more: a table dropped and re-created behind the handle leaves a snapshot whose log is
    /// gone, which `update_incremental` doesn't notice when the new table has no newer version.
    pub async fn fence_epoch(&mut self, pipeline_id: &str) -> Result<Option<u64>, SinkError> {
        let version = match self.txn_version(pipeline_id).await {
            Ok(v) => v,
            Err(_) => {
                // Dropped since the refresh: no table, not a delta-rs `NotATable` (`Permanent`).
                if !self.reopen().await? {
                    return Err(SinkError::NoTable {
                        table: self.name.clone(),
                    });
                }
                self.txn_version(pipeline_id).await?
            }
        };
        version
            .map(|v| u64::try_from(v).map_err(|_| permanent(format!("txn version {v}"))))
            .transpose()
    }

    async fn txn_version(&self, pipeline_id: &str) -> Result<Option<i64>, SinkError> {
        let snapshot = self.table.snapshot().map_err(sink_error)?;
        snapshot
            .transaction_version(self.table.log_store().as_ref(), app_id(pipeline_id))
            .await
            .map_err(sink_error)
    }

    /// The position of the newest commit of `pipeline_id`, walking back past foreign commits.
    /// `None` when none is left in the log.
    pub async fn stored_position(
        &mut self,
        pipeline_id: &str,
    ) -> Result<Option<SourcePosition>, SinkError> {
        let latest = self.version();
        let table_id = self.table_id()?;
        // A commit known of another table (dropped and re-created under the same root), or of a
        // version this log doesn't reach, says nothing about this one: neither a stop nor a
        // fallback.
        if self
            .known
            .as_ref()
            .is_some_and(|k| k.table_id != table_id || k.version > latest)
        {
            self.known = None;
        }
        let stop = self.known.as_ref().map(|k| k.version);
        let log = self.table.log_store();
        match walk_back(log.as_ref(), latest, stop, pipeline_id)
            .await
            .map_err(permanent)?
        {
            WalkBack::Found { version, position } => {
                self.known = Some(Known {
                    table_id,
                    version,
                    position: position.clone(),
                });
                Ok(Some(position))
            }
            // A missing commit file ends the walk; an older known position is still a safe
            // guard (it skips less).
            WalkBack::Reached | WalkBack::NotFound => {
                Ok(self.known.as_ref().map(|k| k.position.clone()))
            }
        }
    }

    /// The loaded table's identity (`metaData.id`).
    fn table_id(&self) -> Result<String, SinkError> {
        let snapshot = self.table.snapshot().map_err(sink_error)?;
        Ok(snapshot.metadata().id().to_string())
    }

    /// Records an own commit this handle just wrote, to the table it holds. Without a loaded
    /// table nothing is recorded (the walk-back then reads further, which is always safe).
    pub fn remember(&mut self, version: u64, position: SourcePosition) {
        self.known = self.table_id().ok().map(|table_id| Known {
            table_id,
            version,
            position,
        });
    }

    /// Every row in Hop 2 terms, in identifier order: the Delta counterpart of `MemorySink::content`.
    pub async fn content(&self) -> Result<RecordBatch, SinkError> {
        let schema = self.schema()?;
        let ctx = SessionContext::new();
        self.table
            .update_datafusion_session(&ctx.state())
            .map_err(sink_error)?;
        let provider = self
            .table
            .table_provider()
            .await
            .map_err(|e| sink_error(e.into()))?;
        let batches = ctx
            .read_table(provider)
            .map_err(|e| sink_error(e.into()))?
            .collect()
            .await
            .map_err(|e| sink_error(e.into()))?;
        let snapshot = self.table.snapshot().map_err(sink_error)?;
        let delta_schema = snapshot.schema();
        let mut columns: Vec<ArrayRef> = Vec::new();
        for field in schema.fields() {
            let delta = delta_schema
                .field(field.name())
                .expect("the Hop 2 schema comes from this snapshot");
            let parts: Vec<ArrayRef> = batches
                .iter()
                .map(|b| {
                    b.column_by_name(field.name())
                        .cloned()
                        .ok_or_else(|| permanent(format!("the scan lacks {:?}", field.name())))
                })
                .collect::<Result<_, _>>()?;
            let logical = column_logical(delta).map_err(permanent)?;
            let physical = physical_type(delta.data_type()).map_err(permanent)?;
            let mut read = Vec::new();
            for p in &parts {
                read.push(arrow_cast::cast(p, &physical).map_err(|e| permanent(e.to_string()))?);
            }
            let joined = if read.is_empty() {
                arrow_array::new_empty_array(&physical)
            } else {
                let refs: Vec<&dyn arrow_array::Array> = read.iter().map(|a| a.as_ref()).collect();
                arrow_select::concat::concat(&refs).map_err(|e| permanent(e.to_string()))?
            };
            columns.push(from_delta(&joined, &logical, delta.data_type()).map_err(permanent)?);
        }
        let batch =
            RecordBatch::try_new(schema.clone(), columns).map_err(|e| permanent(e.to_string()))?;
        let identifier = self.identifier()?;
        if identifier.is_empty() || batch.num_rows() == 0 {
            return Ok(batch);
        }
        let keys: Vec<ArrayRef> = identifier
            .iter()
            .map(|k| {
                batch
                    .column_by_name(k)
                    .cloned()
                    .ok_or_else(|| permanent(format!("identifier column {k:?} is gone")))
            })
            .collect::<Result<_, _>>()?;
        let converter = RowConverter::new(
            keys.iter()
                .map(|k| SortField::new(k.data_type().clone()))
                .collect(),
        )
        .map_err(|e| permanent(e.to_string()))?;
        let rows = converter
            .convert_columns(&keys)
            .map_err(|e| permanent(e.to_string()))?;
        let mut order: Vec<u32> = (0..batch.num_rows() as u32).collect();
        order.sort_by(|a, b| rows.row(*a as usize).cmp(&rows.row(*b as usize)));
        let order = arrow_array::UInt32Array::from(order);
        let sorted = batch
            .columns()
            .iter()
            .map(|c| arrow_select::take::take(c, &order, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| permanent(e.to_string()))?;
        RecordBatch::try_new(Arc::clone(&schema), sorted).map_err(|e| permanent(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use data_avalanche_core::position::Lsn;
    use data_avalanche_core::{PartitionField, TableRef, upsert_schema};
    use deltalake::kernel::Transaction;
    use deltalake::kernel::transaction::CommitProperties;

    use super::*;
    use crate::NtzAs;
    use crate::marker::commit_metadata;
    use crate::test_support::{handle, orders, spec};

    fn t() -> TableRef {
        TableRef::new("public", "orders")
    }

    /// Physical rows of `orders` (uuid as text, `at` as timestamp_ntz), in the order given.
    fn rows(ids: &[i64]) -> RecordBatch {
        let utc = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let text = |n: &str, null| Field::new(n, DataType::Utf8, null);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            text("doc", true),
            text("ref", true),
            Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            text("_op", false),
            Field::new("_ts", utc(), false),
            text("_lsn", false),
            text("_seq", false),
            text("_source_db", false),
            text("_pipeline", false),
        ]));
        let n = ids.len();
        let s = |v: &str| Arc::new(StringArray::from(vec![v; n])) as ArrayRef;
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                s("doc"),
                s("00000000-0000-0000-0000-00000000000a"),
                Arc::new(TimestampMicrosecondArray::from(vec![0; n])),
                s("I"),
                Arc::new(TimestampMicrosecondArray::from(vec![0; n]).with_timezone("UTC")),
                s("0/1"),
                s("00000000/00000001"),
                s("h/db"),
                s("p"),
            ],
        )
        .unwrap()
    }

    /// Appends `ids` as one commit of pipeline `p` at `epoch`, positioned at LSN `lsn`.
    async fn own_append(h: &mut TableHandle, ids: &[i64], epoch: u64, lsn: u64) {
        let props = CommitProperties::default()
            .with_application_transaction(Transaction::new(app_id("p"), epoch as i64))
            .with_metadata(commit_metadata(
                "p",
                epoch,
                Some(&SourcePosition::Pg(Lsn(lsn))),
            ));
        let table = h
            .table()
            .clone()
            .write(vec![rows(ids)])
            .with_commit_properties(props)
            .await
            .unwrap();
        h.set_table(table);
    }

    async fn created(dir: &tempfile::TempDir, defaults: &TableDefaults) -> TableHandle {
        let mut h = handle(dir, &t()).await;
        assert!(!h.refresh().await.unwrap(), "no table yet");
        h.create(&spec(&orders()), defaults).await.unwrap();
        h
    }

    #[tokio::test]
    async fn a_created_table_reads_back_its_hop2_schema_and_settings() {
        let dir = tempfile::tempdir().unwrap();
        let defaults = TableDefaults {
            checkpoint_interval: 7,
            log_retention: Duration::from_secs(3600),
            timestamp_ntz_as: NtzAs::Timestamp,
            table_properties: [("owner".to_string(), "team-a".to_string())].into(),
        };
        let mut h = created(&dir, &defaults).await;
        assert_eq!(h.schema().unwrap(), upsert_schema(&orders()));
        assert_eq!(h.identifier().unwrap(), ["id"]);
        let snapshot = h.table().snapshot().unwrap();
        let config = snapshot.metadata().configuration();
        assert_eq!(config["delta.checkpointInterval"], "7");
        assert_eq!(
            config["delta.logRetentionDuration"],
            "interval 3600 seconds"
        );
        assert_eq!(config["delta.enableDeletionVectors"], "false");
        assert_eq!(config["owner"], "team-a");
        assert!(!config.contains_key("delta.columnMapping.mode"));
        assert_eq!(snapshot.metadata().name(), Some("public.orders"));
        let protocol = snapshot.protocol();
        assert_eq!(
            (protocol.min_reader_version(), protocol.min_writer_version()),
            (1, 2),
            "no timestamp_ntz column: the minimal protocol"
        );
        // A second create keeps the table.
        h.create(&spec(&orders()), &TableDefaults::default())
            .await
            .unwrap();
        assert_eq!(h.version(), 0);
        assert!(h.refresh().await.unwrap());
    }

    #[tokio::test]
    async fn timestamp_ntz_raises_the_protocol() {
        let dir = tempfile::tempdir().unwrap();
        let h = created(&dir, &TableDefaults::default()).await;
        let protocol = h.table().snapshot().unwrap().protocol().clone();
        assert_eq!(
            (protocol.min_reader_version(), protocol.min_writer_version()),
            (3, 7)
        );
    }

    #[tokio::test]
    async fn only_identity_partitions_are_created() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = handle(&dir, &t()).await;
        let mut s = spec(&orders());
        s.partition_by = vec![PartitionField {
            column: "at".into(),
            transform: data_avalanche_core::PartitionTransform::Day,
        }];
        let err = h.create(&s, &TableDefaults::default()).await.unwrap_err();
        assert!(err.to_string().contains("identity only"), "{err}");
        s.partition_by[0].transform = data_avalanche_core::PartitionTransform::Identity;
        s.partition_by[0].column = "doc".into();
        h.create(&s, &TableDefaults::default()).await.unwrap();
        let snapshot = h.table().snapshot().unwrap();
        assert_eq!(
            snapshot.metadata().partition_columns(),
            &["doc".to_string()]
        );
    }

    #[tokio::test]
    async fn iceberg_metadata_under_the_root_is_a_foreign_format() {
        let dir = tempfile::tempdir().unwrap();
        let h = handle(&dir, &t()).await;
        assert_eq!(h.foreign_format().await.unwrap(), None);
        let meta = dir.path().join("public/orders/metadata");
        std::fs::create_dir_all(&meta).unwrap();
        std::fs::write(meta.join("00001-abc.metadata.json"), "{}").unwrap();
        let found = h.foreign_format().await.unwrap().unwrap();
        assert!(found.contains("Iceberg"), "{found}");
    }

    /// The table is dropped after the handle's last refresh: the fence's read fails, the re-open
    /// finds nothing, and the table is missing (`NoTable`), not a `Permanent` delta-rs error.
    #[tokio::test]
    async fn a_table_dropped_since_the_refresh_is_no_table() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir, &TableDefaults::default()).await;
        own_append(&mut h, &[1], 4, 0x10).await;
        std::fs::remove_dir_all(dir.path().join("public/orders")).unwrap();
        let err = h.fence_epoch("p").await.unwrap_err();
        assert!(
            matches!(err, SinkError::NoTable { ref table } if table == &t()),
            "{err}"
        );
    }

    /// The handle notices a log that isn't the one it loaded, by the commit file it recorded.
    #[tokio::test]
    async fn a_refresh_reopens_a_table_whose_log_was_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir, &TableDefaults::default()).await;
        own_append(&mut h, &[1], 4, 0x10).await;
        assert!(h.refresh().await.unwrap());
        assert!(h.same_log().await.unwrap());
        std::fs::remove_dir_all(dir.path().join("public/orders")).unwrap();
        let mut other = created(&dir, &TableDefaults::default()).await;
        for id in 2..=4 {
            own_append(&mut other, &[id], 1, 0x100).await;
        }
        assert!(!h.same_log().await.unwrap());
        assert!(h.refresh().await.unwrap());
        assert_eq!(h.version(), other.version());
        assert!(h.same_log().await.unwrap());
    }

    #[tokio::test]
    async fn the_marker_is_the_txn_epoch_and_the_last_own_position() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir, &TableDefaults::default()).await;
        assert_eq!(h.fence_epoch("p").await.unwrap(), None);
        assert_eq!(h.stored_position("p").await.unwrap(), None);
        own_append(&mut h, &[1], 4, 0x10).await;
        own_append(&mut h, &[2], 5, 0x20).await;
        assert_eq!(h.fence_epoch("p").await.unwrap(), Some(5));
        assert_eq!(h.fence_epoch("other").await.unwrap(), None);
        assert_eq!(
            h.stored_position("p").await.unwrap(),
            Some(SourcePosition::Pg(Lsn(0x20)))
        );
        // Foreign commits (an append without a marker, an OPTIMIZE) don't hide the position or
        // the epoch.
        let table = h.table().clone().write(vec![rows(&[3])]).await.unwrap();
        let (table, _) = table.optimize().await.unwrap();
        h.set_table(table);
        let mut fresh = handle(&dir, &t()).await;
        assert!(fresh.refresh().await.unwrap());
        assert_eq!(fresh.version(), 4);
        assert_eq!(fresh.fence_epoch("p").await.unwrap(), Some(5));
        assert_eq!(
            fresh.stored_position("p").await.unwrap(),
            Some(SourcePosition::Pg(Lsn(0x20)))
        );
    }

    #[tokio::test]
    async fn content_is_hop2_rows_in_key_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = created(&dir, &TableDefaults::default()).await;
        assert_eq!(h.content().await.unwrap().num_rows(), 0);
        own_append(&mut h, &[3, 1], 1, 1).await;
        own_append(&mut h, &[2], 1, 2).await;
        let c = h.content().await.unwrap();
        assert_eq!(c.schema(), upsert_schema(&orders()));
        let ids: Vec<i64> = c.column(0).as_primitive::<Int64Type>().values().to_vec();
        assert_eq!(ids, [1, 2, 3]);
        let uuid = c.column(2).as_fixed_size_binary();
        assert_eq!(uuid.value(0)[15], 0x0a, "uuid text back to bytes");
    }
}
