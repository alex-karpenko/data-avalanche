//! Shared pieces of the G2b probes: the sink's marked commit, marker readers and the pinned-base
//! catalog view (see `probe_epoch_property.rs` for the rationale).

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};

pub const EPOCH: &str = "cdc.owner_epoch";
pub const PIPELINE: &str = "cdc.pipeline";
pub const POSITION: &str = "cdc.committed_position";
pub const ME: &str = "orders-pg";

pub fn data_file(table: &Table, file_no: i64) -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("{}/data/data-{file_no}.parquet", table.metadata().location()))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .partition(Struct::from_iter([]))
        .build()
        .unwrap()
}

/// The sink's data commit: table property + append with summary markers, ONE transaction.
/// `props_first` flips the action order to show the order does not matter.
pub async fn sink_commit(
    catalog: &dyn Catalog,
    base: &Table,
    epoch: u64,
    file_no: i64,
    props_first: bool,
) -> Result<Table> {
    let summary = HashMap::from([
        (EPOCH.to_string(), epoch.to_string()),
        (PIPELINE.to_string(), ME.to_string()),
        (POSITION.to_string(), format!("0/{file_no:08X}")),
    ]);
    let tx = Transaction::new(base);
    let set_prop = |tx: Transaction| {
        let a = tx.update_table_properties().set(EPOCH.to_string(), epoch.to_string());
        a.apply(tx)
    };
    let append = |tx: Transaction| {
        let a = tx
            .fast_append()
            .add_data_files([data_file(base, file_no)])
            .set_snapshot_properties(summary.clone());
        a.apply(tx)
    };
    let tx = if props_first {
        append(set_prop(tx)?)?
    } else {
        set_prop(append(tx)?)?
    };
    tx.commit(catalog).await
}

pub fn prop<'a>(t: &'a Table, k: &str) -> Option<&'a str> {
    t.metadata().properties().get(k).map(String::as_str)
}

pub fn summary<'a>(t: &'a Table, k: &str) -> Option<&'a str> {
    let s = t.metadata().current_snapshot()?;
    s.summary().additional_properties.get(k).map(String::as_str)
}

/// Number of metadata commits so far (MemoryCatalog logs the previous file on each commit).
pub fn commits(t: &Table) -> usize {
    t.metadata().metadata_log().len()
}

/// DA-X-10 fallback: newest retained snapshot in main's lineage authored by this pipeline.
pub fn walk_back_epoch(t: &Table) -> Option<String> {
    let md = t.metadata();
    let mut cur = md.current_snapshot().cloned();
    while let Some(s) = cur {
        let p = &s.summary().additional_properties;
        if p.get(PIPELINE).map(String::as_str) == Some(ME) {
            return p.get(EPOCH).cloned();
        }
        cur = s.parent_snapshot_id().and_then(|id| md.snapshot_by_id(id).cloned());
    }
    None
}

// ---------------------------------------------------------------------------------------------
// the pinned-base wrapper (probe 4)

/// Per-commit catalog view. `load_table` returns the base the sink checked its fence against, so
/// `Transaction::commit` never rebases; `fast_append` then asserts `main == base.main` and the
/// underlying catalog's requirement check turns the commit into a compare-and-swap. A conflict is
/// surfaced as non-retryable: the SINK owns the retry (reload → re-check fence → re-pin).
#[derive(Debug)]
pub struct PinnedBase {
    pub inner: Arc<dyn Catalog>,
    pub base: Table,
}

#[async_trait]
impl Catalog for PinnedBase {
    async fn load_table(&self, ident: &TableIdent) -> Result<Table> {
        if ident == self.base.identifier() {
            Ok(self.base.clone())
        } else {
            self.inner.load_table(ident).await
        }
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(commit).await.map_err(|e| {
            if e.kind() == ErrorKind::CatalogCommitConflicts {
                // fresh Error: retryable = false, so Transaction::commit does not loop
                Error::new(ErrorKind::CatalogCommitConflicts, format!("base moved: {e}"))
            } else {
                e
            }
        })
    }

    async fn list_namespaces(&self, p: Option<&NamespaceIdent>) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(p).await
    }
    async fn create_namespace(
        &self,
        ns: &NamespaceIdent,
        props: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(ns, props).await
    }
    async fn get_namespace(&self, ns: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(ns).await
    }
    async fn namespace_exists(&self, ns: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(ns).await
    }
    async fn update_namespace(
        &self,
        ns: &NamespaceIdent,
        props: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(ns, props).await
    }
    async fn drop_namespace(&self, ns: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(ns).await
    }
    async fn list_tables(&self, ns: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(ns).await
    }
    async fn create_table(&self, ns: &NamespaceIdent, c: TableCreation) -> Result<Table> {
        self.inner.create_table(ns, c).await
    }
    async fn drop_table(&self, t: &TableIdent) -> Result<()> {
        self.inner.drop_table(t).await
    }
    async fn purge_table(&self, t: &TableIdent) -> Result<()> {
        self.inner.purge_table(t).await
    }
    async fn table_exists(&self, t: &TableIdent) -> Result<bool> {
        self.inner.table_exists(t).await
    }
    async fn rename_table(&self, s: &TableIdent, d: &TableIdent) -> Result<()> {
        self.inner.rename_table(s, d).await
    }
    async fn register_table(&self, t: &TableIdent, loc: String) -> Result<Table> {
        self.inner.register_table(t, loc).await
    }
}

/// The sink's fenced commit: check the fence on `base`, then commit pinned to `base`.
pub async fn fenced_commit(
    catalog: &Arc<dyn Catalog>,
    base: &Table,
    my_epoch: u64,
    file_no: i64,
) -> std::result::Result<Table, String> {
    let recorded = prop(base, EPOCH)
        .map(str::to_string)
        .or_else(|| walk_back_epoch(base))
        .map(|v| v.parse::<u64>().unwrap());
    if let Some(r) = recorded {
        if r > my_epoch {
            return Err(format!("FENCED: recorded epoch {r} > mine {my_epoch}"));
        }
    }
    let pinned = PinnedBase { inner: Arc::clone(catalog), base: base.clone() };
    sink_commit(&pinned, base, my_epoch, file_no, true)
        .await
        .map_err(|e| format!("{:?}: {e}", e.kind()))
}

