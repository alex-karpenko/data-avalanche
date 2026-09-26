//! G2b — the DA-X-10 Iceberg fence carrier (2026-09-26).
//!
//! Design under test (`Clustering_Architecture_Research.md` §5.5, `Data-Flow.md` Phase 4 Iceberg
//! item 5): every sink data commit writes `cdc.owner_epoch` **twice in one transaction** — in the
//! snapshot summary (per-snapshot audit) and as a **table property** (authoritative; survives
//! foreign commits and snapshot expiry). The commit path rejects a commit when the recorded epoch is
//! higher than its own.
//!
//! Probes, against iceberg 0.10.1 + MemoryCatalog:
//!   1. `update_table_properties` + `fast_append` in one `Transaction` = ONE metadata commit.
//!   2. The property survives a foreign append, a foreign property edit and `expire_snapshots` of
//!      our own snapshot (where the summary walk-back finds nothing).
//!   3. FINDING: `Transaction::commit` reloads the table and re-applies its actions on **every**
//!      attempt (not only on retry), so a zombie holding a stale `Table` is **rebased, not
//!      rejected** — it lands and regresses the property. The metadata-pointer CAS never sees a
//!      stale base.
//!   4. Fix, on the public API only: a per-commit `Catalog` wrapper whose `load_table` returns the
//!      base the fence was checked against. `fast_append`'s `RefSnapshotIdMatch(main)` then makes
//!      the commit a true CAS: a zombie is rejected with NEITHER the property NOR the snapshot
//!      applied; foreign metadata-only edits are not clobbered.
//!
//!   cargo test -p iceberg-capability --features probe --test probe_epoch_property -- --nocapture

#![cfg(feature = "probe")]

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, NestedField, PrimitiveType,
    Schema, Struct, Type,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit,
    TableCreation, TableIdent,
};

const EPOCH: &str = "cdc.owner_epoch";
const PIPELINE: &str = "cdc.pipeline";
const POSITION: &str = "cdc.committed_position";
const ME: &str = "orders-pg";

// ---------------------------------------------------------------------------------------------
// fixtures

async fn setup() -> (Arc<dyn Catalog>, TableIdent) {
    let catalog = MemoryCatalogBuilder::default()
        .load(
            "probe",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                "memory://probe/warehouse".to_string(),
            )]),
        )
        .await
        .unwrap();
    let ns = NamespaceIdent::new("db".to_string());
    catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    let id = NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long));
    let creation = TableCreation {
        name: "t".to_string(),
        location: None,
        schema: Schema::builder().with_fields(vec![Arc::new(id)]).build().unwrap(),
        partition_spec: None,
        sort_order: None,
        properties: HashMap::new(),
        format_version: FormatVersion::V2,
    };
    catalog.create_table(&ns, creation).await.unwrap();
    (Arc::new(catalog), TableIdent::new(ns, "t".to_string()))
}

fn data_file(table: &Table, file_no: i64) -> iceberg::spec::DataFile {
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
async fn sink_commit(
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

/// A foreign writer (another engine / external maintenance): no `cdc.*` markers.
async fn foreign_append(catalog: &dyn Catalog, ident: &TableIdent, file_no: i64) -> Table {
    let t = catalog.load_table(ident).await.unwrap();
    let tx = Transaction::new(&t);
    let a = tx.fast_append().add_data_files([data_file(&t, file_no)]);
    a.apply(tx).unwrap().commit(catalog).await.unwrap()
}

async fn foreign_set_property(catalog: &dyn Catalog, ident: &TableIdent, k: &str, v: &str) {
    let t = catalog.load_table(ident).await.unwrap();
    let tx = Transaction::new(&t);
    let a = tx.update_table_properties().set(k.to_string(), v.to_string());
    a.apply(tx).unwrap().commit(catalog).await.unwrap();
}

fn prop<'a>(t: &'a Table, k: &str) -> Option<&'a str> {
    t.metadata().properties().get(k).map(String::as_str)
}

fn summary<'a>(t: &'a Table, k: &str) -> Option<&'a str> {
    let s = t.metadata().current_snapshot()?;
    s.summary().additional_properties.get(k).map(String::as_str)
}

/// Number of metadata commits so far (MemoryCatalog logs the previous file on each commit).
fn commits(t: &Table) -> usize {
    t.metadata().metadata_log().len()
}

/// DA-X-10 fallback: newest retained snapshot in main's lineage authored by this pipeline.
fn walk_back_epoch(t: &Table) -> Option<String> {
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
struct PinnedBase {
    inner: Arc<dyn Catalog>,
    base: Table,
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
async fn fenced_commit(
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

// ---------------------------------------------------------------------------------------------
// probes

#[tokio::test]
async fn g2b_1_property_and_append_are_one_metadata_commit() {
    let (cat, ident) = setup().await;
    let t0 = cat.load_table(&ident).await.unwrap();

    // properties action first, append second
    let t1 = sink_commit(cat.as_ref(), &t0, 5, 1, true).await.unwrap();
    let r1 = cat.load_table(&ident).await.unwrap();
    assert_eq!(commits(&r1), commits(&t0) + 1, "exactly one metadata commit");
    assert_eq!(r1.metadata().snapshots().len(), 1);
    assert_eq!(prop(&r1, EPOCH), Some("5"), "table property landed");
    assert_eq!(summary(&r1, EPOCH), Some("5"), "summary landed in the same commit");
    assert_eq!(summary(&r1, PIPELINE), Some(ME));
    assert_eq!(r1.metadata_location(), t1.metadata_location());

    // reversed action order: still one commit
    sink_commit(cat.as_ref(), &r1, 6, 2, false).await.unwrap();
    let r2 = cat.load_table(&ident).await.unwrap();
    assert_eq!(commits(&r2), commits(&r1) + 1, "exactly one metadata commit (reversed order)");
    assert_eq!(prop(&r2, EPOCH), Some("6"));
    assert_eq!(summary(&r2, EPOCH), Some("6"));
    println!(
        "G2b-1 ✅ property + append = 1 metadata commit each (log {} → {} → {})",
        commits(&t0),
        commits(&r1),
        commits(&r2)
    );
}

#[tokio::test]
async fn g2b_2_property_survives_foreign_commits_and_expiry() {
    let (cat, ident) = setup().await;
    let t0 = cat.load_table(&ident).await.unwrap();
    let own = sink_commit(cat.as_ref(), &t0, 5, 1, true).await.unwrap();
    let own_snap = own.metadata().current_snapshot_id().unwrap();

    foreign_append(cat.as_ref(), &ident, 100).await; // e.g. another engine / compaction
    foreign_set_property(cat.as_ref(), &ident, "write.target-file-size-bytes", "134217728").await;

    let t = cat.load_table(&ident).await.unwrap();
    assert_eq!(summary(&t, EPOCH), None, "latest snapshot is foreign: no marker (DA-X-10)");
    assert_eq!(walk_back_epoch(&t).as_deref(), Some("5"), "walk-back still finds ours");

    // external expire_snapshots removes OUR snapshot
    let tx = Transaction::new(&t);
    let a = tx.expire_snapshots().expire_snapshot_ids([own_snap]);
    a.apply(tx).unwrap().commit(cat.as_ref()).await.unwrap();

    let t = cat.load_table(&ident).await.unwrap();
    assert!(t.metadata().snapshot_by_id(own_snap).is_none(), "own snapshot expired");
    assert_eq!(walk_back_epoch(&t), None, "summary walk-back finds nothing any more");
    assert_eq!(prop(&t, EPOCH), Some("5"), "table property survives all three");
    assert_eq!(prop(&t, "write.target-file-size-bytes"), Some("134217728"));
    println!("G2b-2 ✅ cdc.owner_epoch property survives foreign append, foreign property edit, expiry of own snapshot");
}

#[tokio::test]
async fn g2b_3_finding_transaction_commit_rebases_a_zombie() {
    let (cat, ident) = setup().await;
    let t0 = cat.load_table(&ident).await.unwrap();
    // owner A (epoch 5) commits; A then stalls holding this Table (its last view)
    let zombie_view = sink_commit(cat.as_ref(), &t0, 5, 1, true).await.unwrap();
    // A's fence pre-check passes on its (stale) view
    assert_eq!(prop(&zombie_view, EPOCH), Some("5"));

    // owner B (epoch 6) takes over and commits
    let b_base = cat.load_table(&ident).await.unwrap();
    let b = sink_commit(cat.as_ref(), &b_base, 6, 2, true).await.unwrap();
    let b_snap = b.metadata().current_snapshot_id().unwrap();

    // zombie A wakes and commits on its stale view through plain Transaction::commit
    let before = commits(&cat.load_table(&ident).await.unwrap());
    let res = sink_commit(cat.as_ref(), &zombie_view, 5, 3, true).await;
    let after = cat.load_table(&ident).await.unwrap();

    assert!(res.is_ok(), "zombie was expected to be rebased, got {res:?}");
    assert_eq!(commits(&after), before + 1, "zombie commit landed");
    assert_eq!(
        after.metadata().current_snapshot().unwrap().parent_snapshot_id(),
        Some(b_snap),
        "zombie's snapshot was re-parented onto the new owner's"
    );
    assert_eq!(prop(&after, EPOCH), Some("5"), "fence property REGRESSED 6 → 5");
    println!(
        "G2b-3 ⚠️ FINDING: Transaction::commit reloads + re-applies on every attempt; a stale zombie \
         is rebased onto the new owner's snapshot and lands (property regressed 6 → 5). \
         The metadata-pointer CAS never sees a stale base."
    );
}

#[tokio::test]
async fn g2b_4_pinned_base_makes_the_commit_a_cas() {
    let (cat, ident) = setup().await;
    let t0 = cat.load_table(&ident).await.unwrap();

    // (a) zombie on a stale base is rejected; nothing applied
    let zombie_view = fenced_commit(&cat, &t0, 5, 1).await.unwrap();
    let b_base = cat.load_table(&ident).await.unwrap();
    fenced_commit(&cat, &b_base, 6, 2).await.unwrap();
    let before = cat.load_table(&ident).await.unwrap();

    let res = fenced_commit(&cat, &zombie_view, 5, 3).await;
    let after = cat.load_table(&ident).await.unwrap();
    let err = res.expect_err("zombie on a stale base must be rejected");
    assert!(err.starts_with("CatalogCommitConflicts"), "{err}");
    assert_eq!(commits(&after), commits(&before), "no metadata commit");
    assert_eq!(after.metadata_location(), before.metadata_location());
    assert_eq!(prop(&after, EPOCH), Some("6"), "property NOT applied");
    assert_eq!(
        after.metadata().current_snapshot_id(),
        before.metadata().current_snapshot_id(),
        "snapshot NOT applied"
    );
    // the zombie's own retry: reload → fence check refuses before committing
    let reload = fenced_commit(&cat, &after, 5, 3).await.expect_err("reloaded zombie");
    assert!(reload.starts_with("FENCED"), "{reload}");
    println!("G2b-4a ✅ stale zombie rejected ({err}); property and snapshot both unapplied; reloaded zombie: {reload}");

    // (b) a foreign APPEND between the owner's read and commit → conflict; the sink's loop
    //     reloads, re-checks, re-pins and succeeds
    let base = cat.load_table(&ident).await.unwrap();
    foreign_append(cat.as_ref(), &ident, 100).await;
    let err = fenced_commit(&cat, &base, 6, 4).await.expect_err("main moved under us");
    let base = cat.load_table(&ident).await.unwrap();
    let ok = fenced_commit(&cat, &base, 6, 4).await.unwrap();
    assert_eq!(prop(&ok, EPOCH), Some("6"));
    println!("G2b-4b ✅ foreign append under the pin → {err}; reload + re-pin commits");

    // (c) a foreign METADATA-ONLY edit between read and commit does not move main: the commit
    //     lands and does not clobber it (updates apply to the catalog's current metadata)
    let base = cat.load_table(&ident).await.unwrap();
    foreign_set_property(cat.as_ref(), &ident, "foreign.maint", "x").await;
    let ok = fenced_commit(&cat, &base, 6, 5).await.unwrap();
    let t = cat.load_table(&ident).await.unwrap();
    assert_eq!(t.metadata_location(), ok.metadata_location());
    assert_eq!(prop(&t, "foreign.maint"), Some("x"), "foreign property preserved");
    assert_eq!(prop(&t, EPOCH), Some("6"));
    println!("G2b-4c ✅ foreign metadata-only edit under the pin: commit lands, foreign property preserved");
}
