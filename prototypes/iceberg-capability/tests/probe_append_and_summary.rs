//! Real probe (first runnable implementation — 2026-09-19, first recheck session
//! with a Rust toolchain available). Covers what the ONLY existing commit path
//! (append) can do on iceberg 0.10.1:
//!
//!   fast_append commits data files AND arbitrary snapshot-summary properties in
//!   one atomic metadata commit, and the values read back from the catalog.
//!
//! That is the carrier mechanism behind **DA-SINK-2** (`cdc.owner_epoch` /
//! `cdc.committed_position` in the snapshot summary) on the one commit path that
//! exists today. **G1 (DA-SINK-1) still cannot be implemented**: `Transaction`
//! exposes no row-delta / overwrite / rewrite action, so there is no way to commit
//! an equality-delete or position-delete file. See gap-analysis §24.2.4.
//!
//!   cargo test -p iceberg-capability --features probe -- --nocapture

#![cfg(feature = "probe")]

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, NestedField, PrimitiveType,
    Schema, Struct, Type,
};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

fn schema() -> Schema {
    let id = NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long));
    Schema::builder().with_fields(vec![Arc::new(id)]).build().unwrap()
}

fn data_file(table: &Table, file_no: i64) -> iceberg::spec::DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!(
            "{}/data/data-{file_no}.parquet",
            table.metadata().location()
        ))
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(100)
        .record_count(1)
        .partition_spec_id(table.metadata().default_partition_spec_id())
        .partition(Struct::from_iter([]))
        .build()
        .unwrap()
}

async fn append_with_props(
    catalog: &dyn Catalog,
    table: &Table,
    props: HashMap<String, String>,
    file_no: i64,
) -> Table {
    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files([data_file(table, file_no)]);
    let action = action.set_snapshot_properties(props);
    let tx = action.apply(tx).expect("apply fast append");
    tx.commit(catalog).await.expect("append commit")
}

#[tokio::test]
async fn g2_fast_append_carries_cdc_summary_properties_across_commits() {
    // MemoryCatalog is fully in-memory (MemoryStorageFactory default); the
    // warehouse string is just a location prefix.
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

    let creation = TableCreation {
        name: "t".to_string(),
        location: None,
        schema: schema(),
        partition_spec: None,
        sort_order: None,
        properties: HashMap::new(),
        format_version: FormatVersion::V2,
    };
    let table = catalog.create_table(&ns, creation).await.expect("create table");
    let ident = TableIdent::new(ns, "t".to_string());

    // Commit 1: append a data file + the fencing/guard summary properties in one
    // atomic `Transaction`.
    let table = append_with_props(
        &catalog,
        &table,
        HashMap::from([
            ("cdc.owner_epoch".to_string(), "5".to_string()),
            ("cdc.committed_position".to_string(), "0/1A2B3C4D".to_string()),
        ]),
        1,
    )
    .await;

    let snapshot = table.metadata().current_snapshot().expect("snapshot after C1");
    let props = &snapshot.summary().additional_properties;
    assert_eq!(props.get("cdc.owner_epoch").map(String::as_str), Some("5"));
    assert_eq!(
        props.get("cdc.committed_position").map(String::as_str),
        Some("0/1A2B3C4D")
    );

    // Reload from the catalog: the properties must survive (they are serialized
    // in the committed snapshot summary).
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    let snapshot = reloaded.metadata().current_snapshot().expect("snapshot on reload");
    let props = &snapshot.summary().additional_properties;
    assert_eq!(props.get("cdc.owner_epoch").map(String::as_str), Some("5"));

    // Commit 2: re-assert a monotonic fence value — the design's read-latest ->
    // assert -> write loop. Summaries are per-snapshot and independent, so the
    // writer re-asserts the running value on every commit.
    let table = append_with_props(
        &catalog,
        &reloaded,
        HashMap::from([("cdc.owner_epoch".to_string(), "6".to_string())]),
        2,
    )
    .await;

    let snapshot = table.metadata().current_snapshot().expect("snapshot after C2");
    let props = &snapshot.summary().additional_properties;
    assert_eq!(
        props.get("cdc.owner_epoch").map(String::as_str),
        Some("6"),
        "the latest commit's summary must carry the re-asserted value"
    );

    // Two-snapshot history: C1 still carries epoch 5, C2 carries 6 — the
    // monotonically-incremented-epoch fence is answerable from summary history.
    let epochs: HashMap<i64, Option<&str>> = table
        .metadata()
        .snapshots()
        .map(|s| {
            (
                s.snapshot_id(),
                s.summary().additional_properties.get("cdc.owner_epoch").map(String::as_str),
            )
        })
        .collect();
    assert_eq!(epochs.len(), 2, "two snapshots retained: {epochs:?}");
    assert!(
        epochs.values().any(|v| *v == Some("5")),
        "C1 snapshot must have kept epoch 5: {epochs:?}"
    );
    assert!(
        epochs.values().any(|v| *v == Some("6")),
        "C2 snapshot must carry epoch 6: {epochs:?}"
    );
}