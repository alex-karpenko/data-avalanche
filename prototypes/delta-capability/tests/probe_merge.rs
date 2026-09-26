//! Delta sink capability probes (research `Multi_Sink_Architecture_and_Delta_Lake.md` §10).
//!
//! Every probe runs against a fresh `file://` table in a temp dir. The sink realization
//! under test is §7.2: ONE MERGE per flush whose source is the collapsed change set
//! (upserts ∪ deletes) with a boolean `__avalanche_del` flag, carrying the owner-epoch
//! fence as a `txn` app-transaction and the committed position as `commitInfo` metadata.
//!
//!   cargo test -p delta-capability --features probe -- --nocapture --test-threads=1

#![cfg(feature = "probe")]

use std::ops::Not;
use std::sync::Arc;
use std::time::Instant;

use deltalake::arrow::array::{
    Array, BooleanArray, Int64Array, RecordBatch, StringArray,
};
use deltalake::arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema};
use deltalake::datafusion::prelude::{col, SessionContext};
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::{DataType, StructField, Transaction};
use deltalake::operations::merge::MergeMetrics;
use deltalake::{DeltaTable, TableProperty};
use futures::TryStreamExt;
use url::Url;

const OWNER_APP_ID: &str = "avalanche/orders/owner";
const POS_KEY: &str = "avalanche.committed_position";

// ---------------------------------------------------------------- helpers

fn table_url(dir: &tempfile::TempDir, name: &str) -> Url {
    let p = dir.path().join(name);
    std::fs::create_dir_all(&p).unwrap();
    Url::from_directory_path(p).unwrap()
}

async fn create_table(url: &Url, props: Vec<(TableProperty, &str)>) -> DeltaTable {
    let mut b = DeltaTable::try_from_url(url.clone())
        .await
        .unwrap()
        .create()
        .with_columns([
            StructField::new("id", DataType::LONG, false),
            StructField::new("val", DataType::STRING, true),
            StructField::new("_op", DataType::STRING, true),
            StructField::new("_seq", DataType::LONG, true),
        ]);
    for (k, v) in props {
        b = b.with_configuration_property(k, Some(v));
    }
    b.await.unwrap()
}

fn data_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int64, false),
        Field::new("val", ArrowType::Utf8, true),
        Field::new("_op", ArrowType::Utf8, true),
        Field::new("_seq", ArrowType::Int64, true),
    ]))
}

/// Plain append (initial load helper; not the CDC path).
async fn append(table: DeltaTable, ids: &[i64], val: &str) -> DeltaTable {
    let batch = RecordBatch::try_new(
        data_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec![val; ids.len()])),
            Arc::new(StringArray::from(vec!["r"; ids.len()])),
            Arc::new(Int64Array::from(ids.to_vec())),
        ],
    )
    .unwrap();
    table.write(vec![batch]).await.unwrap()
}

/// One collapsed change set: upserts (id, val) and deletes (id).
#[derive(Clone)]
struct ChangeSet {
    upserts: Vec<(i64, String)>,
    deletes: Vec<i64>,
    seq_base: i64,
}

fn merge_source(cs: &ChangeSet) -> RecordBatch {
    let mut ids = vec![];
    let mut vals: Vec<Option<String>> = vec![];
    let mut ops: Vec<Option<&str>> = vec![];
    let mut seqs = vec![];
    let mut dels = vec![];
    for (i, (id, v)) in cs.upserts.iter().enumerate() {
        ids.push(*id);
        vals.push(Some(v.clone()));
        ops.push(Some("U"));
        seqs.push(cs.seq_base + i as i64);
        dels.push(false);
    }
    for (i, id) in cs.deletes.iter().enumerate() {
        ids.push(*id);
        vals.push(None);
        ops.push(None);
        seqs.push(cs.seq_base + (cs.upserts.len() + i) as i64);
        dels.push(true);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int64, false),
        Field::new("val", ArrowType::Utf8, true),
        Field::new("_op", ArrowType::Utf8, true),
        Field::new("_seq", ArrowType::Int64, true),
        Field::new("__avalanche_del", ArrowType::Boolean, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(vals)),
            Arc::new(StringArray::from(ops)),
            Arc::new(Int64Array::from(seqs)),
            Arc::new(BooleanArray::from(dels)),
        ],
    )
    .unwrap()
}

/// The §7.2 Delta sink commit: fence pre-check, then one MERGE carrying txn + commitInfo.
async fn sink_commit(
    table: DeltaTable,
    cs: &ChangeSet,
    epoch: i64,
    position: &str,
) -> anyhow::Result<(DeltaTable, MergeMetrics)> {
    // Fence pre-check (§7.2 step 5).
    if let Some(latest) = owner_epoch(&table).await {
        anyhow::ensure!(latest <= epoch, "FENCED: table epoch {latest} > my epoch {epoch}");
    }
    let ctx = SessionContext::new();
    let df = ctx.read_batch(merge_source(cs))?;
    let props = CommitProperties::default()
        .with_application_transaction(Transaction::new(OWNER_APP_ID, epoch))
        .with_metadata([
            (POS_KEY.to_string(), serde_json_str(position)),
            ("avalanche.owner_epoch".to_string(), serde_json_str(&epoch.to_string())),
        ]);
    let (table, metrics) = table
        .merge(df, col("target.id").eq(col("source.id")))
        .with_source_alias("source")
        .with_target_alias("target")
        .with_commit_properties(props)
        .when_matched_delete(|d| d.predicate(col("source.__avalanche_del")))?
        .when_matched_update(|u| {
            u.predicate(col("source.__avalanche_del").not())
                .update("val", col("source.val"))
                .update("_op", col("source._op"))
                .update("_seq", col("source._seq"))
        })?
        .when_not_matched_insert(|i| {
            i.predicate(col("source.__avalanche_del").not())
                .set("id", col("source.id"))
                .set("val", col("source.val"))
                .set("_op", col("source._op"))
                .set("_seq", col("source._seq"))
        })?
        .await?;
    Ok((table, metrics))
}

fn serde_json_str(s: &str) -> serde_json::Value {
    serde_json::Value::String(s.to_string())
}

async fn owner_epoch(table: &DeltaTable) -> Option<i64> {
    let snap = table.snapshot().ok()?;
    snap.transaction_version(table.log_store().as_ref(), OWNER_APP_ID)
        .await
        .unwrap()
}

/// Walk back through history to the latest commit authored by us (has POS_KEY).
async fn own_committed_position(table: &DeltaTable) -> Option<String> {
    let hist: Vec<_> = table.history(None).try_collect().await.unwrap();
    hist.iter()
        .find_map(|ci| ci.info.get(POS_KEY).and_then(|v| v.as_str().map(str::to_string)))
}

async fn contents(table: &DeltaTable) -> Vec<(i64, Option<String>)> {
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state()).unwrap();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    let batches = ctx
        .sql("SELECT id, val FROM t ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut out = vec![];
    for b in batches {
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        // DataFusion 55 scans strings as Utf8View; normalize to Utf8 for comparison.
        let vals = deltalake::arrow::compute::cast(b.column(1), &ArrowType::Utf8).unwrap();
        let vals = vals.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), (!vals.is_null(i)).then(|| vals.value(i).to_string())));
        }
    }
    out
}

async fn reopen(url: &Url) -> DeltaTable {
    deltalake::open_table(url.clone()).await.unwrap()
}

fn cs1() -> ChangeSet {
    ChangeSet {
        upserts: vec![(1, "one-v2".into()), (4, "four".into())],
        deletes: vec![2],
        seq_base: 100,
    }
}

// ---------------------------------------------------------------- probes

/// P1 — delete + update + insert land in ONE new table version.
#[tokio::test]
async fn p1_merge_upsert_and_delete_in_one_commit() {
    let dir = tempfile::tempdir().unwrap();
    let url = table_url(&dir, "p1");
    let t = append(create_table(&url, vec![]).await, &[1, 2, 3], "v1").await;
    let v_before = t.version().unwrap();
    let (t, m) = sink_commit(t, &cs1(), 1, "pg:0/10").await.unwrap();
    println!("P1 metrics: {m:?}");
    assert_eq!(t.version().unwrap(), v_before + 1, "exactly one new version");
    assert_eq!(
        contents(&t).await,
        vec![
            (1, Some("one-v2".into())),
            (3, Some("v1".into())),
            (4, Some("four".into()))
        ]
    );
    assert_eq!((m.num_target_rows_updated, m.num_target_rows_deleted, m.num_target_rows_inserted), (1, 1, 1));
}

/// P2 — replaying the same change set is a no-op on content (idempotent).
#[tokio::test]
async fn p2_replay_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let url = table_url(&dir, "p2");
    let t = append(create_table(&url, vec![]).await, &[1, 2, 3], "v1").await;
    let (t, _) = sink_commit(t, &cs1(), 1, "pg:0/10").await.unwrap();
    let once = contents(&t).await;
    let (t, m) = sink_commit(t, &cs1(), 1, "pg:0/10").await.unwrap();
    println!("P2 replay metrics: {m:?}");
    assert_eq!(contents(&t).await, once);
}

/// P3 — txn + commitInfo written atomically, survive reload and a foreign OPTIMIZE.
#[tokio::test]
async fn p3_marker_survives_reload_and_foreign_optimize() {
    let dir = tempfile::tempdir().unwrap();
    let url = table_url(&dir, "p3");
    let mut t = create_table(&url, vec![]).await;
    for k in 0..5 {
        t = append(t, &[10 * k, 10 * k + 1], "small").await; // many small files
    }
    let (t, _) = sink_commit(t, &cs1(), 7, "pg:0/2A").await.unwrap();
    drop(t);

    let t = reopen(&url).await;
    assert_eq!(owner_epoch(&t).await, Some(7));
    assert_eq!(own_committed_position(&t).await.as_deref(), Some("pg:0/2A"));

    // Foreign commit: compaction by "someone else" (no avalanche metadata).
    let (_t, om) = t.optimize().await.unwrap();
    println!("P3 optimize: files added {} removed {}", om.num_files_added, om.num_files_removed);
    let t = reopen(&url).await;
    let hist: Vec<_> = t.history(Some(1)).try_collect().await.unwrap();
    println!("P3 latest op after optimize: {:?}", hist[0].operation);
    assert!(hist[0].info.get(POS_KEY).is_none(), "latest commit is foreign");
    assert_eq!(owner_epoch(&t).await, Some(7), "txn survives foreign commit");
    assert_eq!(
        own_committed_position(&t).await.as_deref(),
        Some("pg:0/2A"),
        "walk-back finds our last position"
    );
    drop(t);
}

/// P4 — fence: a zombie (epoch 5, stale snapshot) must not land after the new owner (epoch 6).
#[tokio::test]
async fn p4_zombie_writer_is_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let url = table_url(&dir, "p4");
    let t = append(create_table(&url, vec![]).await, &[1, 2, 3], "v1").await;
    let (_t, _) = sink_commit(t, &cs1(), 5, "pg:0/1").await.unwrap();

    let zombie = reopen(&url).await; // loaded while epoch 5 is current
    let owner = reopen(&url).await;
    let new_cs = ChangeSet { upserts: vec![(9, "owner".into())], deletes: vec![], seq_base: 200 };
    let (owner, _) = sink_commit(owner, &new_cs, 6, "pg:0/2").await.unwrap();

    // (a) zombie with a stale snapshot: its pre-check passes (sees 5), the commit must conflict.
    let zombie_cs = ChangeSet { upserts: vec![(3, "zombie".into())], deletes: vec![], seq_base: 150 };
    let r = sink_commit(zombie, &zombie_cs, 5, "pg:0/1b").await;
    println!("P4 stale-zombie result: {:?}", r.as_ref().err().map(|e| e.to_string()));
    assert!(r.is_err(), "stale zombie commit must be rejected");

    // (b) zombie that reloads first: the pre-check fences it.
    let zombie = reopen(&url).await;
    let r = sink_commit(zombie, &zombie_cs, 5, "pg:0/1b").await;
    println!("P4 reloaded-zombie result: {:?}", r.as_ref().err().map(|e| e.to_string()));
    assert!(r.unwrap_err().to_string().contains("FENCED"));

    let t = reopen(&url).await;
    assert_eq!(owner_epoch(&t).await, Some(6));
    assert!(!contents(&t).await.contains(&(3, Some("zombie".into()))));
    drop(owner);
}

/// P5 — TRUNCATE = delete-all (metadata-only), then post-truncate rows via MERGE.
#[tokio::test]
async fn p5_truncate_then_merge() {
    let dir = tempfile::tempdir().unwrap();
    let url = table_url(&dir, "p5");
    let t = append(create_table(&url, vec![]).await, &[1, 2, 3], "v1").await;
    let (t, dm) = t.delete().await.unwrap();
    println!("P5 delete-all metrics: {dm:?}");
    assert_eq!(dm.num_copied_rows, 0, "delete-all rewrites nothing");
    assert_eq!(dm.num_added_files, 0);
    assert!(contents(&t).await.is_empty());
    let cs = ChangeSet { upserts: vec![(2, "after".into())], deletes: vec![1], seq_base: 300 };
    let (t, _) = sink_commit(t, &cs, 1, "pg:0/5").await.unwrap();
    assert_eq!(contents(&t).await, vec![(2, Some("after".into()))]);
    // Replay of the truncate is a no-op on an already-empty-then-refilled table only if
    // sequenced before the MERGE — the barrier ordering is the core's job (§6.4).
}

/// P6 — cost: sequential vs random keys against a multi-file table.
#[tokio::test]
async fn p6_merge_cost_sequential_vs_random_keys() {
    const FILES: i64 = 40;
    const ROWS_PER_FILE: i64 = 25_000; // 1M rows total
    const CHANGES: i64 = 5_000;

    async fn build(url: &Url) -> DeltaTable {
        let mut t = create_table(url, vec![]).await;
        for f in 0..FILES {
            let ids: Vec<i64> = (f * ROWS_PER_FILE..(f + 1) * ROWS_PER_FILE).collect();
            t = append(t, &ids, "base").await;
        }
        t
    }
    let total = FILES * ROWS_PER_FILE;

    // (a) sequential: the most recent key range (updates to "hot tail" rows)
    let dir = tempfile::tempdir().unwrap();
    let url = table_url(&dir, "p6a");
    let t = build(&url).await;
    let seq_cs = ChangeSet {
        upserts: (total - CHANGES..total).map(|k| (k, "upd".to_string())).collect(),
        deletes: vec![],
        seq_base: 1,
    };
    let start = Instant::now();
    let (_t, ma) = sink_commit(t, &seq_cs, 1, "pg:0/a").await.unwrap();
    let ta = start.elapsed();

    // (b) random: keys spread uniformly over the whole table (deterministic LCG)
    let dir_b = tempfile::tempdir().unwrap();
    let url_b = table_url(&dir_b, "p6b");
    let t = build(&url_b).await;
    let mut x: u64 = 0x9E3779B97F4A7C15;
    let mut keys = std::collections::BTreeSet::new();
    while keys.len() < CHANGES as usize {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        keys.insert(((x >> 33) % total as u64) as i64);
    }
    let rnd_cs = ChangeSet {
        upserts: keys.into_iter().map(|k| (k, "upd".to_string())).collect(),
        deletes: vec![],
        seq_base: 1,
    };
    let start = Instant::now();
    let (_t, mb) = sink_commit(t, &rnd_cs, 1, "pg:0/b").await.unwrap();
    let tb = start.elapsed();

    println!(
        "P6 table={total} rows in {FILES} files; change set={CHANGES} updates\n\
         P6 (a) sequential: {ta:?}  files scanned={} skipped={} removed={} added={} rows_copied={}\n\
         P6 (b) random:     {tb:?}  files scanned={} skipped={} removed={} added={} rows_copied={}",
        ma.num_target_files_scanned, ma.num_target_files_skipped_during_scan,
        ma.num_target_files_removed, ma.num_target_files_added, ma.num_target_rows_copied,
        mb.num_target_files_scanned, mb.num_target_files_skipped_during_scan,
        mb.num_target_files_removed, mb.num_target_files_added, mb.num_target_rows_copied,
    );
    assert_eq!(ma.num_target_rows_updated, CHANGES as usize);
    assert_eq!(mb.num_target_rows_updated, CHANGES as usize);
}

/// P7a — add column (the evolve() path, research §7.3) on a DEFAULT table (no column mapping).
#[tokio::test]
async fn p7a_add_column_default_table() {
    let dir = tempfile::tempdir().unwrap();
    let url = table_url(&dir, "p7a");
    let t = append(create_table(&url, vec![]).await, &[1, 2], "v1").await;
    let (t, _) = sink_commit(t, &cs1(), 1, "pg:0/7").await.unwrap();
    let t = t
        .add_columns()
        .with_fields([StructField::new("extra", DataType::STRING, true)])
        .await
        .unwrap();
    let names: Vec<String> = t
        .snapshot()
        .unwrap()
        .schema()
        .fields()
        .map(|f| f.name().to_string())
        .collect();
    println!("P7a schema after add: {names:?}");
    assert!(names.contains(&"extra".to_string()));
    // The pre-evolution MERGE shape (no `extra`) still commits; new column reads NULL.
    let cs = ChangeSet { upserts: vec![(5, "five".into())], deletes: vec![], seq_base: 500 };
    let (t, _) = sink_commit(t, &cs, 1, "pg:0/8").await.unwrap();
    assert!(contents(&t).await.contains(&(5, Some("five".into()))));
}

/// P7b — FINDING: on a column-mapping `name` table, delta-rs 1.0 REFUSES add-column.
/// Recorded as an assertion so a future delta-rs that lifts the limit flips this test.
#[tokio::test]
async fn p7b_column_mapping_name_blocks_add_column() {
    let dir = tempfile::tempdir().unwrap();
    let url = table_url(&dir, "p7b");
    let t = create_table(&url, vec![(TableProperty::ColumnMappingMode, "name")]).await;
    let t = append(t, &[1, 2], "v1").await;
    let (t, _) = sink_commit(t, &cs1(), 1, "pg:0/7").await.unwrap();
    assert_eq!(contents(&t).await.len(), 2, "MERGE works on a column-mapped table");
    let err = t
        .add_columns()
        .with_fields([StructField::new("extra", DataType::STRING, true)])
        .await
        .expect_err("delta-rs 1.0: ADD COLUMN unsupported under column mapping");
    println!("P7b add-column on column-mapped table: {err}");
}
