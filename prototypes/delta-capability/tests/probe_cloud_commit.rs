//! P8 — Delta commit atomicity on REAL cloud storage (research §10, gap DA-MS-4), 2026-09-26.
//!
//! delta-rs 1.0 uses the `DefaultLogStore` on S3 and GCS: a commit is ONE create-only
//! `PUT _delta_log/<N>.json` — S3 `If-None-Match: *` (object_store 0.13 `S3ConditionalPut::ETagMatch`,
//! the default; no DynamoDB), GCS `x-goog-if-generation-match: 0`. The Delta sink's atomicity,
//! idempotency and epoch fence all reduce to that put.
//!
//!   P8a  raw conditional put: W concurrent creators of one key → exactly one wins, content = winner's
//!   P8b  same-version race: W sink MERGEs on one snapshot, retries OFF → exactly one lands at v+1,
//!        the log has no v+2, table content = base + winner's change set
//!   P8c  concurrent owners MERGE the same row, retries ON → exactly one lands; the losers are stopped
//!        by delta-rs's DATA conflict check (the winner rewrote the file they read) before the fence
//!   P8e  the fence ALONE: concurrent owners append disjoint rows (no data conflict possible), retries
//!        ON → exactly one lands; the rest fail the `txn` app-id check (P4 on real storage)
//!   P8d  foreign concurrent appends, retries ON → all land, on contiguous versions, no row lost
//!
//! Skipped unless `P8_URL` is set (`s3://…` with `--features s3`, `gs://…` with `--features gcs`).
//!
//!   # S3 — standard AWS env vars
//!   eval "$(aws configure export-credentials --profile kav --format env)"
//!   AWS_REGION=us-east-1 P8_URL=s3://<bucket>/delta-probe \
//!   cargo test --release --features s3 --test probe_cloud_commit -- --nocapture --test-threads=1
//!
//!   # GCS — service-account key file
//!   GOOGLE_SERVICE_ACCOUNT_PATH=<key.json> P8_URL=gs://<bucket>/delta-probe \
//!   cargo test --release --features gcs --test probe_cloud_commit -- --nocapture --test-threads=1

#![cfg(any(feature = "s3", feature = "gcs"))]

use std::collections::BTreeSet;
use std::ops::Not;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use deltalake::arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType as ArrowType, Field, Schema as ArrowSchema};
use deltalake::datafusion::prelude::{col, SessionContext};
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::{DataType, StructField, Transaction};
use deltalake::logstore::object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};
use deltalake::{DeltaTable, ObjectStoreError, Path};
use futures::TryStreamExt;
use url::Url;

const OWNER_APP_ID: &str = "avalanche/orders/owner";
const POS_KEY: &str = "avalanche.committed_position";

fn env_num(k: &str, default: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// `s3://…/delta-probe/<run>/<name>/`, or None when P8 is not configured.
fn table_url(name: &str) -> Option<Url> {
    let base = std::env::var("P8_URL").ok().filter(|v| !v.is_empty())?;
    static RUN: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let run = RUN.get_or_init(|| SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
    #[cfg(feature = "s3")]
    deltalake::aws::register_handlers(None);
    #[cfg(feature = "gcs")]
    deltalake::gcp::register_handlers(None);
    Some(Url::parse(&format!("{}/{run}/{name}/", base.trim_end_matches('/'))).unwrap())
}

async fn create_table(url: &Url) -> DeltaTable {
    DeltaTable::try_from_url(url.clone())
        .await
        .unwrap()
        .create()
        .with_columns([
            StructField::new("id", DataType::LONG, false),
            StructField::new("val", DataType::STRING, true),
        ])
        .await
        .unwrap()
}

fn rows(ids: &[i64], val: &str) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int64, false),
        Field::new("val", ArrowType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec![val; ids.len()])),
        ],
    )
    .unwrap()
}

/// The sink's MERGE source: upserts + deletes with the `__avalanche_del` flag.
fn merge_source(upserts: &[(i64, String)], deletes: &[i64]) -> RecordBatch {
    let mut ids: Vec<i64> = upserts.iter().map(|(i, _)| *i).collect();
    let mut vals: Vec<Option<String>> = upserts.iter().map(|(_, v)| Some(v.clone())).collect();
    let mut dels = vec![false; upserts.len()];
    ids.extend(deletes);
    vals.extend(deletes.iter().map(|_| None));
    dels.extend(deletes.iter().map(|_| true));
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int64, false),
        Field::new("val", ArrowType::Utf8, true),
        Field::new("__avalanche_del", ArrowType::Boolean, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(vals)),
            Arc::new(BooleanArray::from(dels)),
        ],
    )
    .unwrap()
}

/// The Delta sink commit (§7.2): one MERGE carrying `txn(owner, epoch)` + `commitInfo` position.
async fn sink_merge(
    table: DeltaTable,
    upserts: &[(i64, String)],
    deletes: &[i64],
    epoch: i64,
    position: &str,
    max_retries: usize,
) -> Result<DeltaTable, String> {
    let ctx = SessionContext::new();
    let df = ctx.read_batch(merge_source(upserts, deletes)).map_err(|e| e.to_string())?;
    let props = CommitProperties::default()
        .with_application_transaction(Transaction::new(OWNER_APP_ID, epoch))
        .with_metadata([(POS_KEY.to_string(), serde_json::Value::String(position.into()))])
        .with_max_retries(max_retries);
    let run = || -> Result<_, deltalake::DeltaTableError> {
        Ok(table
            .merge(df, col("target.id").eq(col("source.id")))
            .with_source_alias("source")
            .with_target_alias("target")
            .with_commit_properties(props)
            .when_matched_delete(|d| d.predicate(col("source.__avalanche_del")))?
            .when_matched_update(|u| {
                u.predicate(col("source.__avalanche_del").not()).update("val", col("source.val"))
            })?
            .when_not_matched_insert(|i| {
                i.predicate(col("source.__avalanche_del").not())
                    .set("id", col("source.id"))
                    .set("val", col("source.val"))
            })?)
    };
    let (t, _) = run().map_err(|e| e.to_string())?.await.map_err(|e| e.to_string())?;
    Ok(t)
}

async fn reload(url: &Url) -> DeltaTable {
    let mut t = DeltaTable::try_from_url(url.clone()).await.unwrap();
    t.load().await.unwrap();
    t
}

async fn contents(table: &DeltaTable) -> Vec<(i64, String)> {
    let ctx = SessionContext::new();
    table.update_datafusion_session(&ctx.state()).unwrap();
    ctx.register_table("t", table.table_provider().await.unwrap()).unwrap();
    let batches =
        ctx.sql("SELECT id, val FROM t ORDER BY id").await.unwrap().collect().await.unwrap();
    let mut out = vec![];
    for b in batches {
        // DataFusion 55 reads strings back as Utf8View
        let vals = deltalake::arrow::compute::cast(b.column(1), &ArrowType::Utf8).unwrap();
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let vals = vals.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..ids.len() {
            out.push((ids.value(i), vals.value(i).to_string()));
        }
    }
    out
}

/// Every version file present under `_delta_log/`.
async fn log_versions(table: &DeltaTable) -> BTreeSet<u64> {
    let store = table.log_store().object_store(None);
    let list: Vec<_> =
        store.list(Some(&Path::from("_delta_log"))).try_collect().await.unwrap();
    list.iter()
        .filter_map(|m| m.location.filename()?.strip_suffix(".json")?.parse::<u64>().ok())
        .collect()
}

// ------------------------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p8a_raw_conditional_put_one_winner() {
    let Some(url) = table_url("p8a") else { return println!("P8a skipped: P8_URL not set") };
    let table = create_table(&url).await;
    let store = table.log_store().object_store(None);
    let rounds = env_num("P8_ROUNDS", 10);
    let writers = env_num("P8_WRITERS", 16);
    for r in 0..rounds {
        let key = Path::from(format!("_probe/race-{r}.json"));
        let mut hs = vec![];
        for w in 0..writers {
            let (store, key) = (Arc::clone(&store), key.clone());
            hs.push(tokio::spawn(async move {
                let opts = PutOptions { mode: PutMode::Create, ..Default::default() };
                let payload = PutPayload::from(format!("writer-{w}").into_bytes());
                (w, store.put_opts(&key, payload, opts).await)
            }));
        }
        let (mut won, mut exists) = (vec![], 0);
        for h in hs {
            match h.await.unwrap() {
                (w, Ok(_)) => won.push(w),
                (_, Err(ObjectStoreError::AlreadyExists { .. })) => exists += 1,
                (_, Err(e)) => panic!("unexpected put error: {e}"),
            }
        }
        assert_eq!(won.len(), 1, "round {r}: exactly one creator wins, got {won:?}");
        let body = store.get(&key).await.unwrap().bytes().await.unwrap();
        assert_eq!(body.as_ref(), format!("writer-{}", won[0]).as_bytes(), "content = winner's");
        println!("  P8a round {r}: winner {} / {exists} AlreadyExists", won[0]);
    }
    println!("P8a ✅ {rounds} rounds × {writers}: create-only put has exactly one winner");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p8b_same_version_merge_race_one_lands() {
    let Some(url) = table_url("p8b") else { return println!("P8b skipped: P8_URL not set") };
    let seeded = create_table(&url).await.write(vec![rows(&[1, 2, 3, 4], "seed")]).await.unwrap();
    let rounds = env_num("P8_ROUNDS", 5);
    let writers = env_num("P8_WRITERS", 8);
    let mut table = seeded;
    for r in 0..rounds {
        let base_v = table.version().unwrap();
        let before = contents(&table).await;
        let started = Instant::now();
        let mut hs = vec![];
        for w in 0..writers {
            let t = table.clone();
            hs.push(tokio::spawn(async move {
                // each writer: update id 1, delete id 2, insert its own id — distinct per writer
                let ups = vec![(1, format!("r{r}w{w}")), (1000 + (r * 100 + w) as i64, format!("r{r}w{w}"))];
                let pos = format!("pos-r{r}-w{w}");
                (w, sink_merge(t, &ups, &[2], (r * 100 + w) as i64 + 1, &pos, 0).await)
            }));
        }
        let (mut won, mut lost) = (vec![], vec![]);
        for h in hs {
            match h.await.unwrap() {
                (w, Ok(t)) => won.push((w, t)),
                (w, Err(e)) => lost.push((w, e)),
            }
        }
        assert_eq!(won.len(), 1, "round {r}: exactly one MERGE lands; lost: {lost:?}");
        for (_, e) in &lost {
            assert!(e.contains("Failed to commit transaction"), "loser must fail at commit: {e}");
        }
        let (w, _) = &won[0];
        let now = reload(&url).await;
        let versions = log_versions(&now).await;
        assert_eq!(now.version(), Some(base_v + 1), "exactly one new version");
        assert!(!versions.contains(&(base_v + 2)), "no stray v+2: {versions:?}");
        // winner's commitInfo is what v+1 holds
        let hist: Vec<_> = now.history(Some(1)).try_collect().await.unwrap();
        let pos = hist[0].info.get(POS_KEY).and_then(|v| v.as_str()).map(str::to_string);
        assert_eq!(pos.as_deref(), Some(format!("pos-r{r}-w{w}").as_str()), "v+1 = winner's commit");
        // content = before + exactly the winner's change set
        let mut expect: Vec<(i64, String)> =
            before.into_iter().filter(|(id, _)| *id != 1 && *id != 2).collect();
        expect.push((1, format!("r{r}w{w}")));
        expect.push((1000 + (r * 100 + w) as i64, format!("r{r}w{w}")));
        expect.sort();
        assert_eq!(contents(&now).await, expect, "content = base + winner only");
        println!(
            "  P8b round {r}: v{} → v{} by writer {w}; {} losers ({}); {} ms",
            base_v,
            base_v + 1,
            lost.len(),
            lost.first().map(|(_, e)| e.chars().take(60).collect::<String>()).unwrap_or_default(),
            started.elapsed().as_millis()
        );
        // re-insert id 2 for the next round's delete
        table = sink_merge(now, &[(2, "back".into())], &[], 0, "reseed", 15).await.unwrap();
    }
    println!("P8b ✅ {rounds} rounds × {writers}: same-version MERGE race → exactly one version, winner's content only");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p8c_epoch_fence_under_concurrency() {
    let Some(url) = table_url("p8c") else { return println!("P8c skipped: P8_URL not set") };
    let table = create_table(&url).await.write(vec![rows(&[1], "seed")]).await.unwrap();
    let base = sink_merge(table, &[(1, "owner-1".into())], &[], 1, "pos-1", 15).await.unwrap();
    let base_v = base.version().unwrap();
    let writers = env_num("P8_WRITERS", 8);
    let mut hs = vec![];
    for w in 0..writers {
        let t = base.clone();
        hs.push(tokio::spawn(async move {
            let epoch = 10 + w as i64; // every contender claims ownership with its own epoch
            (epoch, sink_merge(t, &[(1, format!("owner-{epoch}"))], &[], epoch, "pos", 15).await)
        }));
    }
    let (mut won, mut lost) = (vec![], vec![]);
    for h in hs {
        match h.await.unwrap() {
            (e, Ok(_)) => won.push(e),
            (e, Err(msg)) => lost.push((e, msg)),
        }
    }
    let now = reload(&url).await;
    assert_eq!(won.len(), 1, "exactly one owner commit lands; lost: {lost:?}");
    assert_eq!(now.version(), Some(base_v + 1));
    for (_, msg) in &lost {
        // a data conflict ("a concurrent transactions added new data") fires before the txn check
        assert!(msg.contains("Commit failed") || msg.contains("Concurrent transaction failed"), "{msg}");
    }
    let snap = now.snapshot().unwrap();
    let recorded = snap.transaction_version(now.log_store().as_ref(), OWNER_APP_ID).await.unwrap();
    assert_eq!(recorded, Some(won[0]), "txn version = the winner's epoch");
    assert_eq!(contents(&now).await, vec![(1, format!("owner-{}", won[0]))]);
    println!(
        "P8c ✅ {writers} concurrent owners on one row (retries on): epoch {} landed; {} rejected: {}",
        won[0],
        lost.len(),
        lost[0].1
    );
}

/// P8e — the fence ALONE: contenders append disjoint rows (no data conflict is possible), each
/// carrying `txn(owner, epoch)`. Only the app-id conflict check can stop the losers.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p8e_epoch_fence_alone_disjoint_appends() {
    let Some(url) = table_url("p8e") else { return println!("P8e skipped: P8_URL not set") };
    let seed = CommitProperties::default().with_application_transaction(Transaction::new(OWNER_APP_ID, 1));
    let base = create_table(&url)
        .await
        .write(vec![rows(&[1], "seed")])
        .with_commit_properties(seed)
        .await
        .unwrap();
    let base_v = base.version().unwrap();
    let writers = env_num("P8_WRITERS", 8);
    let mut hs = vec![];
    for w in 0..writers as i64 {
        let t = base.clone();
        hs.push(tokio::spawn(async move {
            let epoch = 10 + w;
            let props = CommitProperties::default()
                .with_application_transaction(Transaction::new(OWNER_APP_ID, epoch));
            let r = t
                .write(vec![rows(&[100 + w], &format!("owner-{epoch}"))])
                .with_commit_properties(props)
                .await;
            (epoch, r.map(|_| ()).map_err(|e| e.to_string()))
        }));
    }
    let (mut won, mut lost) = (vec![], vec![]);
    for h in hs {
        match h.await.unwrap() {
            (e, Ok(())) => won.push(e),
            (e, Err(m)) => lost.push((e, m)),
        }
    }
    let now = reload(&url).await;
    assert_eq!(won.len(), 1, "exactly one owner append lands; lost: {lost:?}");
    for (_, msg) in &lost {
        assert!(msg.contains("Concurrent transaction failed"), "txn app-id conflict only: {msg}");
    }
    assert_eq!(now.version(), Some(base_v + 1));
    let snap = now.snapshot().unwrap();
    let recorded = snap.transaction_version(now.log_store().as_ref(), OWNER_APP_ID).await.unwrap();
    assert_eq!(recorded, Some(won[0]));
    assert_eq!(contents(&now).await.len(), 2, "seed + the winner's row only");
    println!("P8e ✅ {writers} owners, disjoint appends: epoch {} landed; {} fenced: {}", won[0], lost.len(), lost[0].1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p8d_concurrent_appends_all_land() {
    let Some(url) = table_url("p8d") else { return println!("P8d skipped: P8_URL not set") };
    let base = create_table(&url).await;
    let base_v = base.version().unwrap();
    let writers = env_num("P8_WRITERS", 8);
    let per = 100i64;
    let mut hs = vec![];
    for w in 0..writers as i64 {
        let t = base.clone();
        hs.push(tokio::spawn(async move {
            let ids: Vec<i64> = (w * per..(w + 1) * per).collect();
            t.write(vec![rows(&ids, &format!("w{w}"))]).await.map(|_| ()).map_err(|e| e.to_string())
        }));
    }
    for h in hs {
        h.await.unwrap().expect("append must land (non-conflicting, retried)");
    }
    let now = reload(&url).await;
    let versions = log_versions(&now).await;
    let expect: BTreeSet<u64> = (0..=base_v + writers as u64).collect();
    assert_eq!(versions, expect, "contiguous versions, one per append");
    let got = contents(&now).await;
    assert_eq!(got.len() as i64, writers as i64 * per, "no appended row lost");
    println!(
        "P8d ✅ {writers} concurrent appends → versions {}..={}, {} rows, none lost",
        base_v + 1,
        base_v + writers as u64,
        got.len()
    );
}
