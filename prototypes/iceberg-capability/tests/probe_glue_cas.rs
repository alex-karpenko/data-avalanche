//! G2c — the pinned-base commit (G2b-4) against a REAL catalog: AWS Glue + S3 (2026-09-26).
//!
//! Question (DA-X-11 follow-up): does the catalog evaluate the commit requirements atomically with
//! the metadata-pointer swap? iceberg-catalog-glue 0.10.1 checks the requirements CLIENT-side on a
//! freshly loaded table, then calls Glue `UpdateTable` with the `VersionId` it loaded. Concurrent
//! writers that all load the same version pass the client check, so only Glue's `VersionId`
//! optimistic lock can stop the losers. This probe races N pinned writers on one base per round and
//! asserts exactly one wins, with nothing of the losers applied.
//!
//! Skipped unless `GLUE_PROBE_WAREHOUSE` is set. Credentials come from the standard AWS env vars.
//!
//!   eval "$(aws configure export-credentials --profile kav --format env)"
//!   AWS_REGION=us-east-1 GLUE_PROBE_DB=avalanche_probe \
//!   GLUE_PROBE_WAREHOUSE=s3://<bucket>/iceberg-probe \
//!   cargo test --features glue --test probe_glue_cas -- --nocapture

#![cfg(feature = "glue")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_glue::{
    GlueCatalogBuilder, AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN, GLUE_CATALOG_PROP_WAREHOUSE,
};

mod common;
use common::*;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

fn env_num(k: &str, default: usize) -> usize {
    env(k).and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn glue() -> Option<(Arc<dyn Catalog>, NamespaceIdent)> {
    let warehouse = env("GLUE_PROBE_WAREHOUSE")?;
    let mut props = HashMap::from([
        (GLUE_CATALOG_PROP_WAREHOUSE.to_string(), warehouse),
        (AWS_REGION_NAME.to_string(), env("AWS_REGION").unwrap_or("us-east-1".into())),
    ]);
    for (prop, var) in [
        (AWS_ACCESS_KEY_ID, "AWS_ACCESS_KEY_ID"),
        (AWS_SECRET_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY"),
        (AWS_SESSION_TOKEN, "AWS_SESSION_TOKEN"),
    ] {
        if let Some(v) = env(var) {
            props.insert(prop.to_string(), v);
        }
    }
    let catalog = GlueCatalogBuilder::default().load("glue", props).await.unwrap();
    let ns = NamespaceIdent::new(env("GLUE_PROBE_DB").unwrap_or("avalanche_probe".into()));
    if !catalog.namespace_exists(&ns).await.unwrap() {
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
    }
    Some((Arc::new(catalog), ns))
}

#[derive(Default, Debug)]
struct Tally {
    won: usize,
    lost_client_check: usize, // requirement failed on the freshly loaded table (client side)
    lost_glue_version: usize, // Glue UpdateTable ConcurrentModificationException (VersionId)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn g2c_glue_pinned_commit_is_a_cas_under_concurrency() {
    let Some((cat, ns)) = glue().await else {
        println!("G2c skipped: GLUE_PROBE_WAREHOUSE not set");
        return;
    };
    let rounds = env_num("GLUE_PROBE_ROUNDS", 6);
    let writers = env_num("GLUE_PROBE_WRITERS", 8);

    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let name = format!("cas_{ts}");
    let id = NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long));
    let creation = TableCreation {
        name: name.clone(),
        location: None,
        schema: Schema::builder().with_fields(vec![Arc::new(id)]).build().unwrap(),
        partition_spec: None,
        sort_order: None,
        properties: HashMap::new(),
        format_version: FormatVersion::V2,
    };
    cat.create_table(&ns, creation).await.unwrap();
    let ident = TableIdent::new(ns, name.clone());
    println!("G2c table {ident} — {rounds} rounds × {writers} concurrent pinned writers");

    // seed commit (epoch 1)
    let t0 = cat.load_table(&ident).await.unwrap();
    fenced_commit(&cat, &t0, 1, 1).await.unwrap();
    let first_base = cat.load_table(&ident).await.unwrap();

    let mut total = Tally::default();
    for round in 0..rounds {
        let base = cat.load_table(&ident).await.unwrap();
        let base_snap = base.metadata().current_snapshot_id().unwrap();
        let base_snaps = base.metadata().snapshots().len();
        let started = Instant::now();

        let mut handles = Vec::new();
        for w in 0..writers {
            let (cat, base) = (Arc::clone(&cat), base.clone());
            let epoch = ((round + 1) * 100 + w) as u64;
            let file_no = (1_000 + round * 100 + w) as i64;
            handles.push(tokio::spawn(async move {
                (epoch, fenced_commit(&cat, &base, epoch, file_no).await)
            }));
        }
        let mut tally = Tally::default();
        let mut winner = None;
        for h in handles {
            match h.await.unwrap() {
                (epoch, Ok(_)) => {
                    tally.won += 1;
                    winner = Some(epoch);
                }
                (_, Err(e)) if e.contains("Requirement failed") => tally.lost_client_check += 1,
                (_, Err(e)) if e.contains("Commit failed for table") => tally.lost_glue_version += 1,
                (_, Err(e)) => panic!("unexpected commit error: {e}"),
            }
        }
        let after = cat.load_table(&ident).await.unwrap();
        let winner = winner.expect("one writer must win");
        assert_eq!(tally.won, 1, "exactly one winner per round: {tally:?}");
        assert_eq!(after.metadata().snapshots().len(), base_snaps + 1, "one new snapshot only");
        let cur = after.metadata().current_snapshot().unwrap();
        assert_eq!(cur.parent_snapshot_id(), Some(base_snap), "winner committed on the pinned base");
        assert_eq!(prop(&after, EPOCH), Some(winner.to_string().as_str()), "property = winner's");
        assert_eq!(summary(&after, EPOCH), Some(winner.to_string().as_str()), "summary = winner's");
        println!(
            "  round {round}: {tally:?} winner epoch {winner} ({} ms)",
            started.elapsed().as_millis()
        );
        total.won += tally.won;
        total.lost_client_check += tally.lost_client_check;
        total.lost_glue_version += tally.lost_glue_version;
    }
    println!("G2c totals: {total:?}");
    assert!(
        total.lost_glue_version > 0,
        "the race never reached Glue's VersionId check (all losers stopped client-side) — \
         raise GLUE_PROBE_WRITERS: {total:?}"
    );

    // a zombie pinned to the first base (many commits ago) is rejected; nothing applied
    let before = cat.load_table(&ident).await.unwrap();
    let err = fenced_commit(&cat, &first_base, 1, 9_001).await.expect_err("stale zombie");
    let after = cat.load_table(&ident).await.unwrap();
    assert_eq!(after.metadata_location(), before.metadata_location(), "nothing applied");
    println!("G2c zombie (pinned stale base) ✅ rejected: {err}");

    // G2b-3 on real Glue: the SAME zombie through plain Transaction::commit is rebased and lands
    let res = sink_commit(cat.as_ref(), &first_base, 1, 9_002, true).await;
    let after = cat.load_table(&ident).await.unwrap();
    assert!(res.is_ok(), "expected the rebase to land on Glue too: {res:?}");
    assert_eq!(prop(&after, EPOCH), Some("1"), "fence property regressed to the zombie's epoch");
    println!(
        "G2c zombie (plain Transaction::commit) ⚠️ rebased and landed on Glue — property regressed {} → 1",
        prop(&before, EPOCH).unwrap()
    );
}
