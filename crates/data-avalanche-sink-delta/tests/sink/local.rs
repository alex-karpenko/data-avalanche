//! The scenarios on a local directory, and what only runs locally: a column-mapped table,
//! checkpoints and log cleanup, the MERGE cost model.

use std::time::Duration;

use data_avalanche_core::{
    ColumnSchema, ColumnValueType, CommitOutcome, SchemaChange, SchemaDelta, Sink,
};
use data_avalanche_sink_delta::{DeltaLocation, TableDefaults};
use deltalake::kernel::{DataType, StructField};
use deltalake::{DeltaTable, TableProperty};

use crate::common::{commit, cs, items, lsn, owned, rows, sink, spec, upserts};
use crate::scenarios;

fn local() -> (tempfile::TempDir, DeltaLocation) {
    let dir = tempfile::tempdir().unwrap();
    let location = DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    };
    (dir, location)
}

#[tokio::test]
async fn mirror_and_replay() {
    let (_dir, location) = local();
    scenarios::mirror_and_replay(location).await;
}

#[tokio::test]
async fn fence_and_guard_survive_a_foreign_optimize() {
    let (_dir, location) = local();
    scenarios::fence_and_guard_survive_a_foreign_optimize(location).await;
}

#[tokio::test]
async fn truncate_then_rows() {
    let (_dir, location) = local();
    scenarios::truncate_then_rows(location).await;
}

#[tokio::test]
async fn evolution() {
    let (_dir, location) = local();
    scenarios::evolution(location).await;
}

#[tokio::test]
async fn masks_and_empty_sets() {
    let (_dir, location) = local();
    scenarios::masks_and_empty_sets(location).await;
}

#[tokio::test]
async fn escaped_names_land_under_the_storage_crates_keys() {
    let (dir, location) = local();
    let odd = scenarios::escaped_names(location, scenarios::odd_names()).await;
    let root = data_avalanche_storage::table_root("", "", &odd.schema, &odd.name).root;
    assert!(dir.path().join(&root).join("_delta_log").is_dir(), "{root}");
}

/// P7b, as a canary: delta-rs 1.0 refuses ADD COLUMN on a column-mapped table, which is why the
/// sink never creates one (V-092). A delta-rs that lifts the limit fails this test.
#[tokio::test]
async fn delta_rs_still_refuses_add_column_under_column_mapping() {
    let (_dir, location) = local();
    let url = location.table_url(&items()).unwrap();
    std::fs::create_dir_all(url.to_file_path().unwrap()).unwrap();
    DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns([StructField::new("id", DataType::LONG, false)])
        .with_configuration_property(TableProperty::ColumnMappingMode, Some("name"))
        .await
        .unwrap();
    let s = sink(location, TableDefaults::default());
    let add = SchemaDelta {
        table: items(),
        changes: vec![SchemaChange::AddColumn(ColumnSchema::new(
            "n",
            ColumnValueType::Int32,
            "integer",
        ))],
    };
    let err = s.evolve(&items(), &add).await.unwrap_err();
    assert!(err.to_string().contains("Column mapping"), "{err}");
}

/// Checkpoints and expired-log cleanup run as post-commit hooks; the marker survives both (the
/// `txn` lives in the checkpoint, the walk-back stops at the first missing commit).
#[tokio::test]
async fn checkpoints_and_log_cleanup_keep_the_marker() {
    let (dir, location) = local();
    let defaults = TableDefaults {
        checkpoint_interval: 2,
        log_retention: Duration::ZERO,
        ..TableDefaults::default()
    };
    let s = sink(location.clone(), defaults.clone());
    s.ensure_table(&spec()).await.unwrap();
    for end in 1..=6 {
        commit(
            &s,
            cs(
                end * 10,
                vec![upserts(&[(end as i64, Some("v"))], None)],
                vec![],
            ),
            3,
        )
        .await;
    }
    let log: Vec<String> = std::fs::read_dir(dir.path().join("public/items/_delta_log"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        log.iter().any(|f| f.ends_with(".checkpoint.parquet")),
        "{log:?}"
    );
    assert!(
        !log.contains(&"00000000000000000001.json".to_string()),
        "expired commits are cleaned up: {log:?}"
    );
    let fresh = sink(location, defaults);
    let marker = fresh.table_state(&items()).await.unwrap().marker.unwrap();
    assert_eq!(
        (marker.owner_epoch, marker.committed_position),
        (3, lsn(60))
    );
}

/// Probe P6, the Delta cost model (D-MS.15): changes spread over the key space rewrite every file
/// they touch, changes to a key range rewrite only its files. The counters show it.
#[tokio::test]
async fn random_keys_rewrite_more_files_than_a_key_range() {
    const FILES: i64 = 8;
    const PER_FILE: i64 = 5_000;
    const CHANGES: i64 = 400;
    async fn build(location: DeltaLocation) -> data_avalanche_sink_delta::DeltaSink {
        let s = sink(location, TableDefaults::default());
        s.ensure_table(&spec()).await.unwrap();
        for f in 0..FILES {
            let rows: Vec<_> = (f * PER_FILE..(f + 1) * PER_FILE)
                .map(|k| (k, Some("base")))
                .collect();
            commit(&s, cs(f as u64 + 1, vec![upserts(&rows, None)], vec![]), 1).await;
        }
        s
    }
    /// (files rewritten, rows copied) by one MERGE of `keys` over the built table.
    async fn cost(keys: Vec<i64>) -> (u64, u64) {
        let (_dir, location) = local();
        let s = build(location).await;
        let before = s.stats();
        let rows: Vec<_> = keys.into_iter().map(|k| (k, Some("upd"))).collect();
        commit(&s, cs(100, vec![upserts(&rows, None)], vec![]), 1).await;
        let after = s.stats();
        (
            after.files_removed - before.files_removed,
            after.rows_copied - before.rows_copied,
        )
    }
    let total = FILES * PER_FILE;
    let (seq_files, seq_copied) = cost((total - CHANGES..total).collect()).await;
    let (rnd_files, rnd_copied) = cost((0..CHANGES).map(|i| i * (total / CHANGES)).collect()).await;
    // A key range rewrites its one file, copying the rows it didn't change; keys spread over the
    // table rewrite every file (probe P6: 40/40 files for 5k random updates of 1M rows).
    assert_eq!((seq_files, seq_copied), (1, (PER_FILE - CHANGES) as u64));
    assert_eq!(
        (rnd_files, rnd_copied),
        (FILES as u64, (total - CHANGES) as u64)
    );
}

/// Commits rows 1, 2, 3 at positions 10, 20, 30, then removes the table directory (an operator
/// dropping the table to re-snapshot it).
async fn three_commits_then_drop(
    dir: &tempfile::TempDir,
    s: &data_avalanche_sink_delta::DeltaSink,
) {
    s.ensure_table(&spec()).await.unwrap();
    for (end, id) in [(10, 1), (20, 2), (30, 3)] {
        commit(
            s,
            cs(end, vec![upserts(&[(id, Some("a"))], None)], vec![]),
            1,
        )
        .await;
    }
    std::fs::remove_dir_all(dir.path().join("public/items")).unwrap();
}

/// A table dropped and re-created behind the sink is a new table: the position the sink cached
/// for the old one (30) must not skip a commit to the new one (review Task 9 M2).
#[tokio::test]
async fn a_recreated_table_is_not_skipped_by_the_old_position() {
    let (dir, location) = local();
    let s = sink(location, TableDefaults::default());
    three_commits_then_drop(&dir, &s).await;
    let state = s.table_state(&items()).await.unwrap();
    assert!(!state.exists);
    s.ensure_table(&spec()).await.unwrap();
    let state = s.table_state(&items()).await.unwrap();
    assert_eq!(state.marker, None, "a new table has no own commit");
    let out = commit(
        &s,
        cs(25, vec![upserts(&[(9, Some("re"))], None)], vec![]),
        1,
    )
    .await;
    assert!(matches!(out, CommitOutcome::Committed { .. }), "{out:?}");
    assert_eq!(rows(&s).await, owned(&[(9, Some("re"))]));
    let marker = s.table_state(&items()).await.unwrap().marker.unwrap();
    assert_eq!(marker.committed_position, lsn(25));
}

/// Another writer re-creates the table between two calls of this sink: the live handle's stale
/// snapshot can't be updated incrementally, so the sink re-opens the table and commits.
#[tokio::test]
async fn a_live_handle_recovers_from_a_table_recreated_behind_it() {
    let (dir, location) = local();
    let s = sink(location.clone(), TableDefaults::default());
    three_commits_then_drop(&dir, &s).await;
    sink(location, TableDefaults::default())
        .ensure_table(&spec())
        .await
        .unwrap();
    let out = commit(
        &s,
        cs(40, vec![upserts(&[(9, Some("re"))], None)], vec![]),
        1,
    )
    .await;
    assert!(matches!(out, CommitOutcome::Committed { .. }), "{out:?}");
    assert_eq!(rows(&s).await, owned(&[(9, Some("re"))]));
}
