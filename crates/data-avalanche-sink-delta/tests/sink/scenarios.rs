//! The delta-capability probes (P1–P5, P7a) as scenarios of the sink, run on any location.

use data_avalanche_core::{
    ColumnSchema, ColumnValueType, CommitOutcome, EvolveOutcome, SchemaChange, SchemaDelta, Sink,
    TableRef, TableSchema, TableSpec,
};
use data_avalanche_sink_delta::{DeltaLocation, TableDefaults};
use deltalake::DeltaTable;

use crate::common::{commit, cs, deletes, items, lsn, owned, rows, schema, sink, spec, upserts};

/// A handle on the sink's table as another writer (an external engine) would open it.
async fn foreign(location: &DeltaLocation) -> DeltaTable {
    let url = location.table_url(&items()).unwrap();
    let mut t = DeltaTable::try_from_url_with_storage_options(
        url,
        location.storage_options(std::env::vars()),
    )
    .await
    .unwrap();
    t.load().await.unwrap();
    t
}

/// P1 and P2: updates, deletes and inserts land in one version; a replay is skipped by the guard,
/// and the same sets on a fresh table give the same content.
pub async fn mirror_and_replay(location: DeltaLocation) {
    let s = sink(location.clone(), TableDefaults::default());
    s.ensure_table(&spec()).await.unwrap();
    let first = cs(
        10,
        vec![upserts(
            &[(1, Some("a")), (2, Some("b")), (3, Some("c"))],
            None,
        )],
        vec![],
    );
    let second = cs(
        20,
        vec![upserts(&[(1, Some("A")), (4, Some("d"))], None)],
        vec![deletes(&[2])],
    );
    let CommitOutcome::Committed { version: v1 } = commit(&s, first.clone(), 1).await else {
        panic!("first commit")
    };
    assert_eq!(
        commit(&s, second.clone(), 1).await,
        CommitOutcome::Committed { version: v1 + 1 }
    );
    let want = owned(&[(1, Some("A")), (3, Some("c")), (4, Some("d"))]);
    assert_eq!(rows(&s).await, want);
    assert_eq!(commit(&s, first, 1).await, CommitOutcome::SkippedCovered);
    assert_eq!(rows(&s).await, want);
    let stats = s.stats();
    assert_eq!((stats.commits, stats.skipped), (2, 1));
    assert_eq!(
        (stats.rows_inserted, stats.rows_updated, stats.rows_deleted),
        (4, 1, 1)
    );
    // A new process (a fresh sink) reads the marker back.
    let state = sink(location, TableDefaults::default())
        .table_state(&items())
        .await
        .unwrap();
    let marker = state.marker.unwrap();
    assert_eq!(
        (marker.owner_epoch, marker.committed_position),
        (1, lsn(20))
    );
    assert_eq!(state.schema.unwrap(), spec().schema);
}

/// P3 and P4 (the M4 exit): a foreign OPTIMIZE doesn't disarm the fence or the guard.
pub async fn fence_and_guard_survive_a_foreign_optimize(location: DeltaLocation) {
    let owner = sink(location.clone(), TableDefaults::default());
    owner.ensure_table(&spec()).await.unwrap();
    for (end, id) in [(10, 1), (20, 2), (30, 3)] {
        commit(
            &owner,
            cs(end, vec![upserts(&[(id, Some("v"))], None)], vec![]),
            7,
        )
        .await;
    }
    let (optimized, metrics) = foreign(&location).await.optimize().await.unwrap();
    assert!(metrics.num_files_removed > 1, "{metrics:?}");
    drop(optimized);
    let fresh = sink(location, TableDefaults::default());
    let marker = fresh.table_state(&items()).await.unwrap().marker.unwrap();
    assert_eq!(
        (marker.owner_epoch, marker.committed_position),
        (7, lsn(30))
    );
    // A zombie of epoch 6 is fenced; a replay of an old range is skipped.
    let zombie = cs(40, vec![upserts(&[(9, Some("zombie"))], None)], vec![]);
    assert_eq!(
        commit(&fresh, zombie, 6).await,
        CommitOutcome::Fenced { current_epoch: 7 }
    );
    let replay = cs(20, vec![upserts(&[(2, Some("old"))], None)], vec![]);
    assert_eq!(
        commit(&fresh, replay, 7).await,
        CommitOutcome::SkippedCovered
    );
    assert_eq!(
        rows(&fresh).await,
        owned(&[(1, Some("v")), (2, Some("v")), (3, Some("v"))])
    );
}

/// P5: a truncate clears the table before the set's rows; a replay of it after later commits is
/// skipped.
pub async fn truncate_then_rows(location: DeltaLocation) {
    let s = sink(location, TableDefaults::default());
    s.ensure_table(&spec()).await.unwrap();
    commit(
        &s,
        cs(
            10,
            vec![upserts(&[(1, Some("a")), (2, Some("b"))], None)],
            vec![],
        ),
        1,
    )
    .await;
    let mut t = cs(
        20,
        vec![upserts(&[(3, Some("c"))], None)],
        vec![deletes(&[1])],
    );
    t.truncate = true;
    let before = s.stats();
    commit(&s, t.clone(), 1).await;
    assert_eq!(rows(&s).await, owned(&[(3, Some("c"))]));
    let after = s.stats();
    // The truncate's DELETE counts: it removed the one file holding rows 1 and 2.
    assert_eq!(
        (
            after.files_removed - before.files_removed,
            after.rows_deleted - before.rows_deleted,
            after.rows_inserted - before.rows_inserted,
        ),
        (1, 2, 1)
    );
    let mut clear = cs(30, vec![], vec![]);
    clear.truncate = true;
    commit(&s, clear, 1).await;
    assert_eq!(rows(&s).await, vec![]);
    commit(
        &s,
        cs(40, vec![upserts(&[(4, Some("d"))], None)], vec![]),
        1,
    )
    .await;
    assert_eq!(commit(&s, t, 1).await, CommitOutcome::SkippedCovered);
    assert_eq!(rows(&s).await, owned(&[(4, Some("d"))]));
}

/// P7a and the Delta evolution contract: ADD COLUMN applies and the pre-evolution set shape still
/// commits (the new column is NULL); a drop is a soft-drop, written NULL from then on.
pub async fn evolution(location: DeltaLocation) {
    let s = sink(location, TableDefaults::default());
    s.ensure_table(&spec()).await.unwrap();
    commit(
        &s,
        cs(10, vec![upserts(&[(1, Some("a"))], None)], vec![]),
        1,
    )
    .await;
    let add = SchemaDelta {
        table: items(),
        changes: vec![SchemaChange::AddColumn(ColumnSchema::new(
            "n",
            ColumnValueType::Int32,
            "integer",
        ))],
    };
    assert_eq!(
        s.evolve(&items(), &add).await.unwrap(),
        EvolveOutcome::Applied
    );
    commit(
        &s,
        cs(20, vec![upserts(&[(2, Some("b"))], None)], vec![]),
        1,
    )
    .await;
    let content = s.content(&items()).await.unwrap().unwrap();
    assert_eq!(
        content.schema().field(content.num_columns() - 1).name(),
        "n"
    );
    assert_eq!(content.column_by_name("n").unwrap().null_count(), 2);
    let drop = SchemaDelta {
        table: items(),
        changes: vec![SchemaChange::DropColumn("doc".into())],
    };
    assert!(matches!(
        s.evolve(&items(), &drop).await.unwrap(),
        EvolveOutcome::Degraded { .. }
    ));
    let mut narrow = cs(30, vec![], vec![]);
    let fields: Vec<_> = narrow
        .schema
        .fields()
        .iter()
        .filter(|f| f.name() != "doc")
        .cloned()
        .collect();
    narrow.schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
    let batch = upserts(&[(1, None)], None);
    narrow.upserts = vec![batch.project(&[0, 2, 3, 4, 5, 6, 7]).unwrap()];
    commit(&s, narrow, 1).await;
    assert_eq!(rows(&s).await, owned(&[(1, None), (2, Some("b"))]));
}

/// Unchanged TOAST cells keep the stored value; a set that changes no row still moves the marker.
pub async fn masks_and_empty_sets(location: DeltaLocation) {
    let s = sink(location, TableDefaults::default());
    s.ensure_table(&spec()).await.unwrap();
    commit(
        &s,
        cs(10, vec![upserts(&[(1, Some("big"))], None)], vec![]),
        1,
    )
    .await;
    let masked = upserts(&[(1, None), (2, None)], Some(&[true, true]));
    commit(&s, cs(20, vec![masked], vec![]), 1).await;
    assert_eq!(rows(&s).await, owned(&[(1, Some("big")), (2, None)]));
    for (end, set) in [
        (30, cs(30, vec![], vec![])),
        (40, cs(40, vec![], vec![deletes(&[99])])),
    ] {
        assert!(matches!(
            commit(&s, set, 1).await,
            CommitOutcome::Committed { .. }
        ));
        let marker = s.table_state(&items()).await.unwrap().marker.unwrap();
        assert_eq!(marker.committed_position, lsn(end));
    }
}

/// The table name the escaped-name scenario uses by default: `%` and `/` are escaped (`~XX`,
/// Q-M4-5), a space and non-ASCII text are kept.
pub fn odd_names() -> TableRef {
    TableRef::new("sales 50%", "a/b.ü")
}

/// A table whose names need escaping: it is written and read back, and a fresh sink finds it.
pub async fn escaped_names(location: DeltaLocation, odd: TableRef) -> TableRef {
    let odd_schema =
        TableSchema::new(odd.clone(), schema().columns().to_vec(), vec!["id".into()]).unwrap();
    let s = sink(location.clone(), TableDefaults::default());
    s.ensure_table(&TableSpec::new(&odd_schema, vec![]).unwrap())
        .await
        .unwrap();
    let mut c = cs(10, vec![upserts(&[(1, Some("a"))], None)], vec![]);
    c.table = odd.clone();
    assert!(matches!(
        commit(&s, c, 1).await,
        CommitOutcome::Committed { .. }
    ));
    assert_eq!(s.content(&odd).await.unwrap().unwrap().num_rows(), 1);
    let state = sink(location, TableDefaults::default())
        .table_state(&odd)
        .await
        .unwrap();
    assert!(state.exists);
    assert_eq!(state.marker.unwrap().committed_position, lsn(10));
    odd
}
