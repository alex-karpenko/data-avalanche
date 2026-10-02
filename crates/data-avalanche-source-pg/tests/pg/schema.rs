//! Channels C and D against a live server, through `dyn SourceConnector` (`Schema_Change…` §3,
//! §6).

use std::collections::BTreeMap;

use data_avalanche_core::{
    ChangeStream, ColumnValueType as V, DeltaClass, HaltReason, SchemaChange, SchemaHint,
    SourceConnector, SourceError, StartPlan, StreamItem, TableRef, TableSchema, classify,
};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, connector, each, fresh, pipeline};

#[tokio::test(flavor = "multi_thread")]
async fn channel_c_proves_a_rename_scoped_to_the_signal() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_c",
        "CREATE TABLE t (id int PRIMARY KEY, a varchar(100), b int);",
    )
    .await;
    let c: Box<dyn SourceConnector> = Box::new(connector(&pipeline(&n, &["*"], "", "")));
    let r = c.resolve_tables().await.unwrap();
    let cached = r.tables[0].clone();
    let p = c.prepare(None, &[n.table("t")]).await.unwrap();
    let StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("ALTER TABLE {sc}.t RENAME COLUMN a TO a2"),
            format!("INSERT INTO {sc}.t VALUES (1, 'x', 1)"),
            // After the signal's position: the catalog runs ahead of the stream.
            format!("ALTER TABLE {sc}.t ADD COLUMN z text"),
            format!("ALTER TABLE {sc}.t ALTER COLUMN a2 TYPE varchar(5)"),
        ],
    )
    .await;
    let hint = loop {
        match s.next().await.unwrap() {
            StreamItem::SchemaSignal { hint, .. } => break hint,
            StreamItem::Change(e) => panic!("a change before the signal: {e:?}"),
            _ => {}
        }
    };
    let SchemaHint::Columns(columns) = &hint else {
        panic!()
    };
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["id", "a2", "b"]);
    let (delta, new) = c.augment(&cached, &hint).await.unwrap();
    assert_eq!(
        delta.changes,
        [SchemaChange::RenameColumn {
            from: "a".into(),
            to: "a2".into(),
            proven: true
        }],
        "the rename only: not the later column, not the later narrowing"
    );
    let new = new.unwrap();
    assert_eq!(
        new.column("a2").unwrap().source_type,
        "character varying(100)"
    );
    assert_eq!(new.column("a2").unwrap().source_ordinal, Some(2));
    assert_eq!(classify(&cached, &delta)[0].1, DeltaClass::Safe);
    s.close().await.unwrap();

    a.batch_execute(&format!("DROP TABLE {sc}.t"))
        .await
        .unwrap();
    let (gone, schema) = c.augment(&new, &hint).await.unwrap();
    assert_eq!(gone.changes, [SchemaChange::TableDropped]);
    assert!(schema.is_none());
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn channel_d_finds_dropped_changed_and_broken_tables() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_d",
        "CREATE TABLE t (id int PRIMARY KEY); CREATE TABLE u (id int PRIMARY KEY);
         CREATE TABLE w (id int PRIMARY KEY); CREATE TABLE same (id int PRIMARY KEY);
         CREATE TABLE l (id int PRIMARY KEY);",
    )
    .await;
    let c: Box<dyn SourceConnector> = Box::new(connector(&pipeline(&n, &["*"], "", "")));
    let known = c.resolve_tables().await.unwrap().tables;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("DROP TABLE {sc}.u"),
            format!("ALTER TABLE {sc}.t ADD COLUMN v bigint"),
            format!("ALTER TABLE {sc}.w REPLICA IDENTITY NOTHING"),
            // Allowed outside a FOR TABLE publication (under FOR ALL TABLES too): the table
            // stops publishing.
            format!("ALTER TABLE {sc}.l SET UNLOGGED"),
        ],
    )
    .await;
    let report = c.rescan(&known).await.unwrap();
    assert_eq!(report.dropped, [n.table("u")]);
    let changed: Vec<(&TableRef, Vec<&V>)> = report
        .changed
        .iter()
        .map(|s| {
            (
                s.table(),
                s.columns().iter().map(|c| &c.value_type).collect(),
            )
        })
        .collect();
    assert_eq!(changed, [(&n.table("t"), vec![&V::Int32, &V::Int64])]);
    let mut halted: Vec<(&TableRef, &str)> =
        report.halted.iter().map(|(t, r)| (t, r.code())).collect();
    halted.sort();
    assert_eq!(
        halted,
        [
            (&n.table("l"), "TABLE_NOT_REPLICABLE"),
            (&n.table("w"), "IDENTIFIER_NOT_IN_REPLICA_IDENTITY")
        ]
    );
    cleanup(&a, &n).await;
}

/// The signals of `tables` (one each), in any order.
async fn signals(s: &mut Box<dyn ChangeStream>, tables: usize) -> BTreeMap<String, SchemaHint> {
    let mut out = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while out.len() < tables {
        let item = tokio::time::timeout_at(deadline, s.next())
            .await
            .unwrap_or_else(|_| panic!("signals after 20 s: {:?}", out.keys()))
            .unwrap();
        if let StreamItem::SchemaSignal { table, hint, .. } = item {
            out.insert(table.name, hint);
        }
    }
    out
}

fn cached<'a>(r: &'a [TableSchema], name: &str) -> &'a TableSchema {
    r.iter().find(|t| t.table().name == name).unwrap()
}

/// Q-M5-17: with the catalog ahead of the stream, a column dropped since the signal keeps its
/// place, and renames since the signal that a placement would have to guess halt the table.
#[tokio::test(flavor = "multi_thread")]
async fn channel_c_with_the_catalog_ahead() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_c_ahead",
        "CREATE TABLE t (id int PRIMARY KEY, a int, b int);
         CREATE TABLE u (id int PRIMARY KEY, a int, b int);",
    )
    .await;
    let c: Box<dyn SourceConnector> = Box::new(connector(&pipeline(&n, &["*"], "", "")));
    let r = c.resolve_tables().await.unwrap();
    let p = c
        .prepare(None, &[n.table("t"), n.table("u")])
        .await
        .unwrap();
    let StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("ALTER TABLE {sc}.t ADD COLUMN c int"),
            format!("INSERT INTO {sc}.t VALUES (1, 1, 1, 1)"),
            format!("ALTER TABLE {sc}.u ADD COLUMN c int"),
            format!("INSERT INTO {sc}.u VALUES (1, 1, 1, 1)"),
            // After the signals: expand and contract on t, a rename chain on u.
            format!("ALTER TABLE {sc}.t DROP COLUMN b"),
            format!("ALTER TABLE {sc}.u RENAME COLUMN a TO x"),
            format!("ALTER TABLE {sc}.u RENAME COLUMN b TO a"),
        ],
    )
    .await;
    let hints = signals(&mut s, 2).await;
    let (delta, new) = c
        .augment(cached(&r.tables, "t"), &hints["t"])
        .await
        .unwrap();
    let new = new.unwrap();
    assert_eq!(
        delta.changes,
        [SchemaChange::AddColumn(new.column("c").unwrap().clone())],
        "the later drop isn't in this change"
    );
    match c.augment(cached(&r.tables, "u"), &hints["u"]).await {
        Err(SourceError::TableHalt {
            reason: HaltReason::SchemaMismatch { .. },
            ..
        }) => {}
        other => panic!("a guess halts the table: {other:?}"),
    }
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// `Schema_Change…` §7.2 (Q-M5-17): a primary key on other columns is an identifier change
/// (Unsafe); REPLICA IDENTITY FULL over the same key changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn channel_c_classifies_identity_changes() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_c_identity",
        "CREATE TABLE k (id int PRIMARY KEY, code int NOT NULL);
         CREATE TABLE f (id int PRIMARY KEY, v int);",
    )
    .await;
    let c: Box<dyn SourceConnector> = Box::new(connector(&pipeline(&n, &["*"], "", "")));
    let r = c.resolve_tables().await.unwrap();
    let p = c
        .prepare(None, &[n.table("k"), n.table("f")])
        .await
        .unwrap();
    let StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("ALTER TABLE {sc}.k DROP CONSTRAINT k_pkey, ADD PRIMARY KEY (code)"),
            format!("INSERT INTO {sc}.k VALUES (1, 1)"),
            // A change first: the session has seen f under its primary key.
            format!("INSERT INTO {sc}.f VALUES (0, 0)"),
            format!("ALTER TABLE {sc}.f REPLICA IDENTITY FULL"),
            format!("INSERT INTO {sc}.f VALUES (1, 1)"),
        ],
    )
    .await;
    let hints = signals(&mut s, 2).await;
    let k = cached(&r.tables, "k");
    let (delta, _) = c.augment(k, &hints["k"]).await.unwrap();
    assert!(
        delta.changes.contains(&SchemaChange::IdentifierChanged),
        "{delta:?}"
    );
    assert!(
        classify(k, &delta)
            .iter()
            .any(|(_, class)| matches!(class, DeltaClass::Unsafe(_)))
    );
    let (delta, _) = c
        .augment(cached(&r.tables, "f"), &hints["f"])
        .await
        .unwrap();
    assert!(delta.changes.is_empty(), "{delta:?}");
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}
