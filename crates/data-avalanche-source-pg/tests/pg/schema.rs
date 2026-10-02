//! Channels C and D against a live server, through `dyn SourceConnector` (`Schema_Change…` §3,
//! §6).

use data_avalanche_core::{
    ColumnValueType as V, DeltaClass, HaltReason, SchemaChange, SchemaHint, SourceConnector,
    StartPlan, StreamItem, TableRef, classify,
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
         CREATE TABLE w (id int PRIMARY KEY); CREATE TABLE same (id int PRIMARY KEY);",
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
    assert_eq!(
        report.halted,
        [(
            n.table("w"),
            HaltReason::IdentifierNotInReplicaIdentity {
                missing: vec!["id".into()]
            }
        )]
    );
    cleanup(&a, &n).await;
}
