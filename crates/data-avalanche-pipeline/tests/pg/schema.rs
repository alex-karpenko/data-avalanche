use arrow_array::{Array, Int32Array};
use data_avalanche_core::HaltReason;
use data_avalanche_testkit::require_it;

use crate::common::{Lake, admin, cleanup, converge, fresh, pipeline, pipeline_with, start};

#[tokio::test]
async fn an_added_column_reaches_the_sink() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "add_column",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a');",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(&pipeline(&n, &["t"], "mode: initial"), &lake, None);
    run.snapshotted().await;
    converge(&a, &lake, &t).await;
    a.batch_execute(&format!(
        "ALTER TABLE {t} ADD COLUMN w int; INSERT INTO {t} VALUES (2, 'b', 7);"
    ))
    .await
    .unwrap();
    converge(&a, &lake, &t).await;
    let b = lake.sink.content(&t).unwrap();
    let w = b
        .column_by_name("w")
        .expect("the added column")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int");
    assert!(w.is_null(0), "the row from before the change");
    assert_eq!(w.value(1), 7);
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}

/// Channel D finds the table gone; it halts once the stream has passed the rescan, after its
/// last change is in the sink.
#[tokio::test]
async fn a_dropped_table_halts_and_the_others_go_on() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "drop_table",
        "CREATE TABLE gone (id bigint PRIMARY KEY, v text);
         CREATE TABLE kept (id bigint PRIMARY KEY, v text);",
    )
    .await;
    let (gone, kept) = (n.table("gone"), n.table("kept"));
    let lake = Lake::new();
    let p = pipeline_with(
        &n,
        &["gone", "kept"],
        "",
        "\n    schema_evolution: { rescan_interval: 1s }",
    );
    let run = start(&p, &lake, None);
    run.snapshotted().await;
    for sql in [
        format!("INSERT INTO {gone} VALUES (1, 'x')"),
        format!("DROP TABLE {gone}"),
        format!("INSERT INTO {kept} VALUES (1, 'y')"),
    ] {
        a.batch_execute(&sql).await.unwrap();
    }
    run.wait("the dropped table halts", |st| {
        st.halted.get(&gone) == Some(&HaltReason::TableDropped)
    })
    .await;
    assert_eq!(lake.rows(&gone), [(1, Some("x".into()))], "its last change");
    a.batch_execute(&format!("INSERT INTO {kept} VALUES (2, 'z')"))
        .await
        .unwrap();
    converge(&a, &lake, &kept).await;
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
