use std::time::Duration;

use arrow_array::{Array, Int32Array, StringArray};
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

/// Channel D finds the table gone; it halts once the stream has passed the rescan, and its last
/// change reaches the sink. The halt is in the status when it is handed to the writer, before the
/// flush ahead of it is durable, so the sink is polled.
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
        "",
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while lake.rows(&gone) != [(1, Some("x".into()))] {
        assert!(
            tokio::time::Instant::now() < deadline,
            "its last change: {:?}",
            lake.rows(&gone)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    a.batch_execute(&format!("INSERT INTO {kept} VALUES (2, 'z')"))
        .await
        .unwrap();
    converge(&a, &lake, &kept).await;
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}

/// A column is dropped while the table's snapshot runs (Task 14, the run-time case of
/// D-M6a.11): the next chunk read under the cached schema fails transiently ("column extra is
/// gone"), the rescan the failure asks for finds the drop, and once the stream has passed it the
/// snapshot reads on under the new schema. The sink soft-drops `extra`: it keeps the column, and
/// the rows read after the drop have it NULL. Without the rescan the reads would fail until the
/// 60-minute periodic rescan.
#[tokio::test]
async fn a_column_dropped_while_the_snapshot_runs_is_soft_dropped() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "drop_mid_snapshot",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text, extra text);
         INSERT INTO t SELECT i, 'v' || i, 'x' || i FROM generate_series(1, 200) i;",
    )
    .await;
    let t = n.table("t");
    let p = pipeline(&n, &["t"], "mode: initial, chunk_size: 1");
    let lake = Lake::new();
    let run = start(&p, &lake, None);
    run.wait("the snapshot has emitted a row", |st| {
        st.snapshots.get(&t).is_some_and(|s| s.rows_done >= 1)
    })
    .await;
    a.batch_execute(&format!("ALTER TABLE {t} DROP COLUMN extra"))
        .await
        .unwrap();
    // One read at a time, a row each: rows 199 and 200 not emitted yet means the read of row 200
    // starts after the drop.
    let done = run
        .handle
        .status
        .borrow()
        .snapshots
        .get(&t)
        .map(|s| s.rows_done);
    assert!(
        done.is_some_and(|d| d <= 198),
        "the snapshot reads on after the drop: {done:?}"
    );
    run.snapshotted().await;
    converge(&a, &lake, &t).await;
    let st = run.handle.status.borrow().clone();
    assert!(st.halted.is_empty(), "{st:?}");
    let dropped = format!("table {t}: column \"extra\" soft-dropped: kept, written NULL");
    assert_eq!(st.warnings, [dropped], "{st:?}");
    let b = lake.sink.content(&t).unwrap();
    let extra = b
        .column_by_name("extra")
        .expect("soft-dropped: the sink keeps it")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("text");
    assert_eq!(extra.value(0), "x1", "read before the drop");
    assert!(extra.is_null(199), "read after the drop");
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}

/// An `always` table gains a column while the worker is down, and stays idle. PostgreSQL's chunk
/// read wouldn't fail (it checks the cached columns only), so without a rescan first the
/// snapshot would read the old columns and leave the new one NULL. The restart rescans before
/// it reads (D-M6a.11).
#[tokio::test]
async fn an_idle_table_that_gained_a_column_while_down_is_snapshotted_with_it() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "idle_add",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a');",
    )
    .await;
    let t = n.table("t");
    let p = pipeline(&n, &["t"], "mode: always");
    let lake = Lake::new();
    let run = start(&p, &lake, None);
    run.snapshotted().await;
    run.drain().await.unwrap();

    a.batch_execute(&format!("ALTER TABLE {t} ADD COLUMN w int DEFAULT 7"))
        .await
        .unwrap();
    let run = start(&p, &lake, None);
    run.snapshotted().await;
    let b = lake.sink.content(&t).unwrap();
    let w = b
        .column_by_name("w")
        .expect("the added column")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int");
    assert_eq!(w.value(0), 7, "the row read under the new schema");
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
