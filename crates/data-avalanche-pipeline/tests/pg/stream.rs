use data_avalanche_pipeline::Exit;
use data_avalanche_testkit::require_it;

use crate::common::{Lake, admin, cleanup, converge, fresh, pipeline, start};

#[tokio::test]
async fn inserts_updates_deletes_and_key_changes_reach_the_sink() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "dml",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(&pipeline(&n, &["t"], "mode: initial"), &lake, None);
    converge(&a, &lake, &t).await;
    // One statement per call: a multi-statement simple query is a single transaction.
    for sql in [
        format!("INSERT INTO {t} VALUES (4, 'd')"),
        format!(
            "BEGIN; UPDATE {t} SET v = NULL WHERE id = 1; DELETE FROM {t} WHERE id = 3; COMMIT"
        ),
        format!("UPDATE {t} SET id = 20 WHERE id = 2"),
        format!(
            "BEGIN; INSERT INTO {t} VALUES (5, 'e'); UPDATE {t} SET v = 'E' WHERE id = 5; COMMIT"
        ),
        format!("BEGIN; INSERT INTO {t} VALUES (6, 'f'); DELETE FROM {t} WHERE id = 6; COMMIT"),
    ] {
        a.batch_execute(&sql).await.unwrap();
    }
    converge(&a, &lake, &t).await;
    assert_eq!(
        lake.rows(&t),
        [
            (1, None),
            (4, Some("d".into())),
            (5, Some("E".into())),
            (20, Some("b".into()))
        ]
    );
    assert_eq!(run.drain().await.unwrap(), Exit::Drained);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_truncate_empties_the_sink_table() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "truncate",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);
         INSERT INTO t VALUES (1, 'a'), (2, 'b');",
    )
    .await;
    let t = n.table("t");
    let lake = Lake::new();
    let run = start(&pipeline(&n, &["t"], "mode: initial"), &lake, None);
    converge(&a, &lake, &t).await;
    a.batch_execute(&format!(
        "BEGIN; TRUNCATE {t}; INSERT INTO {t} VALUES (3, 'c'); COMMIT"
    ))
    .await
    .unwrap();
    converge(&a, &lake, &t).await;
    assert_eq!(lake.rows(&t), [(3, Some("c".into()))]);
    run.drain().await.unwrap();
    cleanup(&a, &n).await;
}
