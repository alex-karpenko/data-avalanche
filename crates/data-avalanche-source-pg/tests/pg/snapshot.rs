//! Snapshot chunks and keyed re-reads against a live server (Snapshot_Resumability §1–§2).

use data_avalanche_core::{
    ColumnValue as V, KeysetCursor, Row, SourceError, TableSchema, identifier_values,
};
use data_avalanche_source_pg::{FETCH_BATCH, PgConnector};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, connector, fresh, pipeline, resolved};

/// Every row of `table`, chunk by chunk.
async fn read_all(c: &PgConnector, table: &TableSchema, limit: usize) -> (Vec<Row>, usize) {
    let mut rows = vec![];
    let mut cursor: Option<KeysetCursor> = None;
    let mut chunks = 0;
    loop {
        let chunk = c
            .read_chunk(table, cursor.as_ref(), limit)
            .await
            .expect("chunk");
        chunks += 1;
        assert!(chunk.rows.len() <= limit);
        let Some(next) = chunk.next else {
            assert!(chunk.rows.is_empty());
            return (rows, chunks);
        };
        assert_eq!(
            Some(&next.key),
            identifier_values(table, chunk.rows.last().unwrap()).as_ref()
        );
        rows.extend(chunk.rows);
        cursor = Some(next);
    }
}

#[tokio::test]
async fn chunks_walk_the_table_in_key_order() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks",
        "CREATE TABLE t (id int PRIMARY KEY, v text);
         INSERT INTO t SELECT g, 'v' || g FROM generate_series(25, 1, -1) g;",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = &resolved(&c).await.tables[0];
    let (rows, chunks) = read_all(&c, t, 10).await;
    assert_eq!(chunks, 4, "10 + 10 + 5, then an empty chunk");
    let ids: Vec<&V> = rows.iter().map(|r| r.get("id").unwrap()).collect();
    assert_eq!(
        ids,
        (1..=25)
            .map(V::Int32)
            .collect::<Vec<_>>()
            .iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        rows[0],
        Row(vec![
            ("id".into(), V::Int32(1)),
            ("v".into(), V::Text("v1".into()))
        ])
    );
    let first = c.read_chunk(t, None, 3).await.unwrap();
    assert_eq!(
        first.next.unwrap().collation,
        None,
        "no text key, no collation"
    );
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn text_keys_page_in_the_c_collation() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks_c",
        "CREATE TABLE t (code text COLLATE \"en_US.utf8\" PRIMARY KEY);
         INSERT INTO t VALUES ('b'), ('B'), ('a'), ('Z'), ('é'), ('a b'), ('ab');",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = &resolved(&c).await.tables[0];
    let (rows, _) = read_all(&c, t, 2).await;
    let codes: Vec<String> = rows
        .iter()
        .map(|r| match r.get("code") {
            Some(V::Text(s)) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    let mut bytewise = codes.clone();
    bytewise.sort();
    assert_eq!(
        codes, bytewise,
        "byte order, whatever the column's collation"
    );
    assert_eq!(codes.len(), 7, "every row once");
    let chunk = c.read_chunk(t, None, 2).await.unwrap();
    assert_eq!(chunk.next.unwrap().collation.as_deref(), Some("C"));
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn composite_keys_of_every_key_type_resume_exactly() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks_keys",
        "CREATE TABLE t (d date, ts timestamptz, n numeric(10,2), u uuid, b bytea, c \"char\",
                         v varchar(5), PRIMARY KEY (d, ts, n, u, b, c, v));
         INSERT INTO t
         SELECT d, ts, n, u, b, c, v
           FROM (VALUES ('-infinity'::date), ('0044-03-15 BC'), ('2024-02-29'), ('infinity')) AS d(d),
                (VALUES ('-infinity'::timestamptz), ('2024-02-29 12:34:56.789012+00'), ('0001-01-01 00:00:00+00 BC')) AS ts(ts),
                (VALUES (-1.5), (0), (12345678.99)) AS n(n),
                (VALUES ('00000000-0000-0000-0000-000000000001'::uuid)) AS u(u),
                (VALUES ('\\x00'::bytea), ('\\xff')) AS b(b),
                (VALUES ('a'::\"char\"), ('\\377')) AS c(c),
                (VALUES ('x'), ('x y')) AS v(v);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = &resolved(&c).await.tables[0];
    let expected: i64 = a
        .query_one(&format!("SELECT count(*) FROM {}.t", n.schema), &[])
        .await
        .unwrap()
        .get(0);
    let (one_by_one, _) = read_all(&c, t, 1).await;
    let (all, _) = read_all(&c, t, 1000).await;
    assert_eq!(
        one_by_one.len() as i64,
        expected,
        "the cursor never skips or repeats a row"
    );
    assert_eq!(one_by_one, all);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn fetch_rows_rereads_keys_in_batches() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "fetch_rows",
        "CREATE TABLE t (k text, id int, doc text, PRIMARY KEY (k, id));
         INSERT INTO t SELECT 'k''' || (g % 3), g, repeat('d', g) FROM generate_series(1, 2500) g;",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = &resolved(&c).await.tables[0];
    let key = |k: &str, id: i32| {
        Row(vec![
            ("k".into(), V::Text(k.into())),
            ("id".into(), V::Int32(id)),
        ])
    };
    let mut keys: Vec<Row> = (1..=2500)
        .map(|g| key(&format!("k'{}", g % 3), g))
        .collect();
    keys.push(key("k'1", 99_999));
    assert!(keys.len() > 2 * FETCH_BATCH);
    let rows = c.fetch_rows(t, &keys).await.unwrap();
    assert_eq!(rows.len(), 2500, "the missing key is left out");
    let r7 = rows
        .iter()
        .find(|r| r.get("id") == Some(&V::Int32(7)))
        .unwrap();
    assert_eq!(r7.get("doc"), Some(&V::Text("d".repeat(7))));
    assert!(c.fetch_rows(t, &[]).await.unwrap().is_empty());
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_table_changed_under_the_snapshot_is_retried() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks_drift",
        "CREATE TABLE t (id int PRIMARY KEY, v int, w int);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = resolved(&c).await.tables[0].clone();
    a.batch_execute(&format!("ALTER TABLE {}.t DROP COLUMN w", n.schema))
        .await
        .unwrap();
    match c.read_chunk(&t, None, 10).await {
        Err(SourceError::Transient(m)) => assert!(m.contains("column w is gone"), "{m}"),
        other => panic!("transient expected: {other:?}"),
    }
    a.batch_execute(&format!(
        "ALTER TABLE {}.t ALTER COLUMN v TYPE text",
        n.schema
    ))
    .await
    .unwrap();
    let t = resolved(&c).await.tables[0].clone();
    assert!(
        c.read_chunk(&t, None, 10).await.is_ok(),
        "the current schema reads"
    );
    cleanup(&a, &n).await;
}
