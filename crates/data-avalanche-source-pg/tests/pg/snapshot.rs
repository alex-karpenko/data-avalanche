//! Snapshot chunks and keyed re-reads against a live server (Snapshot_Resumability §1–§2).

use std::time::Duration;

use data_avalanche_core::{
    ColumnValue as V, HaltReason, KeysetCursor, Row, SourceError, TableSchema, identifier_values,
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
        assert!(chunks <= 10_000, "the cursor doesn't advance");
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

fn texts(rows: &[Row]) -> Vec<String> {
    rows.iter()
        .map(|r| match r.get("code") {
            Some(V::Text(s)) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

/// Snapshot_Resumability §2: a text key pages in its own collation when it is deterministic (its
/// index serves the order), in `"C"` when it isn't, with a warning at resolution. The cursor
/// records the collation; a cursor of another one halts the table.
#[tokio::test]
async fn text_keys_page_in_one_deterministic_collation() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks_collation",
        "CREATE COLLATION folding (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
         CREATE TABLE \"Codes\" (code text COLLATE \"en_US.utf8\" PRIMARY KEY);
         CREATE TABLE folded (code text COLLATE folding PRIMARY KEY);
         INSERT INTO \"Codes\" VALUES ('b'), ('B'), ('a'), ('Z'), ('é'), ('a b'), ('ab');
         INSERT INTO folded VALUES ('b'), ('A'), ('é'), ('Z'), ('a b');",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let r = resolved(&c).await;
    let table = |name: &str| {
        r.tables
            .iter()
            .find(|t| t.table().name == name)
            .unwrap()
            .clone()
    };
    let server = |sql: String| {
        let a = &a;
        async move {
            a.query(&sql, &[])
                .await
                .unwrap()
                .iter()
                .map(|r| r.get::<_, String>(0))
                .collect::<Vec<_>>()
        }
    };

    let codes = table("Codes");
    let (rows, _) = read_all(&c, &codes, 2).await;
    assert_eq!(
        texts(&rows),
        server(format!(
            "SELECT code FROM {}.\"Codes\" ORDER BY code",
            n.schema
        ))
        .await,
        "every row once, in the column's own collation"
    );
    let chunk = c.read_chunk(&codes, None, 2).await.unwrap();
    let cursor = chunk.next.unwrap();
    assert_eq!(cursor.collation.as_deref(), Some("en_US.utf8"));
    let stale = KeysetCursor {
        collation: Some("C".into()),
        ..cursor
    };
    match c.read_chunk(&codes, Some(&stale), 2).await {
        Err(SourceError::TableHalt {
            reason: HaltReason::SchemaMismatch { detail, .. },
            ..
        }) => assert!(detail.contains("re-snapshot"), "{detail}"),
        other => panic!("a cursor of another collation halts: {other:?}"),
    }

    let folded = table("folded");
    let (rows, _) = read_all(&c, &folded, 1).await;
    assert_eq!(
        texts(&rows),
        server(format!(
            "SELECT code FROM {}.folded ORDER BY code COLLATE \"C\"",
            n.schema
        ))
        .await,
        "a nondeterministic collation pages in byte order"
    );
    let chunk = c.read_chunk(&folded, None, 1).await.unwrap();
    assert_eq!(chunk.next.unwrap().collation.as_deref(), Some("C"));
    assert!(
        r.warnings
            .iter()
            .any(|w| w.contains(".folded.code") && w.contains("nondeterministic")),
        "{:?}",
        r.warnings
    );
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
                (VALUES ('a'::\"char\"), ((-1)::int4::\"char\")) AS c(c),
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
    let high = one_by_one
        .iter()
        .filter(|r| r.get("c") == Some(&V::Int16(255)))
        .count();
    assert_eq!(
        high as i64 * 2,
        expected,
        "the \"char\" byte 255 (PostgreSQL 14 prints it raw)"
    );
    cleanup(&a, &n).await;
}

/// A key of a domain over `"char"` travels as its byte too.
#[tokio::test]
async fn a_domain_over_char_key_resumes() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "chunks_domain",
        "CREATE DOMAIN flag AS \"char\";
         CREATE TABLE t (k flag PRIMARY KEY);
         INSERT INTO t VALUES ('a'), ('b'), ((-1)::int4::\"char\");",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let t = &resolved(&c).await.tables[0];
    let (rows, _) = read_all(&c, t, 1).await;
    let keys: Vec<&V> = rows.iter().map(|r| r.get("k").unwrap()).collect();
    assert_eq!(keys, [&V::Int16(97), &V::Int16(98), &V::Int16(255)]);
    let again = c.fetch_rows(t, &rows).await.unwrap();
    assert_eq!(again.len(), 3);
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
    a.batch_execute(&format!(
        "ALTER TABLE {}.t ALTER COLUMN v TYPE bigint USING 0",
        n.schema
    ))
    .await
    .unwrap();
    match c.read_chunk(&t, None, 10).await {
        Err(SourceError::Transient(m)) => assert!(m.contains("column v is now"), "{m}"),
        other => panic!("transient expected: {other:?}"),
    }
    let t = resolved(&c).await.tables[0].clone();
    assert!(
        c.read_chunk(&t, None, 10).await.is_ok(),
        "the current schema reads"
    );
    cleanup(&a, &n).await;
}

/// The server cancels a chunk read past `timeouts.command`, so the catalog connection the reads
/// share is free right after: a client-side timeout alone would leave the statement running.
#[tokio::test]
async fn a_timed_out_chunk_frees_the_connection() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "chunks_timeout", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let mut p = pipeline(&n, &["*"], "", "");
    p.source.timeouts.command = Duration::from_secs(1);
    let c = connector(&p);
    let t = &resolved(&c).await.tables[0];
    let locker = admin().await;
    locker
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {}.t IN ACCESS EXCLUSIVE MODE",
            n.schema
        ))
        .await
        .unwrap();
    match c.read_chunk(t, None, 10).await {
        Err(SourceError::Transient(m)) => assert!(m.contains("57014"), "{m}"),
        other => panic!("a statement timeout is transient: {other:?}"),
    }
    tokio::time::timeout(Duration::from_secs(2), c.current_position())
        .await
        .expect("the connection is free while the lock is still held")
        .unwrap();
    locker.batch_execute("ROLLBACK").await.unwrap();
    cleanup(&a, &n).await;
}
