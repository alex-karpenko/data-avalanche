//! Snapshot chunks and keyed re-reads against a live server (Snapshot_Resumability §1–§2).

use std::time::Duration;

use data_avalanche_core::{
    ChunkVisibility, ColumnValue as V, HaltReason, KeysetCursor, Row, SourceError, TableSchema,
    identifier_values,
};
use data_avalanche_source_pg::{FETCH_BATCH, PgConnector};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, connector, fresh, pipeline, resolved};

/// DA-SNAP-9: a chunk is read in one REPEATABLE READ transaction with its snapshot, which doesn't
/// see a transaction still open; `in_flight` lists that transaction with the table it writes.
/// Once it commits, the next chunk sees it and its row.
#[tokio::test]
async fn a_chunk_carries_its_snapshot_and_in_flight_lists_open_writers() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "snapshot_visibility",
        "CREATE TABLE t (id int PRIMARY KEY, v int NOT NULL);
         INSERT INTO t VALUES (1, 0), (2, 0);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let r = resolved(&c).await;
    let t = &r.tables[0];
    let oid = t.source_id().expect("PostgreSQL tables carry their OID");
    let w = admin().await;
    w.batch_execute(&format!(
        "BEGIN; UPDATE {}.t SET v = 1 WHERE id = 1",
        n.schema
    ))
    .await
    .unwrap();
    let xid: String = w
        .query_one("SELECT pg_current_xact_id()::text", &[])
        .await
        .unwrap()
        .get(0);
    let full: u64 = xid.parse().unwrap();
    let short = full & 0xFFFF_FFFF;

    let open = c.in_flight().await.unwrap().expect("PostgreSQL lists them");
    let writer = open
        .iter()
        .find(|x| x.xid == short)
        .unwrap_or_else(|| panic!("{short} in flight: {open:?}"));
    assert!(writer.tables.contains(&oid), "{writer:?} writes {oid}");
    let before = c.read_chunk(t, None, 10).await.unwrap();
    let Some(ChunkVisibility::Xids(s)) = &before.visibility else {
        panic!("a PostgreSQL chunk carries its snapshot: {before:?}")
    };
    assert!(!s.sees(short) && !s.sees(full), "{s:?} doesn't see {full}");
    assert_eq!(before.rows[0].get("v"), Some(&V::Int32(0)));

    w.batch_execute("COMMIT").await.unwrap();
    let after = c.read_chunk(t, None, 10).await.unwrap();
    let Some(ChunkVisibility::Xids(s)) = &after.visibility else {
        panic!("{after:?}")
    };
    assert!(s.sees(short), "{s:?} sees the committed {short}");
    assert_eq!(after.rows[0].get("v"), Some(&V::Int32(1)));
    let open = c.in_flight().await.unwrap().unwrap();
    assert!(open.iter().all(|x| x.xid != short), "{open:?}");
    cleanup(&a, &n).await;
}

/// Final review I3: a table dropped under the snapshot halts that table (`TABLE_DROPPED`), and so
/// does a key column that lost NOT NULL (`IDENTIFIER_NOT_USABLE`): a NULL key sorts after every
/// cursor, and its rows would be skipped. Neither stops the pipeline.
#[tokio::test]
async fn a_dropped_table_or_a_null_key_halts_only_its_table() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "snapshot_halts",
        "CREATE TABLE d (id int PRIMARY KEY);
         CREATE TABLE k (k int NOT NULL, v int);
         ALTER TABLE k REPLICA IDENTITY FULL;",
    )
    .await;
    let tables = format!("      primary_key: {{ {}.k: [k] }}", n.schema);
    let c = connector(&pipeline(&n, &["*"], &tables, ""));
    let r = resolved(&c).await;
    let table = |name: &str| r.tables.iter().find(|t| t.table().name == name).unwrap();
    let sc = &n.schema;
    a.batch_execute(&format!(
        "DROP TABLE {sc}.d;
         ALTER TABLE {sc}.k ALTER COLUMN k DROP NOT NULL;
         INSERT INTO {sc}.k VALUES (1, 1), (NULL, 2);"
    ))
    .await
    .unwrap();
    let dropped = |r: Result<(), SourceError>| {
        matches!(
            r,
            Err(SourceError::TableHalt {
                reason: HaltReason::TableDropped,
                ..
            })
        )
    };
    let chunk = c.read_chunk(table("d"), None, 10).await.map(|_| ());
    assert!(dropped(chunk.clone()), "read_chunk: {chunk:?}");
    let key = Row(vec![("id".into(), V::Int32(1))]);
    let rows = c.fetch_rows(table("d"), &[key]).await.map(|_| ());
    assert!(dropped(rows.clone()), "fetch_rows: {rows:?}");
    // The NULL row is the last of three: a cursor never reaches it.
    a.execute(&format!("INSERT INTO {sc}.k VALUES (2, 3)"), &[])
        .await
        .unwrap();
    match c.read_chunk(table("k"), None, 1).await {
        Err(SourceError::TableHalt {
            reason: HaltReason::IdentifierNotUsable { column, .. },
            ..
        }) => assert_eq!(column, "k"),
        other => panic!("a nullable key halts the table: {other:?}"),
    }
    // The table's schema dropped: `to_regclass` finds nothing (the second round's re-review).
    a.batch_execute(&format!("DROP SCHEMA {sc} CASCADE"))
        .await
        .unwrap();
    let chunk = c.read_chunk(table("k"), None, 10).await.map(|_| ());
    assert!(dropped(chunk.clone()), "read_chunk, schema gone: {chunk:?}");
    let key = Row(vec![("k".into(), V::Int32(1))]);
    let rows = c.fetch_rows(table("k"), &[key]).await.map(|_| ());
    assert!(dropped(rows.clone()), "fetch_rows, schema gone: {rows:?}");
    cleanup(&a, &n).await;
}

/// PostgreSQL 18 sets `attnotnull` under `NOT NULL … NOT VALID`, though older rows may still be
/// NULL: such a key column is nullable, at resolve and in a snapshot read (the second round's
/// re-review). Older servers have no such constraint; the test passes there without running.
#[tokio::test]
async fn a_not_null_constraint_not_validated_is_nullable() {
    require_it!();
    let a = admin().await;
    let version: i32 = a
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await
        .unwrap()
        .get(0);
    if version < 180_000 {
        return;
    }
    let n = fresh(
        &a,
        "snapshot_not_valid",
        "CREATE TABLE r (k int, v int);
         INSERT INTO r VALUES (1, 1), (NULL, 2);
         ALTER TABLE r ADD CONSTRAINT r_nn NOT NULL k NOT VALID;
         ALTER TABLE r REPLICA IDENTITY FULL;
         CREATE TABLE s (k int NOT NULL, v int);
         ALTER TABLE s REPLICA IDENTITY FULL;",
    )
    .await;
    let tables = format!("      primary_key: {{ {0}.r: [k], {0}.s: [k] }}", n.schema);
    let c = connector(&pipeline(&n, &["r", "s"], &tables, ""));
    let r = resolved(&c).await;
    assert!(
        r.halted.iter().any(|(t, h)| t.name == "r"
            && matches!(h, HaltReason::IdentifierNotUsable { column, .. } if column == "k")),
        "{:?}",
        r.halted
    );
    let s = r.tables.iter().find(|t| t.table().name == "s").unwrap();
    let sc = &n.schema;
    a.batch_execute(&format!(
        "ALTER TABLE {sc}.s ALTER COLUMN k DROP NOT NULL;
         INSERT INTO {sc}.s VALUES (1, 1), (2, 2), (NULL, 3);
         ALTER TABLE {sc}.s ADD CONSTRAINT s_nn NOT NULL k NOT VALID;"
    ))
    .await
    .unwrap();
    match c.read_chunk(s, None, 1).await {
        Err(SourceError::TableHalt {
            reason: HaltReason::IdentifierNotUsable { column, .. },
            ..
        }) => assert_eq!(column, "k"),
        other => panic!("a key under a constraint not validated halts the table: {other:?}"),
    }
    cleanup(&a, &n).await;
}

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
