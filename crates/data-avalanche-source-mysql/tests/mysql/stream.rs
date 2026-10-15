//! The binlog stream against the target server (Data-Flow Phase 2b): transactions, positions and
//! resume; statements routed to signals, truncates and halts; table maps and row images checked;
//! heartbeats; and on MySQL compressed payloads, partial JSON, tagged GTIDs and XA.

use std::time::Duration;

use data_avalanche_core::{
    ChangeStream, ColumnValue, HaltReason, Op, SourceError, SourcePosition, StreamItem, TableSchema,
};
use data_avalanche_source_mysql::{Flavour, MySqlConnector, MySqlStream};
use data_avalanche_testkit::require_it;
use mysql_async::prelude::Queryable;

use crate::common::{cleanup, connector, fresh, pipeline, target};

/// Items until `commits` transactions committed (keep-alives left out).
async fn items(s: &mut MySqlStream, commits: usize) -> Vec<StreamItem> {
    let mut out = vec![];
    let mut seen = 0;
    while seen < commits {
        let item = tokio::time::timeout(Duration::from_secs(20), s.next())
            .await
            .expect("an item within 20 s")
            .expect("the stream");
        match item {
            StreamItem::KeepAlive(_) => continue,
            StreamItem::Commit(_) => seen += 1,
            _ => {}
        }
        out.push(item);
    }
    out
}

/// The first stream error.
async fn error(s: &mut MySqlStream) -> SourceError {
    loop {
        match tokio::time::timeout(Duration::from_secs(20), s.next())
            .await
            .expect("an item within 20 s")
        {
            Ok(_) => continue,
            Err(e) => return e,
        }
    }
}

async fn shut(s: MySqlStream) {
    Box::new(s).close().await.expect("close");
}

async fn open(c: &MySqlConnector, tables: &[TableSchema]) -> MySqlStream {
    let from = c.position_now().await.expect("position");
    c.open_stream(&from, tables).await.expect("stream")
}

fn changes(items: &[StreamItem]) -> Vec<(Op, Option<ColumnValue>)> {
    items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => {
                let image = e.after.as_ref().or(e.before.as_ref()).unwrap();
                Some((e.op, image.get("id").cloned()))
            }
            _ => None,
        })
        .collect()
}

fn commits(items: &[StreamItem]) -> Vec<SourcePosition> {
    items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Commit(b) => Some(b.commit_pos.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn changes_arrive_as_transactions_and_resume_after_a_commit() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_txn",
        "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10)); CREATE TABLE other (id INT PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    a.query_drop(
        "BEGIN; INSERT INTO t VALUES (1, 'a'), (2, 'b'); COMMIT;
         INSERT INTO other VALUES (1);
         UPDATE t SET v = 'c' WHERE id = 1;
         DELETE FROM t WHERE id = 2;",
    )
    .await
    .unwrap();
    let got = items(&mut s, 3).await;
    let one = Some(ColumnValue::Int32(1));
    let two = Some(ColumnValue::Int32(2));
    assert_eq!(
        changes(&got),
        [
            (Op::Insert, one.clone()),
            (Op::Insert, two.clone()),
            (Op::Update, one.clone()),
            (Op::Delete, two)
        ]
    );
    assert!(matches!(got[0], StreamItem::Begin { .. }));
    // The update carries both images; the order key grows, equal only within a transaction.
    let events: Vec<_> = got
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => Some(e.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        events[2].before.as_ref().unwrap().get("v"),
        Some(&ColumnValue::Text("a".into()))
    );
    assert!(events[0].change_seq <= events[1].change_seq);
    assert!(events[1].change_seq < events[2].change_seq);
    assert!(events[2].change_seq < events[3].change_seq);
    // Each commit covers the one before it.
    let pos = commits(&got);
    assert!(pos[1].covers(&pos[0]).unwrap() && pos[2].covers(&pos[1]).unwrap());
    // A row's position is its transaction's own; the commit's covers it.
    assert!(pos[0].covers(&events[0].source_pos).unwrap());
    assert_eq!(events[0].source_pos, events[1].source_pos);
    shut(s).await;
    // Resume after the second commit: the third transaction, once.
    let mut s = c.open_stream(&pos[1], &tables).await.unwrap();
    let again = items(&mut s, 1).await;
    assert_eq!(changes(&again), [(Op::Delete, Some(ColumnValue::Int32(2)))]);
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn statements_route_to_signals_truncates_and_halts() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_ddl",
        "CREATE TABLE t (id INT PRIMARY KEY, v INT);
         CREATE TABLE gone (id INT PRIMARY KEY);
         CREATE TABLE parts (id INT PRIMARY KEY) PARTITION BY RANGE (id)
             (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE);
         CREATE TABLE emptied (id INT PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t", "gone", "parts", "emptied"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let cached_t = tables
        .iter()
        .find(|s| s.table().name == "t")
        .unwrap()
        .clone();
    let mut s = open(&c, &tables).await;
    a.query_drop(
        "ALTER TABLE t ADD COLUMN w VARCHAR(5) AFTER id;
         INSERT INTO t VALUES (1, 'x', 2);
         TRUNCATE emptied;
         DROP TABLE gone;
         ALTER TABLE parts DROP PARTITION p0;",
    )
    .await
    .unwrap();
    let got = items(&mut s, 5).await;
    let signal = got.iter().find_map(|i| match i {
        StreamItem::SchemaSignal { table, hint, .. } => Some((table.name.clone(), hint.clone())),
        _ => None,
    });
    let (table, hint) = signal.expect("a schema signal");
    assert_eq!(table, "t");
    // The pipeline's augment gets the answer the stream read.
    let (delta, now) = c.augment(&cached_t, &hint).await.unwrap();
    assert_eq!(delta.changes.len(), 1, "{:?}", delta.changes);
    assert_eq!(now.unwrap().columns()[1].name, "w");
    let row = got
        .iter()
        .find_map(|i| match i {
            StreamItem::Change(e) => e.after.clone(),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        row.get("w"),
        Some(&ColumnValue::Text("x".into())),
        "decoded by the new schema"
    );
    assert_eq!(row.get("v"), Some(&ColumnValue::Int32(2)));
    let truncated = got
        .iter()
        .any(|i| matches!(i, StreamItem::Truncate { tables, .. } if tables[0].name == "emptied"));
    assert!(truncated);
    let halts: Vec<(String, &str)> = got
        .iter()
        .filter_map(|i| match i {
            StreamItem::TableHalt { table, reason, .. } => {
                Some((table.name.clone(), reason.code()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        halts,
        [
            ("gone".to_owned(), "TABLE_DROPPED"),
            ("parts".to_owned(), "UNSAFE_DDL")
        ]
    );
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn a_cut_over_swap_is_followed() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_swap",
        "CREATE TABLE t (id INT PRIMARY KEY, v INT); INSERT INTO t VALUES (1, 1);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    // gh-ost: the shadow table gets the rows and the new column, then swaps in.
    a.query_drop(
        "CREATE TABLE _t_gho (id INT PRIMARY KEY, v INT, w INT);
         INSERT INTO _t_gho SELECT id, v, 7 FROM t;
         RENAME TABLE t TO _t_del, _t_gho TO t;
         INSERT INTO t VALUES (2, 2, 8);",
    )
    .await
    .unwrap();
    let got = items(&mut s, 2).await;
    assert!(
        got.iter()
            .any(|i| matches!(i, StreamItem::SchemaSignal { table, .. } if table.name == "t"))
    );
    assert_eq!(
        changes(&got),
        [(Op::Insert, Some(ColumnValue::Int32(2)))],
        "the shadow's own rows aren't tracked"
    );
    let row = got.iter().find_map(|i| match i {
        StreamItem::Change(e) => e.after.clone(),
        _ => None,
    });
    assert_eq!(row.unwrap().get("w"), Some(&ColumnValue::Int32(8)));
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn a_table_map_or_row_image_the_schema_doesnt_read_halts_the_table() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_map",
        "CREATE TABLE stale (id INT PRIMARY KEY, v INT); CREATE TABLE minimal (id INT PRIMARY KEY, v INT);
         INSERT INTO minimal VALUES (1, 1);",
    )
    .await;
    let c = connector(&pipeline(&n, &["stale", "minimal"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    // The schema is behind the server: its statement is before the stream's start.
    a.query_drop("ALTER TABLE stale ADD COLUMN w INT")
        .await
        .unwrap();
    let mut s = open(&c, &tables).await;
    a.query_drop(
        "INSERT INTO stale VALUES (1, 1, 1);
         SET SESSION binlog_row_image = MINIMAL; UPDATE minimal SET v = 2 WHERE id = 1;
         SET SESSION binlog_row_image = FULL;",
    )
    .await
    .unwrap();
    let got = items(&mut s, 2).await;
    let halts: Vec<(String, String)> = got
        .iter()
        .filter_map(|i| match i {
            StreamItem::TableHalt {
                table,
                reason: HaltReason::SchemaMismatch { detail, .. },
                ..
            } => Some((table.name.clone(), detail.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(halts.len(), 2, "{halts:?}");
    assert!(halts[0].1.contains("3 columns, the schema 2"), "{halts:?}");
    assert!(halts[1].1.contains("isn't FULL"), "{halts:?}");
    assert!(changes(&got).is_empty());
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn an_idle_stream_keeps_alive() {
    require_it!();
    let (n, _a) = fresh("stream_idle", "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    let from = c.position_now().await.unwrap();
    // Other tests write meanwhile: a keep-alive comes from a heartbeat or a skipped write.
    let item = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let StreamItem::KeepAlive(p) = s.next().await.unwrap() {
                return p;
            }
        }
    })
    .await
    .expect("a keep-alive within 5 s at a 1 s heartbeat");
    assert!(item.covers(&from).unwrap() || from.covers(&item).unwrap());
    shut(s).await;
    cleanup(&n).await;
}

#[tokio::test]
async fn mysql_payloads_partial_json_and_tagged_gtids() {
    require_it!();
    if target().flavour != Flavour::Mysql {
        return;
    }
    let (n, mut a) = fresh(
        "stream_mysql",
        "CREATE TABLE t (id INT PRIMARY KEY, j JSON); INSERT INTO t VALUES (1, '{\"a\": 1, \"b\": [1, 2]}');",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    let tagged = target().replication == "gtid";
    a.query_drop(
        "SET SESSION binlog_transaction_compression = ON;
         INSERT INTO t VALUES (2, '[]'), (3, '{}');
         SET SESSION binlog_transaction_compression = OFF;
         SET SESSION binlog_row_value_options = 'PARTIAL_JSON';
         UPDATE t SET j = JSON_SET(j, '$.a', 5, '$.b[1]', 9) WHERE id = 1;
         SET SESSION binlog_row_value_options = '';",
    )
    .await
    .unwrap();
    if tagged {
        a.query_drop(
            "SET gtid_next = 'AUTOMATIC:m7a'; INSERT INTO t VALUES (4, 'null'); SET gtid_next = 'AUTOMATIC';",
        )
        .await
        .unwrap();
    }
    let got = items(&mut s, if tagged { 3 } else { 2 }).await;
    let ids: Vec<_> = changes(&got).into_iter().map(|(_, id)| id).collect();
    assert_eq!(
        ids[..3],
        [
            Some(ColumnValue::Int32(2)),
            Some(ColumnValue::Int32(3)),
            Some(ColumnValue::Int32(1))
        ]
    );
    let updated = got
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) if e.op == Op::Update => e.after.clone(),
            _ => None,
        })
        .next()
        .unwrap();
    assert_eq!(
        updated.get("j"),
        Some(&ColumnValue::Json(serde_json::json!({"a": 5, "b": [1, 9]})))
    );
    if tagged {
        let last = commits(&got).pop().unwrap();
        assert!(last.untagged().contains(":m7a:"), "{last}");
        shut(s).await;
        // A tagged set resumes.
        let mut s = c.open_stream(&last, &tables).await.unwrap();
        a.query_drop("INSERT INTO t VALUES (5, 'null')")
            .await
            .unwrap();
        assert_eq!(
            changes(&items(&mut s, 1).await),
            [(Op::Insert, Some(ColumnValue::Int32(5)))]
        );
        shut(s).await;
    } else {
        shut(s).await;
    }
    cleanup(&n).await;
}

#[tokio::test]
async fn a_two_phase_xa_of_tracked_rows_halts_the_stream() {
    require_it!();
    let (n, mut a) = fresh(
        "stream_xa",
        "CREATE TABLE t (id INT PRIMARY KEY); CREATE TABLE other (id INT PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    // Untracked two-phase XA, and a one-phase commit of tracked rows: both stream on.
    a.query_drop(
        "XA START 'u'; INSERT INTO other VALUES (1); XA END 'u'; XA PREPARE 'u'; XA COMMIT 'u';
         XA START 'one'; INSERT INTO t VALUES (1); XA END 'one'; XA COMMIT 'one' ONE PHASE;",
    )
    .await
    .unwrap();
    assert_eq!(
        changes(&items(&mut s, 1).await),
        [(Op::Insert, Some(ColumnValue::Int32(1)))]
    );
    a.query_drop(
        "XA START 'two'; INSERT INTO t VALUES (2); XA END 'two'; XA PREPARE 'two'; XA COMMIT 'two';",
    )
    .await
    .unwrap();
    let e = error(&mut s).await;
    assert!(
        matches!(&e, SourceError::Permanent(m) if m.contains("XA PREPARE")),
        "{e}"
    );
    cleanup(&n).await;
}

#[tokio::test]
async fn a_row_change_logged_as_its_statement_halts_the_stream() {
    require_it!();
    let (n, mut a) = fresh("stream_statement", "CREATE TABLE t (id INT PRIMARY KEY);").await;
    let other = format!("{}_o", n.db);
    a.query_drop(format!(
        "CREATE DATABASE `{other}`; CREATE TABLE `{other}`.u (id INT PRIMARY KEY);"
    ))
    .await
    .unwrap();
    let c = connector(&pipeline(&n, &["t"], "", ""));
    let tables = c.resolve_tables().await.unwrap().tables;
    let mut s = open(&c, &tables).await;
    // In another database, inside a transaction: not the source's, and the stream goes on.
    a.query_drop(format!(
        "USE `{other}`; SET SESSION binlog_format = 'STATEMENT';
         BEGIN; INSERT INTO u VALUES (1); COMMIT;
         SET SESSION binlog_format = 'ROW'; USE `{db}`; INSERT INTO t VALUES (1);",
        db = n.db
    ))
    .await
    .unwrap();
    assert_eq!(
        changes(&items(&mut s, 1).await),
        [(Op::Insert, Some(ColumnValue::Int32(1)))]
    );
    // In the source's database: its rows aren't in the binlog.
    a.query_drop("SET SESSION binlog_format = 'STATEMENT'; INSERT INTO t VALUES (2);")
        .await
        .unwrap();
    let e = error(&mut s).await;
    assert!(
        matches!(&e, SourceError::Permanent(m) if m.contains("logged as its statement")),
        "{e}"
    );
    a.query_drop(format!("DROP DATABASE `{other}`"))
        .await
        .unwrap();
    cleanup(&n).await;
}
