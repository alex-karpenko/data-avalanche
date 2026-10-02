//! The change stream against a live server: the S-PG0…S-PG6 probe gates as regressions (the M5
//! exit), channel B signals, per-table halts and acknowledgments.

use std::time::Duration;

use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ChangeStream, ColumnValue as V, Decimal, HaltReason, Op, Row, SchemaHint,
    SourceError, SourcePosition, SourceSeq, StreamItem, TableRef,
};
use data_avalanche_testkit::require_it;

use crate::common::{admin, cleanup, collect, connector, each, first_stream, fresh, pipeline};

fn changes(items: &[StreamItem]) -> Vec<&CdcEvent> {
    items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => Some(e),
            _ => None,
        })
        .collect()
}

fn row(values: &[(&str, V)]) -> Row {
    Row(values
        .iter()
        .map(|(n, v)| (n.to_string(), v.clone()))
        .collect())
}

fn lsn(p: &SourcePosition) -> Lsn {
    match p {
        SourcePosition::Pg(l) => *l,
        other => panic!("a PostgreSQL position, got {other}"),
    }
}

/// S-PG0: one insert round-trips; the positions are the commit's.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg0_an_insert_round_trips() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg0",
        "CREATE TABLE t (id bigint PRIMARY KEY, v text);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, from) = first_stream(&c).await;
    a.execute(
        &format!("INSERT INTO {}.t VALUES (1, 'one')", n.schema),
        &[],
    )
    .await
    .unwrap();
    let items = collect(&mut s, 1, 20).await;
    let [
        StreamItem::Begin {
            xid,
            commit_ts_micros,
        },
        StreamItem::Change(e),
        StreamItem::Commit(b),
    ] = &items[..]
    else {
        panic!("Begin, Change, Commit: {items:?}")
    };
    assert_eq!(e.op, Op::Insert);
    assert_eq!(e.table, n.table("t"));
    assert_eq!(
        e.after,
        Some(row(&[("id", V::Int64(1)), ("v", V::Text("one".into()))]))
    );
    assert_eq!(e.before, None);
    e.validate().unwrap();
    assert_eq!((e.xid, b.xid), (*xid, *xid));
    assert_eq!(e.commit_ts_micros, *commit_ts_micros);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    assert!(
        (now - commit_ts_micros).abs() < 60_000_000,
        "a Unix-epoch commit time"
    );
    // `_lsn` is the commit record's LSN; the checkpoint is the commit's end, past it; the
    // change's own LSN is before both.
    let SourceSeq::Pg(seq) = e.change_seq else {
        panic!()
    };
    assert!(
        lsn(&from) <= seq && seq < lsn(&e.source_pos) && lsn(&e.source_pos) < lsn(&b.commit_pos)
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG1: every message the design consumes, with the before-image each replica identity sends.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg1_every_change_kind() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg1",
        "CREATE TABLE t (id int PRIMARY KEY, v text);
         CREATE TABLE f (id int PRIMARY KEY, v text);
         ALTER TABLE f REPLICA IDENTITY FULL;
         INSERT INTO t VALUES (1, 'a'), (2, 'b');
         INSERT INTO f VALUES (1, 'x');",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    a.batch_execute(&format!(
        "BEGIN;
         INSERT INTO {sc}.t VALUES (3, 'c');
         UPDATE {sc}.t SET v = 'a2' WHERE id = 1;
         UPDATE {sc}.t SET id = 20 WHERE id = 2;
         DELETE FROM {sc}.t WHERE id = 3;
         UPDATE {sc}.f SET v = 'y' WHERE id = 1;
         DELETE FROM {sc}.f WHERE id = 1;
         COMMIT;
         TRUNCATE {sc}.t, {sc}.f;"
    ))
    .await
    .unwrap();
    let items = collect(&mut s, 2, 20).await;
    let c = changes(&items);
    let id = |i: i32| ("id", V::Int32(i));
    let v = |s: &str| ("v", V::Text(s.into()));
    let shapes: Vec<(Op, Option<Row>, Option<Row>)> = c
        .iter()
        .map(|e| (e.op, e.before.clone(), e.after.clone()))
        .collect();
    assert_eq!(
        shapes,
        [
            (Op::Insert, None, Some(row(&[id(3), v("c")]))),
            (Op::Update, None, Some(row(&[id(1), v("a2")]))),
            (
                Op::Update,
                Some(row(&[id(2)])),
                Some(row(&[id(20), v("b")]))
            ),
            (Op::Delete, Some(row(&[id(3)])), None),
            (
                Op::Update,
                Some(row(&[id(1), v("x")])),
                Some(row(&[id(1), v("y")]))
            ),
            (Op::Delete, Some(row(&[id(1), v("y")])), None),
        ],
        "default identity: no old image unless the key moves, then the key only; FULL: the whole row"
    );
    let seqs: Vec<SourceSeq> = c.iter().map(|e| e.change_seq).collect();
    assert!(
        seqs.windows(2)
            .all(|w| w[0].partial_cmp(&w[1]) == Some(std::cmp::Ordering::Less))
    );
    let truncates: Vec<&Vec<TableRef>> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Truncate { tables, .. } => Some(tables),
            _ => None,
        })
        .collect();
    assert_eq!(
        truncates,
        [&vec![n.table("t"), n.table("f")]],
        "one TRUNCATE of both tables"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG2: an update that doesn't touch an out-of-line TOASTed column sends it unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg2_unchanged_toast() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg2",
        "CREATE TABLE t (id bigint PRIMARY KEY, big text, n int);
         ALTER TABLE t ALTER COLUMN big SET STORAGE EXTERNAL;",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t SELECT 1, string_agg(md5(g::text), ''), 0 FROM generate_series(1, 400) g"),
            format!("UPDATE {sc}.t SET n = 1 WHERE id = 1"),
        ],
    )
    .await;
    let items = collect(&mut s, 2, 20).await;
    let c = changes(&items);
    let big = |e: &CdcEvent| e.after.as_ref().unwrap().get("big").cloned();
    assert!(matches!(big(c[0]), Some(V::Text(t)) if t.len() == 12_800));
    assert_eq!(big(c[1]), Some(V::Unchanged));
    assert_eq!(c[1].after.as_ref().unwrap().get("n"), Some(&V::Int32(1)));
    c[1].validate().unwrap();
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG3: a transaction far beyond `logical_decoding_work_mem` arrives whole at its commit.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg3_a_large_transaction_arrives_whole() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg3",
        "CREATE TABLE t (id bigint PRIMARY KEY, pad text);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    a.execute(
        &format!(
            "INSERT INTO {}.t SELECT g, repeat('x', 200) FROM generate_series(1, 50000) g",
            n.schema
        ),
        &[],
    )
    .await
    .unwrap();
    let items = collect(&mut s, 1, 120).await;
    let begins = items
        .iter()
        .filter(|i| matches!(i, StreamItem::Begin { .. }))
        .count();
    let c = changes(&items);
    assert_eq!((begins, c.len()), (1, 50_000));
    assert_eq!(
        c[49_999].after.as_ref().unwrap().get("id"),
        Some(&V::Int64(50_000))
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG4: a restart from the acknowledged position replays the unacknowledged transaction only;
/// the acknowledgment reaches the slot.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg4_resume_from_the_acknowledged_position() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "s_pg4", "CREATE TABLE t (id bigint PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, tables, _) = first_stream(&c).await;
    a.execute(&format!("INSERT INTO {}.t VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    let first = collect(&mut s, 1, 20).await;
    let Some(StreamItem::Commit(b)) = first.last() else {
        panic!("{first:?}")
    };
    s.acknowledge(&b.commit_pos).await.unwrap();
    s.acknowledge(&SourcePosition::Pg(Lsn(1))).await.unwrap(); // behind: ignored
    a.execute(&format!("INSERT INTO {}.t VALUES (2)", n.schema), &[])
        .await
        .unwrap();
    collect(&mut s, 1, 20).await; // received, not acknowledged
    let mut confirmed = None;
    for _ in 0..50 {
        let r = a
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
                &[&n.slot],
            )
            .await
            .unwrap();
        confirmed = Some(SourcePosition::Pg(r.get::<_, String>(0).parse().unwrap()));
        if confirmed.as_ref() == Some(&b.commit_pos) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        confirmed.as_ref(),
        Some(&b.commit_pos),
        "the status update carried the acknowledgment"
    );
    s.close().await.unwrap();

    let mut s = c.open_stream(&b.commit_pos, &tables).await.unwrap();
    let again = collect(&mut s, 1, 20).await;
    let ids: Vec<&V> = changes(&again)
        .iter()
        .map(|e| e.after.as_ref().unwrap().get("id").unwrap())
        .collect();
    assert_eq!(
        ids,
        [&V::Int64(2)],
        "the acknowledged transaction isn't sent again"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG5: a consumer stalled past `wal_sender_timeout` with the client's buffer full keeps its
/// connection (the replication crate keeps sending status updates).
#[tokio::test(flavor = "multi_thread")]
async fn s_pg5_a_stalled_consumer_survives() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "s_pg5", "CREATE TABLE t (id bigint PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    // Server-wide, so other tests' streams see it too: their status interval is 1 s. The stalled
    // half runs as its own task, so the setting is reset when it fails too.
    a.batch_execute("ALTER SYSTEM SET wal_sender_timeout = '3s'")
        .await
        .unwrap();
    a.batch_execute("SELECT pg_reload_conf()").await.unwrap();
    let schema = n.schema.clone();
    let stalled = tokio::spawn(async move {
        // 10 000 changes overflow the client's 8 192-event buffer.
        admin()
            .await
            .execute(
                &format!("INSERT INTO {schema}.t SELECT g FROM generate_series(1, 10000) g"),
                &[],
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        let items = collect(&mut s, 1, 60).await;
        (s, items)
    })
    .await;
    a.batch_execute("ALTER SYSTEM RESET wal_sender_timeout")
        .await
        .unwrap();
    a.batch_execute("SELECT pg_reload_conf()").await.unwrap();
    let (s, items) = stalled.unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()));
    assert_eq!(changes(&items).len(), 10_000);
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// S-PG6: a table added to the publication mid-stream streams from the ALTER on, without a
/// restart.
#[tokio::test(flavor = "multi_thread")]
async fn s_pg6_a_table_added_to_the_publication_mid_stream() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "s_pg6",
        "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let r = crate::common::resolved(&c).await;
    let p = c.prepare(None, &[n.table("a")]).await.unwrap();
    let data_avalanche_core::StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let mut s = c.open_stream(&from, &r.tables).await.unwrap();
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.b VALUES (1)"),
            format!("INSERT INTO {sc}.a VALUES (1)"),
            format!("ALTER PUBLICATION {} ADD TABLE {sc}.b", n.publication),
            format!("INSERT INTO {sc}.b VALUES (2)"),
        ],
    )
    .await;
    let items = collect(&mut s, 2, 20).await;
    let got: Vec<(String, &V)> = changes(&items)
        .iter()
        .map(|e| {
            (
                e.table.name.clone(),
                e.after.as_ref().unwrap().get("id").unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        [("a".into(), &V::Int32(1)), ("b".into(), &V::Int32(2))]
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Q-M5-5: partitions replicate as tables. A write through the parent arrives under its leaf, a
/// row whose partition key moves it arrives as a delete in one leaf and an insert in the other, and
/// a TRUNCATE of the parent truncates the leaves.
#[tokio::test(flavor = "multi_thread")]
async fn partition_changes_arrive_under_their_leaves() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "stream_partitions",
        "CREATE TABLE events (id int, at date, note text, PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
         CREATE TABLE events_2024 PARTITION OF events FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
         CREATE TABLE events_2025 PARTITION OF events FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, tables, _) = first_stream(&c).await;
    let tracked: Vec<&TableRef> = tables.iter().map(|t| t.table()).collect();
    assert_eq!(
        tracked,
        [&n.table("events_2024"), &n.table("events_2025")],
        "the leaves, not the parent"
    );
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.events VALUES (1, '2024-06-01', 'a')"),
            format!("UPDATE {sc}.events SET at = '2025-06-01' WHERE id = 1"),
            format!("TRUNCATE {sc}.events"),
        ],
    )
    .await;
    let items = collect(&mut s, 3, 20).await;
    let id = ("id", V::Int32(1));
    let at = |days: i32| ("at", V::Date(days));
    let note = ("note", V::Text("a".into()));
    let shapes: Vec<(Op, &TableRef, Option<Row>, Option<Row>)> = changes(&items)
        .iter()
        .map(|e| (e.op, &e.table, e.before.clone(), e.after.clone()))
        .collect();
    assert_eq!(
        shapes,
        [
            (
                Op::Insert,
                &n.table("events_2024"),
                None,
                Some(row(&[id.clone(), at(19875), note.clone()]))
            ),
            (
                Op::Delete,
                &n.table("events_2024"),
                Some(row(&[id.clone(), at(19875)])),
                None
            ),
            (
                Op::Insert,
                &n.table("events_2025"),
                None,
                Some(row(&[id.clone(), at(20240), note.clone()]))
            ),
        ],
        "the moved row leaves one leaf and enters the other"
    );
    let truncates: Vec<&Vec<TableRef>> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Truncate { tables, .. } => Some(tables),
            _ => None,
        })
        .collect();
    assert_eq!(
        truncates,
        [&vec![n.table("events_2024"), n.table("events_2025")]],
        "a TRUNCATE of the parent truncates its leaves"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Channel B: a changed column list signals before the change that carries it; an unchanged one
/// doesn't.
#[tokio::test(flavor = "multi_thread")]
async fn relation_changes_signal_before_the_next_change() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "channel_b",
        "CREATE TABLE t (id int PRIMARY KEY, v int);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t VALUES (1, 1)"),
            format!("ALTER TABLE {sc}.t ADD COLUMN w text"),
            format!("INSERT INTO {sc}.t VALUES (2, 2, 'two')"),
            format!("ALTER TABLE {sc}.t ALTER COLUMN v TYPE bigint"),
            format!("INSERT INTO {sc}.t VALUES (3, 3, 'three')"),
            format!("ALTER TABLE {sc}.t RENAME COLUMN w TO w2"),
            format!("UPDATE {sc}.t SET w2 = 'x' WHERE id = 3"),
        ],
    )
    .await;
    let items = collect(&mut s, 4, 20).await;
    let kinds: Vec<String> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::SchemaSignal {
                hint: SchemaHint::Columns(c),
                ..
            } => {
                let c: Vec<String> = c
                    .iter()
                    .map(|c| format!("{}:{}", c.name, c.source_type))
                    .collect();
                Some(format!("signal {}", c.join(",")))
            }
            StreamItem::Change(_) => Some("change".into()),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "change",
            "signal id:integer,v:integer,w:text",
            "change",
            "signal id:integer,v:bigint,w:text",
            "change",
            "signal id:integer,v:bigint,w2:text",
            "change",
        ],
        "no signal for the first Relation message: the shape is the one the stream opened with"
    );
    // A signal's position is its change's.
    let pos: Vec<SourceSeq> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::SchemaSignal { seq, .. } => Some(*seq),
            StreamItem::Change(e) => Some(e.change_seq),
            _ => None,
        })
        .collect();
    assert_eq!(pos[1], pos[2]);
    let StreamItem::Change(last) = items
        .iter()
        .rev()
        .find(|i| matches!(i, StreamItem::Change(_)))
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(
        last.after.as_ref().unwrap().get("v"),
        Some(&V::Int64(3)),
        "decoded with the new type"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// A value with no representation halts its table once; the stream and the other tables go on.
#[tokio::test(flavor = "multi_thread")]
async fn an_undecodable_value_halts_only_its_table() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "value_halt", "CREATE TABLE t (id int PRIMARY KEY, n numeric(10,2)); CREATE TABLE u (id int PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t VALUES (1, 'NaN')"),
            format!("INSERT INTO {sc}.t VALUES (2, 1.5)"),
            format!("INSERT INTO {sc}.u VALUES (1)"),
        ],
    )
    .await;
    // The second transaction holds only a skipped change: it is empty.
    let items = collect(&mut s, 2, 20).await;
    let halts: Vec<(&TableRef, &HaltReason)> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::TableHalt { table, reason, .. } => Some((table, reason)),
            _ => None,
        })
        .collect();
    assert_eq!(
        halts,
        [(
            &n.table("t"),
            &HaltReason::SchemaMismatch {
                column: "n".into(),
                detail: "NaN can't be stored in a decimal column".into()
            }
        )]
    );
    let tables: Vec<&str> = changes(&items)
        .iter()
        .map(|e| e.table.name.as_str())
        .collect();
    assert_eq!(
        tables,
        ["u"],
        "the halted table's later changes are skipped"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// Tables the stream wasn't opened for are skipped; a busy slot is a permanent error; an idle
/// stream reports the WAL end in keep-alives.
#[tokio::test(flavor = "multi_thread")]
async fn untracked_tables_busy_slots_and_keepalives() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "stream_misc",
        "CREATE TABLE a (id int PRIMARY KEY); CREATE TABLE b (id int PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let r = crate::common::resolved(&c).await;
    let p = c
        .prepare(None, &[n.table("a"), n.table("b")])
        .await
        .unwrap();
    let data_avalanche_core::StartPlan::Snapshot { from, .. } = p.plan else {
        panic!()
    };
    let only_a: Vec<_> = r
        .tables
        .iter()
        .filter(|t| t.table().name == "a")
        .cloned()
        .collect();
    let mut s = c.open_stream(&from, &only_a).await.unwrap();
    each(
        &a,
        &[
            format!("INSERT INTO {}.b VALUES (1)", n.schema),
            format!("INSERT INTO {}.a VALUES (1)", n.schema),
        ],
    )
    .await;
    // The first transaction holds only an untracked change: it is empty.
    let items = collect(&mut s, 1, 20).await;
    let tables: Vec<&str> = changes(&items)
        .iter()
        .map(|e| e.table.name.as_str())
        .collect();
    assert_eq!(tables, ["a"]);
    let Some(StreamItem::Commit(last)) = items.last() else {
        panic!()
    };

    let mut busy = c.open_stream(&from, &only_a).await.unwrap();
    match busy.next().await {
        Err(SourceError::Permanent(m)) => assert!(m.contains("is active for PID"), "{m}"),
        other => panic!("a busy slot is permanent: {other:?}"),
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let keepalive = loop {
        match tokio::time::timeout_at(deadline, s.next())
            .await
            .expect("a keep-alive")
            .unwrap()
        {
            StreamItem::KeepAlive(p) => break p,
            // PostgreSQL 14 sends every transaction of the database, empty ones included.
            StreamItem::Begin { .. } | StreamItem::Commit(_) => {}
            other => panic!("only keep-alives while idle: {other:?}"),
        }
    };
    assert!(
        lsn(&keepalive) >= lsn(&last.commit_pos),
        "{keepalive} vs {}",
        last.commit_pos
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// A publication named with capitals is created quoted (`"It_Mixed_Pub"`), so the stream asks
/// for it quoted: pgoutput would fold `It_Mixed_Pub` to `it_mixed_pub` and stream nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_case_publication_streams() {
    require_it!();
    let a = admin().await;
    let mut n = fresh(&a, "mixed_pub", "CREATE TABLE t (id int PRIMARY KEY);").await;
    n.publication = "It_Mixed_Pub".into();
    a.batch_execute("DROP PUBLICATION IF EXISTS \"It_Mixed_Pub\"")
        .await
        .unwrap();
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    a.execute(&format!("INSERT INTO {}.t VALUES (1)", n.schema), &[])
        .await
        .unwrap();
    let items = collect(&mut s, 1, 20).await;
    assert_eq!(changes(&items).len(), 1, "{items:?}");
    s.close().await.unwrap();
    a.batch_execute("DROP PUBLICATION \"It_Mixed_Pub\"")
        .await
        .unwrap();
    cleanup(&a, &n).await;
}

/// Channel B: a changed primary key or replica identity signals (channel C halts the table on
/// an identifier change). A `"char"` byte above 127 decodes (PostgreSQL 14 sends it raw).
#[tokio::test(flavor = "multi_thread")]
async fn key_and_identity_changes_signal() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "stream_keys",
        "CREATE TABLE t (id int PRIMARY KEY, code int NOT NULL, c \"char\");",
    )
    .await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t VALUES (1, 1, chr(195)::\"char\")"),
            format!("ALTER TABLE {sc}.t DROP CONSTRAINT t_pkey, ADD PRIMARY KEY (code)"),
            format!("INSERT INTO {sc}.t VALUES (2, 2)"),
            format!("ALTER TABLE {sc}.t REPLICA IDENTITY FULL"),
            format!("INSERT INTO {sc}.t VALUES (3, 3)"),
            format!("DELETE FROM {sc}.t WHERE code = 3"),
        ],
    )
    .await;
    let items = collect(&mut s, 4, 20).await;
    let kinds: Vec<&str> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::SchemaSignal { .. } => Some("signal"),
            StreamItem::Change(_) => Some("change"),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        ["change", "signal", "change", "signal", "change", "change"],
        "no signal for the first Relation message: its key is the opening identifier"
    );
    let c = changes(&items);
    assert_eq!(c[0].after.as_ref().unwrap().get("c"), Some(&V::Int16(195)));
    assert_eq!(
        c[3].before,
        Some(row(&[
            ("id", V::Int32(3)),
            ("code", V::Int32(3)),
            ("c", V::Null)
        ])),
        "REPLICA IDENTITY FULL: the whole old row"
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// A tracked table renamed away, or replaced by another table under its name, halts
/// (`TABLE_DROPPED`): a re-created table doesn't resume (`Schema_Change…` §7).
#[tokio::test(flavor = "multi_thread")]
async fn a_renamed_or_replaced_table_halts() {
    require_it!();
    let a = admin().await;
    let n = fresh(
        &a,
        "stream_renames",
        "CREATE TABLE t (id int PRIMARY KEY);
         CREATE TABLE u (id int PRIMARY KEY);
         CREATE TABLE u_new (id int PRIMARY KEY);",
    )
    .await;
    let c = connector(&pipeline(&n, &["t", "u"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            format!("INSERT INTO {sc}.t VALUES (1)"),
            format!("INSERT INTO {sc}.u VALUES (1)"),
            format!("ALTER PUBLICATION {} ADD TABLE {sc}.u_new", n.publication),
            format!("ALTER TABLE {sc}.t RENAME TO t_old"),
            format!("INSERT INTO {sc}.t_old VALUES (2)"),
            format!(
                "BEGIN; ALTER TABLE {sc}.u RENAME TO u_old; ALTER TABLE {sc}.u_new RENAME TO u; COMMIT"
            ),
            format!("INSERT INTO {sc}.u VALUES (2)"),
            format!("INSERT INTO {sc}.u_old VALUES (3)"),
            format!("INSERT INTO {sc}.t_old VALUES (3)"),
            format!("INSERT INTO {sc}.u VALUES (3)"),
        ],
    )
    .await;
    // Four transactions with items: two changes, two halts; the later ones are skipped.
    let items = collect(&mut s, 4, 20).await;
    let got: Vec<String> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::Change(e) => Some(format!("change {}", e.table.name)),
            StreamItem::TableHalt { table, reason, .. } => {
                Some(format!("halt {} {}", table.name, reason.code()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        got,
        [
            "change t",
            "change u",
            "halt t TABLE_DROPPED",
            "halt u TABLE_DROPPED"
        ]
    );
    // Nothing more: the halted tables' later changes are skipped under either OID.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while let Ok(item) = tokio::time::timeout_at(deadline, s.next()).await {
        let item = item.unwrap();
        assert!(
            matches!(
                item,
                StreamItem::KeepAlive(_) | StreamItem::Begin { .. } | StreamItem::Commit(_)
            ),
            "{item:?}"
        );
    }
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}

/// `next` is cancel-safe: calls dropped at every await, the Relation message's catalog queries
/// among them, lose nothing.
#[tokio::test(flavor = "multi_thread")]
async fn next_is_cancel_safe() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "stream_cancel", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let c = connector(&pipeline(&n, &["*"], "", ""));
    let (mut s, _, _) = first_stream(&c).await;
    let sc = &n.schema;
    each(
        &a,
        &[
            // A type the stream hasn't read yet: its Relation message queries the catalog.
            format!("ALTER TABLE {sc}.t ADD COLUMN w numeric(7,3)"),
            format!("INSERT INTO {sc}.t VALUES (1, 1.5)"),
        ],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await; // the events are in the client's buffer
    let mut items = vec![];
    let mut cancelled = 0;
    let mut wait = Duration::from_micros(20);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !items.iter().any(|i| matches!(i, StreamItem::Change(_))) {
        assert!(tokio::time::Instant::now() < deadline, "{items:?}");
        match tokio::time::timeout(wait, s.next()).await {
            Ok(item) => items.push(item.expect("stream item")),
            Err(_) => {
                cancelled += 1;
                wait = (wait * 2).min(Duration::from_millis(200));
            }
        }
    }
    assert!(cancelled > 0, "no call was cancelled");
    let kinds: Vec<&str> = items
        .iter()
        .filter_map(|i| match i {
            StreamItem::SchemaSignal { .. } => Some("signal"),
            StreamItem::Change(_) => Some("change"),
            _ => None,
        })
        .collect();
    assert_eq!(kinds, ["signal", "change"], "{items:?}");
    assert_eq!(
        changes(&items)[0].after,
        Some(row(&[
            ("id", V::Int32(1)),
            ("w", V::Numeric(Decimal::parse("1.500").unwrap()))
        ]))
    );
    s.close().await.unwrap();
    cleanup(&a, &n).await;
}
