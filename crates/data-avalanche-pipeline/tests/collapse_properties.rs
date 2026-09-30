//! M3 exit (roadmap): the collapse is idempotent and insensitive to batch boundaries. Random
//! streams of transactions run through the accumulator into the reference sink under random flush
//! and split schedules, with crashes and replays; the mirrored tables must always equal the
//! source's final state.

use std::collections::BTreeMap;
use std::time::Instant;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ColumnSchema, ColumnValue, ColumnValueType, CommitMarker, CommitOutcome, Op, Row,
    Sink, SourcePosition, SourceSeq, TableRef, TableSchema, TableSpec, TransactionBoundary,
};
use data_avalanche_pipeline::{
    Accumulator, AccumulatorError, Flush, FlushPolicy, FlushReason, Lineage, TxnLimits,
};
use data_avalanche_testkit::MemorySink;
use proptest::prelude::*;

const TABLES: [&str; 2] = ["a", "b"];

/// A source-side action; the model turns it into a valid event, or drops it (an update of a
/// missing row, an insert over an existing one becomes an update).
#[derive(Debug, Clone)]
enum Action {
    Put {
        t: usize,
        key: i64,
        doc: u8,
    },
    /// An UPDATE that doesn't touch `doc`: PostgreSQL sends it as unchanged TOAST.
    Touch {
        t: usize,
        key: i64,
    },
    Delete {
        t: usize,
        key: i64,
    },
    /// An UPDATE of the primary key.
    Move {
        t: usize,
        from: i64,
        to: i64,
    },
    /// An UPDATE of the primary key that doesn't touch `doc`, under REPLICA IDENTITY FULL: the
    /// after-image carries `doc` as unchanged TOAST, the before-image the whole old row.
    MoveFull {
        t: usize,
        from: i64,
        to: i64,
    },
    Truncate {
        t: usize,
    },
}

fn action() -> impl Strategy<Value = Action> {
    let t = 0..TABLES.len();
    let key = 0..6i64;
    prop_oneof![
        4 => (t.clone(), key.clone(), any::<u8>()).prop_map(|(t, key, doc)| Action::Put { t, key, doc }),
        2 => (t.clone(), key.clone()).prop_map(|(t, key)| Action::Touch { t, key }),
        2 => (t.clone(), key.clone()).prop_map(|(t, key)| Action::Delete { t, key }),
        1 => (t.clone(), key.clone(), key.clone()).prop_map(|(t, from, to)| Action::Move { t, from, to }),
        1 => (t.clone(), key.clone(), key).prop_map(|(t, from, to)| Action::MoveFull { t, from, to }),
        1 => t.prop_map(|t| Action::Truncate { t }),
    ]
}

/// Transactions of actions.
fn stream() -> impl Strategy<Value = Vec<Vec<Action>>> {
    prop::collection::vec(prop::collection::vec(action(), 0..8), 1..12)
}

#[derive(Debug, Clone)]
enum Item {
    Begin(u64),
    Event(CdcEvent),
    Truncate(TableRef),
    Commit(TransactionBoundary),
}

type Tables = BTreeMap<TableRef, BTreeMap<i64, String>>;

fn table(t: usize) -> TableRef {
    TableRef::new("public", TABLES[t])
}

fn schema(t: usize) -> TableSchema {
    TableSchema::new(
        table(t),
        vec![
            ColumnSchema::new("id", ColumnValueType::Int64, "bigint").not_null(),
            ColumnSchema::new("doc", ColumnValueType::Text, "text"),
        ],
        vec!["id".into()],
    )
    .unwrap()
}

fn commit_pos(txn: usize) -> SourcePosition {
    SourcePosition::Pg(Lsn(100 * (txn as u64 + 1)))
}

fn row(key: i64, doc: ColumnValue) -> Row {
    Row(vec![
        ("id".into(), ColumnValue::Int64(key)),
        ("doc".into(), doc),
    ])
}

fn key_only(key: i64) -> Row {
    Row(vec![("id".into(), ColumnValue::Int64(key))])
}

/// The stream items of each transaction, and the source's final tables.
fn source(txns: &[Vec<Action>]) -> (Vec<Vec<Item>>, Tables) {
    let mut db: Tables = TABLES
        .iter()
        .enumerate()
        .map(|(t, _)| (table(t), BTreeMap::new()))
        .collect();
    let mut out = Vec::new();
    for (i, actions) in txns.iter().enumerate() {
        let xid = i as u64 + 1;
        let mut items = vec![Item::Begin(xid)];
        let mut seq = 100 * i as u64;
        let mut event = |op, t: usize, before, after| {
            seq += 1;
            Item::Event(CdcEvent {
                op,
                table: table(t),
                before,
                after,
                source_pos: commit_pos(i),
                change_seq: SourceSeq::Pg(Lsn(seq)),
                xid,
                commit_ts_micros: i as i64,
            })
        };
        for a in actions {
            match *a {
                Action::Put { t, key, doc } => {
                    let doc = format!("d{doc}");
                    let rows = db.get_mut(&table(t)).unwrap();
                    let op = if rows.contains_key(&key) {
                        Op::Update
                    } else {
                        Op::Insert
                    };
                    rows.insert(key, doc.clone());
                    items.push(event(op, t, None, Some(row(key, ColumnValue::Text(doc)))));
                }
                Action::Touch { t, key } => {
                    if db[&table(t)].contains_key(&key) {
                        items.push(event(
                            Op::Update,
                            t,
                            None,
                            Some(row(key, ColumnValue::Unchanged)),
                        ));
                    }
                }
                Action::Delete { t, key } => {
                    if db.get_mut(&table(t)).unwrap().remove(&key).is_some() {
                        items.push(event(Op::Delete, t, Some(key_only(key)), None));
                    }
                }
                Action::Move { t, from, to } => {
                    let rows = db.get_mut(&table(t)).unwrap();
                    if from != to
                        && !rows.contains_key(&to)
                        && let Some(doc) = rows.remove(&from)
                    {
                        rows.insert(to, doc.clone());
                        let after = row(to, ColumnValue::Text(doc));
                        items.push(event(Op::Update, t, Some(key_only(from)), Some(after)));
                    }
                }
                Action::MoveFull { t, from, to } => {
                    let rows = db.get_mut(&table(t)).unwrap();
                    if from != to
                        && !rows.contains_key(&to)
                        && let Some(doc) = rows.remove(&from)
                    {
                        rows.insert(to, doc.clone());
                        let before = row(from, ColumnValue::Text(doc));
                        let after = row(to, ColumnValue::Unchanged);
                        items.push(event(Op::Update, t, Some(before), Some(after)));
                    }
                }
                Action::Truncate { t } => {
                    db.get_mut(&table(t)).unwrap().clear();
                    items.push(Item::Truncate(table(t)));
                }
            }
        }
        items.push(Item::Commit(TransactionBoundary {
            xid,
            commit_pos: commit_pos(i),
            commit_ts_micros: i as i64,
        }));
        out.push(items);
    }
    (out, db)
}

#[derive(Debug, Clone)]
struct Schedule {
    /// Flush after transaction `i` when `flush_after[i]`.
    flush_after: Vec<bool>,
    split_rows: Option<u64>,
}

fn schedule() -> impl Strategy<Value = Schedule> {
    (
        prop::collection::vec(any::<bool>(), 12),
        prop::option::of(1..4u64),
    )
        .prop_map(|(flush_after, split_rows)| Schedule {
            flush_after,
            split_rows,
        })
}

/// Where a run stops: in the middle of flush number `flush`, after committing `committed` of its
/// change sets.
#[derive(Debug, Clone, Copy)]
struct Crash {
    flush: usize,
    committed: usize,
}

struct Run<'a> {
    sink: &'a MemorySink,
    acc: Accumulator,
    now: Instant,
    /// Checkpoints of flushes whose change sets all committed.
    durable: Vec<SourcePosition>,
    flushes: usize,
    crash: Option<Crash>,
}

/// The run stopped at its crash point.
struct Crashed;

impl Run<'_> {
    fn new<'s>(
        sink: &'s MemorySink,
        resume_from: SourcePosition,
        schedule: &Schedule,
        crash: Option<Crash>,
    ) -> Run<'s> {
        let never = FlushPolicy {
            seconds: None,
            rows: None,
            bytes: None,
        };
        let lineage = Lineage {
            source_db: "db".into(),
            pipeline: "p".into(),
        };
        let mut acc = Accumulator::new(lineage, never, resume_from.clone());
        acc.set_txn_limits(TxnLimits {
            batch_rows: schedule.split_rows,
            buffer_bytes: 1 << 30,
        });
        for t in 0..TABLES.len() {
            acc.add_table(schema(t), &[]).unwrap();
        }
        Run {
            sink,
            acc,
            now: Instant::now(),
            durable: vec![resume_from],
            flushes: 0,
            crash,
        }
    }

    async fn commit(&mut self, flush: Flush) -> Result<(), Crashed> {
        let crash = self.crash.filter(|c| c.flush == self.flushes);
        self.flushes += 1;
        for (i, cs) in flush.change_sets.into_iter().enumerate() {
            if crash.is_some_and(|c| i >= c.committed) {
                return Err(Crashed);
            }
            let marker = CommitMarker {
                owner_epoch: 1,
                committed_position: cs.position.end.clone(),
                pipeline_id: "p".into(),
            };
            let outcome = self.sink.commit(cs, marker).await.unwrap();
            assert!(!matches!(outcome, CommitOutcome::Fenced { .. }));
        }
        if crash.is_some() {
            return Err(Crashed);
        }
        self.durable.push(flush.checkpoint);
        Ok(())
    }

    async fn feed(&mut self, txns: &[Vec<Item>], schedule: &Schedule) -> Result<(), Crashed> {
        for (i, items) in txns.iter().enumerate() {
            for item in items {
                match item {
                    Item::Begin(xid) => self.acc.begin(*xid).unwrap(),
                    Item::Event(e) => {
                        loop {
                            match self.acc.apply(e, self.now) {
                                Ok(()) => break,
                                Err(AccumulatorError::SplitRequired) => {
                                    let f = self.acc.split().unwrap();
                                    self.commit(f).await?;
                                }
                                Err(e) => panic!("{e}"),
                            }
                        }
                        if self.acc.due(self.now) == Some(FlushReason::Split) {
                            let f = self.acc.split().unwrap();
                            self.commit(f).await?;
                        }
                    }
                    Item::Truncate(t) => loop {
                        match self.acc.truncate(std::slice::from_ref(t), self.now) {
                            Ok(()) => break,
                            Err(AccumulatorError::FlushRequired { .. }) => {
                                let f = self.acc.flush();
                                self.commit(f).await?;
                            }
                            Err(e) => panic!("{e}"),
                        }
                    },
                    Item::Commit(b) => {
                        self.acc.commit(b, self.now).unwrap();
                        if schedule.flush_after.get(i).copied().unwrap_or(false) {
                            let f = self.acc.flush();
                            self.commit(f).await?;
                        }
                    }
                }
            }
        }
        let f = self.acc.flush();
        self.commit(f).await
    }
}

async fn sink() -> MemorySink {
    let sink = MemorySink::new();
    for t in 0..TABLES.len() {
        sink.ensure_table(&TableSpec::new(&schema(t), vec![]).unwrap())
            .await
            .unwrap();
    }
    sink
}

fn mirrored(sink: &MemorySink) -> Tables {
    (0..TABLES.len())
        .map(|t| {
            let b = sink.content(&table(t)).unwrap();
            let ids = b.column(0).as_primitive::<Int64Type>();
            let docs = b.column(1).as_string::<i32>();
            let rows = (0..b.num_rows())
                .map(|i| (ids.value(i), docs.value(i).to_string()))
                .collect();
            (table(t), rows)
        })
        .collect()
}

/// The transactions a pipeline resuming at `from` receives again: those committed after it.
fn after(txns: &[Vec<Item>], from: &SourcePosition) -> Vec<Vec<Item>> {
    txns.iter()
        .enumerate()
        .filter(|(i, _)| !from.covers(&commit_pos(*i)).unwrap())
        .map(|(_, items)| items.clone())
        .collect()
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    /// Any flush and split schedule mirrors the source exactly.
    #[test]
    fn the_mirror_equals_the_source_whatever_the_batch_boundaries(
        txns in stream(),
        schedule in schedule(),
    ) {
        let (items, expected) = source(&txns);
        let got = block_on(async {
            let sink = sink().await;
            let mut run = Run::new(&sink, SourcePosition::Pg(Lsn(0)), &schedule, None);
            assert!(run.feed(&items, &schedule).await.is_ok());
            mirrored(&sink)
        });
        prop_assert_eq!(got, expected);
    }

    /// A crash anywhere, then a restart from any durable checkpoint (the latest, or one a lagging
    /// state write left behind), converges to the source; replaying everything again changes
    /// nothing.
    #[test]
    fn a_crash_and_a_replay_from_any_checkpoint_converge(
        txns in stream(),
        first in schedule(),
        second in schedule(),
        crash_flush in 0..16usize,
        crash_committed in 0..3usize,
        lag in 0..4usize,
    ) {
        let (items, expected) = source(&txns);
        let (got, again) = block_on(async {
            let sink = sink().await;
            let crash = Crash { flush: crash_flush, committed: crash_committed };
            let mut run = Run::new(&sink, SourcePosition::Pg(Lsn(0)), &first, Some(crash));
            let _ = run.feed(&items, &first).await;
            let durable = run.durable.clone();
            let resume = durable[durable.len().saturating_sub(1 + lag)].clone();

            let mut restart = Run::new(&sink, resume.clone(), &second, None);
            assert!(restart.feed(&after(&items, &resume), &second).await.is_ok());
            let got = mirrored(&sink);

            let mut replay = Run::new(&sink, SourcePosition::Pg(Lsn(0)), &second, None);
            assert!(replay.feed(&items, &second).await.is_ok());
            (got, mirrored(&sink))
        });
        prop_assert_eq!(&got, &expected);
        prop_assert_eq!(again, expected);
    }
}
