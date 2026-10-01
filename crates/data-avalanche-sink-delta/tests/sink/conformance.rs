//! M4 exit: the Delta sink and the reference sink (`MemorySink`) give the same table content for
//! the same change sets. Random transactions (puts, unchanged-TOAST updates, deletes, key moves,
//! truncates) run through the accumulator under random flush and split schedules; every change set
//! is committed to both sinks.

use std::collections::BTreeMap;
use std::time::Instant;

use data_avalanche_core::position::Lsn;
use data_avalanche_core::{
    CdcEvent, ColumnValue, CommitMarker, CommitOutcome, Op, Row, Sink, SourcePosition, SourceSeq,
    TransactionBoundary,
};
use data_avalanche_pipeline::{
    Accumulator, AccumulatorError, Flush, FlushPolicy, FlushReason, Lineage, TxnLimits,
};
use data_avalanche_sink_delta::{DeltaLocation, DeltaSink, TableDefaults};
use data_avalanche_testkit::MemorySink;
use proptest::prelude::*;

use crate::common::{items, schema, sink, spec};

#[derive(Debug, Clone)]
enum Action {
    Put {
        key: i64,
        doc: u8,
    },
    /// An UPDATE that doesn't touch `doc` (unchanged TOAST).
    Touch {
        key: i64,
    },
    Delete {
        key: i64,
    },
    /// An UPDATE of the key; the after-image's `doc` is unchanged TOAST when `toast`.
    Move {
        from: i64,
        to: i64,
        toast: bool,
    },
    Truncate,
}

fn action() -> impl Strategy<Value = Action> {
    let key = 0..6i64;
    prop_oneof![
        4 => (key.clone(), any::<u8>()).prop_map(|(key, doc)| Action::Put { key, doc }),
        2 => key.clone().prop_map(|key| Action::Touch { key }),
        2 => key.clone().prop_map(|key| Action::Delete { key }),
        1 => (key.clone(), key, any::<bool>()).prop_map(|(from, to, toast)| Action::Move { from, to, toast }),
        1 => Just(Action::Truncate),
    ]
}

fn row(key: i64, doc: ColumnValue) -> Row {
    Row(vec![
        ("id".into(), ColumnValue::Int64(key)),
        ("doc".into(), doc),
    ])
}

struct Both {
    memory: MemorySink,
    delta: DeltaSink,
}

impl Both {
    async fn commit(&self, flush: Flush) {
        for cs in flush.change_sets {
            let marker = CommitMarker {
                owner_epoch: 1,
                committed_position: cs.position.end.clone(),
                pipeline_id: "p".into(),
            };
            let m = self
                .memory
                .commit(cs.clone(), marker.clone())
                .await
                .unwrap();
            let d = self.delta.commit(cs, marker).await.unwrap();
            // Only the skipped bit compares: the two sinks number their versions differently.
            let skipped = |o: &CommitOutcome| matches!(o, CommitOutcome::SkippedCovered);
            assert_eq!(skipped(&m), skipped(&d), "{m:?} vs {d:?}");
        }
    }
}

async fn run(txns: Vec<Vec<Action>>, flush_after: Vec<bool>, split_rows: Option<u64>) {
    let dir = tempfile::tempdir().unwrap();
    let location = DeltaLocation::Local {
        root: dir.path().to_path_buf(),
    };
    let both = Both {
        memory: MemorySink::new(),
        delta: sink(location, TableDefaults::default()),
    };
    both.memory.ensure_table(&spec()).await.unwrap();
    both.delta.ensure_table(&spec()).await.unwrap();
    let never = FlushPolicy {
        seconds: None,
        rows: None,
        bytes: None,
    };
    let lineage = Lineage {
        source_db: "db".into(),
        pipeline: "p".into(),
    };
    let mut acc = Accumulator::new(lineage, never, SourcePosition::Pg(Lsn(0)));
    acc.set_txn_limits(TxnLimits {
        batch_rows: split_rows,
        buffer_bytes: 1 << 30,
    });
    acc.add_table(schema(), &[]).unwrap();
    let now = Instant::now();
    let mut db: BTreeMap<i64, String> = BTreeMap::new();
    for (i, actions) in txns.iter().enumerate() {
        let xid = i as u64 + 1;
        let pos = SourcePosition::Pg(Lsn(100 * xid));
        let mut seq = 100 * i as u64;
        acc.begin(xid).unwrap();
        for a in actions {
            let mut event = |op, before, after| {
                seq += 1;
                CdcEvent {
                    op,
                    table: items(),
                    before,
                    after,
                    source_pos: pos.clone(),
                    change_seq: SourceSeq::Pg(Lsn(seq)),
                    xid,
                    commit_ts_micros: i as i64,
                }
            };
            let e = match *a {
                Action::Put { key, doc } => {
                    let doc = format!("d{doc}");
                    let op = if db.insert(key, doc.clone()).is_some() {
                        Op::Update
                    } else {
                        Op::Insert
                    };
                    event(op, None, Some(row(key, ColumnValue::Text(doc))))
                }
                Action::Touch { key } if db.contains_key(&key) => {
                    event(Op::Update, None, Some(row(key, ColumnValue::Unchanged)))
                }
                Action::Delete { key } if db.remove(&key).is_some() => event(
                    Op::Delete,
                    Some(Row(vec![("id".into(), ColumnValue::Int64(key))])),
                    None,
                ),
                Action::Move { from, to, toast }
                    if from != to && !db.contains_key(&to) && db.contains_key(&from) =>
                {
                    let doc = db.remove(&from).unwrap();
                    db.insert(to, doc.clone());
                    let (before, after) = if toast {
                        (
                            row(from, ColumnValue::Text(doc)),
                            row(to, ColumnValue::Unchanged),
                        )
                    } else {
                        (
                            Row(vec![("id".into(), ColumnValue::Int64(from))]),
                            row(to, ColumnValue::Text(doc)),
                        )
                    };
                    event(Op::Update, Some(before), Some(after))
                }
                Action::Truncate => {
                    db.clear();
                    loop {
                        match acc.truncate(&[items()], now) {
                            Ok(()) => break,
                            Err(AccumulatorError::FlushRequired { .. }) => {
                                both.commit(acc.flush()).await
                            }
                            Err(e) => panic!("{e}"),
                        }
                    }
                    continue;
                }
                _ => continue,
            };
            loop {
                match acc.apply(&e, now) {
                    Ok(()) => break,
                    Err(AccumulatorError::SplitRequired) => both.commit(acc.split().unwrap()).await,
                    Err(e) => panic!("{e}"),
                }
            }
            if acc.due(now) == Some(FlushReason::Split) {
                both.commit(acc.split().unwrap()).await;
            }
        }
        acc.commit(
            &TransactionBoundary {
                xid,
                commit_pos: pos,
                commit_ts_micros: i as i64,
            },
            now,
        )
        .unwrap();
        if flush_after[i] {
            both.commit(acc.flush()).await;
        }
    }
    both.commit(acc.flush()).await;
    let memory = both.memory.content(&items()).unwrap();
    let delta = both.delta.content(&items()).await.unwrap().unwrap();
    assert_eq!(delta, memory);
    assert_eq!(delta.num_rows(), db.len());
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    fn the_delta_table_equals_the_reference_sinks(
        txns in prop::collection::vec(prop::collection::vec(action(), 0..6), 1..8),
        flush_after in prop::collection::vec(any::<bool>(), 8),
        split_rows in prop::option::of(1..4u64),
    ) {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        rt.block_on(run(txns, flush_after, split_rows));
    }
}
