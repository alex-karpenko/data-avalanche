//! The load: `load_<i>` tables of `(id, doc, n)`, filled with keys `1..=keys`, then changed at a
//! steady rate by one writer on a connection of its own. Every tenth of a second it commits one
//! transaction on one table, chosen in turn: an upsert, an update or a delete of a batch of
//! random keys (6 : 3 : 1), `rate / 10` changes in all. A failed statement (the source restarting)
//! is counted, and the writer reconnects and goes on.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::harness::{Db, E2e, Source};

/// The columns the runs compare, source against lake.
pub const COLS: &[&str] = &["id", "doc", "n"];

/// What one run writes.
#[derive(Debug, Clone)]
pub struct Workload {
    pub tables: Vec<String>,
    /// Keys `1..=keys` per table: the prefill, and the space the changes pick from.
    pub keys: u64,
    /// Changes a second, over all tables.
    pub rate: u32,
}

/// What a writer did.
#[derive(Debug, Clone, Copy, Default)]
pub struct Counts {
    pub transactions: u64,
    pub changes: u64,
    /// Statements that failed (a restarting source): each lost, the writer reconnected.
    pub failed: u64,
}

impl Workload {
    /// `tables` tables `load_0…`, each `keys` keys, changed at `rate` a second.
    pub fn new(tables: usize, keys: u64, rate: u32) -> Self {
        Self {
            tables: (0..tables).map(|i| format!("load_{i}")).collect(),
            keys,
            rate,
        }
    }

    /// The tables' DDL.
    pub fn ddl(&self) -> String {
        self.tables
            .iter()
            .map(|t| format!("CREATE TABLE {t} (id bigint PRIMARY KEY, doc text, n bigint);"))
            .collect()
    }

    /// Fills every table with its keys, `n` 0, in batches of 100 000.
    pub async fn prefill(&self, e: &E2e) {
        for t in &self.tables {
            let mut from = 1;
            while from <= self.keys {
                let to = (from + 99_999).min(self.keys);
                e.exec(&fill(e.source, t, from, to)).await;
                from = to + 1;
            }
        }
    }

    /// Starts the writer.
    pub fn start(&self, e: &E2e) -> Writer {
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(write(
            e.source,
            e.schema.clone(),
            self.clone(),
            Arc::clone(&stop),
        ));
        Writer { stop, task }
    }
}

/// Keys `from..=to` of `table`, in one statement.
fn fill(source: Source, table: &str, from: u64, to: u64) -> String {
    match source {
        Source::Postgres { .. } => format!(
            "INSERT INTO {table} SELECT g, 'row ' || g, 0 FROM generate_series({from}, {to}) g"
        ),
        Source::Mysql(t) => {
            let var = if t.mariadb {
                "max_recursive_iterations"
            } else {
                "cte_max_recursion_depth"
            };
            format!(
                "SET SESSION {var} = {n}; INSERT INTO {table} (id, doc, n) WITH RECURSIVE s (g) AS
                 (SELECT {from} UNION ALL SELECT g + 1 FROM s WHERE g < {to})
                 SELECT g, CONCAT('row ', g), 0 FROM s",
                n = to - from + 1
            )
        }
    }
}

/// The writer, running until [`Writer::stop`].
pub struct Writer {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Counts>,
}

impl Writer {
    /// Stops it after its transaction in flight; what it did.
    pub async fn stop(self) -> Counts {
        self.stop.store(true, Ordering::SeqCst);
        self.task.await.expect("the writer")
    }
}

async fn write(source: Source, schema: String, w: Workload, stop: Arc<AtomicBool>) -> Counts {
    let mut db = Db::connect(source).await;
    let mut counts = Counts::default();
    let batch = (w.rate / 10).max(1) as u64;
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = |bound: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % bound
    };
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut turn = 0usize;
    while !stop.load(Ordering::SeqCst) {
        tick.tick().await;
        let table = format!("{schema}.{}", w.tables[turn % w.tables.len()]);
        turn += 1;
        // Each key once: PostgreSQL's upsert refuses a key twice in one statement.
        let keys: Vec<u64> = (0..batch)
            .map(|_| next(w.keys) + 1)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let list = keys
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let op = turn as u64;
        let sql = match next(10) {
            0..=5 => upsert(source, &table, &keys, op),
            6..=8 => format!("UPDATE {table} SET doc = 'u{op}', n = n + 1 WHERE id IN ({list})"),
            _ => format!("DELETE FROM {table} WHERE id IN ({list})"),
        };
        let at = Instant::now();
        match db.exec(&sql).await {
            Ok(()) => {
                counts.transactions += 1;
                counts.changes += keys.len() as u64;
            }
            Err(e) => {
                counts.failed += 1;
                eprintln!("load: a statement failed after {:?}: {e}", at.elapsed());
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Ok(again) = Db::try_connect(source).await {
                    db = again;
                }
            }
        }
    }
    counts
}

/// One transaction: `keys` inserted, or their `doc` and `n` changed where they exist.
fn upsert(source: Source, table: &str, keys: &[u64], op: u64) -> String {
    let rows = keys
        .iter()
        .map(|k| format!("({k}, 'i{op}', 0)"))
        .collect::<Vec<_>>()
        .join(", ");
    match source {
        Source::Postgres { .. } => format!(
            "INSERT INTO {table} (id, doc, n) VALUES {rows}
             ON CONFLICT (id) DO UPDATE SET doc = excluded.doc, n = {table}.n + 1"
        ),
        // MariaDB has no row alias; MySQL 8.4 deprecates VALUES() for it.
        Source::Mysql(t) if t.mariadb => format!(
            "INSERT INTO {table} (id, doc, n) VALUES {rows}
             ON DUPLICATE KEY UPDATE doc = VALUES(doc), n = {table}.n + 1"
        ),
        Source::Mysql(_) => format!(
            "INSERT INTO {table} (id, doc, n) VALUES {rows} AS new
             ON DUPLICATE KEY UPDATE doc = new.doc, n = {table}.n + 1"
        ),
    }
}
