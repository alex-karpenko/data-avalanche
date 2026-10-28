//! `Schema_Change…` §8 for one table: what changed (channel C, then the §7.2 classification) and
//! the sink's evolve. A stream session runs them at a signal's position, with a flush in between.
//! Phase 0 runs them to baseline a table whose stream position was lost.

use arrow_schema::Schema;
use data_avalanche_core::{
    DeltaClass, EvolveOutcome, HaltReason, INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, SchemaChange,
    SchemaDelta, SchemaHint, Sink, SinkError, SourceConnector, SourceError, TableRef, TableSchema,
    classify,
};
use tokio::sync::watch;

use crate::{CommitGuard, Tuning, WorkerError, WorkerStatus};

/// The sink table's columns `source` doesn't have, the lineage and internal columns aside:
/// soft-dropped, kept and written NULL (`data_avalanche_sink_soft_dropped_columns`).
pub(crate) fn soft_dropped(sink: &Schema, source: &TableSchema) -> usize {
    sink.fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|n| {
            !LINEAGE_COLUMNS.contains(n)
                && !n.starts_with(INTERNAL_COLUMN_PREFIX)
                && source.column(n).is_none()
        })
        .count()
}

/// A change to apply: the table's new schema, and the changes the sink must evolve.
#[derive(Debug)]
pub(crate) struct Planned {
    pub now: TableSchema,
    pub safe: Vec<SchemaChange>,
}

pub(crate) struct Evolver<'a> {
    pub source: &'a dyn SourceConnector,
    pub sink: &'a dyn Sink,
    pub guard: &'a dyn CommitGuard,
    /// The lease epoch the sink's evolve is fenced with.
    pub owner_epoch: u64,
    pub tuning: &'a Tuning,
    pub evolution: bool,
    pub status: &'a watch::Sender<WorkerStatus>,
}

impl Evolver<'_> {
    /// Channel C against the `cached` schema, retried while it fails transiently, then the
    /// classification. `Err` halts the table: an unsafe change, a Safe one with evolution off, a
    /// dropped table, or a failed catalog read (fail-safe, §3 C).
    pub async fn plan(
        &self,
        cached: &TableSchema,
        hint: &SchemaHint,
    ) -> Result<Planned, HaltReason> {
        let attempts = self.tuning.augment_attempts.max(1);
        let mut attempt = 0;
        let (delta, now) = loop {
            match self.source.augment(cached, hint).await {
                Ok(found) => break found,
                Err(SourceError::TableHalt { reason, .. }) => return Err(reason),
                Err(e) if e.is_transient() && attempt + 1 < attempts => {
                    tokio::time::sleep(self.tuning.source_backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(e) => {
                    return Err(HaltReason::UnsafeDdl {
                        detail: format!("the catalog read for a schema change failed: {e}"),
                    });
                }
            }
        };
        let Some(now) = now else {
            return Err(HaltReason::TableDropped);
        };
        let mut safe = vec![];
        for (change, class) in classify(cached, &delta) {
            match class {
                DeltaClass::Unsafe(reason) => return Err(reason),
                DeltaClass::Safe => safe.push(change),
                DeltaClass::Ignorable => {}
            }
        }
        if !safe.is_empty() && !self.evolution {
            return Err(HaltReason::EvolutionDisabled);
        }
        Ok(Planned { now, safe })
    }

    /// The sink's evolve of the Safe changes, retried while it fails transiently. `Ok(Some)` is a
    /// degraded apply's warning; `Ok(Err)` halts the table.
    pub async fn evolve(
        &self,
        table: &TableRef,
        safe: Vec<SchemaChange>,
    ) -> Result<Result<Option<String>, HaltReason>, WorkerError> {
        let delta = SchemaDelta {
            table: table.clone(),
            changes: safe,
        };
        let mut attempt = 0;
        loop {
            if !self.guard.may_commit() {
                return Err(WorkerError::LeaseLost);
            }
            let outcome = self.sink.evolve(table, &delta, self.owner_epoch).await;
            if attempt > 0 && outcome.is_ok() {
                self.status.send_modify(|st| st.degraded = None);
            }
            match outcome {
                Ok(EvolveOutcome::Applied) => return Ok(Ok(None)),
                Ok(EvolveOutcome::Degraded { warning }) => {
                    return Ok(Ok(Some(format!("table {table}: {warning}"))));
                }
                Ok(EvolveOutcome::Unsupported { reason }) => {
                    return Ok(Err(HaltReason::EvolutionUnsupported { detail: reason }));
                }
                Ok(EvolveOutcome::Fenced { current_epoch }) => {
                    return Err(WorkerError::Fenced {
                        table: table.clone(),
                        current_epoch,
                    });
                }
                Err(SinkError::Transient { reason }) => {
                    self.status
                        .send_modify(|st| st.degraded = Some(format!("table {table}: {reason}")));
                    tokio::time::sleep(self.tuning.sink_backoff.delay(attempt)).await;
                    attempt += 1;
                }
                Err(SinkError::Permanent { reason }) => {
                    return Ok(Err(HaltReason::SinkRejected { detail: reason }));
                }
                Err(e) => return Err(WorkerError::Sink(e)),
            }
        }
    }
}
