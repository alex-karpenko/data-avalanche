//! `Schema_Change…` §8 for one table: what changed (channel C, then the §7.2 classification) and
//! the sink's evolve. A stream session runs them at a signal's position, with a flush in between.
//! Phase 0 runs them to baseline a table whose stream position was lost.

use arrow_schema::Schema;
use data_avalanche_core::{
    DeltaClass, EvolveOutcome, HaltReason, INTERNAL_COLUMN_PREFIX, LINEAGE_COLUMNS, SchemaChange,
    SchemaDelta, SchemaHint, Sink, SinkError, SourceConnector, SourceError, TableRef, TableSchema,
    classify, differ_by_case_alone,
};
use tokio::sync::watch;
use tracing::Instrument;
use tracing::field::Empty;

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

/// A table the sink holds while the pipeline has no schema cached for it (a wiped state,
/// DA-SNAP-13, or a table first tracked at a resume, DA-SNAP-22): the source's columns the sink table lacks, by name, as the `AddColumn`s that
/// baseline it before the copy. Names match without case, Unicode's, as Delta's do
/// ([`differ_by_case_alone`]): a sink column that matches by case alone can't sit beside the
/// source's, so it halts, naming both spellings. A
/// column it holds as another type still surfaces at the first commit (`SINK_REJECTED`); a
/// nullable one the source lacks stays, soft-dropped. A key column it lacks can't be added: the
/// table is keyed otherwise, and halts. (The halt text says "a run whose state is gone", as the
/// plan words it; at a resume the table's state was never there.) So does a non-null column the source lacks: no row of the
/// source's could fill it (DA-SNAP-22).
pub(crate) fn missing_columns(
    sink: &Schema,
    source: &TableSchema,
) -> Result<Vec<SchemaChange>, HaltReason> {
    let mut adds = vec![];
    for c in source.columns() {
        if sink.field_with_name(&c.name).is_ok() {
            continue;
        }
        if let Some(f) = sink
            .fields()
            .iter()
            .find(|f| differ_by_case_alone(f.name(), &c.name))
        {
            return Err(HaltReason::SchemaMismatch {
                column: c.name.clone(),
                detail: format!(
                    "the sink table holds this column as {}, the source as {}; they differ by \
                     case alone and the sink can't hold both: drop the sink table and \
                     re-snapshot the table",
                    f.name(),
                    c.name
                ),
            });
        }
        if source.is_identifier(&c.name) {
            return Err(HaltReason::SchemaMismatch {
                column: c.name.clone(),
                detail: "the sink table, made by a run whose state is gone, lacks this key \
                         column; drop the sink table and re-snapshot the table"
                    .into(),
            });
        }
        adds.push(SchemaChange::AddColumn(c.clone()));
    }
    // The case-only term can't decide the result: the loop above has already halted on such a pair.
    if let Some(f) = sink.fields().iter().find(|f| {
        !f.is_nullable()
            && !LINEAGE_COLUMNS.contains(&f.name().as_str())
            && !f.name().starts_with(INTERNAL_COLUMN_PREFIX)
            && !source
                .columns()
                .iter()
                .any(|c| c.name == *f.name() || differ_by_case_alone(f.name(), &c.name))
    }) {
        return Err(HaltReason::SchemaMismatch {
            column: f.name().clone(),
            detail: "the sink table, made by a run whose state is gone, holds this non-null \
                     column, which the source doesn't have: no row of the source's could fill \
                     it; drop the sink table and re-snapshot the table"
                .into(),
        });
    }
    Ok(adds)
}

/// A change to apply: the table's new schema, and the changes the sink must evolve.
#[derive(Debug)]
pub(crate) struct Planned {
    pub now: TableSchema,
    pub safe: Vec<SchemaChange>,
}

pub(crate) struct Evolver<'a> {
    /// The pipeline's name, as the evolve's span names it.
    pub pipeline: &'a str,
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
    /// degraded apply's warning; `Ok(Err)` halts the table. A trace of its own, which records how
    /// it ended.
    pub async fn evolve(
        &self,
        table: &TableRef,
        safe: Vec<SchemaChange>,
    ) -> Result<Result<Option<String>, HaltReason>, WorkerError> {
        let span = tracing::info_span!(
            parent: None,
            "schema.apply",
            pipeline = %self.pipeline,
            table = %table,
            changes = safe.len(),
            outcome = Empty,
        );
        let applied = self.apply(table, safe).instrument(span.clone()).await;
        let outcome = match &applied {
            Ok(Ok(None)) => "applied",
            Ok(Ok(Some(_))) => "degraded",
            Ok(Err(_)) => "halted",
            Err(_) => "failed",
        };
        span.record("outcome", outcome);
        applied
    }

    async fn apply(
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
