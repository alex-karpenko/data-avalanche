//! Data-Flow Phase 0 step 2: whether a table snapshots at this start, by its `snapshot_mode`
//! (System-Overview §Data snapshotting) and what the start found.

use data_avalanche_config::SnapshotMode;
use data_avalanche_core::SnapshotReason;

/// How the stream starts, as `prepare` decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Plan {
    /// From the pipeline's checkpoint.
    Resume,
    /// From a new position (the slot was created or recreated).
    Fresh(SnapshotReason),
}

/// What the start knows about one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Facts {
    /// The pipeline tracked it before (its schema is cached): it "has a confirmed offset".
    pub tracked_before: bool,
    /// Its snapshot document says `in_progress`: a snapshot to resume from its cursor, unless the
    /// start follows a gap.
    pub in_progress: bool,
    /// The sink has the table (`table_state().exists`, before `ensure_table`).
    pub sink_has_table: bool,
    /// `prepare` began publishing it at this start (`Prepared::added`): its changes before now
    /// never reached the stream.
    pub added: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Stream it, without a snapshot.
    Stream,
    /// Snapshot it while streaming, from its cursor when `resume`. A `dump` stops once its
    /// snapshot is done (it isn't streamed further).
    Snapshot { resume: bool, dump: bool },
    /// Neither: a `dump_initial` table the sink already has.
    Stop,
}

/// The decision, and a warning when `never` leaves a gap: the position was lost or the table was
/// added to the stream at this start, and `never` takes no snapshot (Q-M6a-1). After a gap (the
/// position lost, an orphan adopted) a snapshot in progress starts over: rows before its cursor
/// may have changed in the gap (D-M6a.8).
pub(crate) fn decide(mode: SnapshotMode, plan: Plan, f: Facts) -> (Decision, Option<String>) {
    let recovery = matches!(
        plan,
        Plan::Fresh(SnapshotReason::PositionLost | SnapshotReason::AdoptedOrphan)
    );
    let snapshot = |dump| Decision::Snapshot {
        resume: f.in_progress && !recovery,
        dump,
    };
    match mode {
        SnapshotMode::Never => {
            let gap = if recovery {
                Some("the stream position was lost and recreated".to_string())
            } else if f.added {
                Some("the table was added to the stream at this start".to_string())
            } else {
                None
            };
            let warning = gap.map(|why| {
                format!(
                    "{why}, and snapshot mode never takes no snapshot: changes before now are missing; snapshot the table on demand"
                )
            });
            (Decision::Stream, warning)
        }
        SnapshotMode::Initial
            if f.in_progress || !f.tracked_before || f.added || matches!(plan, Plan::Fresh(_)) =>
        {
            (snapshot(false), None)
        }
        SnapshotMode::Initial => (Decision::Stream, None),
        SnapshotMode::Always => (snapshot(false), None),
        SnapshotMode::DumpAlways => (snapshot(true), None),
        SnapshotMode::DumpInitial if f.in_progress || !f.tracked_before || !f.sink_has_table => {
            (snapshot(true), None)
        }
        SnapshotMode::DumpInitial => (Decision::Stop, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SnapshotMode as M;

    const RESUME: Plan = Plan::Resume;
    const FIRST: Plan = Plan::Fresh(SnapshotReason::FirstStart);
    const LOST: Plan = Plan::Fresh(SnapshotReason::PositionLost);
    const ADOPTED: Plan = Plan::Fresh(SnapshotReason::AdoptedOrphan);

    fn facts(tracked_before: bool, in_progress: bool, sink_has_table: bool, added: bool) -> Facts {
        Facts {
            tracked_before,
            in_progress,
            sink_has_table,
            added,
        }
    }

    fn snap(resume: bool, dump: bool) -> Decision {
        Decision::Snapshot { resume, dump }
    }

    /// The Data-Flow Phase 0 table, row by row, with the recovery cases.
    #[test]
    fn the_phase_0_table() {
        let new = facts(false, false, false, false);
        let known = facts(true, false, true, false);
        let cases = [
            // always: snapshot, then stream, either way
            (M::Always, RESUME, known, snap(false, false)),
            (M::Always, FIRST, new, snap(false, false)),
            // never: stream
            (M::Never, FIRST, new, Decision::Stream),
            (M::Never, RESUME, known, Decision::Stream),
            // initial: no confirmed offset → snapshot; confirmed → stream
            (M::Initial, FIRST, new, snap(false, false)),
            (M::Initial, RESUME, new, snap(false, false)),
            (M::Initial, RESUME, known, Decision::Stream),
            (M::Initial, LOST, known, snap(false, false)),
            (M::Initial, ADOPTED, known, snap(false, false)),
            (
                M::Initial,
                RESUME,
                facts(true, false, true, true),
                snap(false, false),
            ),
            // dump_always: snapshot, then stop, either way
            (M::DumpAlways, RESUME, known, snap(false, true)),
            (M::DumpAlways, FIRST, new, snap(false, true)),
            // dump_initial: none → snapshot+stop; confirmed + sink has → stop; sink lacks → snapshot
            (M::DumpInitial, FIRST, new, snap(false, true)),
            (M::DumpInitial, RESUME, known, Decision::Stop),
            (
                M::DumpInitial,
                RESUME,
                facts(true, false, false, false),
                snap(false, true),
            ),
            (M::DumpInitial, LOST, known, Decision::Stop),
        ];
        for (mode, plan, f, want) in cases {
            assert_eq!(decide(mode, plan, f).0, want, "{mode:?} {plan:?} {f:?}");
        }
    }

    #[test]
    fn an_interrupted_snapshot_resumes() {
        let interrupted = facts(true, true, true, false);
        for (mode, dump) in [
            (M::Initial, false),
            (M::Always, false),
            (M::DumpAlways, true),
            (M::DumpInitial, true),
        ] {
            assert_eq!(
                decide(mode, RESUME, interrupted).0,
                snap(true, dump),
                "{mode:?}"
            );
        }
        assert_eq!(
            decide(M::Never, RESUME, interrupted).0,
            Decision::Stream,
            "never doesn't snapshot, not even to finish one"
        );
    }

    #[test]
    fn a_snapshot_interrupted_before_a_gap_starts_over() {
        let interrupted = facts(true, true, true, false);
        for plan in [LOST, ADOPTED] {
            for (mode, dump) in [
                (M::Initial, false),
                (M::Always, false),
                (M::DumpAlways, true),
                (M::DumpInitial, true),
            ] {
                assert_eq!(
                    decide(mode, plan, interrupted).0,
                    snap(false, dump),
                    "{mode:?} {plan:?}"
                );
            }
        }
    }

    #[test]
    fn never_warns_of_a_gap_it_doesnt_fill() {
        let known = facts(true, false, true, false);
        for plan in [LOST, ADOPTED] {
            let (d, w) = decide(M::Never, plan, known);
            assert_eq!(d, Decision::Stream);
            assert!(w.unwrap().contains("position was lost"));
        }
        let (_, w) = decide(M::Never, RESUME, facts(true, false, true, true));
        assert!(w.unwrap().contains("added to the stream"));
        assert_eq!(decide(M::Never, FIRST, Facts::default()).1, None);
        assert_eq!(decide(M::Initial, LOST, known).1, None);
    }
}
