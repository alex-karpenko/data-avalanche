//! What the worker asks of its owner: whether it may still commit (the lease), and, in tests, where
//! to stop as if the process died.

use std::fmt::Debug;

use data_avalanche_core::{SourcePosition, TableRef};

/// Asked before every sink commit. M6b's lease renewer answers `false` once the lease's renewal
/// safety deadline passed (Clustering §19): the worker then stops with `LeaseLost` and writes
/// nothing more.
pub trait CommitGuard: Send + Sync + Debug {
    fn may_commit(&self) -> bool;
}

/// Always commits: a worker that no lease guards (tests, and M6a's single worker).
#[derive(Debug, Clone, Copy, Default)]
pub struct Unguarded;

impl CommitGuard for Unguarded {
    fn may_commit(&self) -> bool {
        true
    }
}

/// A point just after the worker made something durable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookPoint {
    /// One table's change set is committed to the sink; the checkpoint isn't persisted yet.
    SinkCommitted { table: TableRef },
    /// The checkpoint is persisted; the stream isn't acknowledged yet.
    CheckpointPersisted { position: SourcePosition },
    /// A snapshot chunk of the table is in the accumulator; its cursor isn't persisted yet.
    ChunkEmitted { table: TableRef },
    /// A fresh start (Phase 0) cached the tables' schemas; its new checkpoint isn't persisted yet.
    SchemasCached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HookAction {
    #[default]
    Continue,
    /// Stop now, as if the process died: the worker returns `WorkerError::Crashed` and writes
    /// nothing more.
    Crash,
}

/// Test hooks; the default does nothing.
pub trait Hooks: Send + Sync + Debug {
    fn at(&self, point: &HookPoint) -> HookAction {
        let _ = point;
        HookAction::Continue
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoHooks;

impl Hooks for NoHooks {}
