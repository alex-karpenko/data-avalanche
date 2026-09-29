//! Object storage for data-avalanche: `object_store` backends, the path rules of config §10, and
//! [`VersionedStore`], the compare-and-swap interface every coordination and state document uses.
//!
//! - **Opening a bucket:** [`open_bucket`] takes a [`BucketSpec`] — [`Backend`] (S3, GCS, Azure),
//!   bucket or container, extra `object_store` options (emulator endpoints) and [`Timeouts`].
//!   Credentials come from each cloud's standard chain. [`Timeouts::COORDINATION`] suits leases
//!   and state: a hung store fails within about 8 s.
//! - **Where things live:** [`table_root`], [`colocated_state`], [`explicit_state`] and
//!   [`globals_state`] give a [`Location`] (bucket + key prefix, `""` = bucket root).
//!   [`escape_segment`] makes a database identifier safe as a key segment.
//! - **Compare-and-swap:** [`ObjectStoreVersioned`] roots a [`VersionedStore`] at a
//!   [`Location`]'s prefix: `get`, `create` (if absent), `update` (if the version still matches),
//!   `put`, `list`. Failures are a [`StoreError`]: `Conflict` (the condition failed; also an update
//!   of a missing object, on every backend), `Contention` (S3 409 after `object_store`'s retries:
//!   read again), `Transient`, `Permanent`. The version token is the ETag on S3 and Azure and the
//!   generation on GCS; [`ObjectVersion`] carries both and callers treat it as opaque.
//!
//! There is no conditional delete in `object_store`; releasing an object is a CAS write of a
//! tombstone (see `data-avalanche-state`'s leases).

mod backend;
mod paths;
mod store;

pub use backend::{Backend, BucketSpec, OpenError, Timeouts, open_bucket};
pub use paths::{
    COLOCATED_STATE_DIR, Location, colocated_state, escape_segment, explicit_state, globals_state,
    join, table_root,
};
pub use store::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
