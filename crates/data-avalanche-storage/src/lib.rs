//! Object storage for data-avalanche: `object_store` backends, the path rules of config §10, and
//! [`VersionedStore`], the compare-and-swap interface every coordination and state document uses.

mod backend;
mod paths;
mod store;

pub use backend::{Backend, BucketSpec, OpenError, Timeouts, open_bucket};
pub use paths::{
    COLOCATED_STATE_DIR, Location, colocated_state, escape_segment, explicit_state, globals_state,
    join, table_root,
};
pub use store::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
