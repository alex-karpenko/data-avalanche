//! Object storage for data-avalanche: `object_store` backends, the path rules of config §10, and
//! [`VersionedStore`], the compare-and-swap interface every coordination and state document uses.

mod store;

pub use store::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};
