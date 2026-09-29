//! Durable state for data-avalanche: the per-pipeline documents (Data-Flow §State Layout), leases
//! and the `globals.state` documents (Clustering §12), each a JSON [`Document`] with a schema
//! `version`, written with compare-and-swap through `data-avalanche-storage`.

mod doc;

pub use doc::{DocStore, Document, MODIFY_ATTEMPTS, StateError, Versioned};
