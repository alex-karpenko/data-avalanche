//! Test-only fakes shared across `data-avalanche-state`'s unit tests.

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use data_avalanche_storage::{ObjectStoreVersioned, ObjectVersion, StoreError, VersionedStore};

/// A `VersionedStore` whose first `create`/`update` (optionally restricted to one `key`) reports
/// `Contention` — the unknown-outcome case this fake injects (an S3 409 after object_store's own
/// retries). A `Transient` timeout on a conditional write is an unknown outcome for the same
/// reason, even though this fake never injects it; callers must treat it the same way as
/// `Contention`. By default the real write still happens underneath first, so the outcome landed
/// but the caller is told it didn't ([`Self::without_write`] makes it not happen at all: the
/// outcome is unknown *and* it didn't land). Fires once (for a matching key), then behaves
/// normally.
#[derive(Debug)]
pub(crate) struct WriteThenLoseResponse {
    inner: ObjectStoreVersioned,
    armed: AtomicBool,
    write: bool,
    only_key: Option<String>,
}

impl WriteThenLoseResponse {
    pub(crate) fn new(inner: ObjectStoreVersioned) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(true),
            write: true,
            only_key: None,
        }
    }

    /// Reports `Contention` without performing the write underneath at all: the outcome is
    /// unknown, and nothing landed.
    pub(crate) fn without_write(mut self) -> Self {
        self.write = false;
        self
    }

    /// Only fires on this key; a create/update of any other key passes through unaffected.
    pub(crate) fn on_key(mut self, key: &str) -> Self {
        self.only_key = Some(key.to_owned());
        self
    }

    /// True the first time this is called for a matching key; false ever after, and always false
    /// for a key that doesn't match `only_key` (when set).
    fn fire(&self, key: &str) -> bool {
        if self.only_key.as_deref().is_some_and(|k| k != key) {
            return false;
        }
        self.armed.swap(false, Ordering::SeqCst)
    }
}

#[async_trait]
impl VersionedStore for WriteThenLoseResponse {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
        self.inner.get(key).await
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        let fire = self.fire(key);
        if fire && !self.write {
            return Err(StoreError::Contention {
                key: key.to_owned(),
            });
        }
        let result = self.inner.create(key, body).await;
        if fire {
            return Err(StoreError::Contention {
                key: key.to_owned(),
            });
        }
        result
    }

    async fn update(
        &self,
        key: &str,
        expected: &ObjectVersion,
        body: Bytes,
    ) -> Result<ObjectVersion, StoreError> {
        let fire = self.fire(key);
        if fire && !self.write {
            return Err(StoreError::Contention {
                key: key.to_owned(),
            });
        }
        let result = self.inner.update(key, expected, body).await;
        if fire {
            return Err(StoreError::Contention {
                key: key.to_owned(),
            });
        }
        result
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.inner.put(key, body).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list(prefix).await
    }
}
