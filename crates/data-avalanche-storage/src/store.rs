//! Compare-and-swap object access: [`VersionedStore`] and its `object_store` implementation.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, ObjectMeta, ObjectStore, PutMode, PutOptions, PutPayload, PutResult, UpdateVersion,
};

/// The version token of a stored object: whatever the backend compares on a conditional update.
/// S3 and Azure compare the ETag, GCS the generation (`version`). Opaque to callers; only ever
/// passed back to [`VersionedStore::update`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

impl From<PutResult> for ObjectVersion {
    fn from(r: PutResult) -> Self {
        Self {
            e_tag: r.e_tag,
            version: r.version,
        }
    }
}

impl From<&ObjectMeta> for ObjectVersion {
    fn from(m: &ObjectMeta) -> Self {
        Self {
            e_tag: m.e_tag.clone(),
            version: m.version.clone(),
        }
    }
}

impl From<&ObjectVersion> for UpdateVersion {
    fn from(v: &ObjectVersion) -> Self {
        Self {
            e_tag: v.e_tag.clone(),
            version: v.version.clone(),
        }
    }
}

/// Why a storage operation failed. The split follows the lease renewer's taxonomy (Clustering
/// §19.2): `Conflict` and `Contention` are answers from the store, `Transient` may succeed on
/// retry, `Permanent` won't.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A conditional write lost: `create` found the object present, or `update` found it changed
    /// or gone.
    #[error("conflict at {key}: the object exists or changed since it was read")]
    Conflict { key: String },
    /// Concurrent conditional writes on one object (S3 409 `ConditionalRequestConflict`, after
    /// `object_store`'s own retries). The outcome is unknown: read the object again.
    #[error("contention at {key}: concurrent conditional writes; read it again")]
    Contention { key: String },
    /// A network, timeout or server failure.
    #[error("transient storage failure at {key}: {reason}")]
    Transient { key: String, reason: String },
    /// Access denied, bad credentials, an unsupported operation or an invalid key.
    #[error("storage failure at {key}: {reason}")]
    Permanent { key: String, reason: String },
}

/// Conditional object access under one root. Keys are `/`-separated and relative to the root;
/// each key is stored exactly as given (no escaping), so a key must have no empty segment, no `.`
/// or `..` segment and no control character.
#[async_trait]
pub trait VersionedStore: Send + Sync + std::fmt::Debug {
    /// The object's body and version, or `None` when it doesn't exist.
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError>;
    /// Writes only if no object exists at `key`; otherwise `Conflict`.
    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError>;
    /// Writes only if the object still has version `expected`; otherwise `Conflict` (also when it
    /// no longer exists).
    async fn update(
        &self,
        key: &str,
        expected: &ObjectVersion,
        body: Bytes,
    ) -> Result<ObjectVersion, StoreError>;
    /// Writes unconditionally.
    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError>;
    /// Every key under `prefix/`, sorted (`a/b` lists `a/b/c`, not `a/bc`); `""` lists the whole
    /// root.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}

/// [`VersionedStore`] over an `object_store` bucket, rooted at a key prefix (`""` = bucket root).
#[derive(Debug, Clone)]
pub struct ObjectStoreVersioned {
    store: Arc<dyn ObjectStore>,
    root: Path,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Get,
    Create,
    Update,
    Put,
    List,
}

impl ObjectStoreVersioned {
    /// # Errors
    /// `Permanent` when `root` is not a valid key.
    pub fn new(store: Arc<dyn ObjectStore>, root: &str) -> Result<Self, StoreError> {
        let root = if root.is_empty() {
            Path::default()
        } else {
            parse_key(root)?
        };
        Ok(Self { store, root })
    }

    fn path(&self, key: &str) -> Result<Path, StoreError> {
        let key = parse_key(key)?;
        Ok(self.root.parts().chain(key.parts()).collect())
    }

    fn relative(&self, location: &Path) -> String {
        let n = self.root.parts().count();
        location
            .parts()
            .skip(n)
            .map(|p| p.as_ref().to_owned())
            .collect::<Vec<_>>()
            .join("/")
    }

    async fn write(
        &self,
        op: Op,
        key: &str,
        body: Bytes,
        mode: PutMode,
    ) -> Result<ObjectVersion, StoreError> {
        let path = self.path(key)?;
        let opts = PutOptions {
            mode,
            ..Default::default()
        };
        self.store
            .put_opts(&path, PutPayload::from_bytes(body), opts)
            .await
            .map(ObjectVersion::from)
            .map_err(|e| map_error(op, key, e))
    }
}

fn parse_key(key: &str) -> Result<Path, StoreError> {
    let invalid = |reason: String| StoreError::Permanent {
        key: key.to_owned(),
        reason,
    };
    if key.is_empty() || key.starts_with('/') || key.ends_with('/') {
        return Err(invalid(
            "invalid key: empty, or a leading or trailing '/'".into(),
        ));
    }
    Path::parse(key).map_err(|e| invalid(format!("invalid key: {e}")))
}

fn map_error(op: Op, key: &str, e: object_store::Error) -> StoreError {
    use object_store::Error as E;
    let key = key.to_owned();
    match e {
        E::AlreadyExists { .. } if op == Op::Create => StoreError::Conflict { key },
        E::AlreadyExists { .. } => StoreError::Contention { key },
        E::Precondition { .. } => StoreError::Conflict { key },
        E::NotFound { .. } if op == Op::Update => StoreError::Conflict { key },
        e @ (E::PermissionDenied { .. }
        | E::Unauthenticated { .. }
        | E::NotSupported { .. }
        | E::NotImplemented { .. }
        | E::InvalidPath { .. }
        | E::NotFound { .. }
        | E::NotModified { .. }) => StoreError::Permanent {
            key,
            reason: e.to_string(),
        },
        e => StoreError::Transient {
            key,
            reason: e.to_string(),
        },
    }
}

#[async_trait]
impl VersionedStore for ObjectStoreVersioned {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectVersion)>, StoreError> {
        let path = self.path(key)?;
        let result = match self.store.get_opts(&path, GetOptions::default()).await {
            Ok(r) => r,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(map_error(Op::Get, key, e)),
        };
        let version = ObjectVersion::from(&result.meta);
        let body = result
            .bytes()
            .await
            .map_err(|e| map_error(Op::Get, key, e))?;
        Ok(Some((body, version)))
    }

    async fn create(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.write(Op::Create, key, body, PutMode::Create).await
    }

    async fn update(
        &self,
        key: &str,
        expected: &ObjectVersion,
        body: Bytes,
    ) -> Result<ObjectVersion, StoreError> {
        self.write(Op::Update, key, body, PutMode::Update(expected.into()))
            .await
    }

    async fn put(&self, key: &str, body: Bytes) -> Result<ObjectVersion, StoreError> {
        self.write(Op::Put, key, body, PutMode::Overwrite).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let path = if prefix.is_empty() {
            self.root.clone()
        } else {
            self.path(prefix)?
        };
        let metas: Vec<ObjectMeta> = self
            .store
            .list(Some(&path))
            .try_collect()
            .await
            .map_err(|e| map_error(Op::List, prefix, e))?;
        let mut keys: Vec<String> = metas.iter().map(|m| self.relative(&m.location)).collect();
        keys.sort();
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn store(root: &str) -> ObjectStoreVersioned {
        ObjectStoreVersioned::new(Arc::new(InMemory::new()), root).unwrap()
    }

    fn b(s: &'static str) -> Bytes {
        Bytes::from_static(s.as_bytes())
    }

    #[tokio::test]
    async fn create_then_get_returns_body_and_version() {
        let s = store("root");
        let v = s.create("a/doc.json", b("one")).await.unwrap();
        let (body, got) = s.get("a/doc.json").await.unwrap().unwrap();
        assert_eq!(body, b("one"));
        assert_eq!(got, v);
        assert!(s.get("a/missing.json").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn create_on_existing_key_conflicts() {
        let s = store("root");
        s.create("k", b("one")).await.unwrap();
        let e = s.create("k", b("two")).await.unwrap_err();
        assert!(
            matches!(&e, StoreError::Conflict { key } if key == "k"),
            "{e:?}"
        );
        assert_eq!(s.get("k").await.unwrap().unwrap().0, b("one"));
    }

    #[tokio::test]
    async fn update_is_compare_and_swap() {
        let s = store("");
        let v1 = s.create("k", b("one")).await.unwrap();
        let v2 = s.update("k", &v1, b("two")).await.unwrap();
        assert_ne!(v1, v2, "an update must advance the version");
        let stale = s.update("k", &v1, b("three")).await.unwrap_err();
        assert!(matches!(stale, StoreError::Conflict { .. }), "{stale:?}");
        assert_eq!(s.get("k").await.unwrap().unwrap(), (b("two"), v2));
    }

    #[tokio::test]
    async fn update_of_a_missing_object_conflicts() {
        let s = store("root");
        let v = s.create("k", b("one")).await.unwrap();
        let e = s.update("other", &v, b("two")).await.unwrap_err();
        assert!(matches!(e, StoreError::Conflict { .. }), "{e:?}");
        assert!(s.get("other").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_overwrites_unconditionally() {
        let s = store("root");
        s.put("k", b("one")).await.unwrap();
        let v = s.put("k", b("two")).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), (b("two"), v));
    }

    #[tokio::test]
    async fn list_returns_sorted_keys_under_whole_segment_prefix() {
        let s = store("root/sub");
        for k in [
            "snap/b.json",
            "snap/a.json",
            "snap/jobs/j1.json",
            "snapx/c.json",
            "other.json",
        ] {
            s.put(k, b("x")).await.unwrap();
        }
        assert_eq!(
            s.list("snap").await.unwrap(),
            vec!["snap/a.json", "snap/b.json", "snap/jobs/j1.json"]
        );
        assert!(s.list("none").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn roots_are_isolated() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let a = ObjectStoreVersioned::new(mem.clone(), "pipelines/a").unwrap();
        let b2 = ObjectStoreVersioned::new(mem.clone(), "pipelines/ab").unwrap();
        let root = ObjectStoreVersioned::new(mem, "").unwrap();
        a.create("state.json", b("a")).await.unwrap();
        b2.create("state.json", b("b")).await.unwrap();
        assert_eq!(a.get("state.json").await.unwrap().unwrap().0, b("a"));
        assert_eq!(a.list("").await.unwrap(), vec!["state.json"]);
        assert_eq!(
            root.list("pipelines").await.unwrap(),
            vec!["pipelines/a/state.json", "pipelines/ab/state.json"]
        );
    }

    #[tokio::test]
    async fn keys_are_stored_verbatim() {
        let mem: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s = ObjectStoreVersioned::new(mem.clone(), ".pipeline-state").unwrap();
        let key = "snapshots/sales.Заказы-2026 #1.json";
        s.create(key, b("x")).await.unwrap();
        let names: Vec<String> = mem
            .list(None)
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(names, vec![format!(".pipeline-state/{key}")]);
        assert_eq!(s.list("snapshots").await.unwrap(), vec![key]);
    }

    #[tokio::test]
    async fn invalid_keys_are_permanent_errors() {
        let s = store("root");
        for key in ["", "/a", "a/", "a//b", "a/../b", "./a", "a/\u{7}"] {
            let e = s.get(key).await.unwrap_err();
            assert!(
                matches!(&e, StoreError::Permanent { reason, .. } if reason.starts_with("invalid key")),
                "{key:?}: {e:?}"
            );
        }
        assert!(ObjectStoreVersioned::new(Arc::new(InMemory::new()), "a//b").is_err());
    }

    #[test]
    fn error_mapping() {
        use object_store::Error as E;
        let src = || -> Box<dyn std::error::Error + Send + Sync> { "boom".into() };
        let p = || "p".to_owned();
        let cases: Vec<(Op, E, &str)> = vec![
            (
                Op::Create,
                E::AlreadyExists {
                    path: p(),
                    source: src(),
                },
                "Conflict",
            ),
            (
                Op::Update,
                E::AlreadyExists {
                    path: p(),
                    source: src(),
                },
                "Contention",
            ),
            (
                Op::Put,
                E::AlreadyExists {
                    path: p(),
                    source: src(),
                },
                "Contention",
            ),
            (
                Op::Update,
                E::Precondition {
                    path: p(),
                    source: src(),
                },
                "Conflict",
            ),
            (
                Op::Update,
                E::NotFound {
                    path: p(),
                    source: src(),
                },
                "Conflict",
            ),
            (
                Op::List,
                E::NotFound {
                    path: p(),
                    source: src(),
                },
                "Permanent",
            ),
            (
                Op::Get,
                E::PermissionDenied {
                    path: p(),
                    source: src(),
                },
                "Permanent",
            ),
            (
                Op::Get,
                E::Unauthenticated {
                    path: p(),
                    source: src(),
                },
                "Permanent",
            ),
            (
                Op::Create,
                E::NotImplemented {
                    operation: "x".into(),
                    implementer: "y".into(),
                },
                "Permanent",
            ),
            (Op::Put, E::NotSupported { source: src() }, "Permanent"),
            (
                Op::Get,
                E::Generic {
                    store: "S3",
                    source: src(),
                },
                "Transient",
            ),
        ];
        for (op, e, want) in cases {
            let what = format!("{op:?} {e}");
            let got = match map_error(op, "k", e) {
                StoreError::Conflict { .. } => "Conflict",
                StoreError::Contention { .. } => "Contention",
                StoreError::Transient { .. } => "Transient",
                StoreError::Permanent { .. } => "Permanent",
            };
            assert_eq!(got, want, "{what}");
        }
    }
}
