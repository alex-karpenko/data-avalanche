//! JSON documents with a schema `version`, read and written with compare-and-swap.

use std::sync::Arc;

use bytes::Bytes;
use data_avalanche_storage::{ObjectVersion, StoreError, VersionedStore};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A state document type. Stored as pretty JSON with a top-level `"version"` (the schema version)
/// beside the type's own fields; a document with another `version` is refused, never guessed at.
pub trait Document: Serialize + DeserializeOwned + Send + Sync {
    /// Names the document in errors (`"lease"`, `"pipeline state"`).
    const KIND: &'static str;
    const SCHEMA_VERSION: u64;
}

/// A document and the object version it was read or written at.
#[derive(Debug, Clone, PartialEq)]
pub struct Versioned<T> {
    pub doc: T,
    pub version: ObjectVersion,
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("{key}: not a valid {kind} document: {reason}")]
    Corrupt {
        key: String,
        kind: &'static str,
        reason: String,
    },
    #[error(
        "{key}: {kind} document has schema version {found}; this build reads version {supported} (written by another data-avalanche release?)"
    )]
    UnsupportedVersion {
        key: String,
        kind: &'static str,
        found: u64,
        supported: u64,
    },
}

impl StateError {
    /// True when a conditional write lost (`Conflict`) or its outcome is unknown (`Contention`):
    /// read again and decide.
    pub fn is_race(&self) -> bool {
        matches!(
            self,
            Self::Store(StoreError::Conflict { .. } | StoreError::Contention { .. })
        )
    }

    /// True when a conditional write definitely lost (`Conflict`), as opposed to an unknown
    /// outcome (`Contention`).
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Store(StoreError::Conflict { .. }))
    }
}

#[derive(Serialize)]
struct Envelope<'a, T> {
    version: u64,
    #[serde(flatten)]
    doc: &'a T,
}

pub(crate) fn encode<T: Document>(doc: &T) -> Bytes {
    let body = Envelope {
        version: T::SCHEMA_VERSION,
        doc,
    };
    Bytes::from(serde_json::to_vec_pretty(&body).expect("state documents serialize to JSON"))
}

pub(crate) fn decode<T: Document>(key: &str, body: &[u8]) -> Result<T, StateError> {
    let corrupt = |reason: String| StateError::Corrupt {
        key: key.to_owned(),
        kind: T::KIND,
        reason,
    };
    let mut value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| corrupt(e.to_string()))?;
    let map = value
        .as_object_mut()
        .ok_or_else(|| corrupt("not a JSON object".into()))?;
    let found = match map.remove("version") {
        Some(v) if v.is_u64() => v.as_u64().unwrap_or_default(),
        Some(_) => return Err(corrupt("\"version\" is not a non-negative integer".into())),
        None => return Err(corrupt("no \"version\" field".into())),
    };
    if found != T::SCHEMA_VERSION {
        return Err(StateError::UnsupportedVersion {
            key: key.to_owned(),
            kind: T::KIND,
            found,
            supported: T::SCHEMA_VERSION,
        });
    }
    serde_json::from_value(value).map_err(|e| corrupt(e.to_string()))
}

/// Typed documents over a [`VersionedStore`].
#[derive(Debug, Clone)]
pub struct DocStore {
    store: Arc<dyn VersionedStore>,
}

/// How many times [`DocStore::modify`] re-reads after losing a race before it gives up.
pub const MODIFY_ATTEMPTS: usize = 16;

impl DocStore {
    pub fn new(store: Arc<dyn VersionedStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Arc<dyn VersionedStore> {
        &self.store
    }

    /// # Errors
    /// A storage failure, or a body that isn't a valid document of this type and version.
    pub async fn load<T: Document>(&self, key: &str) -> Result<Option<Versioned<T>>, StateError> {
        match self.store.get(key).await? {
            None => Ok(None),
            Some((body, version)) => Ok(Some(Versioned {
                doc: decode(key, &body)?,
                version,
            })),
        }
    }

    /// Writes `doc` only if nothing exists at `key`.
    ///
    /// # Errors
    /// `Store(Conflict)` when the key exists.
    pub async fn create<T: Document>(
        &self,
        key: &str,
        doc: &T,
    ) -> Result<ObjectVersion, StateError> {
        Ok(self.store.create(key, encode(doc)).await?)
    }

    /// Writes `doc` only if the object is still at `expected`.
    ///
    /// # Errors
    /// `Store(Conflict)` when it moved or is gone; `Store(Contention)` when the outcome is unknown.
    pub async fn update<T: Document>(
        &self,
        key: &str,
        expected: &ObjectVersion,
        doc: &T,
    ) -> Result<ObjectVersion, StateError> {
        Ok(self.store.update(key, expected, encode(doc)).await?)
    }

    /// Read-modify-write: `f` gets the current document (`None` if absent) and returns the one to
    /// write, which is created or CAS-updated. On a lost race it reads again and calls `f` again,
    /// up to [`MODIFY_ATTEMPTS`] times. After `Contention` the earlier write may have landed, so
    /// `f` must compute the target state from what it reads (set, don't increment blindly).
    ///
    /// # Errors
    /// An error from `f` (nothing is written), a storage or decode failure, or the last race error
    /// once the attempts run out.
    pub async fn modify<T, F>(&self, key: &str, mut f: F) -> Result<Versioned<T>, StateError>
    where
        T: Document,
        F: FnMut(Option<T>) -> Result<T, StateError> + Send,
    {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let current = self.load::<T>(key).await?;
            let (doc, written) = match current {
                None => {
                    let doc = f(None)?;
                    let r = self.create(key, &doc).await;
                    (doc, r)
                }
                Some(Versioned { doc, version }) => {
                    let doc = f(Some(doc))?;
                    let r = self.update(key, &version, &doc).await;
                    (doc, r)
                }
            };
            match written {
                Ok(version) => return Ok(Versioned { doc, version }),
                Err(e) if e.is_race() && attempt < MODIFY_ATTEMPTS => continue,
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_avalanche_storage::ObjectStoreVersioned;
    use object_store::memory::InMemory;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Counter {
        name: String,
        n: u64,
    }

    impl Document for Counter {
        const KIND: &'static str = "counter";
        const SCHEMA_VERSION: u64 = 3;
    }

    fn docs() -> DocStore {
        let store = ObjectStoreVersioned::new(Arc::new(InMemory::new()), "state").unwrap();
        DocStore::new(Arc::new(store))
    }

    fn counter(n: u64) -> Counter {
        Counter {
            name: "c".into(),
            n,
        }
    }

    #[test]
    fn encodes_version_first_as_pretty_json() {
        let body = encode(&counter(1));
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            "{\n  \"version\": 3,\n  \"name\": \"c\",\n  \"n\": 1\n}"
        );
        assert_eq!(decode::<Counter>("k", &body).unwrap(), counter(1));
    }

    #[test]
    fn refuses_other_schema_versions() {
        for (body, found) in [
            (r#"{"version":4,"name":"c","n":1}"#, 4),
            (r#"{"version":2,"name":"c","n":1}"#, 2),
        ] {
            let e = decode::<Counter>("k.json", body.as_bytes()).unwrap_err();
            assert!(
                matches!(e, StateError::UnsupportedVersion { found: f, supported: 3, .. } if f == found),
                "{e:?}"
            );
        }
        let e = decode::<Counter>("k.json", br#"{"version":4,"name":"c","n":1}"#).unwrap_err();
        assert_eq!(
            e.to_string(),
            "k.json: counter document has schema version 4; this build reads version 3 (written by another data-avalanche release?)"
        );
    }

    #[test]
    fn refuses_corrupt_documents() {
        let cases = [
            (&b"{not json"[..], "key must be a string"),
            (b"[1,2]", "not a JSON object"),
            (br#"{"name":"c","n":1}"#, "no \"version\" field"),
            (
                br#"{"version":"3","name":"c","n":1}"#,
                "\"version\" is not a non-negative integer",
            ),
            (
                br#"{"version":-3,"name":"c","n":1}"#,
                "\"version\" is not a non-negative integer",
            ),
            (br#"{"version":3,"name":"c"}"#, "missing field `n`"),
            (
                br#"{"version":3,"name":"c","n":1,"extra":true}"#,
                "unknown field `extra`",
            ),
            (b"", "EOF while parsing"),
        ];
        for (body, want) in cases {
            let e = decode::<Counter>("k.json", body).unwrap_err();
            match &e {
                StateError::Corrupt {
                    key,
                    kind: "counter",
                    reason,
                } if key == "k.json" => {
                    assert!(reason.contains(want), "{reason:?} should contain {want:?}")
                }
                other => panic!("{:?}: {other:?}", String::from_utf8_lossy(body)),
            }
        }
    }

    #[tokio::test]
    async fn load_create_update_round_trip() {
        let d = docs();
        assert!(d.load::<Counter>("c.json").await.unwrap().is_none());
        let v1 = d.create("c.json", &counter(1)).await.unwrap();
        let got = d.load::<Counter>("c.json").await.unwrap().unwrap();
        assert_eq!(
            got,
            Versioned {
                doc: counter(1),
                version: v1.clone()
            }
        );
        let e = d.create("c.json", &counter(9)).await.unwrap_err();
        assert!(e.is_race(), "{e:?}");
        let v2 = d.update("c.json", &v1, &counter(2)).await.unwrap();
        let e = d.update("c.json", &v1, &counter(9)).await.unwrap_err();
        assert!(e.is_race(), "{e:?}");
        assert_eq!(
            d.load::<Counter>("c.json").await.unwrap().unwrap(),
            Versioned {
                doc: counter(2),
                version: v2
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_creates_have_one_winner() {
        let d = docs();
        let tasks: Vec<_> = (0..8)
            .map(|i| {
                let d = d.clone();
                tokio::spawn(async move { d.create("c.json", &counter(i)).await })
            })
            .collect();
        let mut wins = 0;
        for t in tasks {
            match t.await.unwrap() {
                Ok(_) => wins += 1,
                Err(e) => assert!(e.is_race(), "{e:?}"),
            }
        }
        assert_eq!(wins, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_modifies_all_apply() {
        let d = docs();
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let d = d.clone();
                tokio::spawn(async move {
                    d.modify("c.json", |cur: Option<Counter>| {
                        Ok(counter(cur.map_or(0, |c| c.n) + 1))
                    })
                    .await
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(
            d.load::<Counter>("c.json").await.unwrap().unwrap().doc,
            counter(8)
        );
    }

    #[tokio::test]
    async fn modify_writes_nothing_when_f_fails() {
        let d = docs();
        d.create("c.json", &counter(1)).await.unwrap();
        let e = d
            .modify("c.json", |_: Option<Counter>| {
                Err(StateError::Corrupt {
                    key: "c.json".into(),
                    kind: "counter",
                    reason: "refused".into(),
                })
            })
            .await
            .unwrap_err();
        assert!(e.to_string().contains("refused"));
        assert_eq!(
            d.load::<Counter>("c.json").await.unwrap().unwrap().doc,
            counter(1)
        );
    }

    #[tokio::test]
    async fn a_corrupt_stored_document_is_an_error_not_absent() {
        let d = docs();
        d.store()
            .put("c.json", Bytes::from_static(b"{}"))
            .await
            .unwrap();
        let e = d.load::<Counter>("c.json").await.unwrap_err();
        assert!(matches!(e, StateError::Corrupt { .. }), "{e:?}");
        let e = d
            .modify("c.json", |_: Option<Counter>| Ok(counter(1)))
            .await
            .unwrap_err();
        assert!(
            matches!(e, StateError::Corrupt { .. }),
            "modify must not overwrite a corrupt document: {e:?}"
        );
    }
}
