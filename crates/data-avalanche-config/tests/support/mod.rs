//! Shared test helpers: an environment map, a temp directory for referenced files, and the spec's
//! §14 example with its file paths pointed at that directory.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::OnceLock;

use data_avalanche_config::FingerprintKey;

pub const SPEC_EXAMPLE: &str = include_str!("../fixtures/spec_example.yaml");

/// A fixed cluster-wide fingerprint key for tests: they check the config, not fingerprint
/// agreement across a real cluster, so any key will do as long as it's the same key on both sides
/// of a reload diff.
pub fn fp_key() -> FingerprintKey {
    FingerprintKey::from_bytes([0x42; 32])
}

/// A minimal valid config: one PostgreSQL → Delta pipeline. Needs `PW` in the environment.
pub const MINIMAL: &str = r#"
config_version: 1
globals:
  state: { bucket: app-state }
pipelines:
  orders:
    id: orders
    source:
      host: db.example.com
      db: shop
      user: cdc
      password: ${PW}
      postgres: { publication: orders_pub, slot: orders_slot }
    tables:
      include: [public.orders]
    sink:
      type: delta
      bucket: lake
      delta: {}
"#;

/// A throwaway self-signed certificate and its key, generated once per test binary: no key material
/// is committed.
fn pem_pair() -> &'static (String, String) {
    static PAIR: OnceLock<(String, String)> = OnceLock::new();
    PAIR.get_or_init(|| {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["data-avalanche-test".to_string()]).unwrap();
        (cert.pem(), signing_key.serialize_pem())
    })
}

/// A PEM certificate (self-signed, CN `data-avalanche-test`).
pub fn cert() -> &'static str {
    &pem_pair().0
}

/// The PEM (PKCS#8) private key of [`cert`].
pub fn key() -> &'static str {
    &pem_pair().1
}

pub fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// A temp directory for files a config references; removed on drop.
pub struct Files {
    dir: tempfile::TempDir,
}

impl Files {
    pub fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    pub fn dir(&self) -> String {
        self.dir.path().display().to_string()
    }

    pub fn write(&self, name: &str, content: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, content).unwrap();
        path
    }
}

/// The §14 example, runnable: its `/etc/cdc/tls/` files written to `files`, and every variable it
/// references set.
pub fn spec_example(files: &Files) -> (String, BTreeMap<String, String>) {
    files.write("api.crt", cert());
    files.write("gossip.crt", cert());
    files.write("gossip-ca.crt", cert());
    let creds = files.write(
        "events-db.json",
        r#"{"user": "events", "password": "events-pw"}"#,
    );
    let yaml = SPEC_EXAMPLE.replace("/etc/cdc/tls/", &format!("{}/", files.dir()));
    let env = env(&[
        ("ACCOUNT_ID", "123456789012"),
        ("API_TLS_KEY", key()),
        ("GOSSIP_TLS_KEY", key()),
        ("CATALOG_CLIENT_ID", "cdc-client"),
        ("CATALOG_CLIENT_SECRET", "client-secret"),
        ("ORDERS_DB_PASSWORD", "orders-pw"),
        ("EVENTS_DB_CREDS_FILE", creds.to_str().unwrap()),
    ]);
    (yaml, env)
}
