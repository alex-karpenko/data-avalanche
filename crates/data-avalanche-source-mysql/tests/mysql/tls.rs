//! TLS against `mysql-tls`, which refuses any TCP connection without TLS
//! (`require_secure_transport`): every connection that logs in there is encrypted. Its throwaway
//! CA and client certificate are made when the container starts and read from the git-ignored
//! `test-infra/.tls/mysql`. These run with the default `mysql-84` target only.

use std::collections::BTreeMap;
use std::path::PathBuf;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::SourceError;
use data_avalanche_testkit::require_it;

use crate::common::{connector, target};

const PORT: u16 = 53386;

fn material(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-infra/.tls/mysql")
        .join(name)
}

fn read(name: &str) -> String {
    std::fs::read_to_string(material(name))
        .unwrap_or_else(|e| panic!("{name} from test-infra/.tls/mysql (is mysql-tls up?): {e}"))
}

fn default_target() -> bool {
    if target().name == "mysql-84" {
        return true;
    }
    eprintln!("skipped: the TLS tests run with the mysql-84 target");
    false
}

/// A pipeline on `mysql-tls` at `host` as `user`/`password`, with `ssl` (YAML, indented six
/// spaces) and `env` for its `${…}` references. The server certificate names `localhost` only.
fn pipeline(host: &str, user: &str, password: &str, ssl: &str, env: &[(&str, String)]) -> Pipeline {
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: it_tls
    source:
      type: mysql
      host: {host}
      port: {PORT}
      db: avalanche
      user: {user}
      password: {password}
      {ssl}
      mysql: {{ replication_type: gtid }}
    tables:
      include: [t]
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#
    );
    let env: BTreeMap<String, String> = env
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    let loaded = load_str(&yaml, &env, &FingerprintKey::from_bytes([1; 32]))
        .unwrap_or_else(|r| panic!("test pipeline: {r:?}"));
    loaded
        .config
        .pipelines
        .into_values()
        .next()
        .expect("one pipeline")
}

#[tokio::test]
async fn verify_full_with_an_inline_ca_connects() {
    require_it!();
    if !default_target() {
        return;
    }
    let p = pipeline(
        "localhost",
        "root",
        "avalanche",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
        &[("CA", read("ca.crt"))],
    );
    connector(&p).preflight().await.expect("preflight over TLS");
}

#[tokio::test]
async fn a_client_certificate_with_an_inline_key_authenticates() {
    require_it!();
    if !default_target() {
        return;
    }
    // `avalanche_cert` logs in only with the certificate whose subject it requires.
    let p = pipeline(
        "localhost",
        "avalanche_cert",
        "unused",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"\n      ssl_cert: \"${CERT}\"\n      ssl_key: \"${KEY}\"",
        &[
            ("CA", read("ca.crt")),
            ("CERT", read("client.crt")),
            ("KEY", read("client.key")),
        ],
    );
    connector(&p).preflight().await.expect("certificate login");
    let without = pipeline(
        "localhost",
        "avalanche_cert",
        "unused",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
        &[("CA", read("ca.crt"))],
    );
    match connector(&without).preflight().await {
        Err(SourceError::Permanent(m)) => assert!(m.contains("(error 1045)"), "{m}"),
        other => panic!("refused expected without the certificate, got {other:?}"),
    }
}

/// `verify-ca` should check the chain and not the name, but mysql_async 0.37.1 forgives a name
/// mismatch only when rustls's message contains `NotValidForName`, and rustls 0.23 prints
/// "certificate not valid for name": `verify-ca` behaves like `verify-full` (D-M7a.6, as PG's
/// replication connection, M5 Deviations Task 2). This pins it: once the crate forgives the
/// mismatch, the connection by `127.0.0.1` (not in the certificate) succeeds and this test fails.
#[tokio::test]
async fn verify_ca_by_an_unnamed_address_is_refused_like_verify_full() {
    require_it!();
    if !default_target() {
        return;
    }
    for mode in ["verify-ca", "verify-full"] {
        let p = pipeline(
            "127.0.0.1",
            "root",
            "avalanche",
            &format!("ssl_mode: {mode}\n      ssl_ca: \"${{CA}}\""),
            &[("CA", read("ca.crt"))],
        );
        match connector(&p).preflight().await {
            Err(SourceError::Permanent(m)) => {
                assert!(
                    m.starts_with("TLS:") && m.contains("not valid for name"),
                    "{mode}: {m}"
                )
            }
            other => panic!("{mode}: refused expected, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_wrong_ca_or_no_tls_is_refused() {
    require_it!();
    if !default_target() {
        return;
    }
    let other_ca = {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(vec![]).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.self_signed(&key).unwrap().pem()
    };
    for (what, ssl, env, reason) in [
        (
            "a foreign CA",
            "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
            vec![("CA", other_ca.clone())],
            "UnknownIssuer",
        ),
        (
            "a foreign CA, chain only",
            "ssl_mode: verify-ca\n      ssl_ca: \"${CA}\"",
            vec![("CA", other_ca)],
            "UnknownIssuer",
        ),
        ("no TLS", "ssl_mode: disable", vec![], "(error 3159)"),
    ] {
        let c = connector(&pipeline("localhost", "root", "avalanche", ssl, &env));
        match c.preflight().await {
            Err(SourceError::Permanent(detail)) => {
                assert!(detail.contains(reason), "{what}: {detail}")
            }
            other => panic!("{what}: a permanent error expected, got {other:?}"),
        }
    }
}
