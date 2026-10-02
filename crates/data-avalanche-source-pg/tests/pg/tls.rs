//! TLS on both connections (S-PG8, Q-M5-4) against `postgres-tls`, which refuses any TCP
//! connection without TLS. Its throwaway CA and client certificate are made when the container
//! starts and read from the git-ignored `test-infra/.tls`.

use std::collections::BTreeMap;
use std::path::PathBuf;

use data_avalanche_config::{FingerprintKey, Pipeline, load_str};
use data_avalanche_core::{ChangeStream, SourceError};
use data_avalanche_testkit::require_it;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use tokio_postgres::Client;

use crate::common::{Names, cleanup, collect, connector, first_stream, fresh};

const PORT: u16 = 55419;

fn material(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-infra/.tls")
        .join(name)
}

fn read(name: &str) -> String {
    std::fs::read_to_string(material(name))
        .unwrap_or_else(|e| panic!("{name} from test-infra/.tls (is postgres-tls up?): {e}"))
}

/// A superuser connection over TLS, verifying the test CA.
async fn admin() -> Client {
    data_avalanche_testkit::install_crypto_provider();
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(material("ca.crt")).expect("the test CA") {
        roots.add(cert.unwrap()).unwrap();
    }
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls);
    let dsn = format!(
        "host=localhost port={PORT} user=avalanche password=avalanche dbname=avalanche sslmode=require"
    );
    let (client, conn) = tokio_postgres::connect(&dsn, tls)
        .await
        .expect("connect to postgres-tls");
    tokio::spawn(conn);
    client
}

/// A pipeline on `postgres-tls` at `host` as `user`, with `ssl` (YAML flow entries) and `env` for
/// its `${…}` references. The server certificate names `localhost` only.
fn pipeline(n: &Names, host: &str, user: &str, ssl: &str, env: &[(&str, String)]) -> Pipeline {
    let yaml = format!(
        r#"
config_version: 1
globals:
  state: {{ bucket: app-state }}
pipelines:
  p:
    id: p
    source:
      host: {host}
      port: {PORT}
      db: avalanche
      user: {user}
      password: avalanche
      {ssl}
      postgres: {{ publication: {publication}, slot: {slot}, heartbeat: 1s }}
    tables:
      include: [{schema}.t]
    sink: {{ type: delta, bucket: lake, delta: {{}} }}
"#,
        publication = n.publication,
        slot = n.slot,
        schema = n.schema,
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

/// `(user, backend type, ssl, client_dn)` of `user`'s catalog sessions (`data-avalanche`) and of
/// the walsender holding the test's slot (it reports `pgwire-replication`: the crate hard-codes
/// its `application_name`).
async fn sessions(
    a: &Client,
    user: &str,
    slot: &str,
) -> Vec<(String, String, bool, Option<String>)> {
    a.query(
        "SELECT a.usename::text, a.backend_type, s.ssl, s.client_dn
           FROM pg_stat_activity a JOIN pg_stat_ssl s ON s.pid = a.pid
          WHERE a.usename = $1
            AND (a.application_name = 'data-avalanche'
                 OR a.pid = (SELECT active_pid FROM pg_replication_slots WHERE slot_name = $2))
          ORDER BY 2",
        &[&user, &slot],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
    .collect()
}

/// Opens a stream, streams one insert and returns the sessions while it is open.
async fn stream_one(
    a: &Client,
    p: &Pipeline,
    n: &Names,
) -> Vec<(String, String, bool, Option<String>)> {
    let c = connector(p);
    c.preflight().await.expect("preflight over TLS");
    let (mut s, _, _) = first_stream(&c).await;
    a.batch_execute(&format!("INSERT INTO {}.t VALUES (1)", n.schema))
        .await
        .unwrap();
    let items = collect(&mut s, 1, 20).await;
    assert_eq!(items.len(), 3, "Begin, the insert, Commit: {items:?}");
    let seen = sessions(a, p.source.credentials.user(), &n.slot).await;
    s.close().await.expect("close");
    seen
}

#[tokio::test]
async fn verify_full_with_an_inline_ca_encrypts_both_connections() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "tls_full", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let p = pipeline(
        &n,
        "localhost",
        "avalanche",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
        &[("CA", read("ca.crt"))],
    );
    let seen = stream_one(&a, &p, &n).await;
    let kinds: Vec<&str> = seen.iter().map(|s| s.1.as_str()).collect();
    // The connector's catalog connection, the stream's own, and the replication connection.
    assert_eq!(kinds, ["client backend", "client backend", "walsender"]);
    assert!(seen.iter().all(|s| s.2), "both over TLS: {seen:?}");
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_client_certificate_with_an_inline_key_authenticates_both_connections() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "tls_cert", "CREATE TABLE t (id int PRIMARY KEY);").await;
    // The CA by path (passed through), the certificate and key inline (written to private files
    // for the replication connection).
    let ca = material("ca.crt").to_string_lossy().into_owned();
    let p = pipeline(
        &n,
        "localhost",
        "avalanche_cert",
        "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"\n      ssl_cert: \"${CERT}\"\n      ssl_key: \"${KEY}\"",
        &[
            ("CA", ca),
            ("CERT", read("client.crt")),
            ("KEY", read("client.key")),
        ],
    );
    let seen = stream_one(&a, &p, &n).await;
    assert_eq!(seen.len(), 3, "{seen:?}");
    for (user, _, ssl, dn) in &seen {
        assert_eq!(user, "avalanche_cert");
        assert!(ssl);
        assert_eq!(dn.as_deref(), Some("/CN=avalanche_cert"));
    }
    cleanup(&a, &n).await;
}

/// `verify-ca` ignores the host name on the catalog connection (D-M5.16) but not on the
/// replication connection: pgwire-replication 0.4.1 forgives only rustls's `NotValidForName`, and
/// rustls 0.23 reports `NotValidForNameContext` (the plan's Deviations, Task 2). This pins the
/// difference: once the crate forgives the mismatch, the stream connects and this test fails.
#[tokio::test]
async fn verify_ca_by_an_unnamed_address_passes_the_catalog_but_not_the_stream() {
    require_it!();
    let a = admin().await;
    let n = fresh(&a, "tls_verify_ca", "CREATE TABLE t (id int PRIMARY KEY);").await;
    let p = pipeline(
        &n,
        "127.0.0.1",
        "avalanche",
        "ssl_mode: verify-ca\n      ssl_ca: \"${CA}\"",
        &[("CA", read("ca.crt"))],
    );
    let c = connector(&p);
    c.preflight()
        .await
        .expect("the catalog connection ignores the name");
    let (mut s, _, _) = first_stream(&c).await;
    match s.next().await {
        Err(SourceError::Permanent(detail)) => {
            assert!(detail.contains("not valid for name"), "{detail}")
        }
        other => panic!("the stream should refuse the name, got {other:?}"),
    }
    drop(s);
    cleanup(&a, &n).await;
}

#[tokio::test]
async fn a_wrong_ca_or_no_tls_is_refused() {
    require_it!();
    let n = Names::new("tls_refused");
    let other_ca = {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(vec![]).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.self_signed(&key).unwrap().pem()
    };
    for (what, ssl, env) in [
        (
            "a foreign CA",
            "ssl_mode: verify-full\n      ssl_ca: \"${CA}\"",
            vec![("CA", other_ca.clone())],
        ),
        (
            "a foreign CA, chain only",
            "ssl_mode: verify-ca\n      ssl_ca: \"${CA}\"",
            vec![("CA", other_ca)],
        ),
        ("no TLS", "ssl_mode: disable", vec![]),
    ] {
        let c = connector(&pipeline(&n, "localhost", "avalanche", ssl, &env));
        match c.preflight().await {
            Err(SourceError::Permanent(detail)) => eprintln!("{what}: {detail}"),
            other => panic!("{what}: a permanent error expected, got {other:?}"),
        }
    }
}
