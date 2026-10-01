//! TLS for both connections (config §4.4, `source.ssl_*`). The catalog connection builds rustls
//! from the PEM text; the replication connection (`pgwire-replication`) takes file paths, so an
//! inline PEM is written to a private temporary file (Q-M5-4).

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use data_avalanche_config::{Origin, Pem, Source, SslMode};
use pgwire_replication::TlsConfig;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};

/// PEM files for the replication connection. A file-origin PEM is used where it is; an inline PEM
/// is written to a file of mode 0600 in a directory of mode 0700, both removed when this is
/// dropped.
#[derive(Debug, Default)]
pub(crate) struct PemFiles {
    dir: Option<tempfile::TempDir>,
    pub(crate) ca: Option<PathBuf>,
    pub(crate) cert: Option<PathBuf>,
    pub(crate) key: Option<PathBuf>,
}

impl PemFiles {
    pub(crate) fn new(source: &Source) -> Result<Self, String> {
        let mut files = Self::default();
        files.ca = files.place(source.ssl_ca.as_ref(), "ca.pem")?;
        files.cert = files.place(source.ssl_cert.as_ref(), "cert.pem")?;
        files.key = files.place(source.ssl_key.as_ref(), "key.pem")?;
        Ok(files)
    }

    fn place(&mut self, pem: Option<&Pem>, name: &str) -> Result<Option<PathBuf>, String> {
        let Some(pem) = pem else { return Ok(None) };
        if let Origin::File(path) = &pem.origin {
            return Ok(Some(path.clone()));
        }
        if self.dir.is_none() {
            let mut builder = tempfile::Builder::new();
            builder.prefix("data-avalanche-pg-tls-");
            #[cfg(unix)]
            builder.permissions(std::os::unix::fs::PermissionsExt::from_mode(0o700));
            let dir = builder
                .tempdir()
                .map_err(|e| format!("can't create a directory for the TLS material: {e}"))?;
            self.dir = Some(dir);
        }
        let path = self.dir.as_ref().expect("created above").path().join(name);
        write_private(&path, pem.text.expose().as_bytes())
            .map_err(|e| format!("can't write the TLS material: {e}"))?;
        Ok(Some(path))
    }

    #[cfg(test)]
    pub(crate) fn dir(&self) -> Option<&Path> {
        self.dir.as_ref().map(|d| d.path())
    }
}

fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)?.write_all(bytes)
}

/// The replication connection's TLS (`ssl_mode: allow` connects like `prefer`: the crate has no
/// `allow`, and both accept either kind of connection — D-M5.4).
pub(crate) fn replication_tls(mode: SslMode, files: &PemFiles) -> TlsConfig {
    let mut tls = match mode {
        SslMode::Disable => TlsConfig::disabled(),
        SslMode::Allow | SslMode::Prefer => TlsConfig {
            mode: pgwire_replication::SslMode::Prefer,
            ..TlsConfig::disabled()
        },
        SslMode::Require => TlsConfig::require(),
        SslMode::VerifyCa => TlsConfig::verify_ca(files.ca.clone()),
        SslMode::VerifyFull => TlsConfig::verify_full(files.ca.clone()),
    };
    if let (Some(cert), Some(key)) = (&files.cert, &files.key) {
        tls = tls.with_client_cert(cert.clone(), key.clone());
    }
    tls
}

/// The catalog connection's `sslmode` (tokio-postgres knows disable, prefer and require; the
/// verify modes are require plus a verifying rustls configuration).
pub(crate) fn control_ssl_mode(mode: SslMode) -> tokio_postgres::config::SslMode {
    use tokio_postgres::config::SslMode as T;
    match mode {
        SslMode::Disable => T::Disable,
        SslMode::Allow | SslMode::Prefer => T::Prefer,
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => T::Require,
    }
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

fn certificates(pem: &Pem, what: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_slice_iter(pem.text.expose().as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{what}: not a PEM certificate: {e}"))?;
    if certs.is_empty() {
        return Err(format!("{what}: no certificate in the PEM"));
    }
    Ok(certs)
}

/// The catalog connection's rustls configuration: the same modes as the replication connection
/// (Disable / Prefer / Require: encrypt without verifying; VerifyCa: the chain; VerifyFull: the
/// chain and the host name). Roots are `ssl_ca`, else the Mozilla roots the replication crate uses.
pub(crate) fn control_tls(source: &Source) -> Result<ClientConfig, String> {
    let provider = provider();
    let mut roots = RootCertStore::empty();
    match &source.ssl_ca {
        Some(ca) => {
            let (added, _) = roots.add_parsable_certificates(certificates(ca, "ssl_ca")?);
            if added == 0 {
                return Err("ssl_ca: no usable CA certificate".into());
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let roots = Arc::new(roots);
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots.clone());
    let mut config = match (&source.ssl_cert, &source.ssl_key) {
        (Some(cert), Some(key)) => {
            let key = PrivateKeyDer::from_pem_slice(key.text.expose().as_bytes())
                .map_err(|e| format!("ssl_key: not a PEM private key: {e}"))?;
            builder
                .with_client_auth_cert(certificates(cert, "ssl_cert")?, key)
                .map_err(|e| format!("ssl_cert/ssl_key: {e}"))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => return Err("ssl_cert and ssl_key go together".into()),
    };
    let algorithms = provider.signature_verification_algorithms;
    match source.ssl_mode {
        SslMode::VerifyFull => {}
        SslMode::VerifyCa => {
            let inner = WebPkiServerVerifier::builder_with_provider(roots, provider)
                .build()
                .map_err(|e| e.to_string())?;
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(ChainOnly { inner }));
        }
        _ => {
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(Unverified { algorithms }));
        }
    }
    Ok(config)
}

/// `require`, `prefer`, `allow`: any certificate, as libpq does without a root certificate. The
/// handshake signatures are still checked.
#[derive(Debug)]
struct Unverified {
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Unverified {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// `verify-ca`: the chain must lead to a trusted root; the host name isn't checked.
#[derive(Debug)]
struct ChainOnly {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_avalanche_config::{FingerprintKey, Secret};

    fn key() -> FingerprintKey {
        FingerprintKey::from_bytes([7; 32])
    }

    fn inline(text: &str) -> Pem {
        Pem {
            origin: Origin::Inline,
            text: Secret::new(text, &key()),
        }
    }

    /// A throwaway CA and a client certificate it signed, generated for the test run.
    fn material() -> (String, String, String) {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};
        let ca_key = KeyPair::generate().unwrap();
        let mut ca = CertificateParams::new(vec![]).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = ca.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca, ca_key);
        let client_key = KeyPair::generate().unwrap();
        let client = CertificateParams::new(vec!["avalanche".into()])
            .unwrap()
            .signed_by(&client_key, &issuer)
            .unwrap();
        (ca_cert.pem(), client.pem(), client_key.serialize_pem())
    }

    fn source(yaml_mode: SslMode, ca: Option<Pem>, cert: Option<Pem>, key: Option<Pem>) -> Source {
        let mut s = crate::test_support::pipeline("public.t").source;
        s.ssl_mode = yaml_mode;
        s.ssl_ca = ca;
        s.ssl_cert = cert;
        s.ssl_key = key;
        s
    }

    #[cfg(unix)]
    #[test]
    fn inline_pems_are_private_files_removed_on_drop() {
        use std::os::unix::fs::PermissionsExt;
        let (ca, cert, client_key) = material();
        let s = source(
            SslMode::VerifyFull,
            Some(inline(&ca)),
            Some(inline(&cert)),
            Some(inline(&client_key)),
        );
        let files = PemFiles::new(&s).unwrap();
        let dir = files.dir().unwrap().to_owned();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        for p in [&files.ca, &files.cert, &files.key] {
            let p = p.as_ref().unwrap();
            assert!(p.starts_with(&dir));
            assert_eq!(mode(p), 0o600);
        }
        assert_eq!(
            std::fs::read_to_string(files.key.as_ref().unwrap()).unwrap(),
            client_key
        );
        drop(files);
        assert!(!dir.exists());
    }

    #[test]
    fn file_pems_are_used_where_they_are() {
        let mut ca = inline("unused");
        ca.origin = Origin::File(PathBuf::from("/etc/ssl/ca.pem"));
        let files = PemFiles::new(&source(SslMode::VerifyCa, Some(ca), None, None)).unwrap();
        assert_eq!(files.ca, Some(PathBuf::from("/etc/ssl/ca.pem")));
        assert!(
            files.dir().is_none(),
            "no temporary directory without inline material"
        );
    }

    #[test]
    fn modes_map_to_both_clients() {
        use pgwire_replication::SslMode as R;
        use tokio_postgres::config::SslMode as T;
        let files = PemFiles {
            ca: Some(PathBuf::from("/ca.pem")),
            ..PemFiles::default()
        };
        for (mode, repl, control) in [
            (SslMode::Disable, R::Disable, T::Disable),
            (SslMode::Allow, R::Prefer, T::Prefer),
            (SslMode::Prefer, R::Prefer, T::Prefer),
            (SslMode::Require, R::Require, T::Require),
            (SslMode::VerifyCa, R::VerifyCa, T::Require),
            (SslMode::VerifyFull, R::VerifyFull, T::Require),
        ] {
            let tls = replication_tls(mode, &files);
            assert_eq!(tls.mode, repl, "{mode}");
            assert_eq!(control_ssl_mode(mode), control, "{mode}");
        }
        assert_eq!(
            replication_tls(SslMode::VerifyFull, &files).ca_pem_path,
            Some(PathBuf::from("/ca.pem"))
        );
    }

    #[test]
    fn the_control_config_takes_the_inline_material() {
        let (ca, cert, client_key) = material();
        for mode in [SslMode::Require, SslMode::VerifyCa, SslMode::VerifyFull] {
            let s = source(
                mode,
                Some(inline(&ca)),
                Some(inline(&cert)),
                Some(inline(&client_key)),
            );
            let config = control_tls(&s).unwrap();
            assert!(config.client_auth_cert_resolver.has_certs());
        }
        let no_roots = source(SslMode::VerifyFull, None, None, None);
        control_tls(&no_roots).unwrap();
    }

    #[test]
    fn bad_material_is_refused_by_name() {
        let (ca, cert, _) = material();
        let e = control_tls(&source(
            SslMode::VerifyFull,
            Some(inline("not a pem")),
            None,
            None,
        ))
        .unwrap_err();
        assert!(e.starts_with("ssl_ca:"), "{e}");
        let e = control_tls(&source(
            SslMode::Require,
            Some(inline(&ca)),
            Some(inline(&cert)),
            None,
        ))
        .unwrap_err();
        assert_eq!(e, "ssl_cert and ssl_key go together");
        let e = control_tls(&source(
            SslMode::Require,
            None,
            Some(inline(&cert)),
            Some(inline("junk")),
        ))
        .unwrap_err();
        assert!(e.starts_with("ssl_key:"), "{e}");
    }
}
