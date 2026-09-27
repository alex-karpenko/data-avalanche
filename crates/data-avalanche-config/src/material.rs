//! Phase 2 file material (config §4.4, §4.6, §13.9): credential files, token files, PEM, keytabs.
//! Each function returns the rule's message on failure; the build reports it under the rule.

use std::fs;
use std::path::{Path, PathBuf};

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::Serialize;

use crate::secret::{Secret, fingerprint};

fn read(path: &Path) -> Result<Vec<u8>, String> {
    let bytes = fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    if bytes.is_empty() {
        return Err(format!("{} is empty", path.display()));
    }
    Ok(bytes)
}

/// A JSON object file holding two non-empty string fields (V-067): `{user, password}` or
/// `{client_id, client_secret}`. Other fields are ignored.
pub fn read_json_pair(path: &Path, first: &str, second: &str) -> Result<(String, Secret), String> {
    let bytes = read(path)?;
    let doc: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| format!("{} is not valid JSON: {e}", path.display()))?;
    let field = |name: &str| {
        doc.get(name)
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .ok_or_else(|| format!("{} has no non-empty string field {name:?}", path.display()))
    };
    Ok((field(first)?, Secret::new(field(second)?)))
}

/// A bearer-token file (V-071): the raw contents, trailing whitespace trimmed, non-empty.
pub fn read_token_file(path: &Path) -> Result<Secret, String> {
    let bytes = read(path)?;
    let text =
        String::from_utf8(bytes).map_err(|_| format!("{} is not UTF-8 text", path.display()))?;
    let token = text.trim_end();
    if token.is_empty() {
        return Err(format!("{} holds only whitespace", path.display()));
    }
    Ok(Secret::new(token))
}

/// Any other referenced file (V-069, e.g. a Kerberos keytab): exists, readable, non-empty. Only
/// the path and a content fingerprint are kept: the runtime re-reads the file when it reconnects,
/// and the fingerprint lets a reload notice that the contents changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileRef {
    pub path: PathBuf,
    pub fingerprint: String,
}

pub fn read_file_ref(path: &Path) -> Result<FileRef, String> {
    Ok(FileRef {
        path: path.to_path_buf(),
        fingerprint: fingerprint(&read(path)?),
    })
}

/// Where a PEM value came from (§4.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "path", rename_all = "lowercase")]
pub enum Origin {
    Inline,
    File(PathBuf),
}

/// Validated PEM text (V-068). The runtime builds its rustls objects from `text`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Pem {
    pub origin: Origin,
    pub text: Secret,
}

/// What a PEM value must contain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PemKind {
    /// `cert`, `ca`: at least one certificate.
    Certificates,
    /// `key`: a private key (PKCS#8, PKCS#1 or SEC1).
    PrivateKey,
}

/// A `cert`/`key`/`ca` value (§4.4): inline PEM when it starts with `-----BEGIN`, otherwise a path.
pub fn load_pem(value: &str, kind: PemKind) -> Result<Pem, String> {
    let (origin, text) = if value.starts_with("-----BEGIN") {
        (Origin::Inline, value.to_string())
    } else {
        let path = PathBuf::from(value);
        let bytes = read(&path)?;
        let text =
            String::from_utf8(bytes).map_err(|_| format!("{} is not PEM text", path.display()))?;
        (Origin::File(path), text)
    };
    let from = match &origin {
        Origin::Inline => "inline PEM".to_string(),
        Origin::File(p) => p.display().to_string(),
    };
    match kind {
        PemKind::Certificates => {
            let certs: Result<Vec<_>, _> =
                CertificateDer::pem_slice_iter(text.as_bytes()).collect();
            match certs {
                Ok(c) if !c.is_empty() => {}
                Ok(_) => return Err(format!("{from} holds no certificate")),
                Err(e) => return Err(format!("{from} is not valid PEM: {e}")),
            }
        }
        PemKind::PrivateKey => {
            PrivateKeyDer::from_pem_slice(text.as_bytes())
                .map_err(|e| format!("{from} holds no private key: {e}"))?;
        }
    }
    Ok(Pem {
        origin,
        text: Secret::new(text),
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    /// A throwaway self-signed certificate and its key, generated per run: no key material is
    /// committed.
    fn pem_pair() -> (String, String) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["data-avalanche-test".to_string()]).unwrap();
        (cert.pem(), signing_key.serialize_pem())
    }

    fn file(content: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content).unwrap();
        f
    }

    #[test]
    fn credentials_file_needs_both_fields_non_empty() {
        let ok = file(br#"{"user": "cdc", "password": "pw", "note": "ignored"}"#);
        let (user, password) = read_json_pair(ok.path(), "user", "password").unwrap();
        assert_eq!((user.as_str(), password.expose()), ("cdc", "pw"));
        for (content, why) in [
            (
                &br#"{"user": "cdc"}"#[..],
                "no non-empty string field \"password\"",
            ),
            (
                br#"{"user": "cdc", "password": ""}"#,
                "no non-empty string field \"password\"",
            ),
            (br#"{"user": 1, "password": "p"}"#, "field \"user\""),
            (b"not json", "not valid JSON"),
            (b"", "is empty"),
        ] {
            let f = file(content);
            let err = read_json_pair(f.path(), "user", "password").unwrap_err();
            assert!(err.contains(why), "{err}");
        }
        let err =
            read_json_pair(Path::new("/nonexistent/creds.json"), "user", "password").unwrap_err();
        assert!(
            err.starts_with("cannot read /nonexistent/creds.json"),
            "{err}"
        );
    }

    #[test]
    fn token_file_is_trimmed_raw_text() {
        let f = file(b"tok-123\n\n");
        assert_eq!(read_token_file(f.path()).unwrap().expose(), "tok-123");
        assert!(
            read_token_file(file(b" \n").path())
                .unwrap_err()
                .contains("only whitespace")
        );
    }

    #[test]
    fn pem_is_inline_or_a_path_and_must_hold_the_right_kind() {
        let (cert_pem, key_pem) = pem_pair();
        let inline = load_pem(&cert_pem, PemKind::Certificates).unwrap();
        assert_eq!(inline.origin, Origin::Inline);
        let key_file = file(key_pem.as_bytes());
        let key = load_pem(key_file.path().to_str().unwrap(), PemKind::PrivateKey).unwrap();
        assert_eq!(key.origin, Origin::File(key_file.path().to_path_buf()));
        assert!(
            load_pem(&key_pem, PemKind::Certificates)
                .unwrap_err()
                .contains("no certificate")
        );
        assert!(
            load_pem(&cert_pem, PemKind::PrivateKey)
                .unwrap_err()
                .contains("no private key")
        );
        assert!(
            load_pem(
                "-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n",
                PemKind::Certificates
            )
            .is_err()
        );
        assert!(
            load_pem("/nonexistent/ca.pem", PemKind::Certificates)
                .unwrap_err()
                .contains("cannot read")
        );
    }

    #[test]
    fn file_ref_fingerprints_content() {
        let a = file(b"keytab-1");
        let b = file(b"keytab-2");
        assert_ne!(
            read_file_ref(a.path()).unwrap().fingerprint,
            read_file_ref(b.path()).unwrap().fingerprint
        );
        assert!(
            read_file_ref(file(b"").path())
                .unwrap_err()
                .contains("is empty")
        );
    }
}
