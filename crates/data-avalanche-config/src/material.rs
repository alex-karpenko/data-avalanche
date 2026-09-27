//! Phase 2 file material (config §4.4, §4.6, §13.9): credential files, token files, PEM, keytabs.
//! Each function returns the rule's message on failure; the build reports it under the rule.

use std::fs;
use std::path::{Path, PathBuf};

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::Serialize;

use crate::secret::{Secret, fingerprint};

/// Reads `path`; every message names it by `label`, never by its own text, so a resolved path (an
/// env substitution) or file content never reaches a diagnostic (F1, config §13.9).
fn read(path: &Path, label: &str) -> Result<Vec<u8>, String> {
    let bytes = fs::read(path).map_err(|e| format!("cannot read {label}: {e}"))?;
    if bytes.is_empty() {
        return Err(format!("{label} is empty"));
    }
    Ok(bytes)
}

/// A JSON object file holding two non-empty string fields (V-067): `{user, password}` or
/// `{client_id, client_secret}`. Other fields are ignored. `label` is what a message calls the
/// file: never its resolved path or content.
pub fn read_json_pair(
    path: &Path,
    label: &str,
    first: &str,
    second: &str,
) -> Result<(String, Secret), String> {
    let bytes = read(path, label)?;
    let doc: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("{label} is not valid JSON: {e}"))?;
    let field = |name: &str| {
        doc.get(name)
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .map(str::to_string)
            .ok_or_else(|| format!("{label} has no non-empty string field {name:?}"))
    };
    Ok((field(first)?, Secret::new(field(second)?)))
}

/// A bearer-token file (V-071): the raw contents, trailing whitespace trimmed, non-empty. `label`
/// is what a message calls the file: never its resolved path or content.
pub fn read_token_file(path: &Path, label: &str) -> Result<Secret, String> {
    let bytes = read(path, label)?;
    let text = String::from_utf8(bytes).map_err(|_| format!("{label} is not UTF-8 text"))?;
    let token = text.trim_end();
    if token.is_empty() {
        return Err(format!("{label} holds only whitespace"));
    }
    Ok(Secret::new(token))
}

/// Any other referenced file (V-069, e.g. a Kerberos keytab): exists, readable, non-empty. Only
/// the path and a content fingerprint are kept: the runtime re-reads the file when it reconnects,
/// and the fingerprint lets a reload notice that the contents changed. `label` is what a message
/// calls the file: never its resolved path or content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileRef {
    pub path: PathBuf,
    pub fingerprint: String,
}

pub fn read_file_ref(path: &Path, label: &str) -> Result<FileRef, String> {
    Ok(FileRef {
        path: path.to_path_buf(),
        fingerprint: fingerprint(&read(path, label)?),
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

/// A `cert`/`key`/`ca` value (§4.4): inline PEM when its text, after leading whitespace, starts
/// with `-----BEGIN`, otherwise a path. `label` is what a message calls the file when it is a
/// path: never its resolved path or content (F1).
pub fn load_pem(value: &str, label: &str, kind: PemKind) -> Result<Pem, String> {
    let trimmed = value.trim_start();
    let (origin, text) = if trimmed.starts_with("-----BEGIN") {
        (Origin::Inline, trimmed.to_string())
    } else {
        let path = PathBuf::from(value);
        let bytes = read(&path, label)?;
        let text = String::from_utf8(bytes).map_err(|_| format!("{label} is not PEM text"))?;
        (Origin::File(path), text)
    };
    let from = match &origin {
        Origin::Inline => "inline PEM".to_string(),
        Origin::File(_) => label.to_string(),
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

    /// These unit tests exercise the read mechanics directly, so they pass the path itself as the
    /// label (mirroring the pre-F1 messages); computing a diagnostic-safe label from a leaf is
    /// `build::file_label`'s job, covered by `tests/load.rs`.
    fn label(path: &Path) -> String {
        path.display().to_string()
    }

    #[test]
    fn credentials_file_needs_both_fields_non_empty() {
        let ok = file(br#"{"user": "cdc", "password": "pw", "note": "ignored"}"#);
        let (user, password) =
            read_json_pair(ok.path(), &label(ok.path()), "user", "password").unwrap();
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
            let err = read_json_pair(f.path(), &label(f.path()), "user", "password").unwrap_err();
            assert!(err.contains(why), "{err}");
        }
        let missing = Path::new("/nonexistent/creds.json");
        let err = read_json_pair(missing, &label(missing), "user", "password").unwrap_err();
        assert!(
            err.starts_with("cannot read /nonexistent/creds.json"),
            "{err}"
        );
    }

    #[test]
    fn token_file_is_trimmed_raw_text() {
        let f = file(b"tok-123\n\n");
        assert_eq!(
            read_token_file(f.path(), &label(f.path()))
                .unwrap()
                .expose(),
            "tok-123"
        );
        let empty = file(b" \n");
        assert!(
            read_token_file(empty.path(), &label(empty.path()))
                .unwrap_err()
                .contains("only whitespace")
        );
    }

    #[test]
    fn pem_is_inline_or_a_path_and_must_hold_the_right_kind() {
        let (cert_pem, key_pem) = pem_pair();
        let inline = load_pem(&cert_pem, "unused", PemKind::Certificates).unwrap();
        assert_eq!(inline.origin, Origin::Inline);
        let key_file = file(key_pem.as_bytes());
        let key_label = label(key_file.path());
        let key = load_pem(
            key_file.path().to_str().unwrap(),
            &key_label,
            PemKind::PrivateKey,
        )
        .unwrap();
        assert_eq!(key.origin, Origin::File(key_file.path().to_path_buf()));
        assert!(
            load_pem(&key_pem, "unused", PemKind::Certificates)
                .unwrap_err()
                .contains("no certificate")
        );
        assert!(
            load_pem(&cert_pem, "unused", PemKind::PrivateKey)
                .unwrap_err()
                .contains("no private key")
        );
        assert!(
            load_pem(
                "-----BEGIN CERTIFICATE-----\nnot base64!\n-----END CERTIFICATE-----\n",
                "unused",
                PemKind::Certificates
            )
            .is_err()
        );
        let missing = Path::new("/nonexistent/ca.pem");
        assert!(
            load_pem(
                missing.to_str().unwrap(),
                &label(missing),
                PemKind::Certificates
            )
            .unwrap_err()
            .contains("cannot read")
        );
    }

    /// F1: leading whitespace before `-----BEGIN` still counts as inline PEM, not a path (a
    /// substituted key with a leading newline, or an operator's stray indentation).
    #[test]
    fn leading_whitespace_before_begin_is_still_inline() {
        let (cert_pem, _) = pem_pair();
        for prefix in ["\n", "\n\n", "  ", " \n "] {
            let padded = format!("{prefix}{cert_pem}");
            let pem = load_pem(&padded, "unused", PemKind::Certificates).unwrap();
            assert_eq!(pem.origin, Origin::Inline, "prefix {prefix:?}");
        }
    }

    #[test]
    fn file_ref_fingerprints_content() {
        let a = file(b"keytab-1");
        let b = file(b"keytab-2");
        assert_ne!(
            read_file_ref(a.path(), &label(a.path()))
                .unwrap()
                .fingerprint,
            read_file_ref(b.path(), &label(b.path()))
                .unwrap()
                .fingerprint
        );
        let empty = file(b"");
        assert!(
            read_file_ref(empty.path(), &label(empty.path()))
                .unwrap_err()
                .contains("is empty")
        );
    }
}
