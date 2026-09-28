//! `open_bucket` and the error mapping against a local fake S3 endpoint: each HTTP answer (and a
//! server that never answers) must reach the caller as the right `StoreError`, within the
//! configured time bound. Hermetic: no emulator needed.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use data_avalanche_storage::{
    Backend, BucketSpec, ObjectStoreVersioned, ObjectVersion, StoreError, Timeouts, VersionedStore,
    open_bucket,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Copy)]
enum Reply {
    Status(u16),
    Hang,
}

/// A fake endpoint answering every request with `reply`. Returns its URL and a request counter.
async fn fake_endpoint(reply: Reply) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let c = count.clone();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            let c = c.clone();
            tokio::spawn(answer(sock, reply, c));
        }
    });
    (url, count)
}

async fn answer(mut sock: TcpStream, reply: Reply, count: Arc<AtomicUsize>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let Ok(n) = sock.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
        let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
        let body_len: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse().unwrap());
        if buf.len() < end + 4 + body_len {
            continue;
        }
        count.fetch_add(1, Ordering::SeqCst);
        match reply {
            Reply::Hang => tokio::time::sleep(Duration::from_secs(3600)).await,
            Reply::Status(code) => {
                let resp =
                    format!("HTTP/1.1 {code} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                let _ = sock.write_all(resp.as_bytes()).await;
                return;
            }
        }
    }
}

const FAST: Timeouts = Timeouts {
    request: Duration::from_millis(300),
    connect: Duration::from_millis(300),
    max_retries: 2,
    retry_timeout: Duration::from_secs(1),
};

fn s3_spec(endpoint: &str) -> BucketSpec {
    let opt = |k: &str, v: &str| (k.to_owned(), v.to_owned());
    BucketSpec {
        backend: Backend::S3,
        bucket: "b".into(),
        options: vec![
            opt("endpoint", endpoint),
            opt("allow_http", "true"),
            opt("region", "us-east-1"),
            opt("access_key_id", "test"),
            opt("secret_access_key", "test"),
        ],
        timeouts: FAST,
    }
}

async fn versioned(reply: Reply) -> (ObjectStoreVersioned, Arc<AtomicUsize>) {
    let (url, count) = fake_endpoint(reply).await;
    let store = open_bucket(&s3_spec(&url)).unwrap();
    (ObjectStoreVersioned::new(store, "root").unwrap(), count)
}

fn etag() -> ObjectVersion {
    ObjectVersion {
        e_tag: Some("\"v1\"".into()),
        version: None,
    }
}

fn kind(e: &StoreError) -> &'static str {
    match e {
        StoreError::Conflict { .. } => "Conflict",
        StoreError::Contention { .. } => "Contention",
        StoreError::Transient { .. } => "Transient",
        StoreError::Permanent { .. } => "Permanent",
    }
}

#[tokio::test]
async fn http_answers_map_to_store_errors() {
    let body = || Bytes::from_static(b"{}");
    // (status, create, update); a get sends no precondition, so 409/412 can't answer it.
    let cases = [
        (412, "Conflict", "Conflict"),
        (409, "Conflict", "Contention"),
        (403, "Permanent", "Permanent"),
        (401, "Permanent", "Permanent"),
        (500, "Transient", "Transient"),
        (503, "Transient", "Transient"),
    ];
    for (status, create, update) in cases {
        let (s, _) = versioned(Reply::Status(status)).await;
        let got = kind(&s.create("k", body()).await.unwrap_err());
        assert_eq!(got, create, "create, HTTP {status}");
        let got = kind(&s.update("k", &etag(), body()).await.unwrap_err());
        assert_eq!(got, update, "update, HTTP {status}");
        if status != 409 && status != 412 {
            let got = kind(&s.get("k").await.unwrap_err());
            assert_eq!(got, create, "get, HTTP {status}");
        }
    }
}

#[tokio::test]
async fn get_of_a_missing_object_is_none() {
    let (s, _) = versioned(Reply::Status(404)).await;
    assert!(s.get("k").await.unwrap().is_none());
}

#[tokio::test]
async fn update_retries_s3_409_before_reporting_contention() {
    let (s, count) = versioned(Reply::Status(409)).await;
    let e = s
        .update("k", &etag(), Bytes::from_static(b"{}"))
        .await
        .unwrap_err();
    assert!(matches!(e, StoreError::Contention { .. }), "{e:?}");
    assert_eq!(
        count.load(Ordering::SeqCst),
        1 + FAST.max_retries,
        "object_store retries a 409 on an S3 If-Match put"
    );
}

#[tokio::test]
async fn a_hung_endpoint_fails_within_the_timeout_bound() {
    let (s, _) = versioned(Reply::Hang).await;
    for op in ["get", "create", "update"] {
        let start = Instant::now();
        let r = match op {
            "get" => s.get("k").await.map(|_| ()),
            "create" => s.create("k", Bytes::from_static(b"{}")).await.map(|_| ()),
            _ => s
                .update("k", &etag(), Bytes::from_static(b"{}"))
                .await
                .map(|_| ()),
        };
        let took = start.elapsed();
        let e = r.unwrap_err();
        assert!(matches!(e, StoreError::Transient { .. }), "{op}: {e:?}");
        let bound = FAST.retry_timeout + FAST.request * 2 + Duration::from_secs(2);
        assert!(took < bound, "{op} took {took:?}, bound {bound:?}");
    }
}

#[tokio::test]
async fn a_refused_connection_is_transient() {
    let port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let store = open_bucket(&s3_spec(&format!("http://127.0.0.1:{port}"))).unwrap();
    let s = ObjectStoreVersioned::new(store, "").unwrap();
    let e = s.get("k").await.unwrap_err();
    assert!(matches!(e, StoreError::Transient { .. }), "{e:?}");
}

#[test]
fn unknown_option_is_rejected_without_its_value() {
    let mut spec = s3_spec("http://127.0.0.1:1");
    spec.options
        .push(("secret_acces_key".into(), "s3cr3t-value".into()));
    let e = open_bucket(&spec).unwrap_err().to_string();
    assert_eq!(
        e,
        "cannot open bucket b: unknown option \"secret_acces_key\""
    );
    for backend in [
        Backend::Gcs,
        Backend::Azure {
            account: Some("acct".into()),
        },
    ] {
        let spec = BucketSpec {
            backend,
            bucket: "b".into(),
            options: vec![("no_such_option".into(), "x".into())],
            timeouts: Timeouts::COORDINATION,
        };
        assert!(
            open_bucket(&spec)
                .unwrap_err()
                .to_string()
                .contains("unknown option \"no_such_option\"")
        );
    }
}

#[test]
fn azure_emulator_opens_offline() {
    let azure = BucketSpec {
        backend: Backend::Azure { account: None },
        bucket: "c".into(),
        options: vec![("use_emulator".into(), "true".into())],
        timeouts: Timeouts::COORDINATION,
    };
    open_bucket(&azure).unwrap();
}
