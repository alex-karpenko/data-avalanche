//! The Prometheus reader (Admin_Console §7, D-M13e.6): a pipeline's series older than the history
//! kept come from `globals.console.prometheus` when it is set. Each series is one range query
//! over the node's own metrics, grouped by pipeline, with `extra_labels` in every selector. A
//! table's and a node's series aren't in Prometheus (their metrics carry no such label).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use data_avalanche_config::{Prometheus, PrometheusAuth};
use data_avalanche_state::Series;
use serde::Deserialize;

/// The largest answer read from Prometheus.
pub const MAX_PROMETHEUS_ANSWER: usize = 8 << 20;

/// The shortest window a rate looks over: a few scrapes at the usual intervals.
pub const MIN_RATE_WINDOW: i64 = 300;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PrometheusError {
    #[error("the Prometheus client: {0}")]
    Client(String),
    #[error("Prometheus can't be reached: {0}")]
    Unreachable(String),
    #[error("Prometheus answered {code}: {text}")]
    Answered { code: u16, text: String },
    #[error("Prometheus's answer isn't one: {0}")]
    Invalid(String),
}

/// A PromQL string literal of `v`.
fn quoted(v: &str) -> String {
    let mut out = String::with_capacity(v.len() + 2);
    out.push('"');
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `v` matched literally inside a regular expression.
fn literal(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        if "\\.+*?()|[]{}^$".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// The selector of the pipelines named `names`, `extra` and `more`.
fn selector(names: &[String], extra: &BTreeMap<String, String>, more: &str) -> String {
    let pipelines = names
        .iter()
        .map(|n| literal(n))
        .collect::<Vec<_>>()
        .join("|");
    let mut m = vec![format!("pipeline=~{}", quoted(&pipelines))];
    m.extend(extra.iter().map(|(k, v)| format!("{k}={}", quoted(v))));
    if !more.is_empty() {
        m.push(more.to_owned());
    }
    format!("{{{}}}", m.join(","))
}

/// The query of series `s` for the pipelines named `names`, one result per pipeline, at `step`
/// seconds; `None` for a series Prometheus doesn't have.
pub fn promql(
    s: &Series,
    names: &[String],
    extra: &BTreeMap<String, String>,
    step: i64,
) -> Option<String> {
    let window = step.max(MIN_RATE_WINDOW);
    let gauge =
        |metric: &str| format!("max by (pipeline) ({metric}{})", selector(names, extra, ""));
    let rate = |op: &str| {
        format!(
            "sum by (pipeline) (rate(data_avalanche_rows_read_total{}[{window}s]))",
            selector(names, extra, op)
        )
    };
    match s.name {
        "lag_seconds" => Some(gauge("data_avalanche_replication_lag_seconds")),
        "wal_held_bytes" => Some(gauge("data_avalanche_wal_lag_bytes")),
        "binlog_lag_seconds" => Some(gauge("data_avalanche_binlog_lag_seconds")),
        "changes_per_minute" => Some(format!("60 * {}", rate(r#"op!="snapshot""#))),
        "snapshot_rows_per_second" => Some(rate(r#"op="snapshot""#)),
        _ => None,
    }
}

#[derive(Deserialize)]
struct Answer {
    status: String,
    #[serde(default)]
    data: Option<Data>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct Data {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<Range>,
}

#[derive(Deserialize)]
struct Range {
    metric: BTreeMap<String, String>,
    values: Vec<(f64, String)>,
}

/// A client of one `globals.console.prometheus`.
#[derive(Debug)]
pub struct PrometheusClient {
    config: Prometheus,
    http: reqwest::Client,
}

impl PrometheusClient {
    /// The client of `config`: its CA besides the platform's roots, its time limit per query; no
    /// redirects or proxies.
    ///
    /// # Errors
    /// [`PrometheusError::Client`]: the CA isn't PEM, or the client can't be built.
    pub fn new(config: &Prometheus) -> Result<Self, PrometheusError> {
        let mut b = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(config.timeout);
        if let Some(ca) = &config.ca {
            let certs = reqwest::Certificate::from_pem_bundle(ca.text.expose().as_bytes())
                .map_err(|e| PrometheusError::Client(format!("tls.ca: {e}")))?;
            b = b.tls_certs_merge(certs);
        }
        Ok(Self {
            config: config.clone(),
            http: b
                .build()
                .map_err(|e| PrometheusError::Client(e.to_string()))?,
        })
    }

    /// `query`'s values from `start` to `end` every `step` seconds, by the `pipeline` label;
    /// values that aren't finite are left out.
    ///
    /// # Errors
    /// [`PrometheusError`]: unreachable, past the time limit, an error answer, or one that
    /// isn't a range.
    pub async fn range(
        &self,
        query: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        step: i64,
    ) -> Result<BTreeMap<String, Vec<(i64, f64)>>, PrometheusError> {
        let url = format!(
            "{}/api/v1/query_range",
            self.config.url.trim_end_matches('/')
        );
        let form = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("query", query)
            .append_pair("start", &start.timestamp().to_string())
            .append_pair("end", &end.timestamp().to_string())
            .append_pair("step", &format!("{step}s"))
            .finish();
        let mut req = self
            .http
            .post(url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form);
        req = match &self.config.auth {
            PrometheusAuth::None => req,
            PrometheusAuth::Basic { user, password } => {
                req.basic_auth(user, Some(password.expose()))
            }
            PrometheusAuth::Bearer { token } => req.bearer_auth(token.expose()),
        };
        for (name, value) in &self.config.headers {
            req = req.header(name.as_str(), value.expose());
        }
        let answer = req
            .send()
            .await
            .map_err(|e| PrometheusError::Unreachable(e.without_url().to_string()))?;
        let code = answer.status().as_u16();
        if answer
            .content_length()
            .is_some_and(|n| n > MAX_PROMETHEUS_ANSWER as u64)
        {
            return Err(PrometheusError::Invalid(format!(
                "over {MAX_PROMETHEUS_ANSWER} bytes"
            )));
        }
        let mut answer = answer;
        let mut bytes: Vec<u8> = Vec::new();
        while let Some(chunk) = answer
            .chunk()
            .await
            .map_err(|e| PrometheusError::Unreachable(e.without_url().to_string()))?
        {
            if bytes.len() + chunk.len() > MAX_PROMETHEUS_ANSWER {
                return Err(PrometheusError::Invalid(format!(
                    "over {MAX_PROMETHEUS_ANSWER} bytes"
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        let parsed: Result<Answer, _> = serde_json::from_slice(&bytes);
        let a = match parsed {
            Ok(a) if a.status == "success" && code == 200 => a,
            Ok(a) => {
                return Err(PrometheusError::Answered {
                    code,
                    text: a.error.unwrap_or_else(|| a.status.clone()),
                });
            }
            Err(_) if code != 200 => {
                let text = String::from_utf8_lossy(&bytes);
                return Err(PrometheusError::Answered {
                    code,
                    text: text.chars().take(200).collect(),
                });
            }
            Err(e) => return Err(PrometheusError::Invalid(e.to_string())),
        };
        let data = a
            .data
            .filter(|d| d.result_type == "matrix")
            .ok_or_else(|| PrometheusError::Invalid("not a range (matrix)".into()))?;
        let mut out = BTreeMap::new();
        for r in data.result {
            let Some(pipeline) = r.metric.get("pipeline") else {
                continue;
            };
            let points: Vec<(i64, f64)> = r
                .values
                .iter()
                .filter_map(|(t, v)| {
                    let v: f64 = v.parse().ok()?;
                    v.is_finite().then_some((t.round() as i64, v))
                })
                .collect();
            out.insert(pipeline.clone(), points);
        }
        Ok(out)
    }

    pub fn config(&self) -> &Prometheus {
        &self.config
    }
}

/// How the node's last read of Prometheus went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub at: DateTime<Utc>,
    /// The error; `None` when it answered.
    pub error: Option<String>,
}

/// The node's Prometheus client, made again when the config changes, and how its last read
/// went.
#[derive(Debug, Default)]
pub struct PrometheusReader {
    client: Mutex<Option<Arc<PrometheusClient>>>,
    last: Mutex<Option<Outcome>>,
}

impl PrometheusReader {
    /// The client of `config`, the one made before while it is the same.
    ///
    /// # Errors
    /// [`PrometheusError::Client`]: it can't be made.
    pub fn client(&self, config: &Prometheus) -> Result<Arc<PrometheusClient>, PrometheusError> {
        let mut held = self.client.lock().expect("unpoisoned");
        if let Some(c) = held.as_ref()
            && c.config() == config
        {
            return Ok(Arc::clone(c));
        }
        let c = Arc::new(PrometheusClient::new(config)?);
        *held = Some(Arc::clone(&c));
        Ok(c)
    }

    pub fn record(&self, outcome: Outcome) {
        *self.last.lock().expect("unpoisoned") = Some(outcome);
    }

    pub fn last(&self) -> Option<Outcome> {
        self.last.lock().expect("unpoisoned").clone()
    }
}

#[cfg(test)]
mod tests {
    use data_avalanche_state::{SeriesScope, series_of};

    use super::*;

    fn s(name: &str) -> &'static Series {
        series_of(SeriesScope::Pipeline, name).unwrap()
    }

    #[test]
    fn each_pipeline_series_has_its_query() {
        let names = vec!["orders".to_owned(), "bill.ing".to_owned()];
        let extra = BTreeMap::from([("cluster".to_owned(), "prod \"eu\"".to_owned())]);
        assert_eq!(
            promql(s("lag_seconds"), &names, &extra, 60).unwrap(),
            r#"max by (pipeline) (data_avalanche_replication_lag_seconds{pipeline=~"orders|bill\\.ing",cluster="prod \"eu\""})"#
        );
        assert_eq!(
            promql(s("changes_per_minute"), &names[..1], &BTreeMap::new(), 600).unwrap(),
            r#"60 * sum by (pipeline) (rate(data_avalanche_rows_read_total{pipeline=~"orders",op!="snapshot"}[600s]))"#
        );
        assert_eq!(
            promql(
                s("snapshot_rows_per_second"),
                &names[..1],
                &BTreeMap::new(),
                15
            )
            .unwrap(),
            r#"sum by (pipeline) (rate(data_avalanche_rows_read_total{pipeline=~"orders",op="snapshot"}[300s]))"#
        );
        for name in ["wal_held_bytes", "binlog_lag_seconds"] {
            assert!(promql(s(name), &names, &extra, 60).is_some(), "{name}");
        }
        let table = series_of(SeriesScope::Table, "commit_seconds").unwrap();
        assert_eq!(promql(table, &names, &extra, 60), None);
    }

    #[test]
    fn a_reader_keeps_its_client_while_the_config_stays() {
        let config = Prometheus {
            url: "http://127.0.0.1:9".into(),
            auth: PrometheusAuth::None,
            headers: BTreeMap::new(),
            ca: None,
            extra_labels: BTreeMap::new(),
            timeout: std::time::Duration::from_secs(5),
        };
        let r = PrometheusReader::default();
        let a = r.client(&config).unwrap();
        assert!(Arc::ptr_eq(&a, &r.client(&config).unwrap()));
        let other = Prometheus {
            url: "http://127.0.0.1:10".into(),
            ..config
        };
        assert!(!Arc::ptr_eq(&a, &r.client(&other).unwrap()));
    }

    /// A fake Prometheus on a loopback port: for each connection it reads the request, then
    /// runs `reply` on the socket. Returns its URL.
    async fn fake<F, Fut>(reply: F) -> String
    where
        F: Fn(tokio::net::TcpStream) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let mut seen = Vec::new();
                let mut buf = [0u8; 4096];
                while !seen.windows(4).any(|w| w == b"\r\n\r\n") {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => seen.extend_from_slice(&buf[..n]),
                    }
                }
                tokio::spawn(reply(sock));
            }
        });
        url
    }

    fn client_of(url: String, timeout: std::time::Duration) -> PrometheusClient {
        PrometheusClient::new(&Prometheus {
            url,
            auth: PrometheusAuth::None,
            headers: BTreeMap::new(),
            ca: None,
            extra_labels: BTreeMap::new(),
            timeout,
        })
        .unwrap()
    }

    async fn ask(
        c: &PrometheusClient,
    ) -> Result<BTreeMap<String, Vec<(i64, f64)>>, PrometheusError> {
        let end = Utc::now();
        c.range("up", end - chrono::TimeDelta::hours(1), end, 60)
            .await
    }

    /// A 200 answer with `body` and a Content-Length.
    async fn plain(mut sock: tokio::net::TcpStream, body: &'static str) {
        use tokio::io::AsyncWriteExt;
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = sock.write_all(head.as_bytes()).await;
        let _ = sock.write_all(body.as_bytes()).await;
    }

    #[tokio::test]
    async fn an_answer_over_the_cap_is_refused_while_it_is_read() {
        use tokio::io::AsyncWriteExt;
        let five = std::time::Duration::from_secs(5);
        let two = std::time::Duration::from_secs(2);
        // Endless chunked body, no Content-Length: only the read itself can stop it.
        let url = fake(|mut sock| async move {
            let _ = sock
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
                )
                .await;
            let block = vec![b'x'; 1 << 20];
            // Endless: writes until the client hangs up.
            loop {
                if sock.write_all(b"100000\r\n").await.is_err()
                    || sock.write_all(&block).await.is_err()
                    || sock.write_all(b"\r\n").await.is_err()
                {
                    return;
                }
            }
        })
        .await;
        match ask(&client_of(url, two)).await {
            Err(PrometheusError::Invalid(t)) => assert!(t.contains("over"), "{t}"),
            other => panic!("{other:?}"),
        }
        // A Content-Length over the cap: refused before the body.
        let url = fake(|mut sock| async move {
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_PROMETHEUS_ANSWER + 1
            );
            let _ = sock.write_all(head.as_bytes()).await;
        })
        .await;
        match ask(&client_of(url, five)).await {
            Err(PrometheusError::Invalid(t)) => assert!(t.contains("over"), "{t}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn an_answer_that_isn_t_a_matrix_is_invalid() {
        let five = std::time::Duration::from_secs(5);
        let url = fake(|sock| plain(sock, "this is not json")).await;
        assert!(matches!(
            ask(&client_of(url, five)).await,
            Err(PrometheusError::Invalid(_))
        ));
        let url = fake(|sock| {
            plain(
                sock,
                r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#,
            )
        })
        .await;
        match ask(&client_of(url, five)).await {
            Err(PrometheusError::Invalid(t)) => assert!(t.contains("matrix"), "{t}"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_prometheus_slower_than_its_timeout_is_unreachable() {
        let url = fake(|sock| async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            plain(
                sock,
                r#"{"status":"success","data":{"resultType":"matrix","result":[]}}"#,
            )
            .await;
        })
        .await;
        let c = client_of(url, std::time::Duration::from_millis(200));
        assert!(matches!(
            ask(&c).await,
            Err(PrometheusError::Unreachable(_))
        ));
    }
}
