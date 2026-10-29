//! Traces (config §6.6, `Observability_Metrics_and_Tracing` §3): the `tracing` spans of a flush,
//! a snapshot chunk and a schema change, exported over OTLP/HTTP (protobuf) to
//! `<otlp_endpoint>/v1/traces` (Q-M10-1), head-sampled by `sample_ratio`.
//!
//! One provider and one `tracing` layer serve the whole run: a reload of `globals.tracing` swaps
//! the exporter behind them and the sampling ratio (D-M10.6). A layer can't be reloaded: the
//! logs find a span's ids by downcasting to it, which a reload layer doesn't pass through. The
//! batch exporter runs on a thread of its own and exports with a blocking client.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use opentelemetry::trace::{Link, SpanKind, TraceContextExt, TraceId, TracerProvider};
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{
    Sampler, SamplingResult, SdkTracerProvider, ShouldSample, SpanData, SpanExporter,
};
use tracing::{Dispatch, Subscriber, span};
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

/// How long a shutdown waits for the spans still queued.
const SHUTDOWN: Duration = Duration::from_secs(5);

/// While the exports keep failing, how often the failure is warned about again (config §6.6).
const FAILING_WARNED_EVERY: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, thiserror::Error)]
pub enum TracesError {
    #[error("otlp_endpoint {:?} can't be exported to: {}", shown(.endpoint), shown_in(.reason, .endpoint))]
    Endpoint { endpoint: String, reason: String },
}

/// `endpoint` as it may be logged: without its `user[:password]@` (an operator may put basic-auth
/// credentials in a templated endpoint) and without its query string.
#[must_use]
pub fn shown(endpoint: &str) -> String {
    let (scheme, rest) = endpoint
        .split_once("://")
        .map_or(("", endpoint), |(s, r)| (s, r));
    // Over-redacts on purpose: any `@` ends the userinfo, even one in a path or query, because an
    // unencoded password may hold `/`, `?` or `#` and a log line must err on the safe side.
    let rest = rest.rsplit_once('@').map_or(rest, |(_, after)| after);
    let rest = &rest[..rest.find(['?', '#']).unwrap_or(rest.len())];
    if scheme.is_empty() && rest.len() == endpoint.len() {
        return endpoint.to_owned();
    }
    if endpoint.contains("://") {
        format!("{scheme}://{rest}")
    } else {
        rest.to_owned()
    }
}

/// `text` with `endpoint` (an exporter's error may echo it) replaced by its shown form.
fn shown_in(text: &str, endpoint: &str) -> String {
    let shown = shown(endpoint);
    if shown == endpoint {
        return text.to_owned();
    }
    text.replace(endpoint, &shown)
}

/// The OTLP exporter in force, `None` while tracing is off, and the resource every one carries.
#[derive(Default)]
struct Current {
    exporter: Option<Arc<Exporter>>,
    resource: Option<Resource>,
}

/// An OTLP exporter, the endpoint it exports to, and how its exports have gone: a new one starts
/// as if the last export had worked.
struct Exporter {
    otlp: opentelemetry_otlp::SpanExporter,
    endpoint: String,
    health: Health,
}

impl Exporter {
    /// Logs how an export went (config §6.6: "an endpoint the exporter can't use is a warning
    /// at run time"): the first failure after a success, then at most every
    /// [`FAILING_WARNED_EVERY`], and once a success after failures. The SDK reports a failed
    /// export only through its internal logs, which are off.
    fn observe(&self, result: &OTelSdkResult) {
        match result {
            Err(e) => {
                if self.health.failed(self.health.since.elapsed()) {
                    tracing::warn!(
                        "traces aren't exported to {}: {}",
                        shown(&self.endpoint),
                        shown_in(&e.to_string(), &self.endpoint)
                    );
                }
            }
            Ok(()) => {
                if self.health.recovered() {
                    tracing::info!("traces are exported to {} again", shown(&self.endpoint));
                }
            }
        }
    }
}

/// Whether an exporter's exports are failing, and when (since `since`, in ms) that was last
/// warned about.
struct Health {
    since: Instant,
    failing: AtomicBool,
    warned_ms: AtomicU64,
}

impl Health {
    fn new() -> Self {
        Self {
            since: Instant::now(),
            failing: AtomicBool::new(false),
            warned_ms: AtomicU64::new(0),
        }
    }

    /// An export failed at `now` (since `since`): whether to warn about it.
    fn failed(&self, now: Duration) -> bool {
        let now = u64::try_from(now.as_millis()).unwrap_or(u64::MAX);
        let every = u64::try_from(FAILING_WARNED_EVERY.as_millis()).unwrap_or(u64::MAX);
        let was_failing = self.failing.swap(true, Ordering::Relaxed);
        let due =
            !was_failing || now.saturating_sub(self.warned_ms.load(Ordering::Relaxed)) >= every;
        if due {
            self.warned_ms.store(now, Ordering::Relaxed);
        }
        due
    }

    /// An export worked: whether the ones before it had failed.
    fn recovered(&self) -> bool {
        self.failing.swap(false, Ordering::Relaxed)
    }
}

/// The provider's exporter: whichever [`Traces::set`] put in force.
#[derive(Clone, Default)]
struct Switch(Arc<RwLock<Current>>);

impl fmt::Debug for Switch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Switch").finish_non_exhaustive()
    }
}

impl Switch {
    fn current(&self) -> Option<Arc<Exporter>> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .exporter
            .clone()
    }
}

impl SpanExporter for Switch {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let Some(exporter) = self.current() else {
            return Ok(());
        };
        let result = exporter.otlp.export(batch).await;
        exporter.observe(&result);
        result
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.current()
            .map_or(Ok(()), |e| e.otlp.shutdown_with_timeout(timeout))
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.0
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .resource = Some(resource.clone());
    }
}

/// `sample_ratio`, as it is now: a span follows its parent's decision, a root is sampled at the
/// ratio by its trace id.
#[derive(Debug, Clone, Default)]
struct Ratio(Arc<AtomicU64>);

impl Ratio {
    fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
}

impl ShouldSample for Ratio {
    fn should_sample(
        &self,
        parent: Option<&Context>,
        trace_id: TraceId,
        name: &str,
        kind: &SpanKind,
        attributes: &[KeyValue],
        links: &[Link],
    ) -> SamplingResult {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(self.get())))
            .should_sample(parent, trace_id, name, kind, attributes, links)
    }
}

/// The process's traces: exporting nothing until [`Traces::set`] gives them an endpoint.
pub struct Traces {
    provider: SdkTracerProvider,
    switch: Switch,
    ratio: Ratio,
    /// The endpoint in force.
    endpoint: RwLock<Option<String>>,
}

/// Shows the endpoint as it may be logged ([`shown`]): an operator may put credentials in it.
impl fmt::Debug for Traces {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let endpoint = self
            .endpoint
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .as_deref()
            .map(shown);
        f.debug_struct("Traces")
            .field("endpoint", &endpoint)
            .field("sample_ratio", &self.ratio.get())
            .finish_non_exhaustive()
    }
}

impl Traces {
    /// `instance` is the node (`service.instance.id`).
    pub fn new(instance: &str) -> Self {
        let ratio = Ratio::default();
        let resource = Resource::builder()
            .with_service_name("data-avalanche")
            .with_attributes([
                KeyValue::new("service.instance.id", instance.to_owned()),
                KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            ])
            .build();
        // Seeded here: the batch thread applies the resource through `set_resource` only
        // eventually, and a `set()` before that would build an exporter without it.
        let switch = Switch(Arc::new(RwLock::new(Current {
            exporter: None,
            resource: Some(resource.clone()),
        })));
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(switch.clone())
            .with_sampler(ratio.clone())
            .with_resource(resource)
            .build();
        Self {
            provider,
            switch,
            ratio,
            endpoint: RwLock::default(),
        }
    }

    /// Puts `globals.tracing` in force: spans go to `endpoint`'s `/v1/traces`, `sample_ratio` of
    /// the traces; `None` exports nothing. The same endpoint again keeps its exporter. Returns
    /// whether traces are exported now.
    ///
    /// # Errors
    /// [`TracesError::Endpoint`]: `endpoint` isn't a URL the exporter takes; what was in force
    /// stays.
    pub fn set(&self, endpoint: Option<&str>, sample_ratio: f64) -> Result<bool, TracesError> {
        let mut current = self
            .endpoint
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        if current.as_deref() != endpoint {
            let exporter = endpoint.map(|e| self.exporter(e)).transpose()?;
            let old = std::mem::replace(
                &mut self
                    .switch
                    .0
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
                    .exporter,
                exporter.map(Arc::new),
            );
            // Dropping its blocking client blocks, joining the client's own runtime thread
            // (reqwest 0.13.5 `InnerClientHandle::drop`): kept off an async runtime's thread.
            if let Some(old) = old {
                std::thread::spawn(move || drop(old));
            }
            *current = endpoint.map(str::to_owned);
        }
        let ratio = if endpoint.is_some() {
            sample_ratio
        } else {
            0.0
        };
        self.ratio.0.store(ratio.to_bits(), Ordering::Relaxed);
        Ok(endpoint.is_some())
    }

    fn exporter(&self, endpoint: &str) -> Result<Exporter, TracesError> {
        let url = format!("{}/v1/traces", endpoint.trim_end_matches('/'));
        let mut exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(url)
            .build()
            .map_err(|e| TracesError::Endpoint {
                endpoint: endpoint.to_owned(),
                reason: e.to_string(),
            })?;
        let resource = self
            .switch
            .0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .resource
            .clone();
        if let Some(resource) = resource {
            exporter.set_resource(&resource);
        }
        Ok(Exporter {
            otlp: exporter,
            endpoint: endpoint.to_owned(),
            health: Health::new(),
        })
    }

    /// The `tracing` layer that turns spans into this provider's.
    pub fn layer<S>(&self) -> impl Layer<S> + Send + Sync + use<S>
    where
        S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
    {
        tracing_opentelemetry::layer().with_tracer(self.provider.tracer("data-avalanche"))
    }

    /// Exports the spans queued now; it blocks until they are, so it runs off an async runtime's
    /// threads.
    pub fn flush(&self) {
        if let Err(e) = self.provider.force_flush() {
            tracing::warn!("traces weren't exported: {e}");
        }
    }

    /// Exports the spans still queued, then stops; it blocks up to a few seconds, so it runs
    /// off an async runtime's threads (`spawn_blocking`). A failed last export is only logged.
    pub fn shutdown(&self) {
        if let Err(e) = self.provider.shutdown_with_timeout(SHUTDOWN) {
            tracing::warn!("the last traces weren't exported: {e}");
        }
    }
}

/// The trace and span ids of `span`, in hex, when a [`Traces::layer`] in `dispatch` sampled it.
pub fn trace_ids(span: &span::Id, dispatch: &Dispatch) -> Option<(String, String)> {
    let cx = tracing_opentelemetry::get_otel_context(span, dispatch)?;
    let span = cx.span();
    let sc = span.span_context();
    (sc.is_valid() && sc.is_sampled())
        .then(|| (sc.trace_id().to_string(), sc.span_id().to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU16;

    use data_avalanche_testkit::otlp_collector_answering;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;

    /// The events this crate logs, with their level.
    #[derive(Clone, Default)]
    struct Lines(Arc<Mutex<Vec<(tracing::Level, String)>>>);

    impl Lines {
        fn at(&self, level: tracing::Level) -> Vec<String> {
            let lines = self.0.lock().unwrap();
            lines
                .iter()
                .filter(|(l, _)| *l == level)
                .map(|(_, text)| text.clone())
                .collect()
        }
    }

    impl<S: Subscriber> Layer<S> for Lines {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Message<'a>(&'a mut String);
            impl tracing::field::Visit for Message<'_> {
                fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
                    if field.name() == "message" {
                        *self.0 = format!("{value:?}");
                    }
                }
            }
            if !event
                .metadata()
                .target()
                .starts_with("data_avalanche_telemetry")
            {
                return;
            }
            let mut text = String::new();
            event.record(&mut Message(&mut text));
            self.0
                .lock()
                .unwrap()
                .push((*event.metadata().level(), text));
        }
    }

    /// Polls `future` on this thread to its end: the export the batch thread would run.
    fn block_on<F: Future>(future: F) -> F::Output {
        struct Unpark(std::thread::Thread);
        impl std::task::Wake for Unpark {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }
        let waker = std::task::Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut cx = std::task::Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            if let std::task::Poll::Ready(out) = future.as_mut().poll(&mut cx) {
                return out;
            }
            std::thread::park();
        }
    }

    /// Config §6.6: an endpoint the exporter can't use is a warning at run time. A collector that
    /// keeps refusing is warned about once, not at each export, and without its credentials; an
    /// export that works again says so; the next failure, or a new exporter, warns again.
    #[test]
    fn a_failing_export_is_warned_about_once_without_its_credentials() {
        let status = Arc::new(AtomicU16::new(500));
        let (url, requests) = otlp_collector_answering(status.clone());
        let endpoint = url.replace("http://", "http://u:secret@");
        let traces = Traces::new("node-1");
        traces.set(Some(&endpoint), 1.0).unwrap();
        let lines = Lines::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(lines.clone()));
        let export = || {
            tracing::dispatcher::with_default(&dispatch, || {
                block_on(traces.switch.export(Vec::new()))
            })
        };

        for _ in 0..3 {
            assert!(export().is_err());
        }
        for _ in 0..3 {
            requests.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        let warned = lines.at(tracing::Level::WARN);
        assert_eq!(warned.len(), 1, "{warned:?}");
        assert!(
            warned[0].contains(&url.replace("http://", "")),
            "{warned:?}"
        );
        assert!(!warned[0].contains("secret"), "{warned:?}");
        assert!(!warned[0].contains("u:"), "{warned:?}");

        status.store(200, Ordering::Relaxed);
        assert!(export().is_ok());
        assert!(export().is_ok());
        let again = lines.at(tracing::Level::INFO);
        assert_eq!(again.len(), 1, "{again:?}");
        assert!(!again[0].contains("secret"), "{again:?}");

        status.store(500, Ordering::Relaxed);
        assert!(export().is_err());
        assert!(export().is_err());
        assert_eq!(lines.at(tracing::Level::WARN).len(), 2);

        traces.set(None, 1.0).unwrap();
        traces.set(Some(&endpoint), 1.0).unwrap();
        assert!(export().is_err());
        assert_eq!(lines.at(tracing::Level::WARN).len(), 3, "a new exporter");
        assert!(
            lines
                .0
                .lock()
                .unwrap()
                .iter()
                .all(|(_, text)| !text.contains("secret"))
        );
    }

    #[test]
    fn a_failure_that_lasts_is_warned_about_again_only_after_a_while() {
        let health = Health::new();
        let at = |minutes: u64| Duration::from_secs(minutes * 60);
        assert!(health.failed(at(0)), "the first failure");
        assert!(!health.failed(at(1)));
        assert!(!health.failed(at(4)));
        assert!(health.failed(at(5)), "FAILING_WARNED_EVERY later");
        assert!(!health.failed(at(9)));
        assert!(health.recovered());
        assert!(!health.recovered(), "said once");
        assert!(health.failed(at(10)), "the first failure after a success");
    }

    #[test]
    fn the_debug_of_traces_leaves_out_the_endpoint_s_credentials() {
        let (url, _requests) = data_avalanche_testkit::otlp_collector();
        let traces = Traces::new("node-1");
        let endpoint = format!("{}/?token=t0k", url.replace("http://", "http://u:secret@"));
        traces.set(Some(&endpoint), 1.0).unwrap();
        let text = format!("{traces:?}");
        assert!(text.contains(&url.replace("http://", "")), "{text}");
        assert!(!text.contains("secret"), "{text}");
        assert!(!text.contains("t0k"), "{text}");
    }

    #[test]
    fn the_switch_carries_the_resource_before_the_batch_thread_applies_it() {
        let traces = Traces::new("node-1");
        let current = traces.switch.0.read().unwrap();
        let resource = current.resource.as_ref().expect("a resource");
        let id = resource.get(&opentelemetry::Key::from_static_str("service.instance.id"));
        assert_eq!(id.map(|v| v.to_string()).as_deref(), Some("node-1"));
    }

    #[test]
    fn an_endpoint_is_shown_without_its_credentials_or_query() {
        for (given, shown_as) in [
            ("https://u:secret@c:4318/p?k=v", "https://c:4318/p"),
            ("https://u@c:4318", "https://c:4318"),
            ("https://c:4318/p?token=x#f", "https://c:4318/p"),
            ("http://c:4318/a@b", "http://b"),
            ("https://u:pa/ss@c:4318", "https://c:4318"),
            ("https://u:pa?ss@c:4318", "https://c:4318"),
            ("u:secret@c:4318", "c:4318"),
            ("u:1234/x@c", "c"),
            ("http://u:p@[::1]:4318/x", "http://[::1]:4318/x"),
            ("http://c:4318", "http://c:4318"),
            ("not a url", "not a url"),
        ] {
            assert_eq!(shown(given), shown_as, "{given}");
            for secret in ["pa/ss", "pa?ss", "secret", "1234/x"] {
                assert!(!shown(given).contains(secret), "{given}");
            }
        }
    }

    #[test]
    fn the_error_of_an_endpoint_leaves_out_its_password() {
        let e = TracesError::Endpoint {
            endpoint: "https://u:secret@c:4318".to_owned(),
            reason: "bad url https://u:secret@c:4318/v1/traces".to_owned(),
        };
        let text = e.to_string();
        assert!(!text.contains("secret"), "{text}");
        assert!(text.contains("c:4318"), "{text}");
    }
}
