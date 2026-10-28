//! Traces (config §6.6, `Observability_Metrics_and_Tracing` §3): the `tracing` spans of a flush,
//! a snapshot chunk and a schema change, exported over OTLP/HTTP (protobuf) to
//! `<otlp_endpoint>/v1/traces` (Q-M10-1), head-sampled by `sample_ratio`.
//!
//! One provider and one `tracing` layer serve the whole run: a reload of `globals.tracing` swaps
//! the exporter behind them and the sampling ratio (D-M10.6). A layer can't be reloaded: the
//! logs find a span's ids by downcasting to it, which a reload layer doesn't pass through. The
//! batch exporter runs on a thread of its own and exports with a blocking client.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

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
    exporter: Option<Arc<opentelemetry_otlp::SpanExporter>>,
    resource: Option<Resource>,
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
    fn current(&self) -> Option<Arc<opentelemetry_otlp::SpanExporter>> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .exporter
            .clone()
    }
}

impl SpanExporter for Switch {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        match self.current() {
            Some(exporter) => exporter.export(batch).await,
            None => Ok(()),
        }
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.current()
            .map_or(Ok(()), |e| e.shutdown_with_timeout(timeout))
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
#[derive(Debug)]
pub struct Traces {
    provider: SdkTracerProvider,
    switch: Switch,
    ratio: Ratio,
    /// The endpoint in force.
    endpoint: RwLock<Option<String>>,
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
            // Its blocking client can't be dropped on an async runtime's thread.
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

    fn exporter(&self, endpoint: &str) -> Result<opentelemetry_otlp::SpanExporter, TracesError> {
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
        Ok(exporter)
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
    use super::*;

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
