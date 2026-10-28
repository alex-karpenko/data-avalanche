//! The OTLP/HTTP export (config §6.6): a sampled span reaches the collector's `/v1/traces`; one
//! not sampled doesn't; a span's ids are known only when it is sampled.

use std::time::Duration;

use data_avalanche_telemetry::{Traces, trace_ids};
use data_avalanche_testkit::otlp_collector as collector;
use tracing_subscriber::layer::SubscriberExt;

/// Runs a `pipeline.flush` span with a `sink.commit` inside it under `traces`, and returns the
/// flush's ids as the layer saw them.
fn flush(traces: &Traces) -> Option<(String, String)> {
    let subscriber = tracing_subscriber::registry().with(traces.layer());
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("pipeline.flush", pipeline = "orders");
        let ids = span.id().and_then(|id| trace_ids(&id, &dispatch));
        span.in_scope(|| {
            tracing::info_span!("sink.commit", table = "public.a").in_scope(|| {});
        });
        ids
    })
}

#[test]
fn a_sampled_flush_is_exported_to_the_collector() {
    let (url, requests) = collector();
    let traces = Traces::new("node-1");
    assert!(traces.set(Some(&format!("{url}/")), 1.0).unwrap());
    let ids = flush(&traces);
    traces.shutdown();
    let request = requests.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(request.path, "/v1/traces");
    for text in [
        "pipeline.flush",
        "sink.commit",
        "orders",
        "data-avalanche",
        "node-1",
    ] {
        assert!(request.has(text), "{text}");
    }
    let (trace, span) = ids.expect("a sampled span has ids");
    assert_eq!((trace.len(), span.len()), (32, 16));
}

#[test]
fn a_flush_not_sampled_is_neither_exported_nor_named() {
    let (url, requests) = collector();
    let traces = Traces::new("node-1");
    traces.set(Some(&url), 0.0).unwrap();
    assert_eq!(flush(&traces), None);
    traces.shutdown();
    assert!(requests.recv_timeout(Duration::from_millis(500)).is_err());
}

/// A reload swaps the exporter: the spans after it go to the new endpoint; without one, nowhere.
#[test]
fn a_new_endpoint_takes_the_next_spans_and_none_takes_none() {
    let (first, to_first) = collector();
    let (second, to_second) = collector();
    let traces = Traces::new("node-1");
    traces.set(Some(&first), 1.0).unwrap();
    flush(&traces);
    traces.flush();
    assert!(to_first.recv_timeout(Duration::from_secs(10)).is_ok());
    traces.set(Some(&second), 1.0).unwrap();
    flush(&traces);
    traces.flush();
    assert!(to_second.recv_timeout(Duration::from_secs(10)).is_ok());
    assert!(to_first.try_recv().is_err());
    assert!(!traces.set(None, 1.0).unwrap());
    assert_eq!(flush(&traces), None, "off: nothing is sampled");
    traces.shutdown();
    assert!(to_second.recv_timeout(Duration::from_millis(500)).is_err());
}

#[test]
fn an_endpoint_that_isn_t_a_url_is_refused_and_the_one_in_force_stays() {
    let (url, requests) = collector();
    let traces = Traces::new("node-1");
    traces.set(Some(&url), 1.0).unwrap();
    let e = traces.set(Some("not a url"), 1.0).unwrap_err();
    assert!(e.to_string().contains("not a url"), "{e}");
    flush(&traces);
    traces.shutdown();
    assert!(requests.recv_timeout(Duration::from_secs(10)).is_ok());
}

/// A collector that takes the connection and never answers holds nothing up (Review Focus 2): the
/// spans queue in the batch processor, which drops them once it is full, and the shutdown waits a
/// bounded time.
#[test]
fn a_collector_that_never_answers_doesn_t_hold_the_spans_up() {
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", silent.local_addr().unwrap());
    let traces = Traces::new("node-1");
    traces.set(Some(&url), 1.0).unwrap();
    let started = std::time::Instant::now();
    for _ in 0..5_000 {
        flush(&traces);
    }
    let traced = started.elapsed();
    assert!(
        traced < Duration::from_secs(5),
        "5000 flushes traced in {traced:?}"
    );
    let started = std::time::Instant::now();
    traces.shutdown();
    let shut = started.elapsed();
    assert!(shut < Duration::from_secs(15), "shut down in {shut:?}");
    drop(silent);
}
