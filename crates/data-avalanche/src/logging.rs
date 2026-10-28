//! Logs: `globals.log` (level, format), each overridden by the command line, and changed by a
//! config generation (config §1.2: R). Traces: `globals.tracing`, changed the same way
//! (D-M10.6). The log level filters the log alone: the traces' layer has a filter of its own,
//! which passes the pipeline's spans while an endpoint is set and nothing otherwise.

use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use data_avalanche_config::{Log, LogFormat, Tracing};
use data_avalanche_telemetry::{Traces, shown, trace_ids};
use serde_json::Value;
use tracing::dispatcher::WeakDispatch;
use tracing::{Dispatch, Event, Level, Subscriber};
use tracing_subscriber::filter::{FilterExt as _, LevelFilter, Targets, filter_fn};
use tracing_subscriber::fmt::format::{Format as EventFormat, Json, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, MakeWriter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer, Registry, reload};

use crate::{LogFormatArg, RunArgs};

/// The filter and the format in force: the command line's, else the config's.
pub(crate) fn settings(log: &Log, args: &RunArgs) -> (String, LogFormat) {
    let level = args
        .log_level
        .clone()
        .unwrap_or_else(|| log.level.0.clone());
    let format = match args.log_format {
        Some(LogFormatArg::Json) => LogFormat::Json,
        Some(LogFormatArg::Compact) => LogFormat::Compact,
        Some(LogFormatArg::Pretty) => LogFormat::Pretty,
        None => log.format,
    };
    (level, format)
}

/// The Delta libraries' targets, and the most each logs under a level of `info` or quieter:
/// their INFO traces every snapshot build, MERGE and commit, and the MERGE warns "Missing target
/// skipped-file metric" on every run (config §6.4).
const QUIET: &[(&str, LevelFilter)] = &[
    ("buoyant_kernel", LevelFilter::WARN),
    ("deltalake_core", LevelFilter::WARN),
    ("deltalake_core::operations::merge", LevelFilter::ERROR),
];

/// `filter` with [`QUIET`]'s caps added when its bare level is `info` or quieter and it names
/// neither library: a filter that names one, or a `debug` or `trace` level, is the operator's.
pub(crate) fn quieted(filter: &str) -> String {
    let directives: Vec<&str> = filter.split(',').map(str::trim).collect();
    if directives
        .iter()
        .any(|d| d.starts_with("buoyant_kernel") || d.starts_with("deltalake"))
    {
        return filter.to_owned();
    }
    let Some(level) = directives
        .iter()
        .rev()
        .find_map(|d| d.parse::<LevelFilter>().ok())
    else {
        return filter.to_owned();
    };
    let mut out = filter.to_owned();
    if level <= LevelFilter::INFO {
        for (target, most) in QUIET {
            if level > *most {
                out.push_str(&format!(",{target}={most}"));
            }
        }
    }
    out
}

type Format = Box<dyn Layer<Registry> + Send + Sync>;

/// The spans the traces take while an endpoint is set: the pipeline's flushes, commits, chunk
/// reads and schema changes, whatever the log level.
fn traced() -> Targets {
    Targets::new().with_target("data_avalanche", Level::INFO)
}

/// The installed logger: its filter and its format change with each config generation's
/// `globals.log`, except where the command line set them; its traces follow
/// `globals.tracing`.
pub(crate) struct Logger {
    filter: reload::Handle<EnvFilter, Registry>,
    format: reload::Handle<Format, Registry>,
    layer: Box<dyn Fn(LogFormat) -> Format + Send + Sync>,
    args: RunArgs,
    /// The level and the format in force.
    current: Mutex<(String, LogFormat)>,
    traces: Arc<Traces>,
    /// The traces' filter: [`traced`] with an endpoint, nothing without.
    spans: reload::Handle<Targets, Registry>,
    /// `globals.tracing` in force.
    tracing: Mutex<Tracing>,
}

impl std::fmt::Debug for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logger")
            .field("current", &self.current)
            .finish_non_exhaustive()
    }
}

fn filter(level: &str) -> Result<EnvFilter, String> {
    EnvFilter::builder()
        .parse(quieted(level))
        .map_err(|e| format!("{level:?} isn't a log filter: {e}"))
}

/// Passes the pipeline's spans (INFO and above) whatever the log level: a log line is correlated
/// through the spans the format layer sees, and a `warn` level would hide `pipeline.flush` from
/// it. Events follow the level alone, and this layer writes no span lines of its own.
fn spans_kept<S>() -> impl tracing_subscriber::layer::Filter<S> {
    filter_fn(|m| {
        m.is_span() && m.target().starts_with("data_avalanche") && *m.level() <= Level::INFO
    })
}

/// The process's dispatch, once built: the JSON format reads a span's trace ids through it.
/// Inside an event a scoped dispatch isn't the current one any more, so it is held, weakly.
type Process = Arc<OnceLock<WeakDispatch>>;

fn layer<W>(format: LogFormat, writer: W, ansi: bool, process: &Process) -> Format
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    let builder = tracing_subscriber::fmt::layer()
        .with_writer(writer)
        .with_ansi(ansi);
    match format {
        LogFormat::Json => builder
            .json()
            .event_format(Correlated {
                json: tracing_subscriber::fmt::format().json(),
                process: Arc::clone(process),
            })
            .boxed(),
        LogFormat::Compact => builder.compact().boxed(),
        LogFormat::Pretty => builder.pretty().boxed(),
    }
}

/// The process's dispatch, writing to `writer` (in colour when `ansi`), and its [`Logger`]; the
/// traces carry `node` (`service.instance.id`) and export nothing until [`Logger::trace`].
///
/// # Errors
/// A `--log-level` that isn't a filter (the config's was checked by V-017).
pub(crate) fn build<W>(
    log: &Log,
    node: &str,
    args: &RunArgs,
    writer: W,
    ansi: bool,
) -> Result<(Dispatch, Logger), String>
where
    W: for<'w> MakeWriter<'w> + Clone + Send + Sync + 'static,
{
    let (level, format) = settings(log, args);
    let process = Process::default();
    let (filter_layer, filter) =
        reload::Layer::new(filter(&level).map_err(|e| format!("--log-level {e}"))?);
    let (format_layer, format_handle) =
        reload::Layer::new(layer(format, writer.clone(), ansi, &process));
    let traces = Arc::new(Traces::new(node));
    let (spans_filter, spans) = reload::Layer::new(Targets::new());
    let layers: Vec<Format> = vec![
        Box::new(format_layer.with_filter(spans_kept().or(filter_layer))),
        Box::new(traces.layer().with_filter(spans_filter)),
    ];
    let dispatch = Dispatch::new(tracing_subscriber::registry().with(layers));
    let _ = process.set(dispatch.downgrade());
    let logger = Logger {
        filter,
        format: format_handle,
        layer: Box::new(move |f| layer(f, writer.clone(), ansi, &process)),
        args: args.clone(),
        current: Mutex::new((level, format)),
        traces,
        spans,
        tracing: Mutex::new(Tracing {
            otlp_endpoint: None,
            sample_ratio: 0.0,
        }),
    };
    Ok((dispatch, logger))
}

/// Whether the text logs are in colour: only on a terminal, and not under `NO_COLOR` set to
/// anything but empty (DA-OBS-6).
fn coloured(terminal: bool, no_color: Option<&std::ffi::OsStr>) -> bool {
    terminal && no_color.is_none_or(std::ffi::OsStr::is_empty)
}

/// The JSON format, correlated (config §6.6, Observability research §4): a line logged in a
/// sampled span carries the span's `trace_id` and `span_id`, and one logged in a span with a
/// `pipeline` field carries it as `fields.pipeline` unless it names its own.
struct Correlated {
    json: EventFormat<Json>,
    process: Process,
}

impl<S, N> FormatEvent<S, N> for Correlated
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'w> FormatFields<'w> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        let mut line = String::new();
        self.json.format_event(ctx, Writer::new(&mut line), event)?;
        let ids = ctx.parent_span().map(|s| s.id()).and_then(|id| {
            let process = self.process.get()?.upgrade()?;
            trace_ids(&id, &process)
        });
        match correlated(&line, ids) {
            Some(out) => writeln!(writer, "{out}"),
            None => writer.write_str(&line),
        }
    }
}

/// `line`, a JSON log line, with the ids and the spans' `pipeline` added; `None` when it isn't
/// an object.
fn correlated(line: &str, ids: Option<(String, String)>) -> Option<String> {
    let Ok(Value::Object(mut map)) = serde_json::from_str::<Value>(line) else {
        return None;
    };
    let pipeline = map
        .get("spans")
        .and_then(Value::as_array)
        .and_then(|spans| spans.iter().rev().find_map(|s| s.get("pipeline")).cloned());
    if let (Some(pipeline), Some(Value::Object(fields))) = (pipeline, map.get_mut("fields")) {
        fields.entry("pipeline").or_insert(pipeline);
    }
    if let Some((trace, span)) = ids {
        map.insert("trace_id".to_owned(), trace.into());
        map.insert("span_id".to_owned(), span.into());
    }
    Some(Value::Object(map).to_string())
}

/// Installs the process's dispatch; logs go to stderr, in colour on a terminal.
///
/// # Errors
/// A `--log-level` that isn't a filter, or a subscriber is installed already.
pub(crate) fn init(log: &Log, node: &str, args: &RunArgs) -> Result<Logger, String> {
    use std::io::IsTerminal as _;
    let ansi = coloured(
        std::io::stderr().is_terminal(),
        std::env::var_os("NO_COLOR").as_deref(),
    );
    let (dispatch, logger) = build(log, node, args, std::io::stderr, ansi)?;
    tracing::dispatcher::set_global_default(dispatch)
        .map_err(|e| format!("cannot install the logger: {e}"))?;
    Ok(logger)
}

impl Logger {
    /// Takes `log`, a config generation's, where the command line didn't set the level or the
    /// format.
    pub fn apply(&self, log: &Log) {
        let (level, format) = settings(log, &self.args);
        let mut current = self.current.lock().unwrap_or_else(PoisonError::into_inner);
        if level != current.0 {
            match filter(&level).and_then(|f| self.filter.reload(f).map_err(|e| e.to_string())) {
                Ok(()) => {
                    tracing::info!("the log level is {level} now");
                    current.0 = level;
                }
                Err(e) => tracing::warn!("the log level stays {}: {e}", current.0),
            }
        }
        if format != current.1 {
            match self.format.reload((self.layer)(format)) {
                Ok(()) => {
                    tracing::info!("the log format is {format} now");
                    current.1 = format;
                }
                Err(e) => tracing::warn!("the log format stays {}: {e}", current.1),
            }
        }
    }

    /// Takes `tracing`, a config generation's (config §6.6: R). An endpoint that can't be
    /// exported to keeps what was in force, with a warning.
    pub fn trace(&self, tracing: &Tracing) {
        let mut current = self.tracing.lock().unwrap_or_else(PoisonError::into_inner);
        if *current == *tracing {
            return;
        }
        let endpoint = tracing.otlp_endpoint.as_deref();
        match self.traces.set(endpoint, tracing.sample_ratio) {
            Ok(on) => {
                let spans = if on { traced() } else { Targets::new() };
                if let Err(e) = self.spans.reload(spans) {
                    tracing::warn!("the traces' filter stays as it was: {e}");
                }
                match endpoint {
                    Some(e) => tracing::info!(
                        "traces go to {} (OTLP/HTTP), {} of them sampled",
                        shown(e),
                        tracing.sample_ratio
                    ),
                    None if current.otlp_endpoint.is_some() => tracing::info!("traces are off"),
                    None => {}
                }
                *current = tracing.clone();
            }
            Err(e) => tracing::warn!("the traces stay as they were: {e}"),
        }
    }

    /// Exports the traces still queued; at the process's end.
    pub async fn finish(&self) {
        let traces = Arc::clone(&self.traces);
        let _ = tokio::task::spawn_blocking(move || traces.shutdown()).await;
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use data_avalanche_config::LogFilter;

    use super::*;

    fn args(level: Option<&str>, format: Option<LogFormatArg>) -> RunArgs {
        RunArgs {
            config: PathBuf::from("c.yaml"),
            log_level: level.map(str::to_owned),
            log_format: format,
        }
    }

    #[test]
    fn a_bare_level_quiets_the_delta_libraries() {
        assert_eq!(
            quieted("info"),
            "info,buoyant_kernel=warn,deltalake_core=warn,deltalake_core::operations::merge=error"
        );
        assert_eq!(
            quieted("info,data_avalanche=debug"),
            "info,data_avalanche=debug,buoyant_kernel=warn,deltalake_core=warn,deltalake_core::operations::merge=error"
        );
        assert_eq!(
            quieted("warn"),
            "warn,deltalake_core::operations::merge=error",
            "never louder than the level"
        );
        assert_eq!(quieted("error"), "error");
    }

    #[test]
    fn a_filter_that_names_a_library_or_asks_for_more_is_kept() {
        for f in [
            "debug",
            "trace",
            "info,deltalake_core=info",
            "info,buoyant_kernel=debug",
            "data_avalanche=debug",
        ] {
            assert_eq!(quieted(f), f);
        }
    }

    #[test]
    fn the_quieted_filter_parses() {
        for f in ["info", "warn", "info,data_avalanche_cluster=debug"] {
            EnvFilter::builder().parse(quieted(f)).unwrap();
        }
    }

    #[test]
    fn the_command_line_overrides_the_config() {
        let log = Log {
            level: LogFilter("info".into()),
            format: LogFormat::Json,
        };
        assert_eq!(
            settings(&log, &args(None, None)),
            ("info".to_owned(), LogFormat::Json)
        );
        assert_eq!(
            settings(&log, &args(Some("debug"), Some(LogFormatArg::Compact))),
            ("debug".to_owned(), LogFormat::Compact)
        );
    }

    /// A writer into a shared buffer.
    #[derive(Debug, Clone, Default)]
    struct Buf(std::sync::Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'w> MakeWriter<'w> for Buf {
        type Writer = Buf;
        fn make_writer(&'w self) -> Buf {
            self.clone()
        }
    }

    impl Buf {
        /// The lines that contain `what`.
        fn lines(&self, what: &str) -> Vec<String> {
            String::from_utf8(self.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .filter(|l| l.contains(what))
                .map(str::to_owned)
                .collect()
        }
    }

    fn log(level: &str, format: LogFormat) -> Log {
        Log {
            level: LogFilter(level.into()),
            format,
        }
    }

    #[test]
    fn a_generation_changes_the_level_and_the_format() {
        let buf = Buf::default();
        let (dispatch, logger) = build(
            &log("info", LogFormat::Compact),
            "n1",
            &args(None, None),
            buf.clone(),
            false,
        )
        .unwrap();
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::debug!("before");
            logger.apply(&log("debug", LogFormat::Json));
            tracing::debug!("after");
        });
        assert!(buf.lines("before").is_empty());
        let after = buf.lines("after");
        assert_eq!(after.len(), 1, "{after:?}");
        let line: serde_json::Value = serde_json::from_str(&after[0]).unwrap();
        assert_eq!(line["fields"]["message"], "after");
    }

    #[test]
    fn the_command_line_wins_over_a_generation() {
        let buf = Buf::default();
        let cli = args(Some("info"), Some(LogFormatArg::Compact));
        let (dispatch, logger) = build(
            &log("info", LogFormat::Compact),
            "n1",
            &cli,
            buf.clone(),
            false,
        )
        .unwrap();
        tracing::dispatcher::with_default(&dispatch, || {
            logger.apply(&log("debug", LogFormat::Json));
            tracing::debug!("quiet");
            tracing::info!("loud");
        });
        assert!(buf.lines("quiet").is_empty());
        let loud = buf.lines("loud");
        assert_eq!(loud.len(), 1, "{loud:?}");
        assert!(
            serde_json::from_str::<serde_json::Value>(&loud[0]).is_err(),
            "{loud:?}"
        );
    }

    /// The traces take the pipeline's spans once an endpoint is set, whatever the log level:
    /// none before it, and none of the libraries'.
    #[test]
    fn the_pipeline_s_spans_are_exported_once_an_endpoint_is_set() {
        let (url, requests) = data_avalanche_testkit::otlp_collector();
        let (dispatch, logger) = build(
            &log("warn", LogFormat::Compact),
            "n1",
            &args(None, None),
            Buf::default(),
            false,
        )
        .unwrap();
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info_span!("before.endpoint").in_scope(|| {});
            logger.trace(&Tracing {
                otlp_endpoint: Some(url.clone()),
                sample_ratio: 1.0,
            });
            tracing::info_span!(target: "deltalake_core", "a.library.span").in_scope(|| {});
            tracing::info_span!("after.endpoint").in_scope(|| {});
        });
        logger.traces.shutdown();
        let request = requests.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(request.path, "/v1/traces");
        assert!(request.has("after.endpoint"));
        assert!(request.has("n1"), "service.instance.id");
        assert!(!request.has("before.endpoint"));
        assert!(!request.has("a.library.span"));
    }

    /// The JSON lines a sampled `pipeline.flush` logs in, and one logged outside any span.
    fn correlated_lines(endpoint: Option<String>) -> (Buf, Logger) {
        let buf = Buf::default();
        let (dispatch, logger) = build(
            &log("info", LogFormat::Json),
            "n1",
            &args(None, None),
            buf.clone(),
            false,
        )
        .unwrap();
        tracing::dispatcher::with_default(&dispatch, || {
            logger.trace(&Tracing {
                otlp_endpoint: endpoint,
                sample_ratio: 1.0,
            });
            tracing::info_span!("pipeline.flush", pipeline = "orders").in_scope(|| {
                tracing::info_span!("sink.commit", table = "public.a").in_scope(|| {
                    tracing::warn!("in the commit");
                });
                tracing::warn!(pipeline = "named", "naming its own");
            });
            tracing::warn!("outside");
        });
        (buf, logger)
    }

    /// Above INFO the format layer's own level hides the pipeline's INFO spans, but the lines
    /// still carry their ids and `pipeline`, and no INFO event is written.
    #[test]
    fn a_warn_level_line_keeps_its_ids_and_its_pipeline() {
        let (url, _requests) = data_avalanche_testkit::otlp_collector();
        let buf = Buf::default();
        let (dispatch, logger) = build(
            &log("warn", LogFormat::Json),
            "n1",
            &args(None, None),
            buf.clone(),
            false,
        )
        .unwrap();
        tracing::dispatcher::with_default(&dispatch, || {
            logger.trace(&Tracing {
                otlp_endpoint: Some(url),
                sample_ratio: 1.0,
            });
            tracing::info_span!("pipeline.flush", pipeline = "orders").in_scope(|| {
                tracing::info_span!("sink.commit", table = "public.a").in_scope(|| {
                    tracing::warn!("warned in the commit");
                    tracing::info!("informed in the commit");
                });
            });
        });
        logger.traces.shutdown();
        let inside = line(&buf, "warned in the commit");
        assert_eq!(
            inside["trace_id"].as_str().map(str::len),
            Some(32),
            "{inside}"
        );
        assert_eq!(
            inside["span_id"].as_str().map(str::len),
            Some(16),
            "{inside}"
        );
        assert_eq!(inside["fields"]["pipeline"], "orders");
        assert!(buf.lines("informed in the commit").is_empty());
    }

    fn line(buf: &Buf, what: &str) -> serde_json::Value {
        let lines = buf.lines(what);
        assert_eq!(lines.len(), 1, "{lines:?}");
        serde_json::from_str(&lines[0]).unwrap()
    }

    /// Config §6.6 and Observability research §4: a JSON line logged in a sampled span carries
    /// the span's `trace_id` and `span_id`, and the `pipeline` of the spans it is in unless it
    /// names its own; a line outside every span carries neither.
    #[test]
    fn a_json_line_in_a_sampled_span_carries_its_ids_and_its_pipeline() {
        let (url, _requests) = data_avalanche_testkit::otlp_collector();
        let (buf, logger) = correlated_lines(Some(url));
        logger.traces.shutdown();
        let inside = line(&buf, "in the commit");
        let (trace, span) = (inside["trace_id"].as_str(), inside["span_id"].as_str());
        assert_eq!(trace.map(str::len), Some(32), "{inside}");
        assert_eq!(span.map(str::len), Some(16), "{inside}");
        assert_eq!(inside["fields"]["pipeline"], "orders");
        let named = line(&buf, "naming its own");
        assert_eq!(named["fields"]["pipeline"], "named");
        assert_eq!(named["trace_id"].as_str(), trace, "the same trace");
        assert_ne!(named["span_id"].as_str(), span, "the flush, not the commit");
        let outside = line(&buf, "outside");
        assert!(outside.get("trace_id").is_none(), "{outside}");
        assert!(outside["fields"].get("pipeline").is_none(), "{outside}");
    }

    /// Without an endpoint nothing is sampled: no ids, but the `pipeline` still.
    #[test]
    fn without_traces_a_json_line_carries_its_pipeline_but_no_ids() {
        let (buf, _logger) = correlated_lines(None);
        let inside = line(&buf, "in the commit");
        assert!(inside.get("trace_id").is_none(), "{inside}");
        assert!(inside.get("span_id").is_none(), "{inside}");
        assert_eq!(inside["fields"]["pipeline"], "orders");
    }

    /// DA-OBS-6: the text logs carry no escape sequences unless they are in colour.
    #[test]
    fn a_text_log_is_in_colour_only_when_asked() {
        for (ansi, format) in [
            (false, LogFormat::Compact),
            (false, LogFormat::Pretty),
            (true, LogFormat::Compact),
        ] {
            let buf = Buf::default();
            let (dispatch, _logger) = build(
                &log("info", format),
                "n1",
                &args(None, None),
                buf.clone(),
                ansi,
            )
            .unwrap();
            tracing::dispatcher::with_default(&dispatch, || {
                tracing::warn!(code = "X", "coloured?")
            });
            let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
            assert_eq!(text.contains("\x1b["), ansi, "{format}: {text:?}");
        }
    }

    #[test]
    fn colour_needs_a_terminal_and_no_no_color() {
        use std::ffi::OsStr;
        assert!(coloured(true, None));
        assert!(
            coloured(true, Some(OsStr::new(""))),
            "an empty NO_COLOR is unset"
        );
        assert!(!coloured(true, Some(OsStr::new("1"))));
        assert!(!coloured(false, None));
    }
}
