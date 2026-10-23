//! Logs: `globals.log` (level, format), each overridden by the command line, and changed by a
//! config generation (config §1.2: R).

use std::sync::{Mutex, PoisonError};

use data_avalanche_config::{Log, LogFormat};
use tracing::Subscriber;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Layered, SubscriberExt};
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

type Filtered = Layered<reload::Layer<EnvFilter, Registry>, Registry>;
type Format = Box<dyn Layer<Filtered> + Send + Sync>;

/// The installed logger: its filter and its format change with each config generation's
/// `globals.log`, except where the command line set them.
pub(crate) struct Logger {
    filter: reload::Handle<EnvFilter, Registry>,
    format: reload::Handle<Format, Filtered>,
    layer: Box<dyn Fn(LogFormat) -> Format + Send + Sync>,
    args: RunArgs,
    /// The level and the format in force.
    current: Mutex<(String, LogFormat)>,
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

fn layer<W>(format: LogFormat, writer: W) -> Format
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    let builder = tracing_subscriber::fmt::layer().with_writer(writer);
    match format {
        LogFormat::Json => builder.json().boxed(),
        LogFormat::Compact => builder.compact().boxed(),
        LogFormat::Pretty => builder.pretty().boxed(),
    }
}

/// The process's subscriber, writing to `writer`, and its [`Logger`].
///
/// # Errors
/// A `--log-level` that isn't a filter (the config's was checked by V-017).
pub(crate) fn build<W>(
    log: &Log,
    args: &RunArgs,
    writer: W,
) -> Result<(impl Subscriber + Send + Sync + use<W>, Logger), String>
where
    W: for<'w> MakeWriter<'w> + Clone + Send + Sync + 'static,
{
    let (level, format) = settings(log, args);
    let (filter_layer, filter) =
        reload::Layer::new(filter(&level).map_err(|e| format!("--log-level {e}"))?);
    let (format_layer, format_handle) = reload::Layer::new(layer(format, writer.clone()));
    let subscriber = tracing_subscriber::registry()
        .with(filter_layer)
        .with(format_layer);
    let logger = Logger {
        filter,
        format: format_handle,
        layer: Box::new(move |f| layer(f, writer.clone())),
        args: args.clone(),
        current: Mutex::new((level, format)),
    };
    Ok((subscriber, logger))
}

/// Installs the process's subscriber; logs go to stderr.
///
/// # Errors
/// A `--log-level` that isn't a filter, or a subscriber is installed already.
pub(crate) fn init(log: &Log, args: &RunArgs) -> Result<Logger, String> {
    let (subscriber, logger) = build(log, args, std::io::stderr)?;
    tracing::subscriber::set_global_default(subscriber)
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
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

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
        let (subscriber, logger) = build(
            &log("info", LogFormat::Compact),
            &args(None, None),
            buf.clone(),
        )
        .unwrap();
        tracing::subscriber::with_default(subscriber, || {
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
        let (subscriber, logger) =
            build(&log("info", LogFormat::Compact), &cli, buf.clone()).unwrap();
        tracing::subscriber::with_default(subscriber, || {
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
}
