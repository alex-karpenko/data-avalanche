//! Logs: `globals.log` (level, format), each overridden by the command line.

use data_avalanche_config::{Log, LogFormat};
use tracing_subscriber::EnvFilter;

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

/// Installs the process's subscriber; logs go to stderr.
///
/// # Errors
/// A `--log-level` that isn't a filter (the config's was checked by V-017).
pub(crate) fn init(log: &Log, args: &RunArgs) -> Result<(), String> {
    let (level, format) = settings(log, args);
    let filter = EnvFilter::builder()
        .parse(&level)
        .map_err(|e| format!("--log-level {level:?} isn't a log filter: {e}"))?;
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    let installed = match format {
        LogFormat::Json => builder.json().try_init(),
        LogFormat::Compact => builder.compact().try_init(),
        LogFormat::Pretty => builder.pretty().try_init(),
    };
    installed.map_err(|e| format!("cannot install the logger: {e}"))
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
}
