//! Logs: `globals.log` (level, format), each overridden by the command line.

use data_avalanche_config::{Log, LogFormat};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

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

/// Installs the process's subscriber; logs go to stderr.
///
/// # Errors
/// A `--log-level` that isn't a filter (the config's was checked by V-017).
pub(crate) fn init(log: &Log, args: &RunArgs) -> Result<(), String> {
    let (level, format) = settings(log, args);
    let filter = EnvFilter::builder()
        .parse(quieted(&level))
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
}
