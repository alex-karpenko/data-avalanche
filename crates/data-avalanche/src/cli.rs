//! The command line.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "data-avalanche",
    version,
    about = "Change data capture from PostgreSQL into Delta Lake"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Runs every pipeline of the config.
    Run(RunArgs),
}

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    /// The YAML config file.
    #[arg(long)]
    pub config: PathBuf,
    /// Overrides `globals.log.level` (a tracing filter: `info`, `data_avalanche=debug,warn`).
    #[arg(long)]
    pub log_level: Option<String>,
    /// Overrides `globals.log.format`.
    #[arg(long, value_enum)]
    pub log_format: Option<LogFormatArg>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormatArg {
    Json,
    Compact,
    Pretty,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_takes_a_config_and_log_overrides() {
        let cli = Cli::try_parse_from([
            "data-avalanche",
            "run",
            "--config",
            "c.yaml",
            "--log-level",
            "debug",
            "--log-format",
            "pretty",
        ])
        .unwrap();
        let Command::Run(args) = cli.command;
        assert_eq!(args.config, PathBuf::from("c.yaml"));
        assert_eq!(args.log_level.as_deref(), Some("debug"));
        assert_eq!(args.log_format, Some(LogFormatArg::Pretty));
    }

    #[test]
    fn run_needs_a_config() {
        let e = Cli::try_parse_from(["data-avalanche", "run"]).unwrap_err();
        assert!(e.to_string().contains("--config"), "{e}");
    }
}
