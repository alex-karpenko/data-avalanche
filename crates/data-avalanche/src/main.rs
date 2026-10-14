//! `data-avalanche run --config <path>`.

use clap::Parser;
use data_avalanche::{Cli, Command};

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    // One rustls provider before any TLS (DA-DEP-5): two are linked.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("data-avalanche: cannot start the async runtime: {e}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let Command::Run(args) = cli.command;
    match runtime.block_on(data_avalanche::run(args)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("data-avalanche: {e}");
            std::process::ExitCode::from(e.exit_code())
        }
    }
}
