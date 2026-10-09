//! The data-avalanche process (roadmap §2): the only crate that links the PostgreSQL source and
//! the Delta sink. `run` loads the config, joins the cluster anchor and the config generation in
//! `globals.state` (the M2 startup sequence), then runs every pipeline under its lease until
//! SIGTERM or SIGINT, and lets every lease go on the way out.

mod cli;
mod http;
mod logging;
mod run;
mod startup;
mod wiring;

pub use cli::{Cli, Command, LogFormatArg, RunArgs};
pub use run::{Fatal, run};
pub use wiring::{Factory, delta_config};
