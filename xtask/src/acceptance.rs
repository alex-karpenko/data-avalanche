//! `cargo xtask bench|chaos|soak` (roadmap §5.1, Q-M12c-2): the acceptance runs of
//! `crates/data-avalanche/tests/load`, one module each, with the binary built in release mode, the
//! gates set, and the measurements written to `target/acceptance/results.jsonl`. The runs' knobs
//! (`DATA_AVALANCHE_LOAD_RATE`, `…_MINUTES`, `DATA_AVALANCHE_SOAK_MINUTES`, …) pass through from
//! the caller's environment.

use std::path::Path;

/// The acceptance runs, as `cargo xtask` names them and as the load target's modules are named.
pub const RUNS: [&str; 3] = ["bench", "chaos", "soak"];

/// One run's `cargo` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invocation {
    pub args: Vec<String>,
    /// Set unless the caller's environment already sets it.
    pub env: Vec<(String, String)>,
}

/// `run`'s invocation from the repository at `root`; `None` for a name that isn't a run.
pub fn invocation(run: &str, root: &Path) -> Option<Invocation> {
    if !RUNS.contains(&run) {
        return None;
    }
    let args = [
        "test",
        "--release",
        "--locked",
        "-p",
        "data-avalanche",
        "--features",
        "e2e-hooks",
        "--test",
        "load",
        "--",
        "--test-threads=1",
        "--nocapture",
    ]
    .into_iter()
    .map(str::to_owned)
    .chain([format!("{run}::")])
    .collect();
    let out = root.join("target/acceptance");
    let env = [
        ("DATA_AVALANCHE_IT", "1".to_owned()),
        ("DATA_AVALANCHE_LOAD", "1".to_owned()),
        ("DATA_AVALANCHE_SKIP_GCS", "1".to_owned()),
        ("DATA_AVALANCHE_LOAD_OUT", out.display().to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect();
    Some(Invocation { args, env })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_is_its_module_of_the_load_target_in_release_mode() {
        let i = invocation("chaos", Path::new("/repo")).unwrap();
        assert_eq!(
            i.args.join(" "),
            "test --release --locked -p data-avalanche --features e2e-hooks --test load -- \
             --test-threads=1 --nocapture chaos::"
        );
        assert!(i.env.contains(&("DATA_AVALANCHE_LOAD".into(), "1".into())));
        assert!(i.env.contains(&(
            "DATA_AVALANCHE_LOAD_OUT".into(),
            "/repo/target/acceptance".into()
        )));
        assert_eq!(invocation("docs", Path::new("/repo")), None);
    }
}
