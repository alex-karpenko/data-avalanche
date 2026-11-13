//! The repository's dev tool. `cargo xtask docs` writes the documentation site's generated pages;
//! `--check` exits 1, naming them, when a committed copy differs. `cargo xtask bench|chaos|soak`
//! runs an acceptance run (M12c) and says where its measurements are. `cargo xtask deps` checks
//! every pinned dependency against its registry; `--check` exits 1 unless each is current,
//! floating or held, and no exception is stale. `cargo xtask audit` checks the RustSec advisories
//! against `.cargo/audit.toml`; `--check` exits 1 on an unaccepted advisory or a stale exception.
//! Either command exits 1 without `--check` only when it cannot produce a full report.

use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let check = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["docs"] => false,
        ["docs", "--check"] => true,
        ["deps"] => return deps(false),
        ["deps", "--check"] => return deps(true),
        ["audit"] => return audit(false),
        ["audit", "--check"] => return audit(true),
        [run] if xtask::RUNS.contains(&run) => return acceptance(run),
        _ => {
            eprintln!(
                "usage: cargo xtask docs [--check] | deps [--check] | audit [--check] | bench | chaos | soak"
            );
            return ExitCode::from(2);
        }
    };
    let root = xtask::repo_root();
    let mut stale = Vec::new();
    for (path, text) in xtask::generated() {
        let at = root.join(&path);
        if std::fs::read_to_string(&at).ok().as_deref() == Some(text.as_str()) {
            continue;
        }
        if check {
            stale.push(path);
        } else {
            std::fs::create_dir_all(at.parent().expect("a file's directory"))
                .and_then(|()| std::fs::write(&at, text))
                .unwrap_or_else(|e| panic!("{}: {e}", at.display()));
            println!("wrote {}", path.display());
        }
    }
    if stale.is_empty() {
        return ExitCode::SUCCESS;
    }
    for path in stale {
        eprintln!("{} is stale: run `cargo xtask docs`", path.display());
    }
    ExitCode::FAILURE
}

/// Checks every pinned dependency against its registry (`dependency-exceptions.yaml` holds the
/// written exceptions); with `check`, exits 1 unless the report passes.
fn deps(check: bool) -> ExitCode {
    let root = xtask::repo_root();
    let exceptions = match xtask::deps::exceptions(&root) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    let report = xtask::deps::check(&root, &xtask::deps::Live::new(), &exceptions);
    for line in report.lines() {
        println!("{line}");
    }
    // One rule for both commands: no full report (an unreadable inventory) exits 1 even without
    // `--check`; a failing verdict exits 1 only with it.
    if report.incomplete() || (check && !report.passes()) {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Audits `Cargo.lock` against `.cargo/audit.toml`; with `check`, exits 1 unless the report passes.
fn audit(check: bool) -> ExitCode {
    let report = match xtask::audit::run(&xtask::repo_root()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };
    for line in report.lines() {
        println!("{line}");
    }
    // A report that could not be produced exits 1 above; a failing verdict exits 1 only with
    // `--check`, as for `deps`.
    if check && !report.passes() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// Runs `run` (`cargo xtask bench|chaos|soak`), its exit code the test run's.
fn acceptance(run: &str) -> ExitCode {
    let root = xtask::repo_root();
    let i = xtask::invocation(run, &root).expect("a run");
    let mut cmd = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()));
    cmd.args(&i.args).current_dir(&root);
    for (k, v) in &i.env {
        // The two gates are the point of the wrapper: set even when the caller's are empty or
        // `0`, which would make every run skip and the wrapper exit 0. The other values are
        // defaults the caller's environment may override.
        let gate = k == "DATA_AVALANCHE_IT" || k == "DATA_AVALANCHE_LOAD";
        if gate || std::env::var_os(k).is_none() {
            cmd.env(k, v);
        }
    }
    let status = cmd.status().expect("cargo runs");
    println!(
        "the measurements: {}",
        root.join("target/acceptance/results.jsonl").display()
    );
    if status.success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
