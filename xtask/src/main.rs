//! `cargo xtask docs` writes the documentation site's generated pages; `cargo xtask docs --check`
//! exits 1, naming them, when a committed copy differs. `cargo xtask bench|chaos|soak` runs an
//! acceptance run (M12c) and says where its measurements are.

use std::process::{Command, ExitCode};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let check = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["docs"] => false,
        ["docs", "--check"] => true,
        [run] if xtask::RUNS.contains(&run) => return acceptance(run),
        _ => {
            eprintln!("usage: cargo xtask docs [--check] | bench | chaos | soak");
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
