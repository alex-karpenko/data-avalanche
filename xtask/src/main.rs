//! `cargo xtask docs` writes the documentation site's generated pages; `cargo xtask docs --check`
//! exits 1, naming them, when a committed copy differs.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let check = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["docs"] => false,
        ["docs", "--check"] => true,
        _ => {
            eprintln!("usage: cargo xtask docs [--check]");
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
