//! The site's generated pages match the code.

#[test]
fn the_generated_pages_are_current() {
    let root = xtask::repo_root();
    let stale: Vec<_> = xtask::generated()
        .into_iter()
        .filter(|(path, text)| std::fs::read_to_string(root.join(path)).ok().as_ref() != Some(text))
        .map(|(path, _)| path)
        .collect();
    assert!(stale.is_empty(), "stale, run `cargo xtask docs`: {stale:?}");
}
