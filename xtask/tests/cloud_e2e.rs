//! The cloud E2E runs (`ci.yml`'s `e2e-gcs`, `cloud.yml`'s `e2e`) pass `test-infra/cloud-e2e.txt`'s
//! names to `--exact`, which runs nothing for a name that matches no test: each must name a
//! scenario of the E2E suite.

#[test]
fn the_cloud_e2e_list_names_scenarios_of_the_suite() {
    let root = xtask::repo_root();
    let list = std::fs::read_to_string(root.join("test-infra/cloud-e2e.txt")).expect("the list");
    let names: Vec<&str> = list.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(names.len(), 4, "Q-M12c-4: four scenarios");
    for name in names {
        let (module, test) = name.split_once("::").expect("module::test");
        let file = root.join(format!("crates/data-avalanche/tests/e2e/{module}.rs"));
        let text = std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("{name}: {e}"));
        let at = text
            .find(&format!("async fn {test}()"))
            .unwrap_or_else(|| panic!("{name}: no such scenario in {}", file.display()));
        assert!(
            text[..at].trim_end().ends_with("#[tokio::test]"),
            "{name}: not a test"
        );
    }
}
