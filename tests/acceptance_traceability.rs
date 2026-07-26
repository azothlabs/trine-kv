use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

fn feature_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(directory)
        .expect("feature directory reads")
        .map(|entry| entry.expect("feature directory entry reads").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "feature")
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

fn requirement_tags(line: &str) -> impl Iterator<Item = &str> {
    line.split_whitespace()
        .filter(|token| token.starts_with("@REQ-"))
}

fn wildcard_for(tag: &str) -> String {
    let prefix = tag
        .rsplit_once('-')
        .map_or(tag, |(prefix, _)| prefix)
        .trim_start_matches('@');
    format!("{prefix}-*")
}

fn catalog_requirements(catalog: &str) -> BTreeSet<String> {
    catalog
        .lines()
        .filter_map(|line| {
            let start = line.find("`REQ-")? + 1;
            let rest = &line[start..];
            let end = rest.find('`')?;
            Some(format!("@{}", &rest[..end]))
        })
        .collect()
}

fn externally_checked_requirements(root: &Path) -> BTreeSet<String> {
    let async_api =
        fs::read_to_string(root.join("tests/async_api.rs")).expect("async API tests read");
    assert!(
        async_api.contains("fn dropping_polled_persistent_async_write_future_survives_reopen()"),
        "REQ-ASYNC-003 requires its durable polling-boundary integration test"
    );
    BTreeSet::from(["@REQ-ASYNC-003".to_owned()])
}

#[test]
fn every_acceptance_scenario_has_a_documented_requirement_source() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let contract = fs::read_to_string(root.join("docs/acceptance-contract.md"))
        .expect("acceptance contract reads");
    let v2 = fs::read_to_string(root.join("docs/acceptance-requirements-v2.md"))
        .expect("expanded acceptance requirements read");
    let sources = format!("{contract}\n{v2}");
    let mut observed = BTreeSet::new();

    for directory in ["tests/features", "tests/features_native"] {
        for feature in feature_files(&root.join(directory)) {
            let text = fs::read_to_string(&feature).expect("feature file reads");
            assert!(
                !text.to_ascii_lowercase().contains("in-memory"),
                "{} must describe a durable profile, not volatile storage",
                feature.display()
            );
            let mut pending_tags = Vec::new();
            for (index, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with('@') {
                    pending_tags.extend(requirement_tags(trimmed).map(str::to_owned));
                    continue;
                }
                if trimmed.starts_with("Scenario:") || trimmed.starts_with("Scenario Outline:") {
                    assert!(
                        !pending_tags.is_empty(),
                        "{}:{} scenario has no @REQ source",
                        feature.display(),
                        index + 1
                    );
                    for tag in pending_tags.drain(..) {
                        let exact = tag.trim_start_matches('@');
                        assert!(
                            sources.contains(exact) || sources.contains(&wildcard_for(&tag)),
                            "{}:{} uses undocumented requirement {tag}",
                            feature.display(),
                            index + 1
                        );
                        observed.insert(tag);
                    }
                    continue;
                }
                if !trimmed.is_empty() && !trimmed.starts_with('#') {
                    pending_tags.clear();
                }
            }
        }
    }

    let externally_checked = externally_checked_requirements(root);
    let missing = catalog_requirements(&v2)
        .difference(&observed)
        .filter(|requirement| !externally_checked.contains(*requirement))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "catalog requirements lack executable acceptance scenarios: {missing:?}"
    );
}

#[test]
fn acceptance_world_has_no_volatile_database_fallback() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let world = fs::read_to_string(root.join("tests/acceptance_support/world.rs"))
        .expect("acceptance world reads");
    assert!(
        !world.contains("DbOptions::memory"),
        "acceptance setup must never substitute volatile storage"
    );
    assert!(
        world.contains("DurableLocation::Native") && world.contains("DurableLocation::ObjectStore"),
        "backend-neutral acceptance must retain real durable profiles"
    );
}
