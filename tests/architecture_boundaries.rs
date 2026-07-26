use std::path::Path;

const DB_ROOT: &str = include_str!("../src/db.rs");
const DB_STORAGE: &str = include_str!("../src/db/storage_backend.rs");
const TRANSACTION_CORE: &str = include_str!("../src/transaction.rs");
const CONTENT_TRANSACTION: &str = include_str!("../src/content/transaction.rs");
const BRANCH: &str = include_str!("../src/branch.rs");
const STORAGE: &str = include_str!("../src/storage.rs");

#[test]
fn database_owns_one_backend_state() {
    let db_inner = DB_ROOT
        .split("struct DbInner {")
        .nth(1)
        .and_then(|tail| tail.split("\n}").next())
        .expect("DbInner source boundary remains discoverable");
    assert!(db_inner.contains("storage: storage_backend::DatabaseStorage"));
    for removed_field in [
        "native_storage:",
        "content_memory:",
        "object_storage:",
        "object_wal_storage:",
        "browser_storage:",
    ] {
        assert!(
            !db_inner.contains(removed_field),
            "DbInner regained backend-specific optional state: {removed_field}"
        );
    }
    assert!(DB_STORAGE.contains("enum DatabaseStorage"));
}

#[test]
fn transaction_core_is_content_agnostic() {
    for forbidden_dependency in ["Content", "CONTENT_", "content_"] {
        assert!(
            !TRANSACTION_CORE.contains(forbidden_dependency),
            "transaction core regained content dependency: {forbidden_dependency}"
        );
    }
    assert!(CONTENT_TRANSACTION.contains("impl Transaction"));
    assert!(TRANSACTION_CORE.contains("extension_claims:"));
}

#[test]
fn branch_primary_range_keeps_async_storage_reads() {
    assert!(BRANCH.contains("pub async fn range("));
    assert!(BRANCH.contains("Result<AsyncBranchRange>"));
    assert!(BRANCH.contains("rows.next().await"));
    assert!(BRANCH.contains("pub fn range_sync("));
    assert!(BRANCH.contains("Result<BranchRange>"));
}

#[test]
fn broad_storage_god_interface_does_not_return() {
    assert!(!STORAGE.contains("trait StorageBackend"));
    assert!(STORAGE.contains("trait StorageReadBackend"));
    assert!(STORAGE.contains("trait StorageAppendBackend"));
    assert!(STORAGE.contains("trait StorageManifestPublishBackend"));
}

#[test]
fn responsibility_split_remains_visible_in_the_file_tree() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for required in [
        "src/db/open_helpers/options.rs",
        "src/db/open_helpers/open_recovery.rs",
        "src/db/open_helpers/blob_gc.rs",
        "src/db/open_helpers/cleanup.rs",
        "src/io/core.rs",
        "src/io/platform/driver.rs",
        "src/io/platform/scheduler.rs",
        "src/storage/browser.rs",
        "src/content/tests/upload.rs",
        "src/content/tests/reclaim.rs",
    ] {
        assert!(
            root.join(required).is_file(),
            "missing boundary file {required}"
        );
    }
    for retired in [
        "src/db/sync_api.rs",
        "src/db/sync_api",
        "src/db/open_helpers.rs",
        "src/content/tests.rs",
        "src/io.rs",
    ] {
        assert!(
            !root.join(retired).exists(),
            "retired mixed-responsibility path returned: {retired}"
        );
    }
}
