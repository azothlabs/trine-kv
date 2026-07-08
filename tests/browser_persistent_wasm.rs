#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use std::io;

use opfs::{
    CreateWritableOptions, DirectoryHandle as _, FileHandle as _, GetDirectoryHandleOptions,
    GetFileHandleOptions, WritableFileStream as _, persistent,
};
use trine_kv::{BucketOptions, Db, DbOptions, Error, KeyRange};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

const OVERSIZED_MANIFEST_BYTES: usize = 65 * 1024 * 1024;

fn test_namespace(name: &str) -> String {
    let millis = js_sys::Date::now().to_string().replace('.', "-");
    let random = js_sys::Math::random().to_string().replace('.', "-");
    format!("browser-tests/{name}-{millis}-{random}")
}

async fn write_opfs_file(namespace: &str, file_name: &str, bytes: &[u8]) {
    let mut directory = persistent::app_specific_dir()
        .await
        .expect("browser OPFS root opens");
    for segment in namespace.split('/').filter(|segment| !segment.is_empty()) {
        let options = GetDirectoryHandleOptions { create: true };
        directory = directory
            .get_directory_handle_with_options(segment, &options)
            .await
            .expect("browser OPFS namespace directory opens");
    }

    let options = GetFileHandleOptions { create: true };
    let mut file = directory
        .get_file_handle_with_options(file_name, &options)
        .await
        .expect("browser OPFS file opens");
    let write_options = CreateWritableOptions {
        keep_existing_data: false,
    };
    let mut stream = file
        .create_writable_with_options(&write_options)
        .await
        .expect("browser OPFS writable stream opens");
    stream
        .write_at_cursor_pos(bytes)
        .await
        .expect("browser OPFS file writes");
    stream.close().await.expect("browser OPFS file closes");
}

#[wasm_bindgen_test]
async fn browser_persistent_reopens_unflushed_wal_in_namespace() {
    let namespace = test_namespace("wal-reopen");
    let db = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("browser database opens");

    db.put(b"user:001", b"Ada")
        .await
        .expect("browser WAL-backed write succeeds");
    drop(db);

    let db = Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect("browser read-only database reopens");
    assert_eq!(
        db.get(b"user:001").await.expect("browser read succeeds"),
        Some(b"Ada".to_vec())
    );
}

#[wasm_bindgen_test]
async fn browser_persistent_reopens_after_flush() {
    let namespace = test_namespace("flush-reopen");
    let db = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("browser database opens");

    db.put(b"user:002", b"Grace")
        .await
        .expect("browser write succeeds");
    db.flush().await.expect("browser flush succeeds");
    drop(db);

    let db = Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect("browser read-only database reopens after flush");
    assert_eq!(
        db.get(b"user:002").await.expect("browser read succeeds"),
        Some(b"Grace".to_vec())
    );
}

#[wasm_bindgen_test]
async fn browser_persistent_reopens_after_manual_compaction() {
    let namespace = test_namespace("compact-reopen");
    let db = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("browser database opens");

    for index in 0..32_u8 {
        db.put(vec![b'k', index], vec![b'a', index])
            .await
            .expect("first generation writes");
    }
    db.flush().await.expect("first browser flush succeeds");

    for index in 0..32_u8 {
        db.put(vec![b'k', index], vec![b'b', index])
            .await
            .expect("second generation writes");
    }
    db.flush().await.expect("second browser flush succeeds");
    db.compact_range(KeyRange::all())
        .await
        .expect("browser compaction succeeds");
    drop(db);

    let db = Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect("browser read-only database reopens after compaction");
    assert_eq!(
        db.get(&[b'k', 7]).await.expect("browser read succeeds"),
        Some(vec![b'b', 7])
    );
}

#[wasm_bindgen_test]
async fn browser_persistent_reopens_blob_backed_values() {
    let namespace = test_namespace("blob-reopen");
    let mut options = DbOptions::browser_persistent_at(&namespace);
    options.default_bucket_options = BucketOptions::default().with_blob_threshold_bytes(4);
    let db = Db::open(options)
        .await
        .expect("browser database opens with blob threshold");
    let value = b"value-stored-through-browser-blob".to_vec();

    db.put(b"blob-key", value.clone())
        .await
        .expect("blob-backed browser write succeeds");
    db.flush().await.expect("browser blob flush succeeds");
    drop(db);

    let db = Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect("browser read-only database reopens with blob");
    assert_eq!(
        db.get(b"blob-key")
            .await
            .expect("browser blob read succeeds"),
        Some(value)
    );
}

#[wasm_bindgen_test]
async fn browser_persistent_bucket_create_drop_reopens_without_bucket() {
    let namespace = test_namespace("bucket-drop");
    let db = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("browser database opens");
    let bucket = db
        .bucket_with_options("scratch", BucketOptions::default())
        .await
        .expect("browser bucket creates");
    bucket
        .put(b"k", b"scratch-value")
        .await
        .expect("browser bucket write succeeds");
    db.flush().await.expect("browser bucket flush succeeds");
    drop(bucket);

    db.drop_bucket("scratch")
        .await
        .expect("browser bucket drop succeeds");
    drop(db);

    let db = Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect("browser read-only database reopens after bucket drop");
    let error = db
        .bucket("scratch")
        .await
        .expect_err("dropped browser bucket should not reopen read-only");
    assert!(matches!(error, Error::ReadOnly), "{error}");
}

#[wasm_bindgen_test]
async fn browser_persistent_namespaces_are_isolated() {
    let namespace_a = test_namespace("namespace-a");
    let namespace_b = test_namespace("namespace-b");

    let db_a = Db::open(DbOptions::browser_persistent_at(&namespace_a))
        .await
        .expect("first browser database opens");
    db_a.put(b"k", b"from-a")
        .await
        .expect("first namespace write succeeds");
    drop(db_a);

    let db_b = Db::open(DbOptions::browser_persistent_at(&namespace_b))
        .await
        .expect("second browser database opens");
    assert_eq!(
        db_b.get(b"k")
            .await
            .expect("second namespace read succeeds"),
        None
    );
}

#[wasm_bindgen_test]
async fn browser_persistent_web_locks_reject_second_writer() {
    let namespace = test_namespace("writer-lease");
    let first = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("first browser writer opens");

    let error = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect_err("second browser writer should be rejected");
    assert!(matches!(error, Error::RuntimeBusy { .. }), "{error}");

    drop(first);

    Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("writer lease is released after first handle drops");
}

#[wasm_bindgen_test]
async fn browser_persistent_path_aliases_share_writer_lease() {
    let namespace = test_namespace("writer-lease-alias");
    let first = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("first browser writer opens");
    first
        .put(b"k", b"via-relative-path")
        .await
        .expect("write through relative path succeeds");

    let alias = format!("/{namespace}");
    let error = Db::open(DbOptions::browser_persistent_at(&alias))
        .await
        .expect_err("absolute browser namespace alias should share writer lease");
    assert!(matches!(error, Error::RuntimeBusy { .. }), "{error}");
    drop(first);

    let db = Db::open(DbOptions::browser_persistent_read_only_at(&alias))
        .await
        .expect("read-only alias reopens same browser namespace");
    assert_eq!(
        db.get(b"k").await.expect("alias read succeeds"),
        Some(b"via-relative-path".to_vec())
    );
}

#[wasm_bindgen_test]
async fn browser_persistent_read_only_missing_namespace_fails() {
    let namespace = test_namespace("missing-read-only");
    let error = Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect_err("missing browser read-only namespace should fail");
    match error {
        Error::Io(error) => assert_eq!(error.kind(), io::ErrorKind::NotFound),
        other => panic!("expected NotFound for missing browser namespace, got {other}"),
    }
}

#[wasm_bindgen_test]
async fn browser_persistent_rejects_oversized_manifest_before_decode() {
    let namespace = test_namespace("oversized-manifest");
    let oversized = vec![0_u8; OVERSIZED_MANIFEST_BYTES];
    write_opfs_file(&namespace, "MANIFEST", &oversized).await;

    let error = Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect_err("oversized browser manifest should be rejected");
    assert!(
        matches!(error, Error::Corruption { ref message } if message.contains("exceeds maximum")),
        "{error}"
    );
}
