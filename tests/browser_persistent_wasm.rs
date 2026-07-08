#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use trine_kv::{Db, DbOptions, Error};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn test_namespace(name: &str) -> String {
    let millis = js_sys::Date::now().to_string().replace('.', "-");
    let random = js_sys::Math::random().to_string().replace('.', "-");
    format!("browser-tests/{name}-{millis}-{random}")
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
