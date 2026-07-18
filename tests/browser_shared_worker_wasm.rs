#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

#[path = "support/browser_worker.rs"]
mod browser_worker;

use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_shared_worker);

#[wasm_bindgen_test]
async fn shared_worker_runs_trine_db_round_trip() {
    let namespace = browser_worker::test_namespace("shared-worker-db");
    browser_worker::run_trine_db_round_trip(&namespace)
        .await
        .expect("SharedWorker Trine DB round trip succeeds through async OPFS");
}
