#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

#[path = "support/browser_worker.rs"]
mod browser_worker;

use wasm_bindgen::JsValue;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[wasm_bindgen::prelude::wasm_bindgen(inline_js = r#"
function errorReport(error) {
  return {
    ok: false,
    name: error && error.name ? error.name : "Error",
    message: error && error.message ? error.message : String(error),
  };
}

async function probeFile(namespace, fileName) {
  let directory = await navigator.storage.getDirectory();
  for (const segment of namespace.split("/").filter(Boolean)) {
    directory = await directory.getDirectoryHandle(segment, { create: true });
  }
  return await directory.getFileHandle(fileName, { create: true });
}

export async function runDedicatedWorkerOpfsSyncAccessProbe(namespace) {
  let handle;
  try {
    const file = await probeFile(namespace, "sync-access.bin");
    handle = await file.createSyncAccessHandle();
    const bytes = new Uint8Array([116, 114, 105, 110, 101]);
    handle.truncate(0);
    const written = handle.write(bytes, { at: 0 });
    handle.flush();
    const read = new Uint8Array(bytes.length);
    const readLen = handle.read(read, { at: 0 });
    const ok = written === bytes.length && readLen === bytes.length && read[4] === 101;
    return ok
      ? { ok: true }
      : { ok: false, name: "DataMismatch", message: "sync access handle round trip failed" };
  } catch (error) {
    return errorReport(error);
  } finally {
    if (handle) {
      handle.close();
    }
  }
}

export async function runDedicatedWorkerOpfsSyncAccessContentionProbe(namespace) {
  let first;
  let second;
  try {
    const file = await probeFile(namespace, "sync-access-contention.bin");
    first = await file.createSyncAccessHandle();
    try {
      second = await file.createSyncAccessHandle();
      return {
        ok: false,
        name: "MissingExclusiveLock",
        message: "second sync access handle opened while the first handle was live",
      };
    } catch (error) {
      const report = errorReport(error);
      return {
        ...report,
        ok: report.name === "NoModificationAllowedError" || report.name === "InvalidStateError",
      };
    }
  } catch (error) {
    return errorReport(error);
  } finally {
    if (second) {
      second.close();
    }
    if (first) {
      first.close();
    }
  }
}

export async function runDedicatedWorkerOpfsSyncAccessTimingProbe(namespace, iterations) {
  let handle;
  try {
    const file = await probeFile(namespace, "sync-access-timing.bin");
    handle = await file.createSyncAccessHandle();
    handle.truncate(0);
    const chunk = new Uint8Array(256);
    for (let index = 0; index < chunk.length; index += 1) {
      chunk[index] = index & 0xff;
    }
    const start = performance.now();
    for (let index = 0; index < iterations; index += 1) {
      handle.write(chunk, { at: index * chunk.length });
    }
    handle.flush();
    const elapsedMs = performance.now() - start;
    const tail = new Uint8Array(chunk.length);
    const readLen = handle.read(tail, { at: (iterations - 1) * chunk.length });
    return {
      ok: readLen === chunk.length && tail[255] === 255,
      iterations,
      bytes: iterations * chunk.length,
      elapsedMs,
    };
  } catch (error) {
    return errorReport(error);
  } finally {
    if (handle) {
      handle.close();
    }
  }
}
"#)]
extern "C" {
    #[wasm_bindgen::prelude::wasm_bindgen(catch, js_name = runDedicatedWorkerOpfsSyncAccessProbe)]
    async fn run_sync_access_probe(namespace: &str) -> Result<JsValue, JsValue>;

    #[wasm_bindgen::prelude::wasm_bindgen(catch, js_name = runDedicatedWorkerOpfsSyncAccessContentionProbe)]
    async fn run_sync_access_contention_probe(namespace: &str) -> Result<JsValue, JsValue>;

    #[wasm_bindgen::prelude::wasm_bindgen(catch, js_name = runDedicatedWorkerOpfsSyncAccessTimingProbe)]
    async fn run_sync_access_timing_probe(
        namespace: &str,
        iterations: usize,
    ) -> Result<JsValue, JsValue>;
}

fn assert_js_report_ok(result: &JsValue, context: &str) {
    let ok = js_sys::Reflect::get(result, &"ok".into())
        .unwrap_or(JsValue::FALSE)
        .as_bool()
        .unwrap_or(false);
    if ok {
        return;
    }

    let name = js_report_string(result, "name").unwrap_or_else(|| "Error".to_owned());
    let message = js_report_string(result, "message").unwrap_or_else(|| context.to_owned());
    panic!("{context}: {name}: {message}");
}

fn js_report_string(result: &JsValue, field: &str) -> Option<String> {
    js_sys::Reflect::get(result, &field.into())
        .ok()
        .and_then(|value| value.as_string())
}

fn js_report_f64(result: &JsValue, field: &str) -> Option<f64> {
    js_sys::Reflect::get(result, &field.into())
        .ok()
        .and_then(|value| value.as_f64())
}

#[wasm_bindgen_test]
async fn dedicated_worker_can_use_opfs_sync_access_handle() {
    let namespace = browser_worker::test_namespace("dedicated-worker-sync-probe");
    let result = run_sync_access_probe(&namespace)
        .await
        .expect("DedicatedWorker OPFS probe returns");
    assert_js_report_ok(&result, "DedicatedWorker OPFS sync access probe failed");
}

#[wasm_bindgen_test]
async fn dedicated_worker_opfs_sync_access_handle_is_exclusive() {
    let namespace = browser_worker::test_namespace("dedicated-worker-sync-contention");
    let result = run_sync_access_contention_probe(&namespace)
        .await
        .expect("DedicatedWorker OPFS contention probe returns");
    assert_js_report_ok(
        &result,
        "DedicatedWorker OPFS sync access contention probe failed",
    );
}

#[wasm_bindgen_test]
async fn dedicated_worker_opfs_sync_access_handle_reports_timing() {
    let namespace = browser_worker::test_namespace("dedicated-worker-sync-timing");
    let result = run_sync_access_timing_probe(&namespace, 64)
        .await
        .expect("DedicatedWorker OPFS timing probe returns");
    assert_js_report_ok(
        &result,
        "DedicatedWorker OPFS sync access timing probe failed",
    );
    let elapsed_ms =
        js_report_f64(&result, "elapsedMs").expect("DedicatedWorker timing report has elapsedMs");
    let bytes = js_report_f64(&result, "bytes").expect("DedicatedWorker timing report has bytes");
    assert!(
        elapsed_ms.is_finite() && elapsed_ms >= 0.0,
        "elapsedMs should be finite, got {elapsed_ms}"
    );
    assert!(
        (bytes - 16_384.0).abs() < f64::EPSILON,
        "timing report should cover 16384 bytes, got {bytes}"
    );
}

#[wasm_bindgen_test]
async fn dedicated_worker_runs_trine_db_round_trip() {
    let namespace = browser_worker::test_namespace("dedicated-worker-db");
    browser_worker::run_trine_db_round_trip(&namespace)
        .await
        .expect("DedicatedWorker Trine DB round trip succeeds through sync OPFS");
}
