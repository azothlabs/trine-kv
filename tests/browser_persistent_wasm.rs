#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use std::io;

use opfs::{
    CreateWritableOptions, DirectoryHandle as _, FileHandle as _, GetDirectoryHandleOptions,
    GetFileHandleOptions, WritableFileStream as _, persistent::DirectoryHandle,
};
use trine_kv::{
    BucketOptions, Db, DbOptions, Error, FailOnCorruptionPolicy, KeyRange,
    browser::{browser_persistent_storage_granted, browser_storage_estimate},
};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::FileSystemDirectoryHandle;

wasm_bindgen_test_configure!(run_in_browser);

const OVERSIZED_MANIFEST_BYTES: usize = 65 * 1024 * 1024;
const WAL_REWRITE_TMP_FILE_NAME: &str = "trine.wal.tmp";

#[wasm_bindgen::prelude::wasm_bindgen(inline_js = r#"
export async function runSharedWorkerOpfsSyncAccessProbe() {
  return await new Promise((resolve) => {
    if (typeof SharedWorker === "undefined") {
      resolve({ ok: false, name: "NotSupportedError", message: "SharedWorker is unavailable" });
      return;
    }

    const source = `
      self.onconnect = (event) => {
        const port = event.ports[0];
        port.onmessage = async () => {
          try {
            const root = await navigator.storage.getDirectory();
            const dir = await root.getDirectoryHandle("trine-kv-worker-probe", { create: true });
            const file = await dir.getFileHandle("sync-access.bin", { create: true });
            const handle = await file.createSyncAccessHandle();
            const bytes = new Uint8Array([116, 114, 105, 110, 101]);
            handle.truncate(0);
            const written = handle.write(bytes, { at: 0 });
            handle.flush();
            const read = new Uint8Array(bytes.length);
            const readLen = handle.read(read, { at: 0 });
            handle.close();
            const ok = written === bytes.length && readLen === bytes.length && read[4] === 101;
            port.postMessage(ok
              ? { ok: true }
              : { ok: false, name: "DataMismatch", message: "sync access handle round trip failed" });
          } catch (error) {
            port.postMessage({
              ok: false,
              name: error && error.name ? error.name : "Error",
              message: error && error.message ? error.message : String(error),
            });
          }
        };
      };
    `;
    const url = URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
    const worker = new SharedWorker(url);
    const timeout = setTimeout(() => {
      worker.port.close();
      URL.revokeObjectURL(url);
      resolve({ ok: false, name: "Timeout", message: "SharedWorker OPFS probe timed out" });
    }, 5000);
    worker.port.onmessage = (event) => {
      clearTimeout(timeout);
      worker.port.close();
      URL.revokeObjectURL(url);
      resolve(event.data);
    };
    worker.port.start();
    worker.port.postMessage({});
  });
}

function errorReport(error) {
  return {
    ok: false,
    name: error && error.name ? error.name : "Error",
    message: error && error.message ? error.message : String(error),
  };
}

function runSharedWorkerProbe(source, request, timeoutMessage) {
  return new Promise((resolve) => {
    if (typeof SharedWorker === "undefined") {
      resolve({ ok: false, name: "NotSupportedError", message: "SharedWorker is unavailable" });
      return;
    }

    const url = URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
    let worker;
    let settled = false;
    const settle = (result) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      try {
        if (worker) {
          worker.port.close();
        }
      } finally {
        URL.revokeObjectURL(url);
      }
      resolve(result);
    };
    const timeout = setTimeout(() => {
      settle({ ok: false, name: "Timeout", message: timeoutMessage });
    }, 10000);

    try {
      worker = new SharedWorker(url);
      worker.port.onmessage = (event) => settle(event.data);
      worker.port.onmessageerror = (event) => {
        settle({ ok: false, name: "MessageError", message: String(event.data) });
      };
      worker.port.start();
      worker.port.postMessage(request);
    } catch (error) {
      settle(errorReport(error));
    }
  });
}

export async function runSharedWorkerOpfsSyncAccessContentionProbe() {
  const source = `
    self.onconnect = (event) => {
      const port = event.ports[0];
      port.onmessage = async () => {
        let first;
        let second;
        try {
          const root = await navigator.storage.getDirectory();
          const dir = await root.getDirectoryHandle("trine-kv-worker-probe", { create: true });
          const file = await dir.getFileHandle("sync-access-contention.bin", { create: true });
          first = await file.createSyncAccessHandle();
          try {
            second = await file.createSyncAccessHandle();
            port.postMessage({
              ok: false,
              name: "MissingExclusiveLock",
              message: "second sync access handle opened while the first handle was live",
            });
          } catch (error) {
            port.postMessage({
              ok: true,
              name: error && error.name ? error.name : "Error",
              message: error && error.message ? error.message : String(error),
            });
          }
        } catch (error) {
          port.postMessage({
            ok: false,
            name: error && error.name ? error.name : "Error",
            message: error && error.message ? error.message : String(error),
          });
        } finally {
          if (second) {
            second.close();
          }
          if (first) {
            first.close();
          }
        }
      };
    };
  `;
  return await runSharedWorkerProbe(
    source,
    {},
    "SharedWorker OPFS sync access contention probe timed out",
  );
}

export async function runSharedWorkerOpfsSyncAccessTimingProbe(iterations) {
  const source = `
    self.onconnect = (event) => {
      const port = event.ports[0];
      port.onmessage = async (message) => {
        let handle;
        try {
          const iterations = message.data.iterations;
          const chunk = new Uint8Array(256);
          for (let index = 0; index < chunk.length; index += 1) {
            chunk[index] = index & 0xff;
          }
          const root = await navigator.storage.getDirectory();
          const dir = await root.getDirectoryHandle("trine-kv-worker-probe", { create: true });
          const file = await dir.getFileHandle("sync-access-timing.bin", { create: true });
          handle = await file.createSyncAccessHandle();
          handle.truncate(0);
          const start = performance.now();
          for (let index = 0; index < iterations; index += 1) {
            handle.write(chunk, { at: index * chunk.length });
          }
          handle.flush();
          const elapsedMs = performance.now() - start;
          const tail = new Uint8Array(chunk.length);
          const readLen = handle.read(tail, { at: (iterations - 1) * chunk.length });
          port.postMessage({
            ok: readLen === chunk.length && tail[255] === 255,
            iterations,
            bytes: iterations * chunk.length,
            elapsedMs,
          });
        } catch (error) {
          port.postMessage({
            ok: false,
            name: error && error.name ? error.name : "Error",
            message: error && error.message ? error.message : String(error),
          });
        } finally {
          if (handle) {
            handle.close();
          }
        }
      };
    };
  `;
  return await runSharedWorkerProbe(
    source,
    { iterations },
    "SharedWorker OPFS sync access timing probe timed out",
  );
}

export async function runWorkerTrineDbRoundTrip(kind, namespace) {
  return await new Promise((resolve) => {
    const moduleUrl = import.meta.url;
    const source = `
      import init, { workerTrineDbRoundTrip } from ${JSON.stringify(moduleUrl)};

      async function run(namespace) {
        try {
          await init();
          return await workerTrineDbRoundTrip(namespace);
        } catch (error) {
          return {
            ok: false,
            name: error && error.name ? error.name : "Error",
            message: error && error.message ? error.message : String(error),
          };
        }
      }

      if (typeof SharedWorkerGlobalScope !== "undefined" && self instanceof SharedWorkerGlobalScope) {
        self.onconnect = (event) => {
          const port = event.ports[0];
          port.onmessage = async (message) => {
            port.postMessage(await run(message.data.namespace));
          };
          port.start();
        };
      } else {
        self.onmessage = async (message) => {
          self.postMessage(await run(message.data.namespace));
        };
      }
    `;

    const url = URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
    let worker;
    let settled = false;
    const settle = (result) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timeout);
      try {
        if (worker && kind === "dedicated") {
          worker.terminate();
        } else if (worker && worker.port) {
          worker.port.close();
        }
      } finally {
        URL.revokeObjectURL(url);
      }
      resolve(result);
    };
    const timeout = setTimeout(() => {
      settle({ ok: false, name: "Timeout", message: kind + " Worker Trine DB round trip timed out" });
    }, 15000);

    try {
      if (kind === "shared") {
        if (typeof SharedWorker === "undefined") {
          settle({ ok: false, name: "NotSupportedError", message: "SharedWorker is unavailable" });
          return;
        }
        worker = new SharedWorker(url, { type: "module" });
        worker.port.onmessage = (event) => settle(event.data);
        worker.port.onmessageerror = (event) => {
          settle({ ok: false, name: "MessageError", message: String(event.data) });
        };
        worker.port.start();
        worker.port.postMessage({ namespace });
      } else {
        worker = new Worker(url, { type: "module" });
        worker.onmessage = (event) => settle(event.data);
        worker.onerror = (event) => {
          settle({ ok: false, name: "WorkerError", message: event.message || "Dedicated Worker failed" });
        };
        worker.postMessage({ namespace });
      }
    } catch (error) {
      settle(errorReport(error));
    }
  });
}
"#)]
extern "C" {
    #[wasm_bindgen::prelude::wasm_bindgen(catch, js_name = runSharedWorkerOpfsSyncAccessProbe)]
    async fn run_shared_worker_opfs_sync_access_probe() -> Result<JsValue, JsValue>;

    #[wasm_bindgen::prelude::wasm_bindgen(catch, js_name = runSharedWorkerOpfsSyncAccessContentionProbe)]
    async fn run_shared_worker_opfs_sync_access_contention_probe() -> Result<JsValue, JsValue>;

    #[wasm_bindgen::prelude::wasm_bindgen(catch, js_name = runSharedWorkerOpfsSyncAccessTimingProbe)]
    async fn run_shared_worker_opfs_sync_access_timing_probe(
        iterations: usize,
    ) -> Result<JsValue, JsValue>;

    #[wasm_bindgen::prelude::wasm_bindgen(catch, js_name = runWorkerTrineDbRoundTrip)]
    async fn run_worker_trine_db_round_trip(
        kind: &str,
        namespace: &str,
    ) -> Result<JsValue, JsValue>;
}

fn test_namespace(name: &str) -> String {
    let millis = js_sys::Date::now().to_string().replace('.', "-");
    let random = js_sys::Math::random().to_string().replace('.', "-");
    format!("browser-tests/{name}-{millis}-{random}")
}

async fn write_opfs_file(namespace: &str, file_name: &str, bytes: &[u8]) {
    let mut directory = test_opfs_root().await;
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

async fn test_opfs_root() -> DirectoryHandle {
    let navigator = js_sys::Reflect::get(&js_sys::global(), &"navigator".into())
        .expect("browser global navigator is readable");
    let storage = js_sys::Reflect::get(&navigator, &"storage".into())
        .expect("browser storage manager is readable");
    let get_directory = js_sys::Reflect::get(&storage, &"getDirectory".into())
        .expect("browser OPFS root function is readable")
        .dyn_into::<js_sys::Function>()
        .expect("browser OPFS root getter is a function");
    let promise = get_directory
        .call0(&storage)
        .expect("browser OPFS root request starts")
        .dyn_into::<js_sys::Promise>()
        .expect("browser OPFS root request returns a promise");
    let root = JsFuture::from(promise)
        .await
        .expect("browser OPFS root request resolves")
        .dyn_into::<FileSystemDirectoryHandle>()
        .expect("browser OPFS root is a directory handle");
    DirectoryHandle::from(root)
}

#[wasm_bindgen::prelude::wasm_bindgen(js_name = workerTrineDbRoundTrip)]
pub async fn worker_trine_db_round_trip(namespace: String) -> JsValue {
    match worker_trine_db_round_trip_inner(&namespace).await {
        Ok(()) => js_report_ok(),
        Err(message) => js_report_error("TrineWorkerRoundTripError", &message),
    }
}

async fn worker_trine_db_round_trip_inner(namespace: &str) -> std::result::Result<(), String> {
    let mut options = DbOptions::browser_persistent_at(namespace);
    options.default_bucket_options = BucketOptions::default().with_blob_threshold_bytes(4);
    let db = Db::open(options).await.map_err(display_error)?;

    db.put(b"worker:wal", b"first")
        .await
        .map_err(display_error)?;
    db.put(b"worker:deleted", b"gone")
        .await
        .map_err(display_error)?;
    db.delete(b"worker:deleted").await.map_err(display_error)?;

    for index in 0_u8..64 {
        let key = format!("worker:append:{index:03}");
        db.put(key.into_bytes(), vec![index; 128])
            .await
            .map_err(display_error)?;
    }

    let docs = db
        .bucket_with_options(
            "worker-docs",
            BucketOptions::default().with_blob_threshold_bytes(4),
        )
        .await
        .map_err(display_error)?;
    let blob_value = b"value-stored-through-worker-sync-access-handle".to_vec();
    docs.put(b"doc:blob", blob_value.clone())
        .await
        .map_err(display_error)?;

    db.flush().await.map_err(display_error)?;
    db.put(b"worker:after-flush", b"tail")
        .await
        .map_err(display_error)?;
    db.flush().await.map_err(display_error)?;
    db.compact_range(KeyRange::all())
        .await
        .map_err(display_error)?;
    drop(docs);
    drop(db);

    let db = Db::open(DbOptions::browser_persistent_read_only_at(namespace))
        .await
        .map_err(display_error)?;
    expect_worker_value(
        db.get(b"worker:wal").await.map_err(display_error)?,
        b"first",
        "worker:wal",
    )?;
    expect_worker_value(
        db.get(b"worker:append:063").await.map_err(display_error)?,
        &[63_u8; 128],
        "worker:append:063",
    )?;
    expect_worker_none(
        db.get(b"worker:deleted").await.map_err(display_error)?,
        "worker:deleted",
    )?;
    expect_worker_value(
        db.get(b"worker:after-flush").await.map_err(display_error)?,
        b"tail",
        "worker:after-flush",
    )?;

    let docs = db.bucket("worker-docs").await.map_err(display_error)?;
    expect_worker_value(
        docs.get(b"doc:blob").await.map_err(display_error)?,
        &blob_value,
        "worker-docs/doc:blob",
    )?;
    Ok(())
}

fn expect_worker_value(
    actual: Option<Vec<u8>>,
    expected: &[u8],
    label: &str,
) -> std::result::Result<(), String> {
    match actual {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => Err(format!(
            "{label} mismatch: expected {} bytes, read {} bytes",
            expected.len(),
            actual.len()
        )),
        None => Err(format!("{label} was missing")),
    }
}

fn expect_worker_none(actual: Option<Vec<u8>>, label: &str) -> std::result::Result<(), String> {
    match actual {
        None => Ok(()),
        Some(actual) => Err(format!(
            "{label} should have been deleted, read {} bytes",
            actual.len()
        )),
    }
}

fn display_error(error: Error) -> String {
    let message = error.to_string();
    drop(error);
    message
}

fn js_report_ok() -> JsValue {
    let report = js_sys::Object::new();
    js_sys::Reflect::set(&report, &"ok".into(), &true.into()).expect("worker report ok field sets");
    report.into()
}

fn js_report_error(name: &str, message: &str) -> JsValue {
    let report = js_sys::Object::new();
    js_sys::Reflect::set(&report, &"ok".into(), &false.into())
        .expect("worker report ok field sets");
    js_sys::Reflect::set(&report, &"name".into(), &name.into())
        .expect("worker report name field sets");
    js_sys::Reflect::set(&report, &"message".into(), &message.into())
        .expect("worker report message field sets");
    report.into()
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
async fn shared_worker_can_use_opfs_sync_access_handle() {
    let result = run_shared_worker_opfs_sync_access_probe()
        .await
        .expect("SharedWorker OPFS probe returns");
    assert_js_report_ok(&result, "SharedWorker OPFS sync access probe failed");
}

#[wasm_bindgen_test]
async fn shared_worker_opfs_sync_access_handle_is_exclusive() {
    let result = run_shared_worker_opfs_sync_access_contention_probe()
        .await
        .expect("SharedWorker OPFS contention probe returns");
    assert_js_report_ok(
        &result,
        "SharedWorker OPFS sync access contention probe failed",
    );
}

#[wasm_bindgen_test]
async fn shared_worker_opfs_sync_access_handle_reports_timing() {
    let result = run_shared_worker_opfs_sync_access_timing_probe(64)
        .await
        .expect("SharedWorker OPFS timing probe returns");
    assert_js_report_ok(&result, "SharedWorker OPFS sync access timing probe failed");
    let elapsed_ms =
        js_report_f64(&result, "elapsedMs").expect("SharedWorker timing report has elapsedMs");
    let bytes = js_report_f64(&result, "bytes").expect("SharedWorker timing report has bytes");
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
    let namespace = test_namespace("dedicated-worker-db");
    let result = run_worker_trine_db_round_trip("dedicated", &namespace)
        .await
        .expect("Dedicated Worker Trine DB round trip returns");
    assert_js_report_ok(&result, "Dedicated Worker Trine DB round trip failed");
}

#[wasm_bindgen_test]
async fn shared_worker_runs_trine_db_round_trip() {
    let namespace = test_namespace("shared-worker-db");
    let result = run_worker_trine_db_round_trip("shared", &namespace)
        .await
        .expect("SharedWorker Trine DB round trip returns");
    assert_js_report_ok(&result, "SharedWorker Trine DB round trip failed");
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
async fn browser_persistent_reopens_many_unflushed_wal_appends() {
    let namespace = test_namespace("many-wal-appends");
    let db = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("browser database opens");

    for index in 0_u8..192 {
        let key = format!("append-key-{index:03}");
        let value = vec![index; 256];
        db.put(key.into_bytes(), value)
            .await
            .expect("browser WAL append succeeds");
    }
    drop(db);

    let db = Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect("browser read-only database reopens after many WAL appends");
    assert_eq!(
        db.get(b"append-key-191")
            .await
            .expect("browser read succeeds"),
        Some(vec![191_u8; 256])
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
async fn browser_persistent_rejects_leftover_wal_rewrite_temp_by_default() {
    let namespace = test_namespace("wal-temp-fail-closed");
    let db = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("browser database opens");
    db.put(b"k", b"v")
        .await
        .expect("browser write succeeds before temp injection");
    drop(db);

    write_opfs_file(
        &namespace,
        WAL_REWRITE_TMP_FILE_NAME,
        b"partial-wal-rewrite",
    )
    .await;

    let error = Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect_err("safe browser WAL temp should fail closed by default");
    assert!(
        matches!(error, Error::Corruption { ref message } if message.contains("safe temporary")),
        "{error}"
    );
}

#[wasm_bindgen_test]
async fn browser_persistent_repairs_leftover_wal_rewrite_temp_when_requested() {
    let namespace = test_namespace("wal-temp-repair");
    let db = Db::open(DbOptions::browser_persistent_at(&namespace))
        .await
        .expect("browser database opens");
    db.put(b"k", b"survives-temp")
        .await
        .expect("browser write succeeds before temp injection");
    drop(db);

    write_opfs_file(
        &namespace,
        WAL_REWRITE_TMP_FILE_NAME,
        b"partial-wal-rewrite",
    )
    .await;

    let mut options = DbOptions::browser_persistent_at(&namespace);
    options.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;
    let db = Db::open(options)
        .await
        .expect("browser database repairs safe WAL temp");
    assert_eq!(
        db.get(b"k").await.expect("browser read succeeds"),
        Some(b"survives-temp".to_vec())
    );
    drop(db);

    Db::open(DbOptions::browser_persistent_read_only_at(&namespace))
        .await
        .expect("browser read-only reopen confirms WAL temp was removed");
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
async fn browser_storage_manager_reports_estimate_and_persisted_status() {
    let estimate = browser_storage_estimate()
        .await
        .expect("browser storage estimate succeeds");
    if let (Some(usage), Some(quota)) = (estimate.usage_bytes, estimate.quota_bytes) {
        assert!(quota >= usage, "quota {quota} should cover usage {usage}");
    }

    let _persisted = browser_persistent_storage_granted()
        .await
        .expect("browser persisted status succeeds");
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
