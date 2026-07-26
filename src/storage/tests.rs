use std::{
    future::Future,
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::*;
use crate::runtime::{Runtime, RuntimeOptions};

struct ThreadWaker {
    thread: thread::Thread,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

fn test_waker() -> Waker {
    Waker::from(Arc::new(ThreadWaker {
        thread: thread::current(),
    }))
}

fn block_on_test_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let waker = test_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => thread::park_timeout(Duration::from_secs(1)),
        }
    }
}

fn hold_runtime_blocking_worker(runtime: &Runtime) -> mpsc::Sender<()> {
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    runtime
        .spawn_blocking(move || {
            started_tx.send(()).expect("report blocking worker start");
            release_rx.recv().expect("wait for release");
        })
        .expect("spawn worker holder");
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("worker holder starts");
    release_tx
}

fn complete_after_blocking_worker_release<T>(
    runtime: &Runtime,
    mut future: StorageFuture<'_, T>,
    pending_message: &str,
) -> Result<T> {
    let release = hold_runtime_blocking_worker(runtime);
    let waker = test_waker();
    let mut context = Context::from_waker(&waker);
    assert!(
        matches!(future.as_mut().poll(&mut context), Poll::Pending),
        "{pending_message}"
    );

    release.send(()).expect("release blocking worker");
    block_on_test_future(future)
}

fn temp_storage_root(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after epoch")
            .as_nanos()
    ))
}

#[test]
fn backend_capability_sets_are_checked_by_the_compiler() {
    fn reads<T: StorageReadBackend>() {}
    fn appends<T: StorageAppendBackend>() {}
    fn publishes_manifests<T: StorageManifestPublishBackend>() {}
    fn writes_objects<T: StorageObjectWriteBackend>() {}

    reads::<MemoryStorageBackend>();
    writes_objects::<MemoryStorageBackend>();

    reads::<NativeFileBackend>();
    appends::<NativeFileBackend>();
    publishes_manifests::<NativeFileBackend>();
    writes_objects::<NativeFileBackend>();
}

mod memory_capabilities;
mod native_mutations;
mod native_objects;
mod platform_io;
mod runtime_adapter;
