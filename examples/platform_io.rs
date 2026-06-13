use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use trine_kv::{Db, DbOptions, Error, Result, RuntimeOptions};

fn main() -> Result<()> {
    let path = temp_path("trine-kv-platform-io");
    reset_dir(&path)?;
    block_on(run(&path))?;
    cleanup_dir_best_effort(&path);
    Ok(())
}

async fn run(path: &Path) -> Result<()> {
    let mut options = DbOptions::new(path);
    if cfg!(feature = "platform-io") {
        options.runtime = RuntimeOptions::platform_io();
    }
    options.background_worker_count = 0;

    let db = with_context(Db::open(options).await, "open platform_io database")?;
    with_context(
        db.put(b"platform-io:key", b"value").await,
        "write platform_io key",
    )?;
    let value = with_context(db.get(b"platform-io:key").await, "read platform_io key")?;
    assert_eq!(value, Some(b"value".to_vec()));
    with_context(db.flush().await, "flush platform_io database")?;

    let stats = db.stats();
    let platform = stats.storage_platform_io_operations.total();

    if cfg!(all(feature = "platform-io", any(unix, windows))) {
        assert!(
            stats.storage_uses_platform_io_driver,
            "platform-io runtime should select the platform driver",
        );
        assert!(
            stats.storage_uses_platform_async_io,
            "platform-io should return async storage completions",
        );
        assert!(
            platform.total() > 0,
            "platform-io should record operation-level completions",
        );
        assert!(
            platform.true_platform_async
                + platform.platform_native_async_but_partial
                + platform.thread_pool_managed_async
                > 0,
            "platform-io should classify each completed operation",
        );
    } else {
        assert!(
            !stats.storage_uses_platform_io_driver,
            "platform driver is feature and target gated",
        );
    }

    with_context(db.close().await, "close platform_io database")
}

fn with_context<T>(result: Result<T>, operation: &'static str) -> Result<T> {
    result.map_err(|error| match error {
        Error::Io(error) => Error::Io(std::io::Error::new(
            error.kind(),
            format!("{operation} failed: {error}"),
        )),
        error => error,
    })
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    let waker = Waker::from(Arc::new(ThreadWake {
        thread: thread::current(),
    }));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park_timeout(Duration::from_millis(10)),
        }
    }
}

struct ThreadWake {
    thread: thread::Thread,
}

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

fn temp_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!("{name}-{}-{nonce}", std::process::id()))
}

fn reset_dir(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(trine_kv::Error::Io(error)),
    }
    Ok(())
}

fn cleanup_dir_best_effort(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
}
