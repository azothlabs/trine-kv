use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
};

use trine_kv::{
    Db, DbOptions, DurabilityMode, KeyRange, LazyIter, Result, TransactionOptions, WriteBatch,
    WriteOptions,
};

fn main() -> Result<()> {
    let path = temp_path("trine-kv-async-quickstart");
    reset_dir(&path)?;
    block_on(run(&path))?;
    reset_dir(&path)?;
    Ok(())
}

async fn run(path: &Path) -> Result<()> {
    let mut options = DbOptions::persistent(path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;

    let db = Db::open_async(options).await?;
    let users = db.bucket_async("users").await?;

    users
        .put_with_options_async(
            b"user:001".to_vec(),
            b"Ada".to_vec(),
            WriteOptions::sync_all(),
        )
        .await?;

    let mut batch = WriteBatch::new();
    batch.put_bucket("users", b"user:002".to_vec(), b"Lin".to_vec())?;
    batch.put_bucket("users", b"team:core".to_vec(), b"database".to_vec())?;
    db.write_async(batch, WriteOptions::sync_all()).await?;

    assert_eq!(users.get_async(b"user:001").await?, Some(b"Ada".to_vec()));

    let snapshot = db.snapshot();
    users
        .put_async(b"user:003".to_vec(), b"Grace".to_vec())
        .await?;
    assert_eq!(
        users.get_at_async(&snapshot, b"user:003").await?,
        None,
        "snapshot reads stay pinned to their original sequence",
    );
    assert_eq!(users.get_async(b"user:003").await?, Some(b"Grace".to_vec()));

    let prefix_rows = collect_lazy_rows(users.prefix_lazy_async(b"user:".to_vec()).await?).await?;
    assert_eq!(
        prefix_rows,
        [
            ("user:001".to_owned(), "Ada".to_owned()),
            ("user:002".to_owned(), "Lin".to_owned()),
            ("user:003".to_owned(), "Grace".to_owned()),
        ],
    );

    let mut transaction = db.transaction(TransactionOptions::default());
    assert_eq!(
        transaction.get_bucket_async("users", b"user:001").await?,
        Some(b"Ada".to_vec()),
    );
    transaction
        .read_range_bucket_async("users", KeyRange::half_open(b"user:001", b"user:004"))
        .await?;
    transaction.put_bucket("users", b"user:004".to_vec(), b"Barbara".to_vec())?;
    transaction.commit_async().await?;

    db.flush_async().await?;
    db.persist_async(DurabilityMode::Flush).await?;
    drop(users);
    drop(snapshot);
    db.close_async().await?;

    let reopened = Db::open_async(DbOptions::persistent_read_only(path)).await?;
    let users = reopened.bucket_async("users").await?;
    assert_eq!(
        users.get_async(b"user:004").await?,
        Some(b"Barbara".to_vec())
    );

    let stats = reopened.stats();
    assert_eq!(stats.live_buckets, 2);
    assert!(stats.total_tables > 0);
    assert!(stats.storage_uses_blocking_adapter);
    assert!(!stats.storage_uses_platform_async_io);

    drop(users);
    reopened.close_async().await
}

async fn collect_lazy_rows(mut iter: LazyIter) -> Result<Vec<(String, String)>> {
    let mut rows = Vec::new();
    while let Some(item) = iter.next_async().await? {
        let value = item.value.read_async().await?;
        rows.push((display_bytes(&item.key), display_bytes(&value)));
    }
    Ok(rows)
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

fn display_bytes(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{name}-{}", std::process::id()))
}

fn reset_dir(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(trine_kv::Error::Io(error)),
    }
    Ok(())
}
