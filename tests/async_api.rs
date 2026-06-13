use std::{
    fs,
    future::Future,
    path::PathBuf,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use trine_kv::{
    BucketOptions, Db, DbOptions, DurabilityMode, Error, Iter, KeyRange, KeyValue, LazyIter,
    ReadVersion, RuntimeOptions, TransactionOptions, WriteBatch, WriteOptions,
};

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

fn current_thread_waker() -> Waker {
    Waker::from(Arc::new(ThreadWake {
        thread: thread::current(),
    }))
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    let waker = current_thread_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => {
                assert!(
                    Instant::now() < deadline,
                    "async compatibility future did not complete"
                );
                thread::park_timeout(Duration::from_millis(10));
            }
        }
    }
}

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("condition did not become true before timeout");
}

fn temp_db_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "trine-kv-async-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn cleanup_dir(path: &PathBuf) {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to remove {}: {error}", path.display()),
    }
}

fn collect_async(mut iter: Iter) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    while let Some(KeyValue { key, value }) =
        block_on(iter.next()).expect("async iterator item is readable")
    {
        rows.push((key, value));
    }
    rows
}

fn collect_lazy_async(mut iter: LazyIter) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    while let Some(item) = block_on(iter.next()).expect("async lazy iterator item is readable") {
        let value = block_on(item.value.read()).expect("lazy value reads");
        rows.push((item.key, value));
    }
    rows
}

#[test]
fn memory_compatibility_surface_smoke() {
    let db = block_on(Db::open(DbOptions::memory())).expect("memory db opens");

    block_on(db.put(b"a".to_vec(), b"one".to_vec())).expect("put through async API");
    assert_eq!(
        block_on(db.get(b"a")).expect("get through async API"),
        Some(b"one".to_vec())
    );

    let mut batch = WriteBatch::new();
    batch.put(b"b".to_vec(), b"two".to_vec());
    let commit = block_on(db.write(batch, WriteOptions::default())).expect("batch writes");
    assert_eq!(commit.read_version(), db.latest_read_version());

    let default_rows = collect_async(block_on(db.prefix(b"b".to_vec())).expect("prefix opens"));
    assert_eq!(default_rows, vec![(b"b".to_vec(), b"two".to_vec())]);
    assert_eq!(
        collect_lazy_async(block_on(db.prefix_lazy(b"b".to_vec())).expect("prefix opens")),
        vec![(b"b".to_vec(), b"two".to_vec())]
    );

    let events = block_on(db.bucket("events")).expect("bucket opens");
    block_on(events.put(b"e1".to_vec(), b"event".to_vec())).expect("bucket put through async API");
    assert_eq!(
        block_on(events.get(b"e1")).expect("bucket get through async API"),
        Some(b"event".to_vec())
    );
    assert_eq!(
        collect_async(block_on(events.range(&KeyRange::all())).expect("range opens")),
        vec![(b"e1".to_vec(), b"event".to_vec())]
    );
    let mut lazy_events = block_on(events.range_lazy(&KeyRange::all())).expect("lazy range opens");
    let lazy_event = block_on(lazy_events.next())
        .expect("lazy event advances")
        .expect("lazy event exists");
    let lazy_event =
        block_on(lazy_event.into_key_value()).expect("lazy event converts into owned key/value");
    assert_eq!(lazy_event.key, b"e1".to_vec());
    assert_eq!(lazy_event.value, b"event".to_vec());
    assert!(
        block_on(lazy_events.next())
            .expect("lazy event iterator finishes")
            .is_none()
    );

    let mut txn = db.transaction(TransactionOptions::default());
    assert_eq!(
        block_on(txn.get(b"b")).expect("transaction async point read"),
        Some(b"two".to_vec())
    );
    block_on(txn.read_range(KeyRange::all())).expect("transaction async range read");
    txn.put(b"c".to_vec(), b"three".to_vec());
    let txn_commit = block_on(txn.commit()).expect("transaction async commit");
    assert_eq!(txn_commit.read_version(), db.latest_read_version());
    assert_eq!(
        block_on(db.get(b"c")).expect("transaction write reads"),
        Some(b"three".to_vec())
    );

    let mut named_txn = db.transaction(TransactionOptions::default());
    assert_eq!(
        block_on(named_txn.get_bucket("events", b"e1"))
            .expect("named transaction async point read"),
        Some(b"event".to_vec())
    );
    named_txn
        .put_bucket("events", b"e2".to_vec(), b"event-two".to_vec())
        .expect("named transaction stages write");
    block_on(named_txn.commit()).expect("named transaction async commit");
    assert_eq!(
        block_on(events.get(b"e2")).expect("named transaction write reads"),
        Some(b"event-two".to_vec())
    );

    block_on(db.delete(b"a".to_vec())).expect("delete through async API");
    assert_eq!(block_on(db.get(b"a")).expect("deleted key reads"), None);

    block_on(db.persist(DurabilityMode::Buffered)).expect("memory persist is accepted");
    block_on(db.flush()).expect("memory flush is accepted");
    block_on(db.compact_range(KeyRange::all())).expect("memory compact is accepted");
    block_on(db.close()).expect("async close succeeds");
}

#[test]
fn persistent_async_range_and_prefix_advance_flushed_tables() {
    let path = temp_db_path("cursor-advance");
    let mut options = DbOptions::persistent(&path);
    options.default_bucket_options = BucketOptions {
        block_bytes: 128,
        ..BucketOptions::default()
    };
    let db = block_on(Db::open(options)).expect("persistent db opens");
    block_on(db.put(b"tenant:01:key-000".to_vec(), b"one".to_vec()))
        .expect("first async put succeeds");
    block_on(db.put(b"tenant:01:key-001".to_vec(), b"two".to_vec()))
        .expect("second async put succeeds");
    block_on(db.put(b"tenant:02:key-000".to_vec(), b"three".to_vec()))
        .expect("third async put succeeds");
    block_on(db.put(b"zeta".to_vec(), b"four".to_vec())).expect("fourth async put succeeds");
    block_on(db.flush()).expect("flush writes table files");

    let tenant_one = KeyRange::half_open(b"tenant:01:".to_vec(), b"tenant:02:".to_vec());
    let range_rows = collect_async(block_on(db.range(&tenant_one)).expect("async range opens"));
    assert_eq!(
        range_rows,
        vec![
            (b"tenant:01:key-000".to_vec(), b"one".to_vec()),
            (b"tenant:01:key-001".to_vec(), b"two".to_vec()),
        ]
    );

    let prefix_rows = collect_lazy_async(
        block_on(db.prefix_lazy(b"tenant:01:".to_vec())).expect("async lazy prefix opens"),
    );
    assert_eq!(prefix_rows, range_rows);

    let reverse_rows = collect_lazy_async(
        block_on(db.prefix_lazy_reverse(b"tenant:01:".to_vec()))
            .expect("async reverse lazy prefix opens"),
    );
    assert_eq!(
        reverse_rows,
        vec![
            (b"tenant:01:key-001".to_vec(), b"two".to_vec()),
            (b"tenant:01:key-000".to_vec(), b"one".to_vec()),
        ]
    );

    let stats = db.stats();
    assert!(
        stats.total_tables > 0,
        "async cursor coverage should advance over flushed table files"
    );
    assert!(stats.storage_uses_sync_adapter);
    assert!(!stats.storage_uses_platform_io_driver);
    assert!(!stats.storage_uses_platform_async_io);
    assert!(
        stats.storage_sync_adapter_tasks > 0,
        "async table reads should expose native-file sync adapter tasks"
    );
    drop(db);
    cleanup_dir(&path);
}

#[test]
fn persistent_open_replays_wal() {
    let path = temp_db_path("persistent-open-replay");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;

    {
        let db = block_on(Db::open(options.clone())).expect("persistent async open");
        block_on(db.put_with_options(
            b"wal-key".to_vec(),
            b"wal-value".to_vec(),
            WriteOptions::flush(),
        ))
        .expect("async write appends WAL");
    }

    let reopened = block_on(Db::open(options)).expect("persistent async reopen replays WAL");
    assert_eq!(
        block_on(reopened.get(b"wal-key")).expect("async get after replay"),
        Some(b"wal-value".to_vec())
    );
    cleanup_dir(&path);
}

#[test]
fn persistent_async_write_uses_storage_backend_wal_append() {
    let path = temp_db_path("persistent-async-write-wal-storage");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;

    let db = block_on(Db::open(options.clone())).expect("persistent async open");
    let before = db.stats();
    block_on(db.put_with_options(
        b"wal-storage-key".to_vec(),
        b"wal-storage-value".to_vec(),
        WriteOptions::flush(),
    ))
    .expect("async write appends through storage backend");
    let after = db.stats();

    assert!(
        after.storage_operations.append.requests > before.storage_operations.append.requests,
        "async write should append WAL through the storage backend"
    );
    assert!(
        after.storage_sync_adapter_tasks > before.storage_sync_adapter_tasks,
        "native fallback async write should still use the bounded storage adapter"
    );
    drop(db);

    let reopened = block_on(Db::open(options)).expect("persistent async reopen replays WAL");
    assert_eq!(
        block_on(reopened.get(b"wal-storage-key")).expect("async get after replay"),
        Some(b"wal-storage-value".to_vec())
    );
    cleanup_dir(&path);
}

#[cfg(all(feature = "platform-io", target_os = "linux"))]
#[test]
fn platform_io_async_write_awaits_wal_without_whole_commit_adapter() {
    let path = temp_db_path("platform-io-async-write");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.runtime = RuntimeOptions::platform_io();
    options.background_worker_count = 0;

    let db = block_on(Db::open(options.clone())).expect("platform I/O persistent open");
    assert!(db.stats().storage_uses_platform_async_io);
    let before = db.stats();
    block_on(db.put_with_options(
        b"platform-key".to_vec(),
        b"platform-value".to_vec(),
        WriteOptions::flush(),
    ))
    .expect("platform async write commits");
    let after = db.stats();

    assert_eq!(
        after.storage_sync_adapter_submitted_tasks, before.storage_sync_adapter_submitted_tasks,
        "platform async write should not spawn the whole commit through the sync adapter"
    );
    assert!(
        after.storage_platform_async_io_tasks > before.storage_platform_async_io_tasks,
        "platform async write should await platform storage completion"
    );
    assert!(
        after
            .storage_platform_io_operations
            .append
            .true_platform_async
            > before
                .storage_platform_io_operations
                .append
                .true_platform_async,
        "platform async write should report append as true platform async"
    );
    assert!(
        after.storage_operations.append.requests > before.storage_operations.append.requests,
        "platform async write should still append a WAL record"
    );
    drop(db);

    let reopened = block_on(Db::open(options)).expect("platform I/O reopen replays WAL");
    assert_eq!(
        block_on(reopened.get(b"platform-key")).expect("platform replay read"),
        Some(b"platform-value".to_vec())
    );
    cleanup_dir(&path);
}

#[test]
fn persistent_read_only_open_async_skips_clean_wal_reads() {
    let path = temp_db_path("persistent-read-only-async-clean-wal");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    {
        let db = block_on(Db::open(options.clone())).expect("persistent async open");
        block_on(db.put(b"key".to_vec(), b"value".to_vec())).expect("async put succeeds");
        block_on(db.flush()).expect("async flush succeeds");
    }

    let db = block_on(Db::open(options.read_only())).expect("read-only async open");

    assert_eq!(
        block_on(db.get(b"key")).expect("read-only async read succeeds"),
        Some(b"value".to_vec())
    );
    assert_eq!(db.stats().storage_operations.read_object_bytes.requests, 0);
    assert_eq!(
        db.stats().storage_operations.acquire_writer_lease.requests,
        0
    );
    cleanup_dir(&path);
}

#[test]
fn persistent_read_only_open_async_replays_non_empty_wal() {
    let path = temp_db_path("persistent-read-only-async-wal-replay");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;
    {
        let db = block_on(Db::open(options.clone())).expect("persistent async open");
        block_on(db.put_with_options(
            b"wal-key".to_vec(),
            b"wal-value".to_vec(),
            WriteOptions::flush(),
        ))
        .expect("async write appends WAL");
    }

    let db = block_on(Db::open(options.read_only())).expect("read-only async open");

    assert_eq!(
        block_on(db.get(b"wal-key")).expect("read-only async WAL read succeeds"),
        Some(b"wal-value".to_vec())
    );
    assert!(
        db.stats().storage_operations.read_object_bytes.requests > 0,
        "read-only async open must read non-empty WAL shards"
    );
    cleanup_dir(&path);
}

#[test]
fn persistent_open_rejects_inline_runtime() {
    let path = temp_db_path("persistent-open-inline-runtime");
    let mut options = DbOptions::persistent(&path);
    options.runtime = RuntimeOptions::inline();
    options.background_worker_count = 0;

    let error = block_on(Db::open(options)).expect_err("persistent async open needs wait support");

    assert!(matches!(error, Error::Unsupported { .. }));
    cleanup_dir(&path);
}

#[test]
fn persistent_async_reads_load_blob_values_through_storage_backend() {
    let path = temp_db_path("persistent-async-blob-read");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;
    options.default_bucket_options = options.default_bucket_options.with_blob_threshold_bytes(4);
    let point_value = b"large-value-for-point-read".to_vec();
    let lazy_value = b"large-value-for-lazy-read".to_vec();

    let db = block_on(Db::open(options)).expect("persistent async open");
    block_on(db.put(b"a".to_vec(), point_value.clone())).expect("point value writes");
    block_on(db.put(b"b".to_vec(), lazy_value.clone())).expect("lazy value writes");
    block_on(db.flush()).expect("async flush writes blob-backed table");

    let before_point_tasks = db.stats().storage_sync_adapter_tasks;
    assert_eq!(
        block_on(db.get(b"a")).expect("async point read loads blob"),
        Some(point_value)
    );
    let after_point = db.stats();
    assert_eq!(after_point.blob_read_count, 1);
    assert!(
        after_point.storage_sync_adapter_tasks > before_point_tasks,
        "async point read should enter the storage backend"
    );

    let mut iter =
        block_on(db.range_lazy(&KeyRange::half_open(b"b", b"c"))).expect("async lazy range opens");
    let row = block_on(iter.next())
        .expect("async lazy iterator advances")
        .expect("lazy row exists");
    assert_eq!(row.key, b"b".to_vec());
    assert!(!row.value.is_inline());

    let before_lazy_tasks = db.stats().storage_sync_adapter_tasks;
    assert_eq!(
        block_on(row.value.read()).expect("async lazy value reads"),
        lazy_value
    );
    let after_lazy = db.stats();
    assert_eq!(after_lazy.blob_read_count, 2);
    assert!(
        after_lazy.storage_sync_adapter_tasks > before_lazy_tasks,
        "async lazy value read should enter the storage backend"
    );

    cleanup_dir(&path);
}

#[test]
fn persistent_async_maintenance_runs_on_runtime_blocking_task() {
    let path = temp_db_path("persistent-async-maintenance");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    let db = block_on(Db::open(options)).expect("persistent async open");
    block_on(db.put(b"k".to_vec(), b"value".to_vec())).expect("write succeeds");

    let before_flush = db.stats().storage_sync_adapter_submitted_tasks;
    block_on(db.flush()).expect("async flush succeeds");
    let after_flush = db.stats();
    assert_eq!(after_flush.immutable_memtables, 0);
    assert!(
        after_flush.storage_sync_adapter_submitted_tasks > before_flush,
        "native fallback async flush should run through the runtime blocking task boundary"
    );

    let before_compact = db.stats().storage_sync_adapter_submitted_tasks;
    block_on(
        db.compact_range_with_budget(KeyRange::all(), trine_kv::MaintenanceBudget::single_unit()),
    )
    .expect("async budgeted compaction succeeds");
    assert!(
        db.stats().storage_sync_adapter_submitted_tasks > before_compact,
        "native async compaction should run through the runtime blocking task boundary"
    );

    block_on(db.close()).expect("async close succeeds");
    cleanup_dir(&path);
}

#[cfg(all(feature = "platform-io", target_os = "linux"))]
#[test]
fn platform_io_async_flush_awaits_storage_without_whole_flush_adapter() {
    let path = temp_db_path("platform-io-async-flush");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.runtime = RuntimeOptions::platform_io();
    options.background_worker_count = 0;
    options.max_l0_files = 64;

    let db = block_on(Db::open(options.clone())).expect("platform I/O persistent open");
    block_on(db.put_with_options(
        b"flush-key".to_vec(),
        b"flush-value".to_vec(),
        WriteOptions::flush(),
    ))
    .expect("platform async write commits");

    let before = db.stats();
    block_on(db.flush()).expect("platform async flush succeeds");
    let after = db.stats();

    assert_eq!(after.immutable_memtables, 0);
    assert!(
        after.storage_sync_adapter_submitted_tasks
            <= before.storage_sync_adapter_submitted_tasks + 1,
        "platform async flush should not spawn the whole flush through the sync adapter; \
         DbStats may perform one native table-size lookup while observing the result"
    );
    assert!(
        after.storage_platform_async_io_tasks > before.storage_platform_async_io_tasks,
        "platform async flush should await platform storage completions"
    );
    assert!(
        after
            .storage_platform_io_operations
            .temp_write_rename_publish
            .true_platform_async
            > before
                .storage_platform_io_operations
                .temp_write_rename_publish
                .true_platform_async,
        "platform async flush should publish table or manifest bytes as true platform async"
    );
    assert!(
        after.storage_operations.write_object.requests
            > before.storage_operations.write_object.requests,
        "platform async flush should write table objects through storage"
    );
    assert!(
        after.storage_operations.publish_manifest.requests
            > before.storage_operations.publish_manifest.requests,
        "platform async flush should publish the manifest through storage"
    );
    assert!(
        after
            .storage_operations
            .sync_directory_after_renames
            .requests
            > before
                .storage_operations
                .sync_directory_after_renames
                .requests,
        "platform async flush should sync the database directory through storage"
    );
    assert!(
        after.storage_operations.rewrite_wal.requests
            > before.storage_operations.rewrite_wal.requests,
        "platform async flush should rewrite WAL replay floor through storage"
    );
    assert!(
        after
            .storage_platform_io_operations
            .wal_rewrite
            .true_platform_async
            > before
                .storage_platform_io_operations
                .wal_rewrite
                .true_platform_async,
        "platform async flush should report WAL rewrite as true platform async"
    );
    drop(db);

    let reopened = block_on(Db::open(options)).expect("platform I/O reopen after flush");
    assert_eq!(
        block_on(reopened.get(b"flush-key")).expect("platform replay read"),
        Some(b"flush-value".to_vec())
    );
    cleanup_dir(&path);
}

#[cfg(all(feature = "platform-io", target_os = "linux"))]
#[test]
fn platform_io_async_compaction_output_writes_are_not_yet_platform_io() {
    let path = temp_db_path("platform-io-async-compaction");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.runtime = RuntimeOptions::platform_io();
    options.background_worker_count = 1;
    options.max_l0_files = 64;

    let db = block_on(Db::open(options)).expect("platform I/O persistent open");
    block_on(db.put(b"compact-key".to_vec(), b"v1".to_vec())).expect("first write commits");
    block_on(db.flush()).expect("first flush writes an L0 table");
    block_on(db.put(b"compact-key".to_vec(), b"v2".to_vec())).expect("second write commits");
    block_on(db.flush()).expect("second flush writes an overlapping L0 table");

    let before = db.stats();
    let outcome = block_on(
        db.compact_range_with_budget(KeyRange::all(), trine_kv::MaintenanceBudget::unbounded()),
    )
    .expect("platform I/O compaction succeeds");
    let after = db.stats();

    assert!(
        outcome.compactions > 0 || after.compaction_runs > before.compaction_runs,
        "compaction should rewrite overlapping L0 tables"
    );
    assert!(
        after.storage_operations.write_object.requests
            > before.storage_operations.write_object.requests,
        "compaction should write output tables through the storage write operation"
    );
    assert_eq!(
        after
            .storage_platform_io_operations
            .temp_write_rename_publish
            .true_platform_async,
        before
            .storage_platform_io_operations
            .temp_write_rename_publish
            .true_platform_async,
        "native compaction output writes still use the synchronous table writer"
    );
    assert_eq!(
        block_on(db.get(b"compact-key")).expect("read after compaction"),
        Some(b"v2".to_vec())
    );
    cleanup_dir(&path);
}

#[test]
fn dropping_unpolled_async_write_future_has_no_side_effect() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");

    let write = db.put(b"cancelled".to_vec(), b"value".to_vec());
    drop(write);

    assert_eq!(
        db.get_sync(b"cancelled")
            .expect("read after dropped future"),
        None
    );
    assert_eq!(db.latest_read_version(), ReadVersion::ZERO);
}

#[test]
fn dropping_unpolled_persistent_async_write_future_has_no_wal_side_effect() {
    let path = temp_db_path("persistent-unpolled-write");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;
    let db = Db::open_sync(options.clone()).expect("persistent db opens");

    let write = db.put_with_options(
        b"cancelled".to_vec(),
        b"value".to_vec(),
        WriteOptions::flush(),
    );
    drop(write);

    assert_eq!(
        db.get_sync(b"cancelled")
            .expect("read after dropped future"),
        None
    );
    assert_eq!(db.latest_read_version(), ReadVersion::ZERO);
    drop(db);

    let reopened = Db::open_sync(options).expect("persistent db reopens");
    assert_eq!(
        reopened
            .get_sync(b"cancelled")
            .expect("reopen after dropped future"),
        None
    );
    cleanup_dir(&path);
}

#[test]
fn polled_async_write_future_reaches_visible_terminal_commit() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"accepted".to_vec(), b"value".to_vec());

    let commit = block_on(db.write(batch, WriteOptions::default())).expect("async write commits");

    assert_eq!(commit.read_version(), db.latest_read_version());
    assert_eq!(
        db.get_sync(b"accepted")
            .expect("read after accepted future"),
        Some(b"value".to_vec())
    );
}

#[test]
fn dropping_polled_async_write_future_does_not_cancel_accepted_native_write() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"accepted-after-drop".to_vec(), b"value".to_vec());

    let mut write = Box::pin(db.write(batch, WriteOptions::default()));
    let waker = current_thread_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Future::poll(write.as_mut(), &mut context),
        Poll::Pending
    ));
    drop(write);

    wait_until(|| {
        db.get_sync(b"accepted-after-drop")
            .expect("read after accepted future drop")
            .is_some()
    });
    assert_eq!(
        db.get_sync(b"accepted-after-drop")
            .expect("read accepted key after dropped future"),
        Some(b"value".to_vec())
    );
    assert_eq!(db.latest_read_version(), ReadVersion::from_u64(1));
}

#[test]
fn dropping_polled_persistent_async_write_future_survives_reopen() {
    let path = temp_db_path("persistent-polled-write");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;
    let db = Db::open_sync(options.clone()).expect("persistent db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"accepted-after-drop".to_vec(), b"value".to_vec());

    let mut write = Box::pin(db.write(batch, WriteOptions::flush()));
    let waker = current_thread_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Future::poll(write.as_mut(), &mut context),
        Poll::Pending
    ));
    drop(write);

    wait_until(|| {
        db.get_sync(b"accepted-after-drop")
            .expect("read after accepted future drop")
            .is_some()
    });
    assert_eq!(db.latest_read_version(), ReadVersion::from_u64(1));
    drop(db);

    let reopened = Db::open_sync(options).expect("persistent db reopens");
    assert_eq!(
        reopened
            .get_sync(b"accepted-after-drop")
            .expect("replay after accepted future drop"),
        Some(b"value".to_vec())
    );
    assert_eq!(reopened.latest_read_version(), ReadVersion::from_u64(1));
    cleanup_dir(&path);
}

#[test]
fn inline_runtime_async_write_completes_without_background_threads() {
    let mut options = DbOptions::memory();
    options.runtime = RuntimeOptions::inline();
    let db = Db::open_sync(options).expect("inline runtime memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"inline".to_vec(), b"value".to_vec());

    let commit = block_on(db.write(batch, WriteOptions::default()))
        .expect("inline runtime async write commits");

    assert_eq!(commit.read_version(), ReadVersion::from_u64(1));
    assert_eq!(
        db.get_sync(b"inline").expect("read inline runtime write"),
        Some(b"value".to_vec())
    );
}
