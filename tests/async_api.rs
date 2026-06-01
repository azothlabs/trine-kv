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
    RuntimeOptions, Sequence, TransactionOptions, WriteBatch, WriteOptions, wal,
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
        block_on(iter.next_async()).expect("async iterator item is readable")
    {
        rows.push((key, value));
    }
    rows
}

fn collect_lazy_async(mut iter: LazyIter) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    while let Some(item) =
        block_on(iter.next_async()).expect("async lazy iterator item is readable")
    {
        let value = block_on(item.value.read_async()).expect("lazy value reads");
        rows.push((item.key, value));
    }
    rows
}

#[test]
fn memory_async_compatibility_surface_smoke() {
    let db = block_on(Db::open_async(DbOptions::memory())).expect("memory db opens");

    block_on(db.put_async(b"a".to_vec(), b"one".to_vec())).expect("put through async API");
    assert_eq!(
        block_on(db.get_async(b"a")).expect("get through async API"),
        Some(b"one".to_vec())
    );

    let mut batch = WriteBatch::new();
    batch.put(b"b".to_vec(), b"two".to_vec());
    let commit = block_on(db.write_async(batch, WriteOptions::default())).expect("batch writes");
    assert_eq!(commit.sequence(), db.last_committed_sequence());

    let default_rows =
        collect_async(block_on(db.prefix_async(b"b".to_vec())).expect("prefix opens"));
    assert_eq!(default_rows, vec![(b"b".to_vec(), b"two".to_vec())]);
    assert_eq!(
        collect_lazy_async(block_on(db.prefix_lazy_async(b"b".to_vec())).expect("prefix opens")),
        vec![(b"b".to_vec(), b"two".to_vec())]
    );

    let events = block_on(db.bucket_async("events")).expect("bucket opens");
    block_on(events.put_async(b"e1".to_vec(), b"event".to_vec()))
        .expect("bucket put through async API");
    assert_eq!(
        block_on(events.get_async(b"e1")).expect("bucket get through async API"),
        Some(b"event".to_vec())
    );
    assert_eq!(
        collect_async(block_on(events.range_async(&KeyRange::all())).expect("range opens")),
        vec![(b"e1".to_vec(), b"event".to_vec())]
    );
    let mut lazy_events =
        block_on(events.range_lazy_async(&KeyRange::all())).expect("lazy range opens");
    let lazy_event = block_on(lazy_events.next_async())
        .expect("lazy event advances")
        .expect("lazy event exists");
    let lazy_event = block_on(lazy_event.into_key_value_async())
        .expect("lazy event converts into owned key/value");
    assert_eq!(lazy_event.key, b"e1".to_vec());
    assert_eq!(lazy_event.value, b"event".to_vec());
    assert!(
        block_on(lazy_events.next_async())
            .expect("lazy event iterator finishes")
            .is_none()
    );

    let mut txn = db.transaction(TransactionOptions::default());
    assert_eq!(
        block_on(txn.get_async(b"b")).expect("transaction async point read"),
        Some(b"two".to_vec())
    );
    block_on(txn.read_range_async(KeyRange::all())).expect("transaction async range read");
    txn.put(b"c".to_vec(), b"three".to_vec());
    let txn_commit = block_on(txn.commit_async()).expect("transaction async commit");
    assert_eq!(txn_commit.sequence(), db.last_committed_sequence());
    assert_eq!(
        block_on(db.get_async(b"c")).expect("transaction write reads"),
        Some(b"three".to_vec())
    );

    let mut named_txn = db.transaction(TransactionOptions::default());
    assert_eq!(
        block_on(named_txn.get_bucket_async("events", b"e1"))
            .expect("named transaction async point read"),
        Some(b"event".to_vec())
    );
    named_txn
        .put_bucket("events", b"e2".to_vec(), b"event-two".to_vec())
        .expect("named transaction stages write");
    block_on(named_txn.commit_async()).expect("named transaction async commit");
    assert_eq!(
        block_on(events.get_async(b"e2")).expect("named transaction write reads"),
        Some(b"event-two".to_vec())
    );

    block_on(db.delete_async(b"a".to_vec())).expect("delete through async API");
    assert_eq!(
        block_on(db.get_async(b"a")).expect("deleted key reads"),
        None
    );

    block_on(db.persist_async(DurabilityMode::Buffered)).expect("memory persist is accepted");
    block_on(db.flush_async()).expect("memory flush is accepted");
    block_on(db.compact_range_async(KeyRange::all())).expect("memory compact is accepted");
    block_on(db.close_async()).expect("async close succeeds");
}

#[test]
fn persistent_async_range_and_prefix_advance_flushed_tables() {
    let path = temp_db_path("cursor-advance");
    let mut options = DbOptions::persistent(&path);
    options.default_bucket_options = BucketOptions {
        block_bytes: 128,
        ..BucketOptions::default()
    };
    let db = Db::open(options).expect("persistent db opens");
    block_on(db.put_async(b"tenant:01:key-000".to_vec(), b"one".to_vec()))
        .expect("first async put succeeds");
    block_on(db.put_async(b"tenant:01:key-001".to_vec(), b"two".to_vec()))
        .expect("second async put succeeds");
    block_on(db.put_async(b"tenant:02:key-000".to_vec(), b"three".to_vec()))
        .expect("third async put succeeds");
    block_on(db.put_async(b"zeta".to_vec(), b"four".to_vec())).expect("fourth async put succeeds");
    db.flush().expect("flush writes table files");

    let tenant_one = KeyRange::half_open(b"tenant:01:".to_vec(), b"tenant:02:".to_vec());
    let range_rows =
        collect_async(block_on(db.range_async(&tenant_one)).expect("async range opens"));
    assert_eq!(
        range_rows,
        vec![
            (b"tenant:01:key-000".to_vec(), b"one".to_vec()),
            (b"tenant:01:key-001".to_vec(), b"two".to_vec()),
        ]
    );

    let prefix_rows = collect_lazy_async(
        block_on(db.prefix_lazy_async(b"tenant:01:".to_vec())).expect("async lazy prefix opens"),
    );
    assert_eq!(prefix_rows, range_rows);

    let reverse_rows = collect_lazy_async(
        block_on(db.prefix_lazy_reverse_async(b"tenant:01:".to_vec()))
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
    assert!(stats.storage_uses_blocking_adapter);
    assert!(!stats.storage_uses_platform_async_io);
    assert!(
        stats.storage_blocking_adapter_tasks > 0,
        "async table reads should expose native-file blocking adapter tasks"
    );
    drop(db);
    cleanup_dir(&path);
}

#[test]
fn persistent_open_async_replays_wal() {
    let path = temp_db_path("persistent-open-replay");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;

    {
        let db = block_on(Db::open_async(options.clone())).expect("persistent async open");
        block_on(db.put_with_options_async(
            b"wal-key".to_vec(),
            b"wal-value".to_vec(),
            WriteOptions::flush(),
        ))
        .expect("async write appends WAL");
    }

    let reopened = block_on(Db::open_async(options)).expect("persistent async reopen replays WAL");
    assert_eq!(
        block_on(reopened.get_async(b"wal-key")).expect("async get after replay"),
        Some(b"wal-value".to_vec())
    );
    cleanup_dir(&path);
}

#[test]
fn persistent_open_async_rejects_inline_runtime() {
    let path = temp_db_path("persistent-open-inline-runtime");
    let mut options = DbOptions::persistent(&path);
    options.runtime = RuntimeOptions::inline();
    options.background_worker_count = 0;

    let error =
        block_on(Db::open_async(options)).expect_err("persistent async open needs wait support");

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

    let db = block_on(Db::open_async(options)).expect("persistent async open");
    block_on(db.put_async(b"a".to_vec(), point_value.clone())).expect("point value writes");
    block_on(db.put_async(b"b".to_vec(), lazy_value.clone())).expect("lazy value writes");
    block_on(db.flush_async()).expect("async flush writes blob-backed table");

    let before_point_tasks = db.stats().storage_blocking_adapter_tasks;
    assert_eq!(
        block_on(db.get_async(b"a")).expect("async point read loads blob"),
        Some(point_value)
    );
    let after_point = db.stats();
    assert_eq!(after_point.blob_read_count, 1);
    assert!(
        after_point.storage_blocking_adapter_tasks > before_point_tasks,
        "async point read should enter the storage backend"
    );

    let mut iter = block_on(db.range_lazy_async(&KeyRange::half_open(b"b", b"c")))
        .expect("async lazy range opens");
    let row = block_on(iter.next_async())
        .expect("async lazy iterator advances")
        .expect("lazy row exists");
    assert_eq!(row.key, b"b".to_vec());
    assert!(!row.value.is_inline());

    let before_lazy_tasks = db.stats().storage_blocking_adapter_tasks;
    assert_eq!(
        block_on(row.value.read_async()).expect("async lazy value reads"),
        lazy_value
    );
    let after_lazy = db.stats();
    assert_eq!(after_lazy.blob_read_count, 2);
    assert!(
        after_lazy.storage_blocking_adapter_tasks > before_lazy_tasks,
        "async lazy value read should enter the storage backend"
    );

    cleanup_dir(&path);
}

#[test]
fn persistent_async_maintenance_runs_on_runtime_blocking_task() {
    let path = temp_db_path("persistent-async-maintenance");
    let mut options = DbOptions::persistent(&path);
    options.background_worker_count = 0;

    let db = block_on(Db::open_async(options)).expect("persistent async open");
    block_on(db.put_async(b"k".to_vec(), b"value".to_vec())).expect("write succeeds");

    let before_flush = db.stats().storage_blocking_adapter_submitted_tasks;
    block_on(db.flush_async()).expect("async flush succeeds");
    let after_flush = db.stats();
    assert_eq!(after_flush.immutable_memtables, 0);
    assert!(
        after_flush.storage_blocking_adapter_submitted_tasks > before_flush,
        "native async flush should run through the runtime blocking task boundary"
    );

    let before_compact = db.stats().storage_blocking_adapter_submitted_tasks;
    block_on(db.compact_range_with_budget_async(
        KeyRange::all(),
        trine_kv::MaintenanceBudget::single_unit(),
    ))
    .expect("async budgeted compaction succeeds");
    assert!(
        db.stats().storage_blocking_adapter_submitted_tasks > before_compact,
        "native async compaction should run through the runtime blocking task boundary"
    );

    block_on(db.close_async()).expect("async close succeeds");
    cleanup_dir(&path);
}

#[test]
fn dropping_unpolled_async_write_future_has_no_side_effect() {
    let db = Db::open_memory().expect("memory db opens");

    let write = db.put_async(b"cancelled".to_vec(), b"value".to_vec());
    drop(write);

    assert_eq!(
        db.get(b"cancelled").expect("read after dropped future"),
        None
    );
    assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
}

#[test]
fn dropping_unpolled_persistent_async_write_future_has_no_wal_side_effect() {
    let path = temp_db_path("persistent-unpolled-write");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;
    let db = Db::open(options.clone()).expect("persistent db opens");

    let write = db.put_with_options_async(
        b"cancelled".to_vec(),
        b"value".to_vec(),
        WriteOptions::flush(),
    );
    drop(write);

    assert_eq!(
        db.get(b"cancelled").expect("read after dropped future"),
        None
    );
    assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
    drop(db);
    assert!(
        wal::read_all_batches(&path).expect("WAL reads").is_empty(),
        "unpolled write future must not append a WAL record"
    );

    let reopened = Db::open(options).expect("persistent db reopens");
    assert_eq!(
        reopened
            .get(b"cancelled")
            .expect("reopen after dropped future"),
        None
    );
    cleanup_dir(&path);
}

#[test]
fn polled_async_write_future_reaches_visible_terminal_commit() {
    let db = Db::open_memory().expect("memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"accepted".to_vec(), b"value".to_vec());

    let commit =
        block_on(db.write_async(batch, WriteOptions::default())).expect("async write commits");

    assert_eq!(commit.sequence(), db.last_committed_sequence());
    assert_eq!(
        db.get(b"accepted").expect("read after accepted future"),
        Some(b"value".to_vec())
    );
}

#[test]
fn dropping_polled_async_write_future_does_not_cancel_accepted_native_write() {
    let db = Db::open_memory().expect("memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"accepted-after-drop".to_vec(), b"value".to_vec());

    let mut write = Box::pin(db.write_async(batch, WriteOptions::default()));
    let waker = current_thread_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Future::poll(write.as_mut(), &mut context),
        Poll::Pending
    ));
    drop(write);

    wait_until(|| {
        db.get(b"accepted-after-drop")
            .expect("read after accepted future drop")
            .is_some()
    });
    assert_eq!(
        db.get(b"accepted-after-drop")
            .expect("read accepted key after dropped future"),
        Some(b"value".to_vec())
    );
    assert_eq!(db.last_committed_sequence(), Sequence::new(1));
}

#[test]
fn dropping_polled_persistent_async_write_future_survives_reopen() {
    let path = temp_db_path("persistent-polled-write");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;
    let db = Db::open(options.clone()).expect("persistent db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"accepted-after-drop".to_vec(), b"value".to_vec());

    let mut write = Box::pin(db.write_async(batch, WriteOptions::flush()));
    let waker = current_thread_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Future::poll(write.as_mut(), &mut context),
        Poll::Pending
    ));
    drop(write);

    wait_until(|| {
        db.get(b"accepted-after-drop")
            .expect("read after accepted future drop")
            .is_some()
    });
    assert_eq!(db.last_committed_sequence(), Sequence::new(1));
    drop(db);

    let reopened = Db::open(options).expect("persistent db reopens");
    assert_eq!(
        reopened
            .get(b"accepted-after-drop")
            .expect("replay after accepted future drop"),
        Some(b"value".to_vec())
    );
    assert_eq!(reopened.last_committed_sequence(), Sequence::new(1));
    cleanup_dir(&path);
}

#[test]
fn inline_runtime_async_write_completes_without_background_threads() {
    let mut options = DbOptions::memory();
    options.runtime = RuntimeOptions::inline();
    let db = Db::memory(options).expect("inline runtime memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"inline".to_vec(), b"value".to_vec());

    let commit = block_on(db.write_async(batch, WriteOptions::default()))
        .expect("inline runtime async write commits");

    assert_eq!(commit.sequence(), Sequence::new(1));
    assert_eq!(
        db.get(b"inline").expect("read inline runtime write"),
        Some(b"value".to_vec())
    );
}
