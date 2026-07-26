use std::{
    sync::Arc,
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

use crate::runtime::{Runtime, RuntimeOptions};

#[cfg(feature = "platform-io")]
use super::platform::*;
#[cfg(feature = "platform-io")]
use super::platform::{
    driver::reserve_platform_io_queue_slot, scheduler::platform_io_resources_conflict,
};
use super::*;

fn poll_ready_io<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => Err(Error::unsupported_backend("pending inline I/O completion")),
    }
}

fn wait_for_io<T>(future: IoCompletion<T>) -> Result<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(5));
            }
            Poll::Pending => {
                return Err(Error::runtime_busy("I/O completion did not finish"));
            }
        }
    }
}

#[test]
fn inline_driver_completes_read_and_has_no_pending_steps() {
    let driver = InlineIoDriver;
    assert_eq!(driver.info().kind(), IoDriverKind::Inline);

    let completion = driver
        .submit_read_exact_at_owned(|| Ok(StorageReadBuffer::from_vec(4, b"read".to_vec())))
        .expect("inline read submits");
    assert!(completion.is_finished().expect("completion state reads"));
    let buffer = poll_ready_io(completion).expect("inline read completes");

    assert_eq!(buffer.offset(), 4);
    assert_eq!(&*buffer.into_bytes(), b"read");
    assert_eq!(driver.step().expect("inline step succeeds"), 0);
    assert_eq!(driver.drain().expect("inline drain succeeds"), 0);
}

#[test]
fn inline_driver_completes_append_and_sync() {
    let driver = InlineIoDriver;
    let append = driver
        .submit_append(|| Ok(()))
        .expect("inline append submits");
    poll_ready_io(append).expect("inline append completes");

    let sync = driver.submit_sync(|| Ok(())).expect("inline sync submits");
    poll_ready_io(sync).expect("inline sync completes");
}

#[test]
fn blocking_adapter_driver_runs_submitted_operation() {
    let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 4);
    let driver = BlockingAdapterIoDriver::new(runtime);
    assert_eq!(driver.info().kind(), IoDriverKind::BlockingAdapter);

    let completion = driver
        .submit_len(|| Ok(42))
        .expect("blocking adapter submits operation");
    assert_eq!(
        wait_for_io(completion).expect("blocking adapter completes operation"),
        42
    );
    assert_eq!(driver.step().expect("blocking adapter step succeeds"), 0);
    assert_eq!(driver.drain().expect("blocking adapter drain succeeds"), 0);
}

#[cfg(feature = "platform-io")]
#[test]
fn platform_backend_matrix_matches_target_family() {
    let driver = PlatformIoDriver::new(Arc::new(NativeFileStorageMetrics::default()))
        .expect("platform I/O driver starts");
    let matrix = driver.backend_matrix();

    #[cfg(not(feature = "platform-io-native"))]
    {
        #[cfg(any(unix, windows))]
        {
            assert_all_platform_rows(&matrix, PlatformIoTaskClass::ThreadPoolManagedAsync);
            assert_eq!(matrix.kind, PlatformIoBackendKind::ThreadPoolManaged);
        }

        #[cfg(not(any(unix, windows)))]
        {
            assert_unsupported_platform_matrix(&matrix);
        }
    }

    #[cfg(feature = "platform-io-native")]
    {
        #[cfg(target_os = "linux")]
        {
            assert_linux_native_platform_matrix(&matrix);
        }
        #[cfg(windows)]
        {
            assert_partial_native_platform_matrix(
                &matrix,
                PlatformIoBackendKind::WindowsNative,
                WINDOWS_PARTIAL_NATIVE_ROWS,
            );
        }
        #[cfg(target_os = "macos")]
        {
            assert_partial_native_platform_matrix(
                &matrix,
                PlatformIoBackendKind::MacOsNative,
                MACOS_PARTIAL_NATIVE_ROWS,
            );
        }
        #[cfg(target_os = "freebsd")]
        {
            assert_partial_native_platform_matrix(
                &matrix,
                PlatformIoBackendKind::FreeBsdNative,
                BSD_SOLARISH_PARTIAL_NATIVE_ROWS,
            );
        }
        #[cfg(any(target_os = "illumos", target_os = "solaris"))]
        {
            assert_partial_native_platform_matrix(
                &matrix,
                PlatformIoBackendKind::SolarishNative,
                BSD_SOLARISH_PARTIAL_NATIVE_ROWS,
            );
        }
        #[cfg(all(
            unix,
            not(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "freebsd",
                target_os = "illumos",
                target_os = "solaris"
            ))
        ))]
        {
            assert_all_platform_rows(&matrix, PlatformIoTaskClass::ThreadPoolManagedAsync);
            assert_eq!(matrix.kind, PlatformIoBackendKind::UnixFallback);
        }
        #[cfg(not(any(unix, windows)))]
        {
            assert_unsupported_platform_matrix(&matrix);
        }
    }
}

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
const ALL_PLATFORM_OPERATIONS: [PlatformIoOperation; 13] = [
    PlatformIoOperation::LengthLookup,
    PlatformIoOperation::OwnedRandomRead,
    PlatformIoOperation::OptionalWholeObjectRead,
    PlatformIoOperation::TempWriteRenamePublish,
    PlatformIoOperation::AppendObjectOpen,
    PlatformIoOperation::Append,
    PlatformIoOperation::Persist,
    PlatformIoOperation::WalRewrite,
    PlatformIoOperation::ObjectDelete,
    PlatformIoOperation::DirectoryCreate,
    PlatformIoOperation::DirectorySync,
    PlatformIoOperation::DirectoryListing,
    PlatformIoOperation::WriterLeaseAcquire,
];

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
fn assert_all_platform_rows(matrix: &PlatformIoBackendMatrix, class: PlatformIoTaskClass) {
    for operation in ALL_PLATFORM_OPERATIONS {
        assert_eq!(matrix.class_for(operation), class, "{operation:?}");
    }
}

#[cfg(feature = "platform-io")]
#[test]
fn platform_scheduler_queue_has_a_hard_admission_bound() {
    let metrics = PlatformIoSchedulerMetrics::default();
    for _ in 0..PLATFORM_IO_SCHEDULER_QUEUE_DEPTH {
        assert!(reserve_platform_io_queue_slot(&metrics));
    }
    assert!(!reserve_platform_io_queue_slot(&metrics));
    assert_eq!(
        metrics.queued.load(std::sync::atomic::Ordering::Acquire),
        PLATFORM_IO_SCHEDULER_QUEUE_DEPTH
    );
}

#[cfg(feature = "platform-io")]
#[test]
fn platform_scheduler_orders_one_resource_without_serializing_unrelated_resources() {
    let path_a = std::path::PathBuf::from("/trine-test/a.wal");
    let path_b = std::path::PathBuf::from("/trine-test/b.wal");
    let read_a = PlatformIoTask::Len {
        path: path_a.clone(),
        completion: IoCompletion::new(),
    }
    .resources();
    let second_read_a = PlatformIoTask::ReadExactAtOwned {
        path: path_a.clone(),
        offset: 0,
        len: 1,
        completion: IoCompletion::new(),
    }
    .resources();
    let append_a = PlatformIoTask::Append {
        session: PlatformIoAppendSession::opened(path_a),
        bytes: Arc::from(&b"a"[..]),
        durability: DurabilityMode::Buffered,
        completion: IoCompletion::new(),
    }
    .resources();
    let append_b = PlatformIoTask::Append {
        session: PlatformIoAppendSession::opened(path_b),
        bytes: Arc::from(&b"b"[..]),
        durability: DurabilityMode::Buffered,
        completion: IoCompletion::new(),
    }
    .resources();

    let mut resources = PlatformIoResourceTable::default();
    resources.acquire(&read_a);
    assert!(resources.can_acquire(&second_read_a));
    assert!(!resources.can_acquire(&append_a));
    assert!(resources.can_acquire(&append_b));
    assert!(platform_io_resources_conflict(&append_a, &second_read_a));
    assert!(!platform_io_resources_conflict(&append_a, &append_b));
    resources.release(&read_a);
    assert!(resources.can_acquire(&append_a));
}

#[cfg(feature = "platform-io")]
#[test]
fn platform_driver_close_drains_accepted_work_and_rejects_new_work() {
    let root = platform_io_test_directory("close-drain");
    std::fs::create_dir_all(&root).expect("test directory creates");
    let path = root.join("published");
    let temporary = root.join("published.tmp");
    let metrics = Arc::new(NativeFileStorageMetrics::default());
    let driver = PlatformIoDriver::new(metrics).expect("platform I/O driver starts");
    let completion = driver
        .submit_publish(PlatformIoPublishPlan::manifest(
            path.clone(),
            temporary,
            Arc::from(&b"accepted-before-close"[..]),
            DurabilityMode::Buffered,
        ))
        .expect("write is accepted");

    driver.close().expect("driver drains and joins");
    wait_for_io(completion).expect("accepted write reaches completion");
    assert_eq!(
        std::fs::read(&path).expect("published bytes read"),
        b"accepted-before-close"
    );
    let stats = driver.stats();
    assert_eq!(stats.submitted, 1);
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.in_flight, 0);

    let error = driver
        .submit_len_path(path)
        .expect_err("closed driver rejects new work");
    assert!(matches!(error, Error::Closed));
    assert_eq!(driver.stats().rejected, 1);
    std::fs::remove_dir_all(root).expect("test directory removes");
}

#[cfg(feature = "platform-io")]
#[test]
fn abandoned_executor_task_completes_waiter_and_releases_resources() {
    let metrics = Arc::new(NativeFileStorageMetrics::default());
    let completion =
        IoCompletion::new_platform(Arc::clone(&metrics), PlatformIoOperation::LengthLookup);
    let waiter = completion.clone();
    let task = PlatformIoTask::Len {
        path: std::path::PathBuf::from("/trine-test/abandoned"),
        completion,
    };
    let resources = task.resources();
    let (completed, released) = crossbeam_channel::unbounded();
    let state = Arc::new(std::sync::atomic::AtomicU8::new(PLATFORM_IO_RUNNING));
    let scheduled = ScheduledPlatformIoTask {
        abandon_completion: Some(task.failure_completion()),
        task: Some(task),
        class: PlatformIoTaskClass::ThreadPoolManagedAsync,
        #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
        control_metrics: Arc::new(PlatformIoSchedulerMetrics::default()),
        resources: Some(resources.clone()),
        completed,
        state: Arc::clone(&state),
        finished: false,
    };

    drop(scheduled);
    let error = wait_for_io(waiter).expect_err("abandoned waiter completes with an error");
    assert!(error.to_string().contains("stopped before task completion"));
    assert_eq!(released.recv().expect("resources release"), resources);
    assert_eq!(
        state.load(std::sync::atomic::Ordering::Acquire),
        PLATFORM_IO_FAILED
    );
    assert_eq!(
        metrics.recorded_platform_io_tasks(),
        0,
        "a task abandoned before an executor starts it must not claim an execution class"
    );
}

#[cfg(feature = "platform-io")]
#[test]
fn platform_async_capability_comes_from_the_selected_backend_matrix() {
    let unsupported = PlatformIoBackendMatrix {
        kind: PlatformIoBackendKind::UnsupportedFallback,
        length_lookup: PlatformIoTaskClass::Unsupported,
        owned_random_read: PlatformIoTaskClass::Unsupported,
        optional_whole_object_read: PlatformIoTaskClass::Unsupported,
        temp_write_rename_publish: PlatformIoTaskClass::Unsupported,
        append_object_open: PlatformIoTaskClass::Unsupported,
        append: PlatformIoTaskClass::Unsupported,
        persist: PlatformIoTaskClass::Unsupported,
        wal_rewrite: PlatformIoTaskClass::Unsupported,
        object_delete: PlatformIoTaskClass::Unsupported,
        directory_create: PlatformIoTaskClass::Unsupported,
        directory_sync: PlatformIoTaskClass::Unsupported,
        directory_listing: PlatformIoTaskClass::Unsupported,
        writer_lease_acquire: PlatformIoTaskClass::Unsupported,
    };
    assert!(!unsupported.supports_platform_async_io());

    let mut managed = unsupported;
    managed.kind = PlatformIoBackendKind::ThreadPoolManaged;
    managed.owned_random_read = PlatformIoTaskClass::ThreadPoolManagedAsync;
    assert!(managed.supports_platform_async_io());
}

#[cfg(all(
    feature = "platform-io-native",
    any(target_os = "linux", target_os = "macos")
))]
#[test]
fn native_platform_runtime_has_multiple_futures_in_flight() {
    let root = platform_io_test_directory("native-concurrency");
    std::fs::create_dir_all(&root).expect("test directory creates");
    let metrics = Arc::new(NativeFileStorageMetrics::default());
    let driver = PlatformIoDriver::new(metrics).expect("platform I/O driver starts");
    if matches!(
        driver.backend_matrix().kind,
        PlatformIoBackendKind::ThreadPoolManaged
    ) {
        driver.close().expect("fallback driver closes");
        std::fs::remove_dir_all(root).expect("test directory removes");
        return;
    }

    let bytes: Arc<[u8]> = Arc::from(vec![0x5a; 1024 * 1024]);
    let mut completions = Vec::new();
    for index in 0..16 {
        let session = wait_for_io(
            driver
                .submit_open_append(root.join(format!("lane-{index}.wal")))
                .expect("append open submits"),
        )
        .expect("append session opens");
        completions.push(
            driver
                .submit_append(&session, Arc::clone(&bytes), DurabilityMode::Buffered)
                .expect("append submits"),
        );
    }
    for completion in completions {
        wait_for_io(completion).expect("append completes");
    }

    let stats = driver.stats();
    assert!(
        stats.native_max_in_flight > 1,
        "native runtime stayed serial: {stats:?}"
    );
    assert_eq!(stats.submitted, stats.completed);
    driver.close().expect("driver closes");
    std::fs::remove_dir_all(root).expect("test directory removes");
}

#[cfg(feature = "platform-io")]
fn platform_io_test_directory(label: &str) -> std::path::PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time follows epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "trine-kv-io-{label}-{}-{nonce}",
        std::process::id()
    ))
}

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
fn assert_unsupported_platform_matrix(matrix: &PlatformIoBackendMatrix) {
    assert_eq!(matrix.kind, PlatformIoBackendKind::UnsupportedFallback);
    assert_all_platform_rows(matrix, PlatformIoTaskClass::Unsupported);
}

#[cfg(feature = "platform-io")]
fn assert_platform_rows(
    matrix: &PlatformIoBackendMatrix,
    rows: &[(PlatformIoOperation, PlatformIoTaskClass)],
) {
    for (operation, class) in rows {
        assert_eq!(matrix.class_for(*operation), *class, "{operation:?}");
    }
}

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
fn assert_linux_native_platform_matrix(matrix: &PlatformIoBackendMatrix) {
    use PlatformIoOperation as Op;
    use PlatformIoTaskClass::{ThreadPoolManagedAsync, TruePlatformAsync};

    assert_eq!(matrix.kind, PlatformIoBackendKind::LinuxNative);
    for operation in ALL_PLATFORM_OPERATIONS {
        let expected = if matches!(operation, Op::DirectoryListing | Op::WriterLeaseAcquire) {
            ThreadPoolManagedAsync
        } else {
            TruePlatformAsync
        };
        assert_eq!(matrix.class_for(operation), expected, "{operation:?}");
    }
}

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
#[derive(Clone, Copy)]
struct PartialNativeRows {
    length_lookup: PlatformIoTaskClass,
    optional_whole_object_read: PlatformIoTaskClass,
    append_object_open: PlatformIoTaskClass,
    persist: PlatformIoTaskClass,
    object_delete: PlatformIoTaskClass,
    directory_create: PlatformIoTaskClass,
    directory_sync: PlatformIoTaskClass,
}

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
const WINDOWS_PARTIAL_NATIVE_ROWS: PartialNativeRows = PartialNativeRows {
    length_lookup: PlatformIoTaskClass::ThreadPoolManagedAsync,
    optional_whole_object_read: PlatformIoTaskClass::PlatformNativeAsyncButPartial,
    append_object_open: PlatformIoTaskClass::ThreadPoolManagedAsync,
    persist: PlatformIoTaskClass::ThreadPoolManagedAsync,
    object_delete: PlatformIoTaskClass::ThreadPoolManagedAsync,
    directory_create: PlatformIoTaskClass::ThreadPoolManagedAsync,
    directory_sync: PlatformIoTaskClass::ThreadPoolManagedAsync,
};

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
const MACOS_PARTIAL_NATIVE_ROWS: PartialNativeRows = PartialNativeRows {
    length_lookup: PlatformIoTaskClass::ThreadPoolManagedAsync,
    optional_whole_object_read: PlatformIoTaskClass::ThreadPoolManagedAsync,
    append_object_open: PlatformIoTaskClass::PlatformNativeAsyncButPartial,
    persist: PlatformIoTaskClass::PlatformNativeAsyncButPartial,
    object_delete: PlatformIoTaskClass::ThreadPoolManagedAsync,
    directory_create: PlatformIoTaskClass::ThreadPoolManagedAsync,
    directory_sync: PlatformIoTaskClass::PlatformNativeAsyncButPartial,
};

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
const BSD_SOLARISH_PARTIAL_NATIVE_ROWS: PartialNativeRows = PartialNativeRows {
    length_lookup: PlatformIoTaskClass::ThreadPoolManagedAsync,
    optional_whole_object_read: PlatformIoTaskClass::PlatformNativeAsyncButPartial,
    append_object_open: PlatformIoTaskClass::ThreadPoolManagedAsync,
    persist: PlatformIoTaskClass::PlatformNativeAsyncButPartial,
    object_delete: PlatformIoTaskClass::ThreadPoolManagedAsync,
    directory_create: PlatformIoTaskClass::ThreadPoolManagedAsync,
    directory_sync: PlatformIoTaskClass::PlatformNativeAsyncButPartial,
};

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
fn assert_partial_native_platform_matrix(
    matrix: &PlatformIoBackendMatrix,
    kind: PlatformIoBackendKind,
    rows: PartialNativeRows,
) {
    use PlatformIoOperation as Op;
    use PlatformIoTaskClass::PlatformNativeAsyncButPartial as Partial;
    use PlatformIoTaskClass::ThreadPoolManagedAsync as ThreadPool;

    assert_eq!(matrix.kind, kind);
    assert_platform_rows(
        matrix,
        &[
            (Op::LengthLookup, rows.length_lookup),
            (Op::OwnedRandomRead, Partial),
            (Op::OptionalWholeObjectRead, rows.optional_whole_object_read),
            (Op::TempWriteRenamePublish, Partial),
            (Op::AppendObjectOpen, rows.append_object_open),
            (Op::Append, Partial),
            (Op::Persist, rows.persist),
            (Op::WalRewrite, Partial),
            (Op::ObjectDelete, rows.object_delete),
            (Op::DirectoryCreate, rows.directory_create),
            (Op::DirectorySync, rows.directory_sync),
            (Op::DirectoryListing, ThreadPool),
            (Op::WriterLeaseAcquire, ThreadPool),
        ],
    );
}

#[test]
fn completion_rejects_double_finish() {
    let completion = IoCompletion::new();
    completion
        .complete(Ok(Arc::<[u8]>::from(&b"first"[..])))
        .expect("first completion succeeds");
    let error = completion
        .complete(Ok(Arc::<[u8]>::from(&b"second"[..])))
        .expect_err("second completion fails");
    assert!(error.to_string().contains("already finished"));
}
