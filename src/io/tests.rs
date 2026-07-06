use std::{
    sync::Arc,
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

use crate::runtime::{Runtime, RuntimeOptions};

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
    let matrix = PlatformIoDriver::backend_matrix();

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
            (Op::OptionalWholeObjectRead, Partial),
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
