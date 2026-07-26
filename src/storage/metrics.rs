use super::{
    Arc, AtomicU64, Duration, Instant, Ordering, PlatformIoOperationStats, Result, StorageFuture,
    StorageOperationMetric, StorageOperationStats,
};
#[cfg(feature = "platform-io")]
use super::{PlatformIoClassCounters, PlatformIoOperation, PlatformIoTaskClass};

#[derive(Debug, Default)]
pub(crate) struct NativeFileStorageMetrics {
    blocking_adapter: AtomicU64,
    platform_async_io: AtomicU64,
    platform_thread_pool_managed_async: AtomicU64,
    platform_blocking_fallback: AtomicU64,
    inline: AtomicU64,
    operations: NativeFileStorageOperationMetrics,
    #[cfg(feature = "platform-io")]
    platform_io_operations: NativeFilePlatformIoOperationMetrics,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StorageOperation {
    OpenRead,
    Len,
    ReadExactAt,
    ReadExactAtOwned,
    ReadObjectBytes,
    OpenAppend,
    Append,
    Persist,
    RewriteWal,
    AcquireWriterLease,
    CreateDirectoryAll,
    ListDirectoryFiles,
    SyncDirectoryAfterRenames,
    ReadCurrentManifest,
    PublishManifest,
    WriteObject,
    DeleteObject,
    ListObjects,
}

#[derive(Debug, Default)]
pub(super) struct NativeFileStorageOperationMetrics {
    open_read: NativeFileStorageOperationMetric,
    len: NativeFileStorageOperationMetric,
    read_exact_at: NativeFileStorageOperationMetric,
    read_exact_at_owned: NativeFileStorageOperationMetric,
    read_object_bytes: NativeFileStorageOperationMetric,
    open_append: NativeFileStorageOperationMetric,
    append: NativeFileStorageOperationMetric,
    persist: NativeFileStorageOperationMetric,
    rewrite_wal: NativeFileStorageOperationMetric,
    acquire_writer_lease: NativeFileStorageOperationMetric,
    create_directory_all: NativeFileStorageOperationMetric,
    list_directory_files: NativeFileStorageOperationMetric,
    sync_directory_after_renames: NativeFileStorageOperationMetric,
    read_current_manifest: NativeFileStorageOperationMetric,
    publish_manifest: NativeFileStorageOperationMetric,
    write_object: NativeFileStorageOperationMetric,
    delete_object: NativeFileStorageOperationMetric,
    list_objects: NativeFileStorageOperationMetric,
}

#[derive(Debug, Default)]
pub(super) struct NativeFileStorageOperationMetric {
    requests: AtomicU64,
    total_latency_micros: AtomicU64,
}

impl NativeFileStorageMetrics {
    pub(super) fn record_blocking_adapter_task(&self) {
        self.blocking_adapter.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "platform-io")]
    pub(super) fn record_platform_io_task(&self, class: PlatformIoTaskClass) {
        match class {
            PlatformIoTaskClass::TruePlatformAsync
            | PlatformIoTaskClass::PlatformNativeAsyncButPartial => {
                self.platform_async_io.fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::ThreadPoolManagedAsync => {
                self.platform_thread_pool_managed_async
                    .fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::BlockingFallback => {
                self.platform_blocking_fallback
                    .fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::Unsupported => {}
        }
    }

    #[cfg(feature = "platform-io")]
    pub(crate) fn record_platform_io_operation(
        &self,
        operation: PlatformIoOperation,
        class: PlatformIoTaskClass,
    ) {
        self.record_platform_io_task(class);
        self.platform_io_operations
            .metric(operation)
            .record_class(class);
    }

    pub(super) fn record_inline_task(&self) {
        self.inline.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_operation(&self, operation: StorageOperation, latency: Duration) {
        self.operations
            .metric(operation)
            .record(duration_to_micros_saturating(latency));
    }

    pub(super) fn blocking_adapter_tasks(&self) -> u64 {
        self.blocking_adapter.load(Ordering::Acquire)
    }

    pub(super) fn platform_async_io_tasks(&self) -> u64 {
        self.platform_async_io.load(Ordering::Acquire)
    }

    pub(super) fn platform_thread_pool_managed_async_tasks(&self) -> u64 {
        self.platform_thread_pool_managed_async
            .load(Ordering::Acquire)
    }

    pub(super) fn platform_blocking_fallback_tasks(&self) -> u64 {
        self.platform_blocking_fallback.load(Ordering::Acquire)
    }

    pub(super) fn inline_tasks(&self) -> u64 {
        self.inline.load(Ordering::Acquire)
    }

    pub(super) fn operation_stats(&self) -> StorageOperationStats {
        self.operations.snapshot()
    }

    pub(super) fn platform_io_operation_stats(&self) -> PlatformIoOperationStats {
        #[cfg(feature = "platform-io")]
        {
            self.platform_io_operations.snapshot()
        }
        #[cfg(not(feature = "platform-io"))]
        {
            let _ = self.inline_tasks();
            PlatformIoOperationStats::default()
        }
    }

    #[cfg(all(test, feature = "platform-io"))]
    pub(crate) fn recorded_platform_io_tasks(&self) -> u64 {
        self.platform_io_operations.snapshot().total().total()
    }
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Default)]
pub(super) struct NativeFilePlatformIoOperationMetrics {
    length_lookup: NativeFilePlatformIoClassMetrics,
    random_read: NativeFilePlatformIoClassMetrics,
    whole_object_read: NativeFilePlatformIoClassMetrics,
    temp_write_rename_publish: NativeFilePlatformIoClassMetrics,
    append_open: NativeFilePlatformIoClassMetrics,
    append: NativeFilePlatformIoClassMetrics,
    persist: NativeFilePlatformIoClassMetrics,
    wal_rewrite: NativeFilePlatformIoClassMetrics,
    delete: NativeFilePlatformIoClassMetrics,
    directory_create: NativeFilePlatformIoClassMetrics,
    directory_sync: NativeFilePlatformIoClassMetrics,
    directory_listing: NativeFilePlatformIoClassMetrics,
    writer_lease: NativeFilePlatformIoClassMetrics,
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Default)]
pub(super) struct NativeFilePlatformIoClassMetrics {
    true_platform_async: AtomicU64,
    platform_native_async_but_partial: AtomicU64,
    thread_pool_managed_async: AtomicU64,
    blocking_fallback: AtomicU64,
    unsupported: AtomicU64,
}

#[cfg(feature = "platform-io")]
impl NativeFilePlatformIoOperationMetrics {
    pub(super) fn metric(
        &self,
        operation: PlatformIoOperation,
    ) -> &NativeFilePlatformIoClassMetrics {
        match operation {
            PlatformIoOperation::LengthLookup => &self.length_lookup,
            PlatformIoOperation::OwnedRandomRead => &self.random_read,
            PlatformIoOperation::OptionalWholeObjectRead => &self.whole_object_read,
            PlatformIoOperation::TempWriteRenamePublish => &self.temp_write_rename_publish,
            PlatformIoOperation::AppendObjectOpen => &self.append_open,
            PlatformIoOperation::Append => &self.append,
            PlatformIoOperation::Persist => &self.persist,
            #[cfg(any(unix, windows))]
            PlatformIoOperation::WalRewrite => &self.wal_rewrite,
            PlatformIoOperation::ObjectDelete => &self.delete,
            PlatformIoOperation::DirectoryCreate => &self.directory_create,
            PlatformIoOperation::DirectorySync => &self.directory_sync,
            PlatformIoOperation::DirectoryListing => &self.directory_listing,
            #[cfg(any(unix, windows))]
            PlatformIoOperation::WriterLeaseAcquire => &self.writer_lease,
        }
    }

    pub(super) fn snapshot(&self) -> PlatformIoOperationStats {
        PlatformIoOperationStats {
            length_lookup: self.length_lookup.snapshot(),
            random_read: self.random_read.snapshot(),
            whole_object_read: self.whole_object_read.snapshot(),
            temp_write_rename_publish: self.temp_write_rename_publish.snapshot(),
            append_open: self.append_open.snapshot(),
            append: self.append.snapshot(),
            persist: self.persist.snapshot(),
            wal_rewrite: self.wal_rewrite.snapshot(),
            delete: self.delete.snapshot(),
            directory_create: self.directory_create.snapshot(),
            directory_sync: self.directory_sync.snapshot(),
            directory_listing: self.directory_listing.snapshot(),
            writer_lease: self.writer_lease.snapshot(),
        }
    }
}

#[cfg(feature = "platform-io")]
impl NativeFilePlatformIoClassMetrics {
    pub(super) fn record_class(&self, class: PlatformIoTaskClass) {
        match class {
            PlatformIoTaskClass::TruePlatformAsync => {
                self.true_platform_async.fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::PlatformNativeAsyncButPartial => {
                self.platform_native_async_but_partial
                    .fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::ThreadPoolManagedAsync => {
                self.thread_pool_managed_async
                    .fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::BlockingFallback => {
                self.blocking_fallback.fetch_add(1, Ordering::Relaxed);
            }
            PlatformIoTaskClass::Unsupported => {
                self.unsupported.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn snapshot(&self) -> PlatformIoClassCounters {
        PlatformIoClassCounters {
            true_platform_async: self.true_platform_async.load(Ordering::Acquire),
            platform_native_async_but_partial: self
                .platform_native_async_but_partial
                .load(Ordering::Acquire),
            thread_pool_managed_async: self.thread_pool_managed_async.load(Ordering::Acquire),
            blocking_fallback: self.blocking_fallback.load(Ordering::Acquire),
            unsupported: self.unsupported.load(Ordering::Acquire),
        }
    }
}

impl NativeFileStorageOperationMetrics {
    pub(super) fn metric(&self, operation: StorageOperation) -> &NativeFileStorageOperationMetric {
        match operation {
            StorageOperation::OpenRead => &self.open_read,
            StorageOperation::Len => &self.len,
            StorageOperation::ReadExactAt => &self.read_exact_at,
            StorageOperation::ReadExactAtOwned => &self.read_exact_at_owned,
            StorageOperation::ReadObjectBytes => &self.read_object_bytes,
            StorageOperation::OpenAppend => &self.open_append,
            StorageOperation::Append => &self.append,
            StorageOperation::Persist => &self.persist,
            StorageOperation::RewriteWal => &self.rewrite_wal,
            StorageOperation::AcquireWriterLease => &self.acquire_writer_lease,
            StorageOperation::CreateDirectoryAll => &self.create_directory_all,
            StorageOperation::ListDirectoryFiles => &self.list_directory_files,
            StorageOperation::SyncDirectoryAfterRenames => &self.sync_directory_after_renames,
            StorageOperation::ReadCurrentManifest => &self.read_current_manifest,
            StorageOperation::PublishManifest => &self.publish_manifest,
            StorageOperation::WriteObject => &self.write_object,
            StorageOperation::DeleteObject => &self.delete_object,
            StorageOperation::ListObjects => &self.list_objects,
        }
    }

    pub(super) fn snapshot(&self) -> StorageOperationStats {
        StorageOperationStats {
            open_read: self.open_read.snapshot(),
            len: self.len.snapshot(),
            read_exact_at: self.read_exact_at.snapshot(),
            read_exact_at_owned: self.read_exact_at_owned.snapshot(),
            read_object_bytes: self.read_object_bytes.snapshot(),
            open_append: self.open_append.snapshot(),
            append: self.append.snapshot(),
            persist: self.persist.snapshot(),
            rewrite_wal: self.rewrite_wal.snapshot(),
            acquire_writer_lease: self.acquire_writer_lease.snapshot(),
            create_directory_all: self.create_directory_all.snapshot(),
            list_directory_files: self.list_directory_files.snapshot(),
            sync_directory_after_renames: self.sync_directory_after_renames.snapshot(),
            read_current_manifest: self.read_current_manifest.snapshot(),
            publish_manifest: self.publish_manifest.snapshot(),
            write_object: self.write_object.snapshot(),
            delete_object: self.delete_object.snapshot(),
            list_objects: self.list_objects.snapshot(),
        }
    }
}

impl NativeFileStorageOperationMetric {
    pub(super) fn record(&self, latency_micros: u64) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.total_latency_micros
            .fetch_add(latency_micros, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> StorageOperationMetric {
        StorageOperationMetric {
            requests: self.requests.load(Ordering::Acquire),
            total_latency_micros: self.total_latency_micros.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct NativeFileStorageStats {
    pub(crate) uses_blocking_adapter: bool,
    pub(crate) uses_platform_io_driver: bool,
    pub(crate) uses_platform_async_io: bool,
    pub(crate) blocking_adapter_tasks: u64,
    pub(crate) blocking_adapter_queue_capacity: usize,
    pub(crate) blocking_adapter_queued_tasks: usize,
    pub(crate) blocking_adapter_submitted_tasks: u64,
    pub(crate) blocking_adapter_completed_tasks: u64,
    pub(crate) blocking_adapter_rejected_tasks: u64,
    pub(crate) blocking_adapter_total_runtime_micros: u64,
    pub(crate) platform_async_io_tasks: u64,
    pub(crate) platform_thread_pool_managed_async_tasks: u64,
    pub(crate) platform_blocking_fallback_tasks: u64,
    pub(crate) inline_tasks: u64,
    pub(crate) operations: StorageOperationStats,
    pub(crate) platform_io_operations: PlatformIoOperationStats,
}

pub(super) fn record_timed_storage_result<T>(
    metrics: &NativeFileStorageMetrics,
    operation: StorageOperation,
    task: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let started = Instant::now();
    let result = task();
    metrics.record_operation(operation, started.elapsed());
    result
}

pub(super) fn record_timed_storage_future<'op, T>(
    metrics: Arc<NativeFileStorageMetrics>,
    operation: StorageOperation,
    future: StorageFuture<'op, T>,
) -> StorageFuture<'op, T>
where
    T: 'op,
{
    Box::pin(async move {
        let started = Instant::now();
        let result = future.await;
        metrics.record_operation(operation, started.elapsed());
        result
    })
}

pub(super) fn duration_to_micros_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}
