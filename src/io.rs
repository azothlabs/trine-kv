use std::{
    future::Future,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

#[cfg(all(feature = "platform-io", any(unix, windows)))]
use std::fs::File;
#[cfg(feature = "platform-io")]
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    thread,
};

#[cfg(feature = "platform-io")]
use crate::storage::NativeFileStorageMetrics;
use crate::{
    error::{Error, Result},
    options::DurabilityMode,
    runtime::Runtime,
    storage::StorageReadBuffer,
};

#[cfg(feature = "platform-io")]
mod blocking_fallback;
#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    any(unix, windows)
))]
mod platform_backend;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IoDriverKind {
    Inline,
    BlockingAdapter,
    ReadinessFallback,
    Platform,
}

impl IoDriverKind {
    pub(crate) const fn is_blocking_adapter(self) -> bool {
        matches!(self, Self::BlockingAdapter)
    }
}

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlatformIoBackendKind {
    ThreadPoolManaged,
    LinuxNative,
    WindowsNative,
    MacOsNative,
    FreeBsdNative,
    SolarishNative,
    UnixFallback,
    UnsupportedFallback,
}

#[cfg(feature = "platform-io")]
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlatformIoTaskClass {
    TruePlatformAsync,
    PlatformNativeAsyncButPartial,
    ThreadPoolManagedAsync,
    BlockingFallback,
    Unsupported,
}

#[cfg(feature = "platform-io")]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlatformIoOperation {
    LengthLookup,
    OwnedRandomRead,
    OptionalWholeObjectRead,
    TempWriteRenamePublish,
    AppendObjectOpen,
    Append,
    Persist,
    #[cfg(any(unix, windows))]
    WalRewrite,
    ObjectDelete,
    DirectoryCreate,
    DirectorySync,
    DirectoryListing,
    #[cfg(any(unix, windows))]
    WriterLeaseAcquire,
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlatformIoBackendMatrix {
    pub(crate) kind: PlatformIoBackendKind,
    pub(crate) length_lookup: PlatformIoTaskClass,
    pub(crate) owned_random_read: PlatformIoTaskClass,
    pub(crate) optional_whole_object_read: PlatformIoTaskClass,
    pub(crate) temp_write_rename_publish: PlatformIoTaskClass,
    pub(crate) append_object_open: PlatformIoTaskClass,
    pub(crate) append: PlatformIoTaskClass,
    pub(crate) persist: PlatformIoTaskClass,
    pub(crate) wal_rewrite: PlatformIoTaskClass,
    pub(crate) object_delete: PlatformIoTaskClass,
    pub(crate) directory_create: PlatformIoTaskClass,
    pub(crate) directory_sync: PlatformIoTaskClass,
    pub(crate) directory_listing: PlatformIoTaskClass,
    pub(crate) writer_lease_acquire: PlatformIoTaskClass,
}

#[cfg(feature = "platform-io")]
impl PlatformIoBackendMatrix {
    pub(crate) const fn class_for(self, operation: PlatformIoOperation) -> PlatformIoTaskClass {
        match operation {
            PlatformIoOperation::LengthLookup => self.length_lookup,
            PlatformIoOperation::OwnedRandomRead => self.owned_random_read,
            PlatformIoOperation::OptionalWholeObjectRead => self.optional_whole_object_read,
            PlatformIoOperation::TempWriteRenamePublish => self.temp_write_rename_publish,
            PlatformIoOperation::AppendObjectOpen => self.append_object_open,
            PlatformIoOperation::Append => self.append,
            PlatformIoOperation::Persist => self.persist,
            #[cfg(any(unix, windows))]
            PlatformIoOperation::WalRewrite => self.wal_rewrite,
            PlatformIoOperation::ObjectDelete => self.object_delete,
            PlatformIoOperation::DirectoryCreate => self.directory_create,
            PlatformIoOperation::DirectorySync => self.directory_sync,
            PlatformIoOperation::DirectoryListing => self.directory_listing,
            #[cfg(any(unix, windows))]
            PlatformIoOperation::WriterLeaseAcquire => self.writer_lease_acquire,
        }
    }

    const fn supports_platform_async_io(self) -> bool {
        self.length_lookup.is_async()
            || self.owned_random_read.is_async()
            || self.optional_whole_object_read.is_async()
            || self.temp_write_rename_publish.is_async()
            || self.append_object_open.is_async()
            || self.append.is_async()
            || self.persist.is_async()
            || self.wal_rewrite.is_async()
            || self.object_delete.is_async()
            || self.directory_create.is_async()
            || self.directory_sync.is_async()
            || self.directory_listing.is_async()
            || self.writer_lease_acquire.is_async()
    }
}

#[cfg(feature = "platform-io")]
impl PlatformIoTaskClass {
    const fn is_async(self) -> bool {
        matches!(
            self,
            Self::TruePlatformAsync
                | Self::PlatformNativeAsyncButPartial
                | Self::ThreadPoolManagedAsync
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IoDriverInfo {
    kind: IoDriverKind,
}

impl IoDriverInfo {
    pub(crate) const fn inline() -> Self {
        Self {
            kind: IoDriverKind::Inline,
        }
    }

    pub(crate) const fn blocking_adapter() -> Self {
        Self {
            kind: IoDriverKind::BlockingAdapter,
        }
    }

    #[allow(dead_code)]
    pub(crate) const fn readiness_fallback() -> Self {
        Self {
            kind: IoDriverKind::ReadinessFallback,
        }
    }

    #[allow(dead_code)]
    pub(crate) const fn platform() -> Self {
        Self {
            kind: IoDriverKind::Platform,
        }
    }

    pub(crate) const fn kind(self) -> IoDriverKind {
        self.kind
    }
}

#[derive(Debug)]
pub(crate) struct IoCompletion<T> {
    state: Arc<Mutex<IoCompletionState<T>>>,
}

#[derive(Debug)]
struct IoCompletionState<T> {
    result: Option<Result<T>>,
    waker: Option<Waker>,
    #[cfg(feature = "platform-io")]
    platform_metric: Option<PlatformIoCompletionMetric>,
}

#[cfg(feature = "platform-io")]
#[derive(Debug)]
struct PlatformIoCompletionMetric {
    metrics: Arc<NativeFileStorageMetrics>,
    operation: PlatformIoOperation,
    execution_class: Option<PlatformIoTaskClass>,
}

impl<T> IoCompletion<T> {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(IoCompletionState {
                result: None,
                waker: None,
                #[cfg(feature = "platform-io")]
                platform_metric: None,
            })),
        }
    }

    #[cfg(feature = "platform-io")]
    fn new_platform(
        metrics: Arc<NativeFileStorageMetrics>,
        operation: PlatformIoOperation,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(IoCompletionState {
                result: None,
                waker: None,
                platform_metric: Some(PlatformIoCompletionMetric {
                    metrics,
                    operation,
                    execution_class: None,
                }),
            })),
        }
    }

    #[cfg(feature = "platform-io")]
    fn mark_platform_execution(&self, class: PlatformIoTaskClass) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::runtime_busy("I/O completion state is poisoned"))?;
        if state.result.is_some() {
            return Err(Error::runtime_busy(
                "platform I/O execution started after completion",
            ));
        }
        let metric = state.platform_metric.as_mut().ok_or_else(|| {
            Error::runtime_busy("platform I/O completion has no execution metric")
        })?;
        if metric.execution_class.replace(class).is_some() {
            return Err(Error::runtime_busy(
                "platform I/O execution class was assigned more than once",
            ));
        }
        Ok(())
    }

    pub(crate) fn complete(&self, result: Result<T>) -> Result<()> {
        let waker = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::runtime_busy("I/O completion state is poisoned"))?;
            if state.result.is_some() {
                return Err(Error::runtime_busy("I/O completion already finished"));
            }
            #[cfg(feature = "platform-io")]
            if let Some(metric) = state.platform_metric.take()
                && let Some(class) = metric.execution_class
            {
                metric
                    .metrics
                    .record_platform_io_operation(metric.operation, class);
            }
            state.result = Some(result);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn is_finished(&self) -> Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::runtime_busy("I/O completion state is poisoned"))?;
        Ok(state.result.is_some())
    }
}

impl<T> Clone for IoCompletion<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<T> Future for IoCompletion<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Ok(mut state) = self.state.lock() else {
            return Poll::Ready(Err(Error::runtime_busy("I/O completion state is poisoned")));
        };
        if let Some(result) = state.result.take() {
            Poll::Ready(result)
        } else {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

pub(crate) trait IoReadObject: Send + Sync {
    fn len_io(&self) -> Result<IoCompletion<u64>>;

    fn read_exact_at_owned_io(
        &self,
        offset: usize,
        len: usize,
    ) -> Result<IoCompletion<StorageReadBuffer>>;
}

pub(crate) trait IoAppendObject: Send {
    fn append_io(&self, bytes: Arc<[u8]>, durability: DurabilityMode) -> Result<IoCompletion<()>>;

    fn persist_io(&self, durability: DurabilityMode) -> Result<IoCompletion<()>>;
}

pub(crate) trait IoDriver {
    fn info(&self) -> IoDriverInfo;

    fn submit_len<F>(&self, operation: F) -> Result<IoCompletion<u64>>
    where
        F: FnOnce() -> Result<u64> + Send + 'static;

    fn submit_read_exact_at_owned<F>(
        &self,
        operation: F,
    ) -> Result<IoCompletion<StorageReadBuffer>>
    where
        F: FnOnce() -> Result<StorageReadBuffer> + Send + 'static;

    fn submit_append<F>(&self, operation: F) -> Result<IoCompletion<()>>
    where
        F: FnOnce() -> Result<()> + Send + 'static;

    fn submit_sync<F>(&self, operation: F) -> Result<IoCompletion<()>>
    where
        F: FnOnce() -> Result<()> + Send + 'static;

    #[allow(dead_code)]
    fn step(&self) -> Result<usize>;

    #[allow(dead_code)]
    fn drain(&self) -> Result<usize>;
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct InlineIoDriver;

impl InlineIoDriver {
    fn submit_inline<T>(operation: impl FnOnce() -> Result<T>) -> Result<IoCompletion<T>> {
        let completion = IoCompletion::new();
        completion.complete(operation())?;
        Ok(completion)
    }
}

impl IoDriver for InlineIoDriver {
    fn info(&self) -> IoDriverInfo {
        IoDriverInfo::inline()
    }

    fn submit_len<F>(&self, operation: F) -> Result<IoCompletion<u64>>
    where
        F: FnOnce() -> Result<u64> + Send + 'static,
    {
        Self::submit_inline(operation)
    }

    fn submit_read_exact_at_owned<F>(&self, operation: F) -> Result<IoCompletion<StorageReadBuffer>>
    where
        F: FnOnce() -> Result<StorageReadBuffer> + Send + 'static,
    {
        Self::submit_inline(operation)
    }

    fn submit_append<F>(&self, operation: F) -> Result<IoCompletion<()>>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        Self::submit_inline(operation)
    }

    fn submit_sync<F>(&self, operation: F) -> Result<IoCompletion<()>>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        Self::submit_inline(operation)
    }

    fn step(&self) -> Result<usize> {
        Ok(0)
    }

    fn drain(&self) -> Result<usize> {
        Ok(0)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BlockingAdapterIoDriver {
    runtime: Runtime,
}

impl BlockingAdapterIoDriver {
    pub(crate) fn new(runtime: Runtime) -> Self {
        Self { runtime }
    }

    fn submit_blocking<T>(
        &self,
        operation: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<IoCompletion<T>>
    where
        T: Send + 'static,
    {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.runtime.spawn_blocking(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(operation))
                .unwrap_or_else(|_| Err(Error::runtime_busy("blocking I/O task panicked")));
            let completed = completion.complete(result);
            debug_assert!(completed.is_ok());
        })?;
        Ok(waiter)
    }
}

impl IoDriver for BlockingAdapterIoDriver {
    fn info(&self) -> IoDriverInfo {
        IoDriverInfo::blocking_adapter()
    }

    fn submit_len<F>(&self, operation: F) -> Result<IoCompletion<u64>>
    where
        F: FnOnce() -> Result<u64> + Send + 'static,
    {
        self.submit_blocking(operation)
    }

    fn submit_read_exact_at_owned<F>(&self, operation: F) -> Result<IoCompletion<StorageReadBuffer>>
    where
        F: FnOnce() -> Result<StorageReadBuffer> + Send + 'static,
    {
        self.submit_blocking(operation)
    }

    fn submit_append<F>(&self, operation: F) -> Result<IoCompletion<()>>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        self.submit_blocking(operation)
    }

    fn submit_sync<F>(&self, operation: F) -> Result<IoCompletion<()>>
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        self.submit_blocking(operation)
    }

    fn step(&self) -> Result<usize> {
        Ok(0)
    }

    fn drain(&self) -> Result<usize> {
        Ok(0)
    }
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Clone)]
pub(crate) struct PlatformIoDriver {
    inner: Arc<PlatformIoDriverInner>,
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlatformIoAppendSession {
    path: Arc<PathBuf>,
}

#[cfg(feature = "platform-io")]
impl PlatformIoAppendSession {
    fn opened(path: PathBuf) -> Self {
        Self {
            path: Arc::new(path),
        }
    }
}

#[cfg(feature = "platform-io")]
#[derive(Debug)]
pub(crate) struct PlatformIoPublishPlan {
    operation: PlatformIoOperation,
    path: PathBuf,
    temporary_path: PathBuf,
    bytes: Arc<[u8]>,
    durability: DurabilityMode,
    create_parent: PlatformIoParentCreation,
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlatformIoParentCreation {
    Existing,
    CreateAll,
}

#[cfg(feature = "platform-io")]
impl PlatformIoPublishPlan {
    pub(crate) fn manifest(
        path: PathBuf,
        temporary_path: PathBuf,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Self {
        Self {
            operation: PlatformIoOperation::TempWriteRenamePublish,
            path,
            temporary_path,
            bytes,
            durability,
            create_parent: PlatformIoParentCreation::Existing,
        }
    }

    pub(crate) fn object(
        path: PathBuf,
        temporary_path: PathBuf,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Self {
        Self {
            operation: PlatformIoOperation::TempWriteRenamePublish,
            path,
            temporary_path,
            bytes,
            durability,
            create_parent: PlatformIoParentCreation::CreateAll,
        }
    }

    pub(crate) fn wal_rewrite(
        path: PathBuf,
        temporary_path: PathBuf,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Self {
        Self {
            operation: PlatformIoOperation::WalRewrite,
            path,
            temporary_path,
            bytes,
            durability,
            create_parent: PlatformIoParentCreation::CreateAll,
        }
    }
}

#[cfg(feature = "platform-io")]
#[derive(Debug)]
struct PlatformIoDriverInner {
    state: Arc<AtomicU8>,
    #[allow(dead_code)]
    matrix: PlatformIoBackendMatrix,
    metrics: Arc<NativeFileStorageMetrics>,
    scheduler_metrics: Arc<PlatformIoSchedulerMetrics>,
    close_lock: Mutex<()>,
    sender: Mutex<Option<crossbeam_channel::Sender<PlatformIoTask>>>,
    scheduler: Mutex<Option<thread::JoinHandle<()>>>,
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Default)]
struct PlatformIoSchedulerMetrics {
    queued: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    native_in_flight: AtomicUsize,
    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    native_max_in_flight: AtomicUsize,
    submitted: AtomicU64,
    completed: AtomicU64,
    rejected: AtomicU64,
}

#[cfg(all(feature = "platform-io", test))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PlatformIoDriverStats {
    pub(crate) queue_capacity: usize,
    pub(crate) queued: usize,
    pub(crate) in_flight: usize,
    pub(crate) max_in_flight: usize,
    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    pub(crate) native_in_flight: usize,
    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    pub(crate) native_max_in_flight: usize,
    pub(crate) submitted: u64,
    pub(crate) completed: u64,
    pub(crate) rejected: u64,
}

#[cfg(feature = "platform-io")]
struct BlockingFallbackExecutor {
    sender: Option<crossbeam_channel::Sender<ScheduledPlatformIoTask>>,
    workers: Vec<thread::JoinHandle<()>>,
}

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    any(unix, windows)
))]
struct NativeCompletionExecutor {
    sender: Option<futures::channel::mpsc::Sender<ScheduledPlatformIoTask>>,
    worker: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "platform-io")]
impl Drop for BlockingFallbackExecutor {
    fn drop(&mut self) {
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    any(unix, windows)
))]
impl Drop for NativeCompletionExecutor {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(feature = "platform-io")]
const PLATFORM_IO_THREAD_POOL_WORKERS: usize = 4;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_THREAD_POOL_QUEUE_DEPTH: usize = 1024;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_SCHEDULER_QUEUE_DEPTH: usize = 1024;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_MAX_IN_FLIGHT: usize = 256;

#[cfg(feature = "platform-io")]
const PLATFORM_IO_RUNNING: u8 = 0;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_CLOSING: u8 = 1;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_CLOSED: u8 = 2;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_FAILED: u8 = 3;

#[cfg(feature = "platform-io")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PlatformIoResourceKey {
    Object(PathBuf),
    Directory(PathBuf),
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlatformIoAccess {
    Shared,
    Exclusive,
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Clone, PartialEq, Eq)]
struct PlatformIoResourceRequest {
    key: PlatformIoResourceKey,
    access: PlatformIoAccess,
}

#[cfg(feature = "platform-io")]
struct PendingPlatformIoTask {
    task: PlatformIoTask,
    resources: Vec<PlatformIoResourceRequest>,
}

#[cfg(feature = "platform-io")]
struct ScheduledPlatformIoTask {
    task: Option<PlatformIoTask>,
    abandon_completion: Option<PlatformIoFailureCompletion>,
    class: PlatformIoTaskClass,
    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    control_metrics: Arc<PlatformIoSchedulerMetrics>,
    resources: Option<Vec<PlatformIoResourceRequest>>,
    completed: crossbeam_channel::Sender<Vec<PlatformIoResourceRequest>>,
    state: Arc<AtomicU8>,
    finished: bool,
}

#[cfg(feature = "platform-io")]
impl ScheduledPlatformIoTask {
    fn take_task(&mut self) -> PlatformIoTask {
        self.task
            .take()
            .expect("scheduled platform I/O task starts only once")
    }

    fn complete_start_error(mut self, message: &str) -> Vec<PlatformIoResourceRequest> {
        self.take_task().complete_start_error(message);
        self.finished = true;
        self.resources
            .take()
            .expect("scheduled platform I/O resources remain available")
    }

    fn complete_unsupported(mut self, message: &str) -> Vec<PlatformIoResourceRequest> {
        let task = self.take_task();
        let marked = task.mark_execution_class(self.class);
        debug_assert!(marked.is_ok());
        task.complete_start_error(message);
        self.finished = true;
        self.resources
            .take()
            .expect("scheduled platform I/O resources remain available")
    }

    fn finish(mut self) {
        self.finished = true;
        let resources = self
            .resources
            .take()
            .expect("scheduled platform I/O resources finish only once");
        let _ = self.completed.send(resources);
    }
}

#[cfg(feature = "platform-io")]
impl Drop for ScheduledPlatformIoTask {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // An executor may disappear after scheduler admission. Completing and
        // releasing here preserves the invariant that every accepted task has
        // one terminal result and one resource release.
        if let Some(completion) = self.abandon_completion.take() {
            completion.complete();
        }
        self.state.store(PLATFORM_IO_FAILED, Ordering::Release);
        if let Some(resources) = self.resources.take() {
            let _ = self.completed.send(resources);
        }
    }
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Default)]
struct PlatformIoResourceState {
    shared: usize,
    exclusive: bool,
}

#[cfg(feature = "platform-io")]
#[derive(Debug, Default)]
struct PlatformIoResourceTable {
    entries: HashMap<PlatformIoResourceKey, PlatformIoResourceState>,
}

#[cfg(feature = "platform-io")]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
enum PlatformIoTask {
    Len {
        path: PathBuf,
        completion: IoCompletion<u64>,
    },
    ReadExactAtOwned {
        path: PathBuf,
        offset: usize,
        len: usize,
        completion: IoCompletion<StorageReadBuffer>,
    },
    ReadOptional {
        path: PathBuf,
        max_bytes: usize,
        completion: IoCompletion<Option<Arc<[u8]>>>,
    },
    Publish {
        plan: PlatformIoPublishPlan,
        completion: IoCompletion<()>,
    },
    Append {
        session: PlatformIoAppendSession,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
        completion: IoCompletion<()>,
    },
    OpenAppend {
        path: PathBuf,
        completion: IoCompletion<PlatformIoAppendSession>,
    },
    Persist {
        session: PlatformIoAppendSession,
        durability: DurabilityMode,
        completion: IoCompletion<()>,
    },
    Delete {
        path: PathBuf,
        completion: IoCompletion<()>,
    },
    CreateDirAll {
        path: PathBuf,
        completion: IoCompletion<()>,
    },
    SyncDir {
        path: PathBuf,
        completion: IoCompletion<()>,
    },
    ListFilePaths {
        path: PathBuf,
        completion: IoCompletion<Vec<PathBuf>>,
    },
    #[cfg(any(unix, windows))]
    AcquireWriterLease {
        path: PathBuf,
        owner: Arc<[u8]>,
        completion: IoCompletion<File>,
    },
}

#[cfg(feature = "platform-io")]
enum PlatformIoFailureCompletion {
    Len(IoCompletion<u64>),
    Read(IoCompletion<StorageReadBuffer>),
    Optional(IoCompletion<Option<Arc<[u8]>>>),
    Paths(IoCompletion<Vec<PathBuf>>),
    AppendSession(IoCompletion<PlatformIoAppendSession>),
    Unit(IoCompletion<()>),
    #[cfg(any(unix, windows))]
    Lease(IoCompletion<File>),
}

#[cfg(feature = "platform-io")]
impl PlatformIoFailureCompletion {
    fn complete(self) {
        let error = || Error::runtime_busy("platform I/O executor stopped before task completion");
        match self {
            Self::Len(completion) => {
                let _ = completion.complete(Err(error()));
            }
            Self::Read(completion) => {
                let _ = completion.complete(Err(error()));
            }
            Self::Optional(completion) => {
                let _ = completion.complete(Err(error()));
            }
            Self::Paths(completion) => {
                let _ = completion.complete(Err(error()));
            }
            Self::AppendSession(completion) => {
                let _ = completion.complete(Err(error()));
            }
            Self::Unit(completion) => {
                let _ = completion.complete(Err(error()));
            }
            #[cfg(any(unix, windows))]
            Self::Lease(completion) => {
                let _ = completion.complete(Err(error()));
            }
        }
    }
}

#[cfg(feature = "platform-io")]
impl PlatformIoDriver {
    pub(crate) fn new(metrics: Arc<NativeFileStorageMetrics>) -> Result<Self> {
        let state = Arc::new(AtomicU8::new(PLATFORM_IO_RUNNING));
        let scheduler_metrics = Arc::new(PlatformIoSchedulerMetrics::default());
        #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
        let native = platform_backend::start_worker(Arc::clone(&state));
        #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
        let matrix = if native.is_some() {
            platform_backend::matrix()
        } else {
            blocking_fallback::matrix()
        };
        #[cfg(not(all(feature = "platform-io-native", any(unix, windows))))]
        let matrix = blocking_fallback::matrix();

        let fallback = start_blocking_fallback_executor()?;
        let (sender, receiver) = crossbeam_channel::bounded(PLATFORM_IO_SCHEDULER_QUEUE_DEPTH);
        let scheduler_state = Arc::clone(&state);
        let scheduler_control_metrics = Arc::clone(&scheduler_metrics);
        let scheduler = thread::Builder::new()
            .name("trine-kv-platform-io-scheduler".to_owned())
            .spawn(move || {
                PlatformIoScheduler::new(
                    receiver,
                    matrix,
                    fallback,
                    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
                    native,
                    scheduler_state,
                    scheduler_control_metrics,
                )
                .run();
            })
            .map_err(Error::Io)?;

        Ok(Self {
            inner: Arc::new(PlatformIoDriverInner {
                state,
                matrix,
                metrics,
                scheduler_metrics,
                close_lock: Mutex::new(()),
                sender: Mutex::new(Some(sender)),
                scheduler: Mutex::new(Some(scheduler)),
            }),
        })
    }

    pub(crate) const fn info() -> IoDriverInfo {
        IoDriverInfo::platform()
    }

    #[allow(dead_code)]
    pub(crate) fn backend_matrix(&self) -> PlatformIoBackendMatrix {
        self.inner.matrix
    }

    pub(crate) fn submit_len_path(&self, path: PathBuf) -> Result<IoCompletion<u64>> {
        let completion = self.completion(PlatformIoOperation::LengthLookup);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::Len { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_read_exact_at_owned_path(
        &self,
        path: PathBuf,
        offset: usize,
        len: usize,
    ) -> Result<IoCompletion<StorageReadBuffer>> {
        let completion = self.completion(PlatformIoOperation::OwnedRandomRead);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::ReadExactAtOwned {
            path,
            offset,
            len,
            completion,
        })?;
        Ok(waiter)
    }

    pub(crate) fn submit_read_optional_path(
        &self,
        path: PathBuf,
        max_bytes: usize,
    ) -> Result<IoCompletion<Option<Arc<[u8]>>>> {
        let completion = self.completion(PlatformIoOperation::OptionalWholeObjectRead);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::ReadOptional {
            path,
            max_bytes,
            completion,
        })?;
        Ok(waiter)
    }

    pub(crate) fn submit_publish(&self, plan: PlatformIoPublishPlan) -> Result<IoCompletion<()>> {
        let completion = self.completion(plan.operation);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::Publish { plan, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_append(
        &self,
        session: &PlatformIoAppendSession,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<IoCompletion<()>> {
        let completion = self.completion(PlatformIoOperation::Append);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::Append {
            session: session.clone(),
            bytes,
            durability,
            completion,
        })?;
        Ok(waiter)
    }

    pub(crate) fn submit_open_append(
        &self,
        path: PathBuf,
    ) -> Result<IoCompletion<PlatformIoAppendSession>> {
        let completion = self.completion(PlatformIoOperation::AppendObjectOpen);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::OpenAppend { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_persist(
        &self,
        session: &PlatformIoAppendSession,
        durability: DurabilityMode,
    ) -> Result<IoCompletion<()>> {
        let completion = self.completion(PlatformIoOperation::Persist);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::Persist {
            session: session.clone(),
            durability,
            completion,
        })?;
        Ok(waiter)
    }

    pub(crate) fn submit_delete_path(&self, path: PathBuf) -> Result<IoCompletion<()>> {
        let completion = self.completion(PlatformIoOperation::ObjectDelete);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::Delete { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_create_dir_all_path(&self, path: PathBuf) -> Result<IoCompletion<()>> {
        let completion = self.completion(PlatformIoOperation::DirectoryCreate);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::CreateDirAll { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_sync_dir_path(&self, path: PathBuf) -> Result<IoCompletion<()>> {
        let completion = self.completion(PlatformIoOperation::DirectorySync);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::SyncDir { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_list_file_paths_path(
        &self,
        path: PathBuf,
    ) -> Result<IoCompletion<Vec<PathBuf>>> {
        let completion = self.completion(PlatformIoOperation::DirectoryListing);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::ListFilePaths { path, completion })?;
        Ok(waiter)
    }

    #[cfg(any(unix, windows))]
    pub(crate) fn submit_acquire_writer_lease_path(
        &self,
        path: PathBuf,
        owner: Arc<[u8]>,
    ) -> Result<IoCompletion<File>> {
        let completion = self.completion(PlatformIoOperation::WriterLeaseAcquire);
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::AcquireWriterLease {
            path,
            owner,
            completion,
        })?;
        Ok(waiter)
    }

    fn completion<T>(&self, operation: PlatformIoOperation) -> IoCompletion<T> {
        IoCompletion::new_platform(Arc::clone(&self.inner.metrics), operation)
    }

    pub(crate) fn supports_platform_async_io(&self) -> bool {
        self.inner.matrix.supports_platform_async_io()
    }

    fn submit_task(&self, task: PlatformIoTask) -> Result<()> {
        match self.inner.state.load(Ordering::Acquire) {
            PLATFORM_IO_RUNNING => {}
            PLATFORM_IO_FAILED => {
                self.inner
                    .scheduler_metrics
                    .rejected
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Error::runtime_busy(
                    "platform I/O scheduler is in a failed state",
                ));
            }
            PLATFORM_IO_CLOSING | PLATFORM_IO_CLOSED => {
                self.inner
                    .scheduler_metrics
                    .rejected
                    .fetch_add(1, Ordering::Relaxed);
                return Err(Error::Closed);
            }
            _ => {
                return Err(Error::runtime_busy(
                    "platform I/O scheduler has an invalid state",
                ));
            }
        }

        let sender = self
            .inner
            .sender
            .lock()
            .map_err(|_| Error::runtime_busy("platform I/O scheduler state is poisoned"))?
            .as_ref()
            .cloned();
        let Some(sender) = sender else {
            self.inner
                .scheduler_metrics
                .rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::Closed);
        };
        if !reserve_platform_io_queue_slot(&self.inner.scheduler_metrics) {
            self.inner
                .scheduler_metrics
                .rejected
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::runtime_busy("platform I/O scheduler queue is full"));
        }
        self.inner
            .scheduler_metrics
            .submitted
            .fetch_add(1, Ordering::Relaxed);
        match sender.try_send(task) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.inner
                    .scheduler_metrics
                    .queued
                    .fetch_sub(1, Ordering::AcqRel);
                self.inner
                    .scheduler_metrics
                    .submitted
                    .fetch_sub(1, Ordering::Relaxed);
                self.inner
                    .scheduler_metrics
                    .rejected
                    .fetch_add(1, Ordering::Relaxed);
                if error.is_full() {
                    Err(Error::runtime_busy("platform I/O scheduler queue is full"))
                } else {
                    Err(Error::Closed)
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn stats(&self) -> PlatformIoDriverStats {
        PlatformIoDriverStats {
            queue_capacity: PLATFORM_IO_SCHEDULER_QUEUE_DEPTH,
            queued: self.inner.scheduler_metrics.queued.load(Ordering::Acquire),
            in_flight: self
                .inner
                .scheduler_metrics
                .in_flight
                .load(Ordering::Acquire),
            max_in_flight: self
                .inner
                .scheduler_metrics
                .max_in_flight
                .load(Ordering::Acquire),
            #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
            native_in_flight: self
                .inner
                .scheduler_metrics
                .native_in_flight
                .load(Ordering::Acquire),
            #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
            native_max_in_flight: self
                .inner
                .scheduler_metrics
                .native_max_in_flight
                .load(Ordering::Acquire),
            submitted: self
                .inner
                .scheduler_metrics
                .submitted
                .load(Ordering::Acquire),
            completed: self
                .inner
                .scheduler_metrics
                .completed
                .load(Ordering::Acquire),
            rejected: self
                .inner
                .scheduler_metrics
                .rejected
                .load(Ordering::Acquire),
        }
    }

    pub(crate) fn close(&self) -> Result<()> {
        self.inner.close()
    }
}

#[cfg(feature = "platform-io")]
fn reserve_platform_io_queue_slot(metrics: &PlatformIoSchedulerMetrics) -> bool {
    metrics
        .queued
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |queued| {
            (queued < PLATFORM_IO_SCHEDULER_QUEUE_DEPTH).then_some(queued + 1)
        })
        .is_ok()
}

#[cfg(feature = "platform-io")]
impl PlatformIoDriverInner {
    fn close(&self) -> Result<()> {
        let _close = self
            .close_lock
            .lock()
            .map_err(|_| Error::runtime_busy("platform I/O close state is poisoned"))?;
        match self.state.compare_exchange(
            PLATFORM_IO_RUNNING,
            PLATFORM_IO_CLOSING,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(PLATFORM_IO_FAILED) => {}
            Err(PLATFORM_IO_CLOSING | PLATFORM_IO_CLOSED) => return Ok(()),
            Err(_) => {
                return Err(Error::runtime_busy(
                    "platform I/O scheduler has an invalid state",
                ));
            }
        }

        self.sender
            .lock()
            .map_err(|_| Error::runtime_busy("platform I/O scheduler state is poisoned"))?
            .take();
        if let Some(scheduler) = self
            .scheduler
            .lock()
            .map_err(|_| Error::runtime_busy("platform I/O scheduler join state is poisoned"))?
            .take()
        {
            scheduler
                .join()
                .map_err(|_| Error::runtime_busy("platform I/O scheduler panicked"))?;
        }
        self.state.store(PLATFORM_IO_CLOSED, Ordering::Release);
        Ok(())
    }
}

#[cfg(feature = "platform-io")]
impl Drop for PlatformIoDriverInner {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(feature = "platform-io")]
impl PlatformIoResourceTable {
    fn can_acquire(&self, requests: &[PlatformIoResourceRequest]) -> bool {
        requests.iter().all(|request| {
            let Some(state) = self.entries.get(&request.key) else {
                return true;
            };
            match request.access {
                PlatformIoAccess::Shared => !state.exclusive,
                PlatformIoAccess::Exclusive => !state.exclusive && state.shared == 0,
            }
        })
    }

    fn acquire(&mut self, requests: &[PlatformIoResourceRequest]) {
        // The scheduler checks the whole normalized set before changing any
        // entry. A task therefore never waits while holding a partial grant.
        debug_assert!(self.can_acquire(requests));
        for request in requests {
            let state = self.entries.entry(request.key.clone()).or_default();
            match request.access {
                PlatformIoAccess::Shared => state.shared += 1,
                PlatformIoAccess::Exclusive => state.exclusive = true,
            }
        }
    }

    fn release(&mut self, requests: &[PlatformIoResourceRequest]) {
        for request in requests {
            let remove = if let Some(state) = self.entries.get_mut(&request.key) {
                match request.access {
                    PlatformIoAccess::Shared => {
                        debug_assert!(state.shared > 0);
                        state.shared = state.shared.saturating_sub(1);
                    }
                    PlatformIoAccess::Exclusive => {
                        debug_assert!(state.exclusive);
                        state.exclusive = false;
                    }
                }
                state.shared == 0 && !state.exclusive
            } else {
                debug_assert!(false, "released a platform I/O resource that was not held");
                false
            };
            if remove {
                self.entries.remove(&request.key);
            }
        }
    }
}

#[cfg(feature = "platform-io")]
fn start_blocking_fallback_executor() -> Result<BlockingFallbackExecutor> {
    let (sender, receiver) = crossbeam_channel::bounded(PLATFORM_IO_THREAD_POOL_QUEUE_DEPTH);
    let mut workers = Vec::with_capacity(PLATFORM_IO_THREAD_POOL_WORKERS);
    for worker_index in 0..PLATFORM_IO_THREAD_POOL_WORKERS {
        let receiver = receiver.clone();
        match thread::Builder::new()
            .name(format!("trine-kv-platform-io-threadpool-{worker_index}"))
            .spawn(move || blocking_fallback::run_worker(receiver))
        {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                drop(sender);
                for worker in workers {
                    let _ = worker.join();
                }
                return Err(Error::Io(error));
            }
        }
    }
    Ok(BlockingFallbackExecutor {
        sender: Some(sender),
        workers,
    })
}

#[cfg(feature = "platform-io")]
struct PlatformIoScheduler {
    receiver: crossbeam_channel::Receiver<PlatformIoTask>,
    completed_sender: crossbeam_channel::Sender<Vec<PlatformIoResourceRequest>>,
    completed_receiver: crossbeam_channel::Receiver<Vec<PlatformIoResourceRequest>>,
    matrix: PlatformIoBackendMatrix,
    fallback: BlockingFallbackExecutor,
    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    native: Option<NativeCompletionExecutor>,
    state: Arc<AtomicU8>,
    metrics: Arc<PlatformIoSchedulerMetrics>,
    resources: PlatformIoResourceTable,
    pending: VecDeque<PendingPlatformIoTask>,
    active: usize,
    input_closed: bool,
}

#[cfg(feature = "platform-io")]
impl PlatformIoScheduler {
    fn new(
        receiver: crossbeam_channel::Receiver<PlatformIoTask>,
        matrix: PlatformIoBackendMatrix,
        fallback: BlockingFallbackExecutor,
        #[cfg(all(feature = "platform-io-native", any(unix, windows)))] native: Option<
            NativeCompletionExecutor,
        >,
        state: Arc<AtomicU8>,
        metrics: Arc<PlatformIoSchedulerMetrics>,
    ) -> Self {
        let (completed_sender, completed_receiver) = crossbeam_channel::unbounded();
        Self {
            receiver,
            completed_sender,
            completed_receiver,
            matrix,
            fallback,
            #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
            native,
            state,
            metrics,
            resources: PlatformIoResourceTable::default(),
            pending: VecDeque::new(),
            active: 0,
            input_closed: false,
        }
    }

    fn run(mut self) {
        loop {
            self.fail_pending_if_needed();
            self.dispatch_ready();
            if self.input_closed && self.pending.is_empty() && self.active == 0 {
                break;
            }
            if self.active == 0 {
                let received = self.receiver.recv();
                self.accept_received(received);
            } else {
                self.wait_for_input_or_completion();
            }
        }
    }

    fn fail_pending_if_needed(&mut self) {
        if self.state.load(Ordering::Acquire) != PLATFORM_IO_FAILED {
            return;
        }
        for pending in self.pending.drain(..) {
            pending
                .task
                .complete_start_error("platform I/O scheduler entered a failed state");
            self.metrics.queued.fetch_sub(1, Ordering::AcqRel);
            self.metrics.completed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dispatch_ready(&mut self) {
        // A blocked task does not form a global head-of-line barrier: later
        // work may pass it only when its complete resource set is unrelated.
        while self.active < PLATFORM_IO_MAX_IN_FLIGHT {
            let Some(index) = self.next_runnable_index() else {
                break;
            };
            let pending = self
                .pending
                .remove(index)
                .expect("pending platform I/O task index remains valid");
            self.metrics.queued.fetch_sub(1, Ordering::AcqRel);
            self.resources.acquire(&pending.resources);
            let task_resources = pending.resources;
            let scheduled = self.schedule(pending.task, &task_resources);
            if self.dispatch(scheduled) {
                self.record_dispatch();
            } else {
                self.resources.release(&task_resources);
                self.metrics.completed.fetch_add(1, Ordering::Relaxed);
                self.state.store(PLATFORM_IO_FAILED, Ordering::Release);
                break;
            }
        }
    }

    fn next_runnable_index(&self) -> Option<usize> {
        self.pending.iter().enumerate().find_map(|(index, task)| {
            if !self.resources.can_acquire(&task.resources) {
                return None;
            }
            let overtakes_conflict =
                self.pending.iter().take(index).any(|earlier| {
                    platform_io_resources_conflict(&earlier.resources, &task.resources)
                });
            (!overtakes_conflict).then_some(index)
        })
    }

    fn schedule(
        &self,
        task: PlatformIoTask,
        resources: &[PlatformIoResourceRequest],
    ) -> ScheduledPlatformIoTask {
        let class = self.matrix.class_for(task.operation());
        let abandon_completion = task.failure_completion();
        ScheduledPlatformIoTask {
            task: Some(task),
            abandon_completion: Some(abandon_completion),
            class,
            #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
            control_metrics: Arc::clone(&self.metrics),
            resources: Some(resources.to_vec()),
            completed: self.completed_sender.clone(),
            state: Arc::clone(&self.state),
            finished: false,
        }
    }

    fn dispatch(&mut self, scheduled: ScheduledPlatformIoTask) -> bool {
        match scheduled.class {
            PlatformIoTaskClass::TruePlatformAsync
            | PlatformIoTaskClass::PlatformNativeAsyncButPartial => self.dispatch_native(scheduled),
            PlatformIoTaskClass::ThreadPoolManagedAsync | PlatformIoTaskClass::BlockingFallback => {
                self.dispatch_fallback(scheduled)
            }
            PlatformIoTaskClass::Unsupported => {
                let completed = scheduled
                    .complete_unsupported("platform I/O operation is unsupported on this target");
                let _ = self.completed_sender.send(completed);
                true
            }
        }
    }

    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    fn dispatch_native(&mut self, scheduled: ScheduledPlatformIoTask) -> bool {
        let Some(executor) = self.native.as_mut() else {
            let _ = scheduled.complete_start_error("native platform I/O executor is unavailable");
            return false;
        };
        match executor
            .sender
            .as_mut()
            .expect("native executor sender remains open while scheduling")
            .try_send(scheduled)
        {
            Ok(()) => true,
            Err(error) => {
                let _ = error
                    .into_inner()
                    .complete_start_error("native platform I/O executor stopped accepting tasks");
                false
            }
        }
    }

    #[cfg(not(all(feature = "platform-io-native", any(unix, windows))))]
    fn dispatch_native(&self, scheduled: ScheduledPlatformIoTask) -> bool {
        let _ = scheduled.complete_start_error("native platform I/O executor is unavailable");
        false
    }

    fn dispatch_fallback(&self, scheduled: ScheduledPlatformIoTask) -> bool {
        match self
            .fallback
            .sender
            .as_ref()
            .expect("thread-pool sender remains open while scheduling")
            .try_send(scheduled)
        {
            Ok(()) => true,
            Err(error) => {
                let _ = error.into_inner().complete_start_error(
                    "platform I/O thread-pool executor stopped accepting tasks",
                );
                false
            }
        }
    }

    fn record_dispatch(&mut self) {
        self.active += 1;
        let in_flight = self
            .metrics
            .in_flight
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        self.metrics
            .max_in_flight
            .fetch_max(in_flight, Ordering::Relaxed);
    }

    fn wait_for_input_or_completion(&mut self) {
        if self.input_closed {
            let completed = self.completed_receiver.recv();
            self.accept_completed(completed);
            return;
        }
        crossbeam_channel::select! {
            recv(self.completed_receiver) -> completed => self.accept_completed(completed),
            recv(self.receiver) -> task => self.accept_received(task),
        }
    }

    fn accept_completed(
        &mut self,
        completed: std::result::Result<
            Vec<PlatformIoResourceRequest>,
            crossbeam_channel::RecvError,
        >,
    ) {
        if let Ok(completed) = completed {
            self.resources.release(&completed);
            self.active = self.active.saturating_sub(1);
            self.metrics.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.metrics.completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.state.store(PLATFORM_IO_FAILED, Ordering::Release);
        }
    }

    fn accept_received(
        &mut self,
        task: std::result::Result<PlatformIoTask, crossbeam_channel::RecvError>,
    ) {
        match task {
            Ok(task) if self.state.load(Ordering::Acquire) == PLATFORM_IO_FAILED => {
                task.complete_start_error("platform I/O scheduler entered a failed state");
                self.metrics.queued.fetch_sub(1, Ordering::AcqRel);
                self.metrics.completed.fetch_add(1, Ordering::Relaxed);
            }
            Ok(task) => {
                let resources = task.resources();
                self.pending
                    .push_back(PendingPlatformIoTask { task, resources });
            }
            Err(_) => self.input_closed = true,
        }
    }
}

#[cfg(feature = "platform-io")]
impl Drop for PlatformIoScheduler {
    fn drop(&mut self) {
        self.state.store(PLATFORM_IO_FAILED, Ordering::Release);
        let mut abandoned = 0_u64;
        for pending in self.pending.drain(..) {
            pending
                .task
                .complete_start_error("platform I/O scheduler stopped before task admission");
            abandoned = abandoned.saturating_add(1);
        }
        while let Ok(task) = self.receiver.try_recv() {
            task.complete_start_error("platform I/O scheduler stopped before task admission");
            abandoned = abandoned.saturating_add(1);
        }
        if abandoned > 0 {
            self.metrics.queued.fetch_sub(
                usize::try_from(abandoned).expect("bounded platform I/O queue count fits usize"),
                Ordering::AcqRel,
            );
            self.metrics
                .completed
                .fetch_add(abandoned, Ordering::Relaxed);
        }
    }
}

#[cfg(feature = "platform-io")]
fn normalize_platform_io_resources(
    requests: Vec<PlatformIoResourceRequest>,
) -> Vec<PlatformIoResourceRequest> {
    let mut normalized = HashMap::with_capacity(requests.len());
    for request in requests {
        normalized
            .entry(request.key)
            .and_modify(|access| {
                if request.access == PlatformIoAccess::Exclusive {
                    *access = PlatformIoAccess::Exclusive;
                }
            })
            .or_insert(request.access);
    }
    normalized
        .into_iter()
        .map(|(key, access)| PlatformIoResourceRequest { key, access })
        .collect()
}

#[cfg(feature = "platform-io")]
fn platform_io_resources_conflict(
    left: &[PlatformIoResourceRequest],
    right: &[PlatformIoResourceRequest],
) -> bool {
    left.iter().any(|left| {
        right.iter().any(|right| {
            left.key == right.key
                && (left.access == PlatformIoAccess::Exclusive
                    || right.access == PlatformIoAccess::Exclusive)
        })
    })
}

#[cfg(feature = "platform-io")]
impl PlatformIoTask {
    const fn operation(&self) -> PlatformIoOperation {
        match self {
            Self::Len { .. } => PlatformIoOperation::LengthLookup,
            Self::ReadExactAtOwned { .. } => PlatformIoOperation::OwnedRandomRead,
            Self::ReadOptional { .. } => PlatformIoOperation::OptionalWholeObjectRead,
            Self::Publish { plan, .. } => plan.operation,
            Self::Append { .. } => PlatformIoOperation::Append,
            Self::OpenAppend { .. } => PlatformIoOperation::AppendObjectOpen,
            Self::Persist { .. } => PlatformIoOperation::Persist,
            Self::Delete { .. } => PlatformIoOperation::ObjectDelete,
            Self::CreateDirAll { .. } => PlatformIoOperation::DirectoryCreate,
            Self::SyncDir { .. } => PlatformIoOperation::DirectorySync,
            Self::ListFilePaths { .. } => PlatformIoOperation::DirectoryListing,
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease { .. } => PlatformIoOperation::WriterLeaseAcquire,
        }
    }

    fn resources(&self) -> Vec<PlatformIoResourceRequest> {
        let shared_object = |path: &Path| PlatformIoResourceRequest {
            key: PlatformIoResourceKey::Object(path.to_path_buf()),
            access: PlatformIoAccess::Shared,
        };
        let exclusive_object = |path: &Path| PlatformIoResourceRequest {
            key: PlatformIoResourceKey::Object(path.to_path_buf()),
            access: PlatformIoAccess::Exclusive,
        };
        let shared_directory = |path: &Path| PlatformIoResourceRequest {
            key: PlatformIoResourceKey::Directory(path.to_path_buf()),
            access: PlatformIoAccess::Shared,
        };
        let exclusive_directory = |path: &Path| PlatformIoResourceRequest {
            key: PlatformIoResourceKey::Directory(path.to_path_buf()),
            access: PlatformIoAccess::Exclusive,
        };
        let parent_directory = |path: &Path| {
            path.parent()
                .map(exclusive_directory)
                .into_iter()
                .collect::<Vec<_>>()
        };

        let requests = match self {
            Self::Len { path, .. }
            | Self::ReadExactAtOwned { path, .. }
            | Self::ReadOptional { path, .. } => vec![shared_object(path)],
            Self::Publish { plan, .. } => {
                let mut requests = vec![
                    exclusive_object(&plan.path),
                    exclusive_object(&plan.temporary_path),
                ];
                requests.extend(parent_directory(&plan.path));
                requests.extend(parent_directory(&plan.temporary_path));
                requests
            }
            Self::Append { session, .. } | Self::Persist { session, .. } => {
                vec![exclusive_object(session.path.as_ref())]
            }
            Self::OpenAppend { path, .. } => {
                let mut requests = vec![exclusive_object(path)];
                requests.extend(parent_directory(path));
                requests
            }
            Self::Delete { path, .. } => {
                let mut requests = vec![exclusive_object(path)];
                requests.extend(parent_directory(path));
                requests
            }
            Self::CreateDirAll { path, .. } | Self::SyncDir { path, .. } => {
                vec![exclusive_directory(path)]
            }
            Self::ListFilePaths { path, .. } => vec![shared_directory(path)],
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease { path, .. } => {
                let mut requests = vec![exclusive_object(path)];
                requests.extend(parent_directory(path));
                requests
            }
        };
        normalize_platform_io_resources(requests)
    }

    fn failure_completion(&self) -> PlatformIoFailureCompletion {
        match self {
            Self::Len { completion, .. } => PlatformIoFailureCompletion::Len(completion.clone()),
            Self::ReadExactAtOwned { completion, .. } => {
                PlatformIoFailureCompletion::Read(completion.clone())
            }
            Self::ReadOptional { completion, .. } => {
                PlatformIoFailureCompletion::Optional(completion.clone())
            }
            Self::ListFilePaths { completion, .. } => {
                PlatformIoFailureCompletion::Paths(completion.clone())
            }
            Self::OpenAppend { completion, .. } => {
                PlatformIoFailureCompletion::AppendSession(completion.clone())
            }
            Self::Append { completion, .. }
            | Self::Persist { completion, .. }
            | Self::Publish { completion, .. }
            | Self::Delete { completion, .. }
            | Self::CreateDirAll { completion, .. }
            | Self::SyncDir { completion, .. } => {
                PlatformIoFailureCompletion::Unit(completion.clone())
            }
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease { completion, .. } => {
                PlatformIoFailureCompletion::Lease(completion.clone())
            }
        }
    }

    fn mark_execution_class(&self, class: PlatformIoTaskClass) -> Result<()> {
        match self {
            Self::Len { completion, .. } => completion.mark_platform_execution(class),
            Self::ReadExactAtOwned { completion, .. } => completion.mark_platform_execution(class),
            Self::ReadOptional { completion, .. } => completion.mark_platform_execution(class),
            Self::ListFilePaths { completion, .. } => completion.mark_platform_execution(class),
            Self::OpenAppend { completion, .. } => completion.mark_platform_execution(class),
            Self::Append { completion, .. }
            | Self::Persist { completion, .. }
            | Self::Publish { completion, .. }
            | Self::Delete { completion, .. }
            | Self::CreateDirAll { completion, .. }
            | Self::SyncDir { completion, .. } => completion.mark_platform_execution(class),
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease { completion, .. } => {
                completion.mark_platform_execution(class)
            }
        }
    }

    fn run_thread_pool(self) {
        match self {
            Self::Len { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::len(path));
            }
            Self::ReadExactAtOwned {
                path,
                offset,
                len,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::read_exact_at_owned(path, offset, len),
                );
            }
            Self::ReadOptional {
                path,
                max_bytes,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::read_optional(&path, max_bytes),
                );
            }
            Self::Publish { plan, completion } => {
                complete_platform_io(&completion, blocking_fallback::publish(&plan));
            }
            Self::Append {
                session,
                bytes,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::append(session.path.as_ref().clone(), &bytes, durability),
                );
            }
            Self::OpenAppend { path, completion } => {
                let result = blocking_fallback::open_append(path.clone())
                    .map(|()| PlatformIoAppendSession::opened(path));
                complete_platform_io(&completion, result);
            }
            Self::Persist {
                session,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::persist_path(session.path.as_ref().clone(), durability),
                );
            }
            Self::Delete { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::delete_path(path));
            }
            Self::CreateDirAll { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::create_dir_all(path));
            }
            Self::SyncDir { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::sync_directory(path));
            }
            Self::ListFilePaths { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::list_file_paths(path));
            }
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease {
                path,
                owner,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::acquire_writer_lease(&path, &owner),
                );
            }
        }
    }

    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    async fn run(self) {
        match self {
            Self::Len { path, completion } => {
                complete_platform_io(&completion, platform_backend::len(path).await);
            }
            Self::ReadExactAtOwned {
                path,
                offset,
                len,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::read_exact_at_owned(path, offset, len).await,
                );
            }
            Self::ReadOptional {
                path,
                max_bytes,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::read_optional(path, max_bytes).await,
                );
            }
            Self::Publish { plan, completion } => {
                complete_platform_io(&completion, platform_backend::publish(plan).await);
            }
            Self::Append {
                session,
                bytes,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::append(session.path.as_ref().clone(), bytes, durability)
                        .await,
                );
            }
            Self::OpenAppend { path, completion } => {
                let result = platform_backend::open_append(path.clone())
                    .await
                    .map(|()| PlatformIoAppendSession::opened(path));
                complete_platform_io(&completion, result);
            }
            Self::Persist {
                session,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::persist_path(session.path.as_ref().clone(), durability).await,
                );
            }
            Self::Delete { path, completion } => {
                complete_platform_io(&completion, platform_backend::delete_path(path).await);
            }
            Self::CreateDirAll { path, completion } => {
                complete_platform_io(&completion, platform_backend::create_dir_all(path).await);
            }
            Self::SyncDir { path, completion } => {
                complete_platform_io(&completion, platform_backend::sync_directory(path).await);
            }
            Self::ListFilePaths { path, completion } => {
                complete_platform_io(&completion, platform_backend::list_file_paths(path).await);
            }
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease {
                path,
                owner,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::acquire_writer_lease(&path, &owner),
                );
            }
        }
    }

    fn complete_start_error(self, message: &str) {
        let error = || Error::runtime_busy(message.to_owned());
        match self {
            Self::Len { completion, .. } => complete_platform_io(&completion, Err(error())),
            Self::ReadExactAtOwned { completion, .. } => {
                complete_platform_io(&completion, Err(error()));
            }
            Self::ReadOptional { completion, .. } => {
                complete_platform_io(&completion, Err(error()));
            }
            Self::ListFilePaths { completion, .. } => {
                complete_platform_io(&completion, Err(error()));
            }
            Self::OpenAppend { completion, .. } => {
                complete_platform_io(&completion, Err(error()));
            }
            Self::Append { completion, .. }
            | Self::Persist { completion, .. }
            | Self::Publish { completion, .. }
            | Self::Delete { completion, .. }
            | Self::CreateDirAll { completion, .. }
            | Self::SyncDir { completion, .. } => {
                complete_platform_io(&completion, Err(error()));
            }
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease { completion, .. } => {
                complete_platform_io(&completion, Err(error()));
            }
        }
    }
}

#[cfg(feature = "platform-io")]
fn complete_platform_io<T>(completion: &IoCompletion<T>, result: Result<T>) {
    let completed = completion.complete(result);
    debug_assert!(completed.is_ok());
}

#[cfg(test)]
mod tests;
