#[cfg(any(unix, windows))]
use super::File;
use super::{
    Arc, AtomicU8, AtomicU64, AtomicUsize, DurabilityMode, Error, HashMap, IoCompletion,
    IoDriverInfo, Mutex, NativeFileStorageMetrics, Ordering, Path, PathBuf, Result,
    StorageReadBuffer, VecDeque, thread,
};

mod blocking_fallback;
pub(in crate::io) mod driver;
#[cfg(all(feature = "platform-io-native", any(unix, windows)))]
mod platform_backend;
pub(in crate::io) mod scheduler;

use scheduler::{PlatformIoScheduler, start_blocking_fallback_executor};

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

    pub(in crate::io) const fn supports_platform_async_io(self) -> bool {
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
    pub(in crate::io) fn opened(path: PathBuf) -> Self {
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

    #[cfg(any(unix, windows))]
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
pub(in crate::io) struct PlatformIoSchedulerMetrics {
    pub(in crate::io) queued: AtomicUsize,
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
pub(in crate::io) const PLATFORM_IO_SCHEDULER_QUEUE_DEPTH: usize = 1024;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_MAX_IN_FLIGHT: usize = 256;

#[cfg(feature = "platform-io")]
pub(in crate::io) const PLATFORM_IO_RUNNING: u8 = 0;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_CLOSING: u8 = 1;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_CLOSED: u8 = 2;
#[cfg(feature = "platform-io")]
pub(in crate::io) const PLATFORM_IO_FAILED: u8 = 3;

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
pub(in crate::io) struct PlatformIoResourceRequest {
    key: PlatformIoResourceKey,
    access: PlatformIoAccess,
}

#[cfg(feature = "platform-io")]
struct PendingPlatformIoTask {
    task: PlatformIoTask,
    resources: Vec<PlatformIoResourceRequest>,
}

#[cfg(feature = "platform-io")]
pub(in crate::io) struct ScheduledPlatformIoTask {
    pub(in crate::io) task: Option<PlatformIoTask>,
    pub(in crate::io) abandon_completion: Option<PlatformIoFailureCompletion>,
    pub(in crate::io) class: PlatformIoTaskClass,
    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    pub(in crate::io) control_metrics: Arc<PlatformIoSchedulerMetrics>,
    pub(in crate::io) resources: Option<Vec<PlatformIoResourceRequest>>,
    pub(in crate::io) completed: crossbeam_channel::Sender<Vec<PlatformIoResourceRequest>>,
    pub(in crate::io) state: Arc<AtomicU8>,
    pub(in crate::io) finished: bool,
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
pub(in crate::io) struct PlatformIoResourceTable {
    entries: HashMap<PlatformIoResourceKey, PlatformIoResourceState>,
}

#[cfg(feature = "platform-io")]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(in crate::io) enum PlatformIoTask {
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
pub(in crate::io) enum PlatformIoFailureCompletion {
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
