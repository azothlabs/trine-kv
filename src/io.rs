use std::{
    future::Future,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    any(unix, windows)
))]
use std::sync::mpsc;
#[cfg(feature = "platform-io")]
use std::{fs::File, path::PathBuf, thread};

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
    runtime::Runtime,
    storage::StorageReadBuffer,
};

#[cfg(all(
    feature = "platform-io",
    feature = "platform-io-native",
    any(unix, windows)
))]
mod platform_backend;
#[cfg(feature = "platform-io")]
mod platform_threadpool;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlatformIoOperation {
    LengthLookup,
    OwnedRandomRead,
    OptionalWholeObjectRead,
    TempWriteRenamePublish,
    AppendObjectOpen,
    Append,
    Persist,
    WalRewrite,
    ObjectDelete,
    DirectoryCreate,
    DirectorySync,
    DirectoryListing,
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
            PlatformIoOperation::WalRewrite => self.wal_rewrite,
            PlatformIoOperation::ObjectDelete => self.object_delete,
            PlatformIoOperation::DirectoryCreate => self.directory_create,
            PlatformIoOperation::DirectorySync => self.directory_sync,
            PlatformIoOperation::DirectoryListing => self.directory_listing,
            PlatformIoOperation::WriterLeaseAcquire => self.writer_lease_acquire,
        }
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
}

impl<T> IoCompletion<T> {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(IoCompletionState {
                result: None,
                waker: None,
            })),
        }
    }

    pub(crate) fn complete(&self, result: Result<T>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::runtime_busy("I/O completion state is poisoned"))?;
        if state.result.is_some() {
            return Err(Error::runtime_busy("I/O completion already finished"));
        }
        state.result = Some(result);
        if let Some(waker) = state.waker.take() {
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
#[derive(Debug, Default, Clone)]
pub(crate) struct PlatformIoDriver {
    thread_pool_sender: Arc<Mutex<Option<crossbeam_channel::Sender<PlatformIoTask>>>>,
    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    native_sender: Arc<Mutex<Option<mpsc::Sender<PlatformIoTask>>>>,
}

#[cfg(feature = "platform-io")]
const PLATFORM_IO_THREAD_POOL_WORKERS: usize = 4;
#[cfg(feature = "platform-io")]
const PLATFORM_IO_THREAD_POOL_QUEUE_DEPTH: usize = 1024;

#[cfg(feature = "platform-io")]
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
    WriteTempRename {
        path: PathBuf,
        tmp_path: PathBuf,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
        create_parent: bool,
        sync_parent_after_rename: bool,
        completion: IoCompletion<()>,
    },
    Append {
        path: PathBuf,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
        completion: IoCompletion<()>,
    },
    OpenAppend {
        path: PathBuf,
        completion: IoCompletion<()>,
    },
    Persist {
        path: PathBuf,
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
    AcquireWriterLease {
        path: PathBuf,
        owner: Arc<[u8]>,
        completion: IoCompletion<File>,
    },
}

#[cfg(feature = "platform-io")]
impl PlatformIoDriver {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) const fn info() -> IoDriverInfo {
        IoDriverInfo::platform()
    }

    pub(crate) fn backend_matrix() -> PlatformIoBackendMatrix {
        #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
        {
            platform_backend::matrix()
        }
        #[cfg(not(all(feature = "platform-io-native", any(unix, windows))))]
        {
            platform_threadpool::matrix()
        }
    }

    pub(crate) fn task_class(operation: PlatformIoOperation) -> PlatformIoTaskClass {
        Self::backend_matrix().class_for(operation)
    }

    pub(crate) fn submit_len_path(&self, path: PathBuf) -> Result<IoCompletion<u64>> {
        let completion = IoCompletion::new();
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
        let completion = IoCompletion::new();
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
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::ReadOptional {
            path,
            max_bytes,
            completion,
        })?;
        Ok(waiter)
    }

    pub(crate) fn submit_write_temp_rename_path(
        &self,
        path: PathBuf,
        tmp_path: PathBuf,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
        create_parent: bool,
        sync_parent_after_rename: bool,
    ) -> Result<IoCompletion<()>> {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::WriteTempRename {
            path,
            tmp_path,
            bytes,
            durability,
            create_parent,
            sync_parent_after_rename,
            completion,
        })?;
        Ok(waiter)
    }

    pub(crate) fn submit_append_path(
        &self,
        path: PathBuf,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<IoCompletion<()>> {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::Append {
            path,
            bytes,
            durability,
            completion,
        })?;
        Ok(waiter)
    }

    pub(crate) fn submit_open_append_path(&self, path: PathBuf) -> Result<IoCompletion<()>> {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::OpenAppend { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_persist_path(
        &self,
        path: PathBuf,
        durability: DurabilityMode,
    ) -> Result<IoCompletion<()>> {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::Persist {
            path,
            durability,
            completion,
        })?;
        Ok(waiter)
    }

    pub(crate) fn submit_delete_path(&self, path: PathBuf) -> Result<IoCompletion<()>> {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::Delete { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_create_dir_all_path(&self, path: PathBuf) -> Result<IoCompletion<()>> {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::CreateDirAll { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_sync_dir_path(&self, path: PathBuf) -> Result<IoCompletion<()>> {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::SyncDir { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_list_file_paths_path(
        &self,
        path: PathBuf,
    ) -> Result<IoCompletion<Vec<PathBuf>>> {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::ListFilePaths { path, completion })?;
        Ok(waiter)
    }

    pub(crate) fn submit_acquire_writer_lease_path(
        &self,
        path: PathBuf,
        owner: Arc<[u8]>,
    ) -> Result<IoCompletion<File>> {
        let completion = IoCompletion::new();
        let waiter = completion.clone();
        self.submit_task(PlatformIoTask::AcquireWriterLease {
            path,
            owner,
            completion,
        })?;
        Ok(waiter)
    }

    fn submit_task(&self, task: PlatformIoTask) -> Result<()> {
        let operation = task.operation();
        match Self::task_class(operation) {
            PlatformIoTaskClass::TruePlatformAsync
            | PlatformIoTaskClass::PlatformNativeAsyncButPartial => self.submit_native_task(task),
            PlatformIoTaskClass::ThreadPoolManagedAsync | PlatformIoTaskClass::BlockingFallback => {
                self.submit_thread_pool_task(task)
            }
            PlatformIoTaskClass::Unsupported => {
                task.complete_start_error("platform I/O operation is unsupported on this target");
                Ok(())
            }
        }
    }

    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    fn submit_native_task(&self, task: PlatformIoTask) -> Result<()> {
        let sender = self.native_sender()?;
        sender.send(task).map_err(|_| Error::Closed)
    }

    #[cfg(not(all(feature = "platform-io-native", any(unix, windows))))]
    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    fn submit_native_task(&self, task: PlatformIoTask) -> Result<()> {
        task.complete_start_error("native platform I/O feature is not enabled");
        Ok(())
    }

    fn submit_thread_pool_task(&self, task: PlatformIoTask) -> Result<()> {
        let sender = self.thread_pool_sender()?;
        sender.try_send(task).map_err(|error| {
            if error.is_full() {
                Error::runtime_busy("platform I/O thread-pool queue is full")
            } else {
                Error::Closed
            }
        })
    }

    fn thread_pool_sender(&self) -> Result<crossbeam_channel::Sender<PlatformIoTask>> {
        let mut sender = self
            .thread_pool_sender
            .lock()
            .map_err(|_| Error::runtime_busy("platform I/O thread-pool state is poisoned"))?;
        if let Some(sender) = sender.as_ref() {
            return Ok(sender.clone());
        }

        let (next_sender, receiver) =
            crossbeam_channel::bounded(PLATFORM_IO_THREAD_POOL_QUEUE_DEPTH);
        for worker_index in 0..PLATFORM_IO_THREAD_POOL_WORKERS {
            let receiver = receiver.clone();
            thread::Builder::new()
                .name(format!("trine-kv-platform-io-threadpool-{worker_index}"))
                .spawn(move || platform_threadpool::run_worker(receiver))
                .map_err(Error::Io)?;
        }
        *sender = Some(next_sender.clone());
        Ok(next_sender)
    }

    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    fn native_sender(&self) -> Result<mpsc::Sender<PlatformIoTask>> {
        let mut sender = self
            .native_sender
            .lock()
            .map_err(|_| Error::runtime_busy("native platform I/O state is poisoned"))?;
        if let Some(sender) = sender.as_ref() {
            return Ok(sender.clone());
        }

        let (next_sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("trine-kv-platform-io-native".to_owned())
            .spawn(move || platform_backend::run_worker(receiver))
            .map_err(Error::Io)?;
        *sender = Some(next_sender.clone());
        Ok(next_sender)
    }
}

#[cfg(feature = "platform-io")]
impl PlatformIoTask {
    const fn operation(&self) -> PlatformIoOperation {
        match self {
            Self::Len { .. } => PlatformIoOperation::LengthLookup,
            Self::ReadExactAtOwned { .. } => PlatformIoOperation::OwnedRandomRead,
            Self::ReadOptional { .. } => PlatformIoOperation::OptionalWholeObjectRead,
            Self::WriteTempRename { .. } => PlatformIoOperation::TempWriteRenamePublish,
            Self::Append { .. } => PlatformIoOperation::Append,
            Self::OpenAppend { .. } => PlatformIoOperation::AppendObjectOpen,
            Self::Persist { .. } => PlatformIoOperation::Persist,
            Self::Delete { .. } => PlatformIoOperation::ObjectDelete,
            Self::CreateDirAll { .. } => PlatformIoOperation::DirectoryCreate,
            Self::SyncDir { .. } => PlatformIoOperation::DirectorySync,
            Self::ListFilePaths { .. } => PlatformIoOperation::DirectoryListing,
            Self::AcquireWriterLease { .. } => PlatformIoOperation::WriterLeaseAcquire,
        }
    }

    fn run_thread_pool(self) {
        match self {
            Self::Len { path, completion } => {
                complete_platform_io(&completion, platform_threadpool::len(path));
            }
            Self::ReadExactAtOwned {
                path,
                offset,
                len,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_threadpool::read_exact_at_owned(path, offset, len),
                );
            }
            Self::ReadOptional {
                path,
                max_bytes,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_threadpool::read_optional(path, max_bytes),
                );
            }
            Self::WriteTempRename {
                path,
                tmp_path,
                bytes,
                durability,
                create_parent,
                sync_parent_after_rename,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_threadpool::write_temp_rename(
                        &path,
                        &tmp_path,
                        &bytes,
                        durability,
                        create_parent,
                        sync_parent_after_rename,
                    ),
                );
            }
            Self::Append {
                path,
                bytes,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_threadpool::append(path, &bytes, durability),
                );
            }
            Self::OpenAppend { path, completion } => {
                complete_platform_io(&completion, platform_threadpool::open_append(path));
            }
            Self::Persist {
                path,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_threadpool::persist_path(path, durability),
                );
            }
            Self::Delete { path, completion } => {
                complete_platform_io(&completion, platform_threadpool::delete_path(path));
            }
            Self::CreateDirAll { path, completion } => {
                complete_platform_io(&completion, platform_threadpool::create_dir_all(path));
            }
            Self::SyncDir { path, completion } => {
                complete_platform_io(&completion, platform_threadpool::sync_directory(path));
            }
            Self::ListFilePaths { path, completion } => {
                complete_platform_io(&completion, platform_threadpool::list_file_paths(path));
            }
            Self::AcquireWriterLease {
                path,
                owner,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_threadpool::acquire_writer_lease(&path, &owner),
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
            Self::WriteTempRename {
                path,
                tmp_path,
                bytes,
                durability,
                create_parent,
                sync_parent_after_rename,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::write_temp_rename(
                        path,
                        tmp_path,
                        bytes,
                        durability,
                        create_parent,
                        sync_parent_after_rename,
                    )
                    .await,
                );
            }
            Self::Append {
                path,
                bytes,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::append(path, bytes, durability).await,
                );
            }
            Self::OpenAppend { path, completion } => {
                complete_platform_io(&completion, platform_backend::open_append(path).await);
            }
            Self::Persist {
                path,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::persist_path(path, durability).await,
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
            Self::Append { completion, .. }
            | Self::OpenAppend { completion, .. }
            | Self::Persist { completion, .. }
            | Self::WriteTempRename { completion, .. }
            | Self::Delete { completion, .. }
            | Self::CreateDirAll { completion, .. }
            | Self::SyncDir { completion, .. } => {
                complete_platform_io(&completion, Err(error()));
            }
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
mod tests {
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
}
