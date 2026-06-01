use std::{
    future::Future,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

#[cfg(feature = "platform-io")]
use std::{path::PathBuf, sync::mpsc, thread};

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
    runtime::Runtime,
    storage::StorageReadBuffer,
};

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

    pub(crate) const fn is_platform_async(self) -> bool {
        matches!(self, Self::Platform)
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
    sender: Arc<Mutex<Option<mpsc::Sender<PlatformIoTask>>>>,
}

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
    Append {
        path: PathBuf,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
        completion: IoCompletion<()>,
    },
    Persist {
        path: PathBuf,
        durability: DurabilityMode,
        completion: IoCompletion<()>,
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

    fn submit_task(&self, task: PlatformIoTask) -> Result<()> {
        let sender = self.sender()?;
        sender.send(task).map_err(|_| Error::Closed)
    }

    fn sender(&self) -> Result<mpsc::Sender<PlatformIoTask>> {
        let mut sender = self
            .sender
            .lock()
            .map_err(|_| Error::runtime_busy("platform I/O driver state is poisoned"))?;
        if let Some(sender) = sender.as_ref() {
            return Ok(sender.clone());
        }

        let (next_sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("trine-kv-platform-io".to_owned())
            .spawn(move || platform_io_worker_loop(receiver))
            .map_err(Error::Io)?;
        *sender = Some(next_sender.clone());
        Ok(next_sender)
    }
}

#[cfg(feature = "platform-io")]
impl PlatformIoTask {
    async fn run(self) {
        match self {
            Self::Len { path, completion } => {
                complete_platform_io(&completion, platform_len(path).await);
            }
            Self::ReadExactAtOwned {
                path,
                offset,
                len,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_read_exact_at_owned(path, offset, len).await,
                );
            }
            Self::Append {
                path,
                bytes,
                durability,
                completion,
            } => {
                complete_platform_io(&completion, platform_append(path, bytes, durability).await);
            }
            Self::Persist {
                path,
                durability,
                completion,
            } => {
                complete_platform_io(&completion, platform_persist_path(path, durability).await);
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
            Self::Append { completion, .. } | Self::Persist { completion, .. } => {
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

#[cfg(feature = "platform-io")]
fn platform_io_worker_loop(receiver: mpsc::Receiver<PlatformIoTask>) {
    let runtime = match compio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            let message = format!("platform I/O runtime failed to start: {error}");
            for task in receiver {
                task.complete_start_error(&message);
            }
            return;
        }
    };

    for task in receiver {
        runtime.block_on(task.run());
    }
}

#[cfg(feature = "platform-io")]
async fn platform_len(path: PathBuf) -> Result<u64> {
    let file = compio::fs::File::open(path).await.map_err(Error::Io)?;
    let metadata = file.metadata().await.map_err(Error::Io)?;
    Ok(metadata.len())
}

#[cfg(feature = "platform-io")]
async fn platform_read_exact_at_owned(
    path: PathBuf,
    offset: usize,
    len: usize,
) -> Result<StorageReadBuffer> {
    use compio::io::AsyncReadAtExt;

    let file = compio::fs::File::open(path).await.map_err(Error::Io)?;
    let buffer = vec![0; len];
    let compio::buf::BufResult(result, buffer) =
        file.read_exact_at(buffer, platform_offset(offset)?).await;
    result.map_err(Error::Io)?;
    Ok(StorageReadBuffer::from_vec(offset, buffer))
}

#[cfg(feature = "platform-io")]
async fn platform_append(
    path: PathBuf,
    bytes: Arc<[u8]>,
    durability: DurabilityMode,
) -> Result<()> {
    use compio::io::AsyncWriteAtExt;

    let mut options = compio::fs::OpenOptions::new();
    options.write(true).create(true);
    let mut file = options.open(&path).await.map_err(Error::Io)?;
    let offset = match std::fs::metadata(&path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(Error::Io(error)),
    };
    let compio::buf::BufResult(result, _buffer) = file.write_all_at(bytes.to_vec(), offset).await;
    result.map_err(Error::Io)?;
    platform_persist_file(&file, durability).await
}

#[cfg(feature = "platform-io")]
async fn platform_persist_path(path: PathBuf, durability: DurabilityMode) -> Result<()> {
    let mut options = compio::fs::OpenOptions::new();
    options.write(true);
    let file = options.open(path).await.map_err(Error::Io)?;
    platform_persist_file(&file, durability).await
}

#[cfg(feature = "platform-io")]
async fn platform_persist_file(file: &compio::fs::File, durability: DurabilityMode) -> Result<()> {
    match durability {
        DurabilityMode::Buffered | DurabilityMode::Flush => Ok(()),
        DurabilityMode::SyncData => file.sync_data().await.map_err(Error::Io),
        DurabilityMode::SyncAll => file.sync_all().await.map_err(Error::Io),
    }
}

#[cfg(feature = "platform-io")]
fn platform_offset(offset: usize) -> Result<u64> {
    u64::try_from(offset).map_err(|_| Error::invalid_options("platform I/O offset overflow"))
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
