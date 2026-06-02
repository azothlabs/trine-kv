use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    thread,
    time::{Duration, Instant},
};

use crate::{
    error::{Error, Result},
    options::StorageMode,
};

/// Runtime strategy used by async-first database operations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RuntimeMode {
    /// Use native threads for background and blocking work.
    #[default]
    NativeThreads,
    /// Prefer platform async I/O when the target and feature set support it.
    PlatformIo,
    /// Run supported work inline on the caller's thread.
    Inline,
}

/// Runtime configuration for a database handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeOptions {
    /// Selected runtime strategy.
    pub mode: RuntimeMode,
}

/// Capabilities exposed by a selected runtime configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    flags: u8,
}

const BACKGROUND_THREADS: u8 = 1 << 0;
const COOPERATIVE_TASKS: u8 = 1 << 1;
const BLOCKING_ADAPTER: u8 = 1 << 2;
const CANCELLATION_TOKENS: u8 = 1 << 3;
const TASK_JOIN: u8 = 1 << 4;
const PLATFORM_ASYNC_IO: u8 = 1 << 5;

const DEFAULT_BLOCKING_WORKERS: usize = 4;
const DEFAULT_BLOCKING_QUEUE_DEPTH: usize = 1024;

type BlockingTask = Box<dyn FnOnce() + Send + 'static>;

#[derive(Debug, Clone)]
pub(crate) struct Runtime {
    options: RuntimeOptions,
    blocking_pool: Option<Arc<BlockingTaskPool>>,
}

#[derive(Debug)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) enum RuntimeTask {
    NativeThread(thread::JoinHandle<()>),
}

pub(crate) struct BlockingResultFuture<T> {
    state: Arc<Mutex<BlockingResultState<T>>>,
}

struct BlockingTaskPool {
    state: Arc<BlockingTaskPoolState>,
    workers: Mutex<BlockingWorkers>,
}

struct BlockingResultState<T> {
    result: Option<Result<T>>,
    waker: Option<Waker>,
}

struct BlockingTaskPoolState {
    queue: Mutex<BlockingTaskQueue>,
    wake: Condvar,
    worker_count: usize,
    queue_depth: usize,
    submitted_tasks: AtomicU64,
    completed_tasks: AtomicU64,
    rejected_tasks: AtomicU64,
    total_runtime_micros: AtomicU64,
}

#[derive(Debug, Default)]
struct BlockingWorkers {
    started: bool,
    handles: Vec<thread::JoinHandle<()>>,
}

#[derive(Default)]
struct BlockingTaskQueue {
    tasks: VecDeque<BlockingTask>,
    shutdown: bool,
}

/// Shareable flag used to request cancellation of cooperative work.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RuntimeBlockingAdapterStats {
    pub(crate) worker_count: usize,
    pub(crate) queue_capacity: usize,
    pub(crate) queued_tasks: usize,
    pub(crate) submitted_tasks: u64,
    pub(crate) completed_tasks: u64,
    pub(crate) rejected_tasks: u64,
    pub(crate) total_runtime_micros: u64,
}

impl RuntimeOptions {
    /// Uses native threads for background work and blocking adapters.
    #[must_use]
    pub const fn native_threads() -> Self {
        Self {
            mode: RuntimeMode::NativeThreads,
        }
    }

    /// Runs supported work inline without background threads.
    #[must_use]
    pub const fn inline() -> Self {
        Self {
            mode: RuntimeMode::Inline,
        }
    }

    /// Requests platform async I/O support when available.
    #[must_use]
    pub const fn platform_io() -> Self {
        Self {
            mode: RuntimeMode::PlatformIo,
        }
    }

    /// Returns the capabilities implied by these runtime options.
    #[must_use]
    pub const fn capabilities(self) -> RuntimeCapabilities {
        const NATIVE_THREAD_FLAGS: u8 = BACKGROUND_THREADS
            | COOPERATIVE_TASKS
            | BLOCKING_ADAPTER
            | CANCELLATION_TOKENS
            | TASK_JOIN;
        match self.mode {
            RuntimeMode::NativeThreads => RuntimeCapabilities::new(NATIVE_THREAD_FLAGS),
            RuntimeMode::PlatformIo => {
                RuntimeCapabilities::new(NATIVE_THREAD_FLAGS | platform_async_io_flag())
            }
            RuntimeMode::Inline => {
                RuntimeCapabilities::new(COOPERATIVE_TASKS | CANCELLATION_TOKENS)
            }
        }
    }
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self::native_threads()
    }
}

impl CancellationToken {
    /// Creates a token in the not-cancelled state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the token as cancelled.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl RuntimeCapabilities {
    const fn new(flags: u8) -> Self {
        Self { flags }
    }

    /// Returns whether the runtime can spawn background threads.
    #[must_use]
    pub const fn background_threads(self) -> bool {
        self.has(BACKGROUND_THREADS)
    }

    /// Returns whether the runtime can cooperatively run maintenance tasks.
    #[must_use]
    pub const fn cooperative_tasks(self) -> bool {
        self.has(COOPERATIVE_TASKS)
    }

    /// Returns whether the runtime can adapt blocking storage work.
    #[must_use]
    pub const fn blocking_adapter(self) -> bool {
        self.has(BLOCKING_ADAPTER)
    }

    /// Returns whether cancellation tokens are supported.
    #[must_use]
    pub const fn cancellation_tokens(self) -> bool {
        self.has(CANCELLATION_TOKENS)
    }

    /// Returns whether spawned tasks can be joined.
    #[must_use]
    pub const fn task_join(self) -> bool {
        self.has(TASK_JOIN)
    }

    /// Returns whether at least one Trine storage operation uses platform async I/O.
    #[must_use]
    pub const fn platform_async_io(self) -> bool {
        self.has(PLATFORM_ASYNC_IO)
    }

    const fn has(self, flag: u8) -> bool {
        self.flags & flag != 0
    }
}

const fn platform_async_io_flag() -> u8 {
    if cfg!(all(feature = "platform-io", target_os = "linux")) {
        PLATFORM_ASYNC_IO
    } else {
        0
    }
}

impl Runtime {
    pub(crate) fn new(options: RuntimeOptions) -> Self {
        Self::with_blocking_limits(
            options,
            DEFAULT_BLOCKING_WORKERS,
            DEFAULT_BLOCKING_QUEUE_DEPTH,
        )
    }

    pub(crate) fn with_blocking_limits(
        options: RuntimeOptions,
        blocking_worker_count: usize,
        blocking_queue_depth: usize,
    ) -> Self {
        let blocking_pool = if options.capabilities().blocking_adapter() {
            Some(Arc::new(BlockingTaskPool::new(
                blocking_worker_count,
                blocking_queue_depth,
            )))
        } else {
            None
        };
        Self {
            options,
            blocking_pool,
        }
    }

    pub(crate) const fn capabilities(&self) -> RuntimeCapabilities {
        self.options.capabilities()
    }

    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    pub(crate) fn spawn_background(
        &self,
        name: String,
        task: impl FnOnce() + Send + 'static,
    ) -> Result<RuntimeTask> {
        match self.options.mode {
            RuntimeMode::NativeThreads | RuntimeMode::PlatformIo => thread::Builder::new()
                .name(name)
                .spawn(task)
                .map(RuntimeTask::NativeThread)
                .map_err(Error::Io),
            RuntimeMode::Inline => Err(Error::unsupported("runtime background threads")),
        }
    }

    pub(crate) fn spawn_blocking(&self, task: impl FnOnce() + Send + 'static) -> Result<()> {
        let Some(pool) = &self.blocking_pool else {
            return Err(Error::unsupported("runtime sync adapter"));
        };
        pool.submit(Box::new(task))
    }

    pub(crate) fn spawn_blocking_result<T>(
        &self,
        task: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<BlockingResultFuture<T>>
    where
        T: Send + 'static,
    {
        let state = Arc::new(Mutex::new(BlockingResultState {
            result: None,
            waker: None,
        }));
        let task_state = Arc::clone(&state);
        self.spawn_blocking(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(task))
                .unwrap_or_else(|_| Err(Error::runtime_busy("blocking task panicked")));
            if let Ok(mut state) = task_state.lock() {
                state.result = Some(result);
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
            }
        })?;
        Ok(BlockingResultFuture { state })
    }

    pub(crate) fn blocking_adapter_stats(&self) -> Option<RuntimeBlockingAdapterStats> {
        self.blocking_pool.as_ref().map(|pool| pool.stats())
    }
}

impl<T> fmt::Debug for BlockingResultFuture<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("BlockingResultFuture").finish()
    }
}

impl<T> Future for BlockingResultFuture<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Ok(mut state) = self.state.lock() else {
            return Poll::Ready(Err(Error::runtime_busy(
                "blocking result state is poisoned",
            )));
        };
        if let Some(result) = state.result.take() {
            Poll::Ready(result)
        } else {
            state.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

impl fmt::Debug for BlockingTaskPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let queued = self.state.queue.lock().map_or(0, |queue| queue.tasks.len());
        let started = self.workers.lock().is_ok_and(|workers| workers.started);
        formatter
            .debug_struct("BlockingTaskPool")
            .field("worker_count", &self.state.worker_count)
            .field("queue_depth", &self.state.queue_depth)
            .field("queued", &queued)
            .field("started", &started)
            .finish()
    }
}

impl BlockingTaskPool {
    fn new(worker_count: usize, queue_depth: usize) -> Self {
        Self {
            state: Arc::new(BlockingTaskPoolState {
                queue: Mutex::new(BlockingTaskQueue::default()),
                wake: Condvar::new(),
                worker_count: worker_count.max(1),
                queue_depth: queue_depth.max(1),
                submitted_tasks: AtomicU64::new(0),
                completed_tasks: AtomicU64::new(0),
                rejected_tasks: AtomicU64::new(0),
                total_runtime_micros: AtomicU64::new(0),
            }),
            workers: Mutex::new(BlockingWorkers::default()),
        }
    }

    fn submit(&self, task: BlockingTask) -> Result<()> {
        self.ensure_started()?;
        self.state.submit(task)
    }

    fn stats(&self) -> RuntimeBlockingAdapterStats {
        self.state.stats()
    }

    fn ensure_started(&self) -> Result<()> {
        let mut workers = self
            .workers
            .lock()
            .map_err(|_| Error::runtime_busy("blocking worker registry is poisoned"))?;
        if workers.started {
            return Ok(());
        }

        let mut handles = Vec::with_capacity(self.state.worker_count);
        for worker_index in 0..self.state.worker_count {
            let state = Arc::clone(&self.state);
            match thread::Builder::new()
                .name(format!("trine-kv-blocking-{worker_index}"))
                .spawn(move || blocking_worker_loop(&state))
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    self.state.shutdown();
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(Error::Io(error));
                }
            }
        }
        workers.handles = handles;
        workers.started = true;
        Ok(())
    }
}

impl Drop for BlockingTaskPool {
    fn drop(&mut self) {
        self.state.shutdown();
        let current_thread = thread::current().id();
        let Ok(mut workers) = self.workers.lock() else {
            return;
        };
        for handle in workers.handles.drain(..) {
            if handle.thread().id() == current_thread {
                continue;
            }
            let _ = handle.join();
        }
    }
}

impl BlockingTaskPoolState {
    fn submit(&self, task: BlockingTask) -> Result<()> {
        let mut queue = self
            .queue
            .lock()
            .map_err(|_| Error::runtime_busy("blocking task queue is poisoned"))?;
        if queue.shutdown {
            self.rejected_tasks.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Closed);
        }
        if queue.tasks.len() >= self.queue_depth {
            self.rejected_tasks.fetch_add(1, Ordering::Relaxed);
            return Err(Error::runtime_busy("blocking task queue is full"));
        }
        queue.tasks.push_back(task);
        self.submitted_tasks.fetch_add(1, Ordering::Relaxed);
        self.wake.notify_one();
        Ok(())
    }

    fn next_task(&self) -> Option<BlockingTask> {
        let Ok(mut queue) = self.queue.lock() else {
            return None;
        };
        loop {
            if let Some(task) = queue.tasks.pop_front() {
                return Some(task);
            }
            if queue.shutdown {
                return None;
            }
            let Ok(next_queue) = self.wake.wait(queue) else {
                return None;
            };
            queue = next_queue;
        }
    }

    fn shutdown(&self) {
        if let Ok(mut queue) = self.queue.lock() {
            queue.shutdown = true;
            self.wake.notify_all();
        }
    }

    fn record_completed(&self, runtime: Duration) {
        self.completed_tasks.fetch_add(1, Ordering::Relaxed);
        self.total_runtime_micros
            .fetch_add(duration_to_micros_saturating(runtime), Ordering::Relaxed);
    }

    fn stats(&self) -> RuntimeBlockingAdapterStats {
        RuntimeBlockingAdapterStats {
            worker_count: self.worker_count,
            queue_capacity: self.queue_depth,
            queued_tasks: self.queue.lock().map_or(0, |queue| queue.tasks.len()),
            submitted_tasks: self.submitted_tasks.load(Ordering::Acquire),
            completed_tasks: self.completed_tasks.load(Ordering::Acquire),
            rejected_tasks: self.rejected_tasks.load(Ordering::Acquire),
            total_runtime_micros: self.total_runtime_micros.load(Ordering::Acquire),
        }
    }
}

fn blocking_worker_loop(state: &BlockingTaskPoolState) {
    while let Some(task) = state.next_task() {
        let started = Instant::now();
        let _ = panic::catch_unwind(AssertUnwindSafe(task));
        state.record_completed(started.elapsed());
    }
}

fn duration_to_micros_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

impl RuntimeTask {
    pub(crate) fn is_current_thread(&self) -> bool {
        match self {
            Self::NativeThread(handle) => handle.thread().id() == thread::current().id(),
        }
    }

    pub(crate) fn join(self) -> thread::Result<()> {
        match self {
            Self::NativeThread(handle) => handle.join(),
        }
    }
}

pub(crate) fn validate_runtime_options(
    runtime: RuntimeOptions,
    storage_mode: &StorageMode,
    read_only: bool,
    background_worker_count: usize,
) -> Result<()> {
    #[cfg(not(feature = "platform-io"))]
    if matches!(runtime.mode, RuntimeMode::PlatformIo) {
        return Err(Error::unsupported_backend(
            "platform async I/O runtime requires the platform-io feature",
        ));
    }

    let persistent_background_workers =
        storage_mode.persistent_path().is_some() && !read_only && background_worker_count != 0;
    if persistent_background_workers && !runtime.capabilities().background_threads() {
        return Err(Error::invalid_options(
            "background workers require runtime background threads",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{Arc, mpsc},
        task::{Context, Poll, Wake, Waker},
        thread,
        time::Duration,
    };

    use crate::{
        Db, DbOptions, Error, Result,
        runtime::{CancellationToken, Runtime, RuntimeOptions},
    };

    struct ThreadWaker {
        thread: thread::Thread,
    }

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.thread.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.thread.unpark();
        }
    }

    fn block_on_test_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
        let waker = Waker::from(Arc::new(ThreadWaker {
            thread: thread::current(),
        }));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(result) => return result,
                Poll::Pending => thread::park_timeout(Duration::from_secs(1)),
            }
        }
    }

    #[test]
    fn runtime_capabilities_follow_selected_mode() {
        let native = RuntimeOptions::native_threads().capabilities();
        assert!(native.background_threads());
        assert!(native.cancellation_tokens());
        assert!(native.task_join());
        assert!(native.blocking_adapter());
        assert!(!native.platform_async_io());

        let platform = RuntimeOptions::platform_io().capabilities();
        assert!(platform.background_threads());
        assert!(platform.cancellation_tokens());
        assert!(platform.task_join());
        assert!(platform.blocking_adapter());
        assert_eq!(
            platform.platform_async_io(),
            cfg!(all(feature = "platform-io", target_os = "linux"))
        );

        let inline = RuntimeOptions::inline().capabilities();
        assert!(!inline.background_threads());
        assert!(inline.cooperative_tasks());
        assert!(inline.cancellation_tokens());
        assert!(!inline.blocking_adapter());
        assert!(!inline.platform_async_io());
        assert!(!inline.task_join());
    }

    #[test]
    fn cancellation_token_clones_share_state() {
        let token = CancellationToken::new();
        let clone = token.clone();

        assert!(!token.is_cancelled());
        clone.cancel();

        assert!(token.is_cancelled());
        assert!(clone.is_cancelled());
    }

    #[test]
    fn native_background_task_observes_cancellation_and_joins() {
        let runtime = Runtime::new(RuntimeOptions::native_threads());
        let token = CancellationToken::new();
        let worker_token = token.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let task = runtime
            .spawn_background("trine-kv-runtime-cancel-test".to_owned(), move || {
                started_tx.send(()).expect("report worker start");
                while !worker_token.is_cancelled() {
                    thread::sleep(Duration::from_millis(1));
                }
                done_tx.send(()).expect("report worker done");
            })
            .expect("spawn background task");

        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker starts");
        token.cancel();
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker observes cancellation");
        task.join().expect("worker joins");
    }

    #[test]
    fn native_blocking_adapter_runs_tasks_on_bounded_workers() {
        let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 2);
        let (done_tx, done_rx) = mpsc::channel();

        runtime
            .spawn_blocking(move || {
                done_tx
                    .send(thread::current().name().map(str::to_owned))
                    .expect("report blocking task completion");
            })
            .expect("spawn blocking task");

        let worker_name = done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking task completes")
            .expect("blocking worker has a name");
        assert!(worker_name.starts_with("trine-kv-blocking-"));
    }

    #[test]
    fn native_blocking_result_future_completes_on_bounded_worker() {
        let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 2);
        let future = runtime
            .spawn_blocking_result(|| {
                thread::current()
                    .name()
                    .map(str::to_owned)
                    .ok_or_else(|| Error::runtime_busy("blocking worker is unnamed"))
            })
            .expect("spawn blocking result task");

        let worker_name = block_on_test_future(future).expect("blocking result completes");

        assert!(worker_name.starts_with("trine-kv-blocking-"));
    }

    #[test]
    fn native_blocking_adapter_rejects_full_queue() {
        let runtime = Runtime::with_blocking_limits(RuntimeOptions::native_threads(), 1, 1);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (queued_tx, queued_rx) = mpsc::channel();

        runtime
            .spawn_blocking(move || {
                started_tx.send(()).expect("report blocking task start");
                release_rx.recv().expect("wait for release");
            })
            .expect("spawn first blocking task");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first blocking task starts");

        runtime
            .spawn_blocking(move || {
                queued_tx.send(()).expect("report queued task completion");
            })
            .expect("queue second blocking task");

        let error = runtime
            .spawn_blocking(|| {})
            .expect_err("third blocking task exceeds bounded queue");
        assert!(matches!(error, Error::RuntimeBusy { .. }));
        let stats = runtime
            .blocking_adapter_stats()
            .expect("sync adapter stats exist");
        assert_eq!(stats.worker_count, 1);
        assert_eq!(stats.queue_capacity, 1);
        assert_eq!(stats.queued_tasks, 1);
        assert_eq!(stats.submitted_tasks, 2);
        assert_eq!(stats.completed_tasks, 0);
        assert_eq!(stats.rejected_tasks, 1);
        assert!(
            queued_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "queued task must wait until the active worker is released"
        );

        release_tx.send(()).expect("release first task");
        queued_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("queued task eventually runs");
    }

    #[test]
    fn inline_runtime_rejects_blocking_adapter_tasks() {
        let runtime = Runtime::new(RuntimeOptions::inline());

        let error = runtime
            .spawn_blocking(|| {})
            .expect_err("inline runtime has no sync adapter");

        assert!(matches!(error, Error::Unsupported { .. }));
    }

    #[cfg(not(feature = "platform-io"))]
    #[test]
    fn platform_io_runtime_requires_feature() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-runtime-no-platform-io-{}",
            std::process::id()
        ));
        let mut options = DbOptions::persistent(path.clone());
        options.runtime = RuntimeOptions::platform_io();
        let error = Db::open_sync(options).expect_err("platform I/O requires feature");

        assert!(matches!(error, Error::UnsupportedBackend { .. }));
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn persistent_background_workers_require_thread_capability() {
        let path = std::env::temp_dir().join(format!(
            "trine-kv-runtime-no-threads-{}",
            std::process::id()
        ));
        let mut options = DbOptions::persistent(path);
        options.runtime = RuntimeOptions::inline();
        options.background_worker_count = 1;

        let error = Db::open_sync(options).expect_err("background threads are required");

        assert!(matches!(error, Error::InvalidOptions { .. }));
    }
}
