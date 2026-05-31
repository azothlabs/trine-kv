use std::{
    collections::VecDeque,
    fmt,
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use crate::{
    error::{Error, Result},
    options::StorageMode,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RuntimeMode {
    #[default]
    NativeThreads,
    Inline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeOptions {
    pub mode: RuntimeMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    flags: u8,
}

const BACKGROUND_THREADS: u8 = 1 << 0;
const COOPERATIVE_TASKS: u8 = 1 << 1;
const BLOCKING_ADAPTER: u8 = 1 << 2;
const CANCELLATION_TOKENS: u8 = 1 << 3;
const TASK_JOIN: u8 = 1 << 4;

const DEFAULT_BLOCKING_WORKERS: usize = 4;
const DEFAULT_BLOCKING_QUEUE_DEPTH: usize = 1024;

type BlockingTask = Box<dyn FnOnce() + Send + 'static>;

#[derive(Debug, Clone)]
pub(crate) struct Runtime {
    options: RuntimeOptions,
    blocking_pool: Option<Arc<BlockingTaskPool>>,
}

#[derive(Debug)]
pub(crate) enum RuntimeTask {
    NativeThread(thread::JoinHandle<()>),
}

struct BlockingTaskPool {
    state: Arc<BlockingTaskPoolState>,
    workers: Mutex<BlockingWorkers>,
}

struct BlockingTaskPoolState {
    queue: Mutex<BlockingTaskQueue>,
    wake: Condvar,
    worker_count: usize,
    queue_depth: usize,
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

#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl RuntimeOptions {
    #[must_use]
    pub const fn native_threads() -> Self {
        Self {
            mode: RuntimeMode::NativeThreads,
        }
    }

    #[must_use]
    pub const fn inline() -> Self {
        Self {
            mode: RuntimeMode::Inline,
        }
    }

    #[must_use]
    pub const fn capabilities(self) -> RuntimeCapabilities {
        match self.mode {
            RuntimeMode::NativeThreads => RuntimeCapabilities::new(
                BACKGROUND_THREADS
                    | COOPERATIVE_TASKS
                    | BLOCKING_ADAPTER
                    | CANCELLATION_TOKENS
                    | TASK_JOIN,
            ),
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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl RuntimeCapabilities {
    const fn new(flags: u8) -> Self {
        Self { flags }
    }

    #[must_use]
    pub const fn background_threads(self) -> bool {
        self.has(BACKGROUND_THREADS)
    }

    #[must_use]
    pub const fn cooperative_tasks(self) -> bool {
        self.has(COOPERATIVE_TASKS)
    }

    #[must_use]
    pub const fn blocking_adapter(self) -> bool {
        self.has(BLOCKING_ADAPTER)
    }

    #[must_use]
    pub const fn cancellation_tokens(self) -> bool {
        self.has(CANCELLATION_TOKENS)
    }

    #[must_use]
    pub const fn task_join(self) -> bool {
        self.has(TASK_JOIN)
    }

    const fn has(self, flag: u8) -> bool {
        self.flags & flag != 0
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

    fn with_blocking_limits(
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

    pub(crate) fn spawn_background(
        &self,
        name: String,
        task: impl FnOnce() + Send + 'static,
    ) -> Result<RuntimeTask> {
        match self.options.mode {
            RuntimeMode::NativeThreads => thread::Builder::new()
                .name(name)
                .spawn(task)
                .map(RuntimeTask::NativeThread)
                .map_err(Error::Io),
            RuntimeMode::Inline => Err(Error::unsupported("runtime background threads")),
        }
    }

    pub(crate) fn spawn_blocking(&self, task: impl FnOnce() + Send + 'static) -> Result<()> {
        let Some(pool) = &self.blocking_pool else {
            return Err(Error::unsupported("runtime blocking adapter"));
        };
        pool.submit(Box::new(task))
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
            }),
            workers: Mutex::new(BlockingWorkers::default()),
        }
    }

    fn submit(&self, task: BlockingTask) -> Result<()> {
        self.ensure_started()?;
        self.state.submit(task)
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
            return Err(Error::Closed);
        }
        if queue.tasks.len() >= self.queue_depth {
            return Err(Error::runtime_busy("blocking task queue is full"));
        }
        queue.tasks.push_back(task);
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
}

fn blocking_worker_loop(state: &BlockingTaskPoolState) {
    while let Some(task) = state.next_task() {
        let _ = panic::catch_unwind(AssertUnwindSafe(task));
    }
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
    let persistent_background_workers = matches!(storage_mode, StorageMode::Persistent { .. })
        && !read_only
        && background_worker_count != 0;
    if persistent_background_workers && !runtime.capabilities().background_threads() {
        return Err(Error::invalid_options(
            "background workers require runtime background threads",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use crate::{
        Db, DbOptions, Error,
        runtime::{CancellationToken, Runtime, RuntimeOptions},
    };

    #[test]
    fn runtime_capabilities_follow_selected_mode() {
        let native = RuntimeOptions::native_threads().capabilities();
        assert!(native.background_threads());
        assert!(native.cancellation_tokens());
        assert!(native.task_join());
        assert!(native.blocking_adapter());

        let inline = RuntimeOptions::inline().capabilities();
        assert!(!inline.background_threads());
        assert!(inline.cooperative_tasks());
        assert!(inline.cancellation_tokens());
        assert!(!inline.blocking_adapter());
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
            .expect_err("inline runtime has no blocking adapter");

        assert!(matches!(error, Error::Unsupported { .. }));
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

        let error = Db::open(options).expect_err("background threads are required");

        assert!(matches!(error, Error::InvalidOptions { .. }));
    }
}
