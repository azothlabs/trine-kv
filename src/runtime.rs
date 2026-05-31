use std::{
    sync::{
        Arc,
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct Runtime {
    options: RuntimeOptions,
}

#[derive(Debug)]
pub(crate) enum RuntimeTask {
    NativeThread(thread::JoinHandle<()>),
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
    pub(crate) const fn new(options: RuntimeOptions) -> Self {
        Self { options }
    }

    pub(crate) const fn capabilities(self) -> RuntimeCapabilities {
        self.options.capabilities()
    }

    pub(crate) fn spawn_background(
        self,
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
