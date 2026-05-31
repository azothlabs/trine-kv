use std::thread;

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
    pub background_threads: bool,
    pub cooperative_tasks: bool,
    pub blocking_adapter: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Runtime {
    options: RuntimeOptions,
}

#[derive(Debug)]
pub(crate) enum RuntimeTask {
    NativeThread(thread::JoinHandle<()>),
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
            RuntimeMode::NativeThreads => RuntimeCapabilities {
                background_threads: true,
                cooperative_tasks: true,
                blocking_adapter: true,
            },
            RuntimeMode::Inline => RuntimeCapabilities {
                background_threads: false,
                cooperative_tasks: true,
                blocking_adapter: false,
            },
        }
    }
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self::native_threads()
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
    if persistent_background_workers && !runtime.capabilities().background_threads {
        return Err(Error::invalid_options(
            "background workers require runtime background threads",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{Db, DbOptions, Error, runtime::RuntimeOptions};

    #[test]
    fn runtime_capabilities_follow_selected_mode() {
        assert!(
            RuntimeOptions::native_threads()
                .capabilities()
                .background_threads
        );
        assert!(!RuntimeOptions::inline().capabilities().background_threads);
        assert!(RuntimeOptions::inline().capabilities().cooperative_tasks);
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
