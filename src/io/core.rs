use super::{
    Arc, AssertUnwindSafe, Context, DurabilityMode, Error, Future, Mutex, Pin, Poll, Result,
    Runtime, StorageReadBuffer, Waker, panic,
};
#[cfg(feature = "platform-io")]
use super::{NativeFileStorageMetrics, PlatformIoOperation, PlatformIoTaskClass};

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
    pub(in crate::io) fn new_platform(
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
    pub(in crate::io) fn mark_platform_execution(&self, class: PlatformIoTaskClass) -> Result<()> {
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
