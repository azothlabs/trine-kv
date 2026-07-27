//! Native-file backend lifecycle and executor selection.

use std::sync::Arc;

use crate::{
    error::Result,
    io::{InlineIoDriver, IoDriver, IoDriverInfo},
    runtime::Runtime,
};

#[cfg(feature = "platform-io")]
use crate::io::PlatformIoDriver;

use super::{
    NativeFileStorageMetrics, NativeFileStorageStats, StorageFuture, StorageOperation,
    record_timed_storage_future,
};

#[derive(Debug, Clone)]
pub(crate) struct NativeFileBackend {
    pub(super) runtime: Option<Runtime>,
    #[cfg(feature = "platform-io")]
    pub(super) platform_io: Option<PlatformIoDriver>,
    pub(super) metrics: Arc<NativeFileStorageMetrics>,
}

impl NativeFileBackend {
    pub(crate) fn new() -> Self {
        Self {
            runtime: None,
            #[cfg(feature = "platform-io")]
            platform_io: None,
            metrics: Arc::new(NativeFileStorageMetrics::default()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn with_runtime(runtime: Runtime) -> Result<Self> {
        let metrics = Arc::new(NativeFileStorageMetrics::default());
        #[cfg(feature = "platform-io")]
        let platform_io = runtime
            .capabilities()
            .platform_io_driver()
            .then(|| PlatformIoDriver::new(Arc::clone(&metrics)))
            .transpose()?;
        Ok(Self {
            runtime: Some(runtime),
            #[cfg(feature = "platform-io")]
            platform_io,
            metrics,
        })
    }

    #[cfg(feature = "platform-io")]
    pub(crate) fn close_platform_io(&self) -> Result<()> {
        if let Some(driver) = &self.platform_io {
            driver.close()?;
        }
        Ok(())
    }

    pub(crate) fn stats(&self) -> NativeFileStorageStats {
        let uses_platform_io_driver = self.uses_platform_io_driver();
        let uses_platform_async_io = self.supports_platform_async_io();
        let blocking_adapter_stats = self
            .runtime
            .as_ref()
            .and_then(Runtime::blocking_adapter_stats)
            .unwrap_or_default();
        NativeFileStorageStats {
            uses_blocking_adapter: !uses_platform_io_driver
                && self
                    .runtime
                    .as_ref()
                    .is_some_and(|runtime| runtime.capabilities().blocking_adapter()),
            uses_platform_io_driver,
            uses_platform_async_io,
            blocking_adapter_tasks: self.metrics.blocking_adapter_tasks(),
            blocking_adapter_queue_capacity: blocking_adapter_stats.queue_capacity,
            blocking_adapter_queued_tasks: blocking_adapter_stats.queued_tasks,
            blocking_adapter_submitted_tasks: blocking_adapter_stats.submitted_tasks,
            blocking_adapter_completed_tasks: blocking_adapter_stats.completed_tasks,
            blocking_adapter_rejected_tasks: blocking_adapter_stats.rejected_tasks,
            blocking_adapter_total_runtime_micros: blocking_adapter_stats.total_runtime_micros,
            platform_async_io_tasks: self.metrics.platform_async_io_tasks(),
            platform_thread_pool_managed_async_tasks: self
                .metrics
                .platform_thread_pool_managed_async_tasks(),
            platform_blocking_fallback_tasks: self.metrics.platform_blocking_fallback_tasks(),
            inline_tasks: self.metrics.inline_tasks(),
            operations: self.metrics.operation_stats(),
            platform_io_operations: self.metrics.platform_io_operation_stats(),
        }
    }

    pub(super) fn io_driver_info(&self) -> IoDriverInfo {
        #[cfg(feature = "platform-io")]
        if self.platform_io.is_some() {
            return PlatformIoDriver::info();
        }

        if self
            .runtime
            .as_ref()
            .is_some_and(|runtime| runtime.capabilities().blocking_adapter())
        {
            IoDriverInfo::blocking_adapter()
        } else {
            InlineIoDriver.info()
        }
    }

    pub(super) fn uses_platform_io_driver(&self) -> bool {
        #[cfg(feature = "platform-io")]
        {
            self.platform_io.is_some()
        }
        #[cfg(not(feature = "platform-io"))]
        {
            let _ = self.runtime.is_some();
            false
        }
    }

    pub(super) fn supports_platform_async_io(&self) -> bool {
        #[cfg(feature = "platform-io")]
        {
            self.platform_io
                .as_ref()
                .is_some_and(PlatformIoDriver::supports_platform_async_io)
        }
        #[cfg(not(feature = "platform-io"))]
        {
            false
        }
    }

    pub(super) fn run_owned_storage_task<T>(
        &self,
        operation: StorageOperation,
        task: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> StorageFuture<'_, T>
    where
        T: Send + 'static,
    {
        if let Some(runtime) = self.runtime.clone()
            && runtime.capabilities().blocking_adapter()
        {
            self.metrics.record_blocking_adapter_task();
            return record_timed_storage_future(
                Arc::clone(&self.metrics),
                operation,
                Box::pin(async move { runtime.spawn_blocking_result(task)?.await }),
            );
        }

        self.metrics.record_inline_task();
        record_timed_storage_future(
            Arc::clone(&self.metrics),
            operation,
            Box::pin(async move { task() }),
        )
    }
}

impl Default for NativeFileBackend {
    fn default() -> Self {
        Self::new()
    }
}
