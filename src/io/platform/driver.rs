#[cfg(any(unix, windows))]
use super::File;
#[cfg(test)]
use super::PlatformIoDriverStats;
#[cfg(all(feature = "platform-io-native", any(unix, windows)))]
use super::platform_backend;
use super::{
    Arc, AtomicU8, DurabilityMode, Error, IoCompletion, IoDriverInfo, Mutex,
    NativeFileStorageMetrics, Ordering, PLATFORM_IO_CLOSED, PLATFORM_IO_CLOSING,
    PLATFORM_IO_FAILED, PLATFORM_IO_RUNNING, PLATFORM_IO_SCHEDULER_QUEUE_DEPTH, PathBuf,
    PlatformIoAppendSession, PlatformIoBackendMatrix, PlatformIoDriver, PlatformIoDriverInner,
    PlatformIoOperation, PlatformIoPublishPlan, PlatformIoScheduler, PlatformIoSchedulerMetrics,
    PlatformIoTask, Result, StorageReadBuffer, blocking_fallback, start_blocking_fallback_executor,
    thread,
};

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

pub(in crate::io) fn reserve_platform_io_queue_slot(metrics: &PlatformIoSchedulerMetrics) -> bool {
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
