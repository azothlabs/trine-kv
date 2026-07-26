use super::{
    Arc, AtomicU8, BlockingFallbackExecutor, Error, HashMap, IoCompletion, Ordering,
    PLATFORM_IO_FAILED, PLATFORM_IO_MAX_IN_FLIGHT, PLATFORM_IO_THREAD_POOL_QUEUE_DEPTH,
    PLATFORM_IO_THREAD_POOL_WORKERS, Path, PendingPlatformIoTask, PlatformIoAccess,
    PlatformIoAppendSession, PlatformIoBackendMatrix, PlatformIoFailureCompletion,
    PlatformIoOperation, PlatformIoResourceKey, PlatformIoResourceRequest, PlatformIoResourceTable,
    PlatformIoSchedulerMetrics, PlatformIoTask, PlatformIoTaskClass, Result,
    ScheduledPlatformIoTask, VecDeque, blocking_fallback, thread,
};
#[cfg(all(feature = "platform-io-native", any(unix, windows)))]
use super::{NativeCompletionExecutor, platform_backend};

impl PlatformIoResourceTable {
    pub(in crate::io) fn can_acquire(&self, requests: &[PlatformIoResourceRequest]) -> bool {
        requests.iter().all(|request| {
            let Some(state) = self.entries.get(&request.key) else {
                return true;
            };
            match request.access {
                PlatformIoAccess::Shared => !state.exclusive,
                PlatformIoAccess::Exclusive => !state.exclusive && state.shared == 0,
            }
        })
    }

    pub(in crate::io) fn acquire(&mut self, requests: &[PlatformIoResourceRequest]) {
        // The scheduler checks the whole normalized set before changing any
        // entry. A task therefore never waits while holding a partial grant.
        debug_assert!(self.can_acquire(requests));
        for request in requests {
            let state = self.entries.entry(request.key.clone()).or_default();
            match request.access {
                PlatformIoAccess::Shared => state.shared += 1,
                PlatformIoAccess::Exclusive => state.exclusive = true,
            }
        }
    }

    pub(in crate::io) fn release(&mut self, requests: &[PlatformIoResourceRequest]) {
        for request in requests {
            let remove = if let Some(state) = self.entries.get_mut(&request.key) {
                match request.access {
                    PlatformIoAccess::Shared => {
                        debug_assert!(state.shared > 0);
                        state.shared = state.shared.saturating_sub(1);
                    }
                    PlatformIoAccess::Exclusive => {
                        debug_assert!(state.exclusive);
                        state.exclusive = false;
                    }
                }
                state.shared == 0 && !state.exclusive
            } else {
                debug_assert!(false, "released a platform I/O resource that was not held");
                false
            };
            if remove {
                self.entries.remove(&request.key);
            }
        }
    }
}

#[cfg(feature = "platform-io")]
pub(super) fn start_blocking_fallback_executor() -> Result<BlockingFallbackExecutor> {
    let (sender, receiver) = crossbeam_channel::bounded(PLATFORM_IO_THREAD_POOL_QUEUE_DEPTH);
    let mut workers = Vec::with_capacity(PLATFORM_IO_THREAD_POOL_WORKERS);
    for worker_index in 0..PLATFORM_IO_THREAD_POOL_WORKERS {
        let receiver = receiver.clone();
        match thread::Builder::new()
            .name(format!("trine-kv-platform-io-threadpool-{worker_index}"))
            .spawn(move || blocking_fallback::run_worker(receiver))
        {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                drop(sender);
                for worker in workers {
                    let _ = worker.join();
                }
                return Err(Error::Io(error));
            }
        }
    }
    Ok(BlockingFallbackExecutor {
        sender: Some(sender),
        workers,
    })
}

#[cfg(feature = "platform-io")]
pub(super) struct PlatformIoScheduler {
    receiver: crossbeam_channel::Receiver<PlatformIoTask>,
    completed_sender: crossbeam_channel::Sender<Vec<PlatformIoResourceRequest>>,
    completed_receiver: crossbeam_channel::Receiver<Vec<PlatformIoResourceRequest>>,
    matrix: PlatformIoBackendMatrix,
    fallback: BlockingFallbackExecutor,
    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    native: Option<NativeCompletionExecutor>,
    state: Arc<AtomicU8>,
    metrics: Arc<PlatformIoSchedulerMetrics>,
    resources: PlatformIoResourceTable,
    pending: VecDeque<PendingPlatformIoTask>,
    active: usize,
    input_closed: bool,
}

#[cfg(feature = "platform-io")]
impl PlatformIoScheduler {
    pub(super) fn new(
        receiver: crossbeam_channel::Receiver<PlatformIoTask>,
        matrix: PlatformIoBackendMatrix,
        fallback: BlockingFallbackExecutor,
        #[cfg(all(feature = "platform-io-native", any(unix, windows)))] native: Option<
            NativeCompletionExecutor,
        >,
        state: Arc<AtomicU8>,
        metrics: Arc<PlatformIoSchedulerMetrics>,
    ) -> Self {
        let (completed_sender, completed_receiver) = crossbeam_channel::unbounded();
        Self {
            receiver,
            completed_sender,
            completed_receiver,
            matrix,
            fallback,
            #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
            native,
            state,
            metrics,
            resources: PlatformIoResourceTable::default(),
            pending: VecDeque::new(),
            active: 0,
            input_closed: false,
        }
    }

    pub(super) fn run(mut self) {
        loop {
            self.fail_pending_if_needed();
            self.dispatch_ready();
            if self.input_closed && self.pending.is_empty() && self.active == 0 {
                break;
            }
            if self.active == 0 {
                let received = self.receiver.recv();
                self.accept_received(received);
            } else {
                self.wait_for_input_or_completion();
            }
        }
    }

    fn fail_pending_if_needed(&mut self) {
        if self.state.load(Ordering::Acquire) != PLATFORM_IO_FAILED {
            return;
        }
        for pending in self.pending.drain(..) {
            pending
                .task
                .complete_start_error("platform I/O scheduler entered a failed state");
            self.metrics.queued.fetch_sub(1, Ordering::AcqRel);
            self.metrics.completed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dispatch_ready(&mut self) {
        // A blocked task does not form a global head-of-line barrier: later
        // work may pass it only when its complete resource set is unrelated.
        while self.active < PLATFORM_IO_MAX_IN_FLIGHT {
            let Some(index) = self.next_runnable_index() else {
                break;
            };
            let pending = self
                .pending
                .remove(index)
                .expect("pending platform I/O task index remains valid");
            self.metrics.queued.fetch_sub(1, Ordering::AcqRel);
            self.resources.acquire(&pending.resources);
            let task_resources = pending.resources;
            let scheduled = self.schedule(pending.task, &task_resources);
            if self.dispatch(scheduled) {
                self.record_dispatch();
            } else {
                self.resources.release(&task_resources);
                self.metrics.completed.fetch_add(1, Ordering::Relaxed);
                self.state.store(PLATFORM_IO_FAILED, Ordering::Release);
                break;
            }
        }
    }

    fn next_runnable_index(&self) -> Option<usize> {
        self.pending.iter().enumerate().find_map(|(index, task)| {
            if !self.resources.can_acquire(&task.resources) {
                return None;
            }
            let overtakes_conflict =
                self.pending.iter().take(index).any(|earlier| {
                    platform_io_resources_conflict(&earlier.resources, &task.resources)
                });
            (!overtakes_conflict).then_some(index)
        })
    }

    fn schedule(
        &self,
        task: PlatformIoTask,
        resources: &[PlatformIoResourceRequest],
    ) -> ScheduledPlatformIoTask {
        let class = self.matrix.class_for(task.operation());
        let abandon_completion = task.failure_completion();
        ScheduledPlatformIoTask {
            task: Some(task),
            abandon_completion: Some(abandon_completion),
            class,
            #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
            control_metrics: Arc::clone(&self.metrics),
            resources: Some(resources.to_vec()),
            completed: self.completed_sender.clone(),
            state: Arc::clone(&self.state),
            finished: false,
        }
    }

    fn dispatch(&mut self, scheduled: ScheduledPlatformIoTask) -> bool {
        match scheduled.class {
            PlatformIoTaskClass::TruePlatformAsync
            | PlatformIoTaskClass::PlatformNativeAsyncButPartial => self.dispatch_native(scheduled),
            PlatformIoTaskClass::ThreadPoolManagedAsync | PlatformIoTaskClass::BlockingFallback => {
                self.dispatch_fallback(scheduled)
            }
            PlatformIoTaskClass::Unsupported => {
                let completed = scheduled
                    .complete_unsupported("platform I/O operation is unsupported on this target");
                let _ = self.completed_sender.send(completed);
                true
            }
        }
    }

    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    fn dispatch_native(&mut self, scheduled: ScheduledPlatformIoTask) -> bool {
        let Some(executor) = self.native.as_mut() else {
            let _ = scheduled.complete_start_error("native platform I/O executor is unavailable");
            return false;
        };
        match executor
            .sender
            .as_mut()
            .expect("native executor sender remains open while scheduling")
            .try_send(scheduled)
        {
            Ok(()) => true,
            Err(error) => {
                let _ = error
                    .into_inner()
                    .complete_start_error("native platform I/O executor stopped accepting tasks");
                false
            }
        }
    }

    #[cfg(not(all(feature = "platform-io-native", any(unix, windows))))]
    fn dispatch_native(&self, scheduled: ScheduledPlatformIoTask) -> bool {
        let _ = self;
        let _ = scheduled.complete_start_error("native platform I/O executor is unavailable");
        false
    }

    fn dispatch_fallback(&self, scheduled: ScheduledPlatformIoTask) -> bool {
        match self
            .fallback
            .sender
            .as_ref()
            .expect("thread-pool sender remains open while scheduling")
            .try_send(scheduled)
        {
            Ok(()) => true,
            Err(error) => {
                let _ = error.into_inner().complete_start_error(
                    "platform I/O thread-pool executor stopped accepting tasks",
                );
                false
            }
        }
    }

    fn record_dispatch(&mut self) {
        self.active += 1;
        let in_flight = self
            .metrics
            .in_flight
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        self.metrics
            .max_in_flight
            .fetch_max(in_flight, Ordering::Relaxed);
    }

    fn wait_for_input_or_completion(&mut self) {
        if self.input_closed {
            let completed = self.completed_receiver.recv();
            self.accept_completed(completed);
            return;
        }
        crossbeam_channel::select! {
            recv(self.completed_receiver) -> completed => self.accept_completed(completed),
            recv(self.receiver) -> task => self.accept_received(task),
        }
    }

    fn accept_completed(
        &mut self,
        completed: std::result::Result<
            Vec<PlatformIoResourceRequest>,
            crossbeam_channel::RecvError,
        >,
    ) {
        if let Ok(completed) = completed {
            self.resources.release(&completed);
            self.active = self.active.saturating_sub(1);
            self.metrics.in_flight.fetch_sub(1, Ordering::AcqRel);
            self.metrics.completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.state.store(PLATFORM_IO_FAILED, Ordering::Release);
        }
    }

    fn accept_received(
        &mut self,
        task: std::result::Result<PlatformIoTask, crossbeam_channel::RecvError>,
    ) {
        match task {
            Ok(task) if self.state.load(Ordering::Acquire) == PLATFORM_IO_FAILED => {
                task.complete_start_error("platform I/O scheduler entered a failed state");
                self.metrics.queued.fetch_sub(1, Ordering::AcqRel);
                self.metrics.completed.fetch_add(1, Ordering::Relaxed);
            }
            Ok(task) => {
                let resources = task.resources();
                self.pending
                    .push_back(PendingPlatformIoTask { task, resources });
            }
            Err(_) => self.input_closed = true,
        }
    }
}

#[cfg(feature = "platform-io")]
impl Drop for PlatformIoScheduler {
    fn drop(&mut self) {
        self.state.store(PLATFORM_IO_FAILED, Ordering::Release);
        let mut abandoned = 0_u64;
        for pending in self.pending.drain(..) {
            pending
                .task
                .complete_start_error("platform I/O scheduler stopped before task admission");
            abandoned = abandoned.saturating_add(1);
        }
        while let Ok(task) = self.receiver.try_recv() {
            task.complete_start_error("platform I/O scheduler stopped before task admission");
            abandoned = abandoned.saturating_add(1);
        }
        if abandoned > 0 {
            self.metrics.queued.fetch_sub(
                usize::try_from(abandoned).expect("bounded platform I/O queue count fits usize"),
                Ordering::AcqRel,
            );
            self.metrics
                .completed
                .fetch_add(abandoned, Ordering::Relaxed);
        }
    }
}

#[cfg(feature = "platform-io")]
fn normalize_platform_io_resources(
    requests: Vec<PlatformIoResourceRequest>,
) -> Vec<PlatformIoResourceRequest> {
    let mut normalized = HashMap::with_capacity(requests.len());
    for request in requests {
        normalized
            .entry(request.key)
            .and_modify(|access| {
                if request.access == PlatformIoAccess::Exclusive {
                    *access = PlatformIoAccess::Exclusive;
                }
            })
            .or_insert(request.access);
    }
    normalized
        .into_iter()
        .map(|(key, access)| PlatformIoResourceRequest { key, access })
        .collect()
}

#[cfg(feature = "platform-io")]
pub(in crate::io) fn platform_io_resources_conflict(
    left: &[PlatformIoResourceRequest],
    right: &[PlatformIoResourceRequest],
) -> bool {
    left.iter().any(|left| {
        right.iter().any(|right| {
            left.key == right.key
                && (left.access == PlatformIoAccess::Exclusive
                    || right.access == PlatformIoAccess::Exclusive)
        })
    })
}

#[cfg(feature = "platform-io")]
impl PlatformIoTask {
    const fn operation(&self) -> PlatformIoOperation {
        match self {
            Self::Len { .. } => PlatformIoOperation::LengthLookup,
            Self::ReadExactAtOwned { .. } => PlatformIoOperation::OwnedRandomRead,
            Self::ReadOptional { .. } => PlatformIoOperation::OptionalWholeObjectRead,
            Self::Publish { plan, .. } => plan.operation,
            Self::Append { .. } => PlatformIoOperation::Append,
            Self::OpenAppend { .. } => PlatformIoOperation::AppendObjectOpen,
            Self::Persist { .. } => PlatformIoOperation::Persist,
            Self::Delete { .. } => PlatformIoOperation::ObjectDelete,
            Self::CreateDirAll { .. } => PlatformIoOperation::DirectoryCreate,
            Self::SyncDir { .. } => PlatformIoOperation::DirectorySync,
            Self::ListFilePaths { .. } => PlatformIoOperation::DirectoryListing,
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease { .. } => PlatformIoOperation::WriterLeaseAcquire,
        }
    }

    pub(in crate::io) fn resources(&self) -> Vec<PlatformIoResourceRequest> {
        let shared_object = |path: &Path| PlatformIoResourceRequest {
            key: PlatformIoResourceKey::Object(path.to_path_buf()),
            access: PlatformIoAccess::Shared,
        };
        let exclusive_object = |path: &Path| PlatformIoResourceRequest {
            key: PlatformIoResourceKey::Object(path.to_path_buf()),
            access: PlatformIoAccess::Exclusive,
        };
        let shared_directory = |path: &Path| PlatformIoResourceRequest {
            key: PlatformIoResourceKey::Directory(path.to_path_buf()),
            access: PlatformIoAccess::Shared,
        };
        let exclusive_directory = |path: &Path| PlatformIoResourceRequest {
            key: PlatformIoResourceKey::Directory(path.to_path_buf()),
            access: PlatformIoAccess::Exclusive,
        };
        let parent_directory = |path: &Path| {
            path.parent()
                .map(exclusive_directory)
                .into_iter()
                .collect::<Vec<_>>()
        };

        let requests = match self {
            Self::Len { path, .. }
            | Self::ReadExactAtOwned { path, .. }
            | Self::ReadOptional { path, .. } => vec![shared_object(path)],
            Self::Publish { plan, .. } => {
                let mut requests = vec![
                    exclusive_object(&plan.path),
                    exclusive_object(&plan.temporary_path),
                ];
                requests.extend(parent_directory(&plan.path));
                requests.extend(parent_directory(&plan.temporary_path));
                requests
            }
            Self::Append { session, .. } | Self::Persist { session, .. } => {
                vec![exclusive_object(session.path.as_ref())]
            }
            Self::OpenAppend { path, .. } => {
                let mut requests = vec![exclusive_object(path)];
                requests.extend(parent_directory(path));
                requests
            }
            Self::Delete { path, .. } => {
                let mut requests = vec![exclusive_object(path)];
                requests.extend(parent_directory(path));
                requests
            }
            Self::CreateDirAll { path, .. } | Self::SyncDir { path, .. } => {
                vec![exclusive_directory(path)]
            }
            Self::ListFilePaths { path, .. } => vec![shared_directory(path)],
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease { path, .. } => {
                let mut requests = vec![exclusive_object(path)];
                requests.extend(parent_directory(path));
                requests
            }
        };
        normalize_platform_io_resources(requests)
    }

    pub(in crate::io) fn failure_completion(&self) -> PlatformIoFailureCompletion {
        match self {
            Self::Len { completion, .. } => PlatformIoFailureCompletion::Len(completion.clone()),
            Self::ReadExactAtOwned { completion, .. } => {
                PlatformIoFailureCompletion::Read(completion.clone())
            }
            Self::ReadOptional { completion, .. } => {
                PlatformIoFailureCompletion::Optional(completion.clone())
            }
            Self::ListFilePaths { completion, .. } => {
                PlatformIoFailureCompletion::Paths(completion.clone())
            }
            Self::OpenAppend { completion, .. } => {
                PlatformIoFailureCompletion::AppendSession(completion.clone())
            }
            Self::Append { completion, .. }
            | Self::Persist { completion, .. }
            | Self::Publish { completion, .. }
            | Self::Delete { completion, .. }
            | Self::CreateDirAll { completion, .. }
            | Self::SyncDir { completion, .. } => {
                PlatformIoFailureCompletion::Unit(completion.clone())
            }
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease { completion, .. } => {
                PlatformIoFailureCompletion::Lease(completion.clone())
            }
        }
    }

    pub(super) fn mark_execution_class(&self, class: PlatformIoTaskClass) -> Result<()> {
        match self {
            Self::Len { completion, .. } => completion.mark_platform_execution(class),
            Self::ReadExactAtOwned { completion, .. } => completion.mark_platform_execution(class),
            Self::ReadOptional { completion, .. } => completion.mark_platform_execution(class),
            Self::ListFilePaths { completion, .. } => completion.mark_platform_execution(class),
            Self::OpenAppend { completion, .. } => completion.mark_platform_execution(class),
            Self::Append { completion, .. }
            | Self::Persist { completion, .. }
            | Self::Publish { completion, .. }
            | Self::Delete { completion, .. }
            | Self::CreateDirAll { completion, .. }
            | Self::SyncDir { completion, .. } => completion.mark_platform_execution(class),
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease { completion, .. } => {
                completion.mark_platform_execution(class)
            }
        }
    }

    pub(super) fn run_thread_pool(self) {
        match self {
            Self::Len { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::len(path));
            }
            Self::ReadExactAtOwned {
                path,
                offset,
                len,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::read_exact_at_owned(path, offset, len),
                );
            }
            Self::ReadOptional {
                path,
                max_bytes,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::read_optional(&path, max_bytes),
                );
            }
            Self::Publish { plan, completion } => {
                complete_platform_io(&completion, blocking_fallback::publish(&plan));
            }
            Self::Append {
                session,
                bytes,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::append(session.path.as_ref().clone(), &bytes, durability),
                );
            }
            Self::OpenAppend { path, completion } => {
                let result = blocking_fallback::open_append(path.clone())
                    .map(|()| PlatformIoAppendSession::opened(path));
                complete_platform_io(&completion, result);
            }
            Self::Persist {
                session,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::persist_path(session.path.as_ref().clone(), durability),
                );
            }
            Self::Delete { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::delete_path(path));
            }
            Self::CreateDirAll { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::create_dir_all(path));
            }
            Self::SyncDir { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::sync_directory(path));
            }
            Self::ListFilePaths { path, completion } => {
                complete_platform_io(&completion, blocking_fallback::list_file_paths(path));
            }
            #[cfg(any(unix, windows))]
            Self::AcquireWriterLease {
                path,
                owner,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    blocking_fallback::acquire_writer_lease(&path, &owner),
                );
            }
        }
    }

    #[cfg(all(feature = "platform-io-native", any(unix, windows)))]
    pub(super) async fn run(self) {
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
            Self::Publish { plan, completion } => {
                complete_platform_io(&completion, platform_backend::publish(plan).await);
            }
            Self::Append {
                session,
                bytes,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::append(session.path.as_ref().clone(), bytes, durability)
                        .await,
                );
            }
            Self::OpenAppend { path, completion } => {
                let result = platform_backend::open_append(path.clone())
                    .await
                    .map(|()| PlatformIoAppendSession::opened(path));
                complete_platform_io(&completion, result);
            }
            Self::Persist {
                session,
                durability,
                completion,
            } => {
                complete_platform_io(
                    &completion,
                    platform_backend::persist_path(session.path.as_ref().clone(), durability).await,
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
            #[cfg(any(unix, windows))]
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

    pub(super) fn complete_start_error(self, message: &str) {
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
            Self::OpenAppend { completion, .. } => {
                complete_platform_io(&completion, Err(error()));
            }
            Self::Append { completion, .. }
            | Self::Persist { completion, .. }
            | Self::Publish { completion, .. }
            | Self::Delete { completion, .. }
            | Self::CreateDirAll { completion, .. }
            | Self::SyncDir { completion, .. } => {
                complete_platform_io(&completion, Err(error()));
            }
            #[cfg(any(unix, windows))]
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
