use super::*;

#[derive(Debug)]
pub(super) struct WriteRequest {
    pub(super) operations: Vec<BatchOperation>,
    pub(super) write_options: WriteOptions,
    pub(super) transaction_reads: Option<TransactionReads>,
}

#[derive(Debug)]
pub(super) enum AcceptedWriteState {
    Noop(CommitInfo),
    Pending(WriterLocalWriteState),
}

#[derive(Debug)]
pub(super) struct WriterLocalWriteState {
    pub(super) prepared: PreparedCommit,
    pub(super) wal_accept: WalAcceptState,
}

#[derive(Debug)]
pub(super) enum SequencedWriteState {
    Noop(CommitInfo),
    Pending(SequencedWrite),
}

#[derive(Debug)]
pub(super) struct SequencedWrite {
    pub(super) prepared: PreparedCommit,
    pub(super) slot: CommitSlot,
    pub(super) durability: DurabilityMode,
    pub(super) accept_wal: bool,
}

#[derive(Debug)]
pub(super) struct DurableSequencedWrite {
    pub(super) prepared: PreparedCommit,
    pub(super) slot: CommitSlot,
}

#[derive(Debug)]
pub(super) struct PreparedCommit {
    pub(super) write_options: WriteOptions,
    pub(super) transaction_reads: Option<TransactionReads>,
    pub(super) wal_operations: Vec<BatchOperation>,
    pub(super) deltas: Vec<PreparedShardDelta>,
    pub(super) touched_states: Vec<Arc<LsmTree>>,
    pub(super) estimated_bytes: u64,
}

#[derive(Debug)]
pub(super) struct PreparedShardDelta {
    pub(super) bucket: String,
    pub(super) shard: PreparedShardId,
    pub(super) state: Arc<LsmTree>,
    pub(super) operations: Vec<PreparedDeltaOperation>,
    pub(super) key_bounds: PreparedDeltaKeyBounds,
    pub(super) estimated_bytes: u64,
}

#[derive(Debug)]
pub(super) struct PreparedDeltaOperation {
    pub(super) batch_index: u32,
    pub(super) operation: BatchOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PreparedShardId(u32);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PreparedDeltaKeyBounds {
    pub(super) lower: Option<Vec<u8>>,
    pub(super) upper: Option<Vec<u8>>,
    pub(super) lower_unbounded: bool,
    pub(super) upper_unbounded: bool,
}

#[derive(Debug)]
pub(super) struct PublishedWrite {
    pub(super) commit_info: CommitInfo,
    pub(super) request_flush: bool,
    pub(super) visible_slot: Option<CommitSlot>,
}

#[derive(Debug)]
pub(super) struct TransactionReads {
    pub(super) read_sequence: Sequence,
    pub(super) read_set: TransactionReadSet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WalAcceptState {
    Deferred,
    Accepted(CommitSlot),
}

#[derive(Debug)]
pub(super) struct AcceptedWrite {
    pub(super) request: WriteRequest,
    pub(super) completion: Arc<WriteCompletion>,
}

#[derive(Debug)]
pub(super) struct WriteWaiter {
    pub(super) completion: Arc<WriteCompletion>,
}

#[derive(Debug)]
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(super) struct BackgroundWriteFuture {
    pub(super) state: BackgroundWriteFutureState,
}

#[derive(Debug)]
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(super) enum BackgroundWriteFutureState {
    Start { db: Db, request: WriteRequest },
    Waiting { waiter: WriteWaiter },
    Done,
}

#[derive(Debug)]
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(super) enum BackgroundWriteStart {
    Ready(Result<CommitInfo>),
    Pending(WriteWaiter),
}

#[derive(Debug)]
pub(super) struct WriteCompletion {
    pub(super) result: Mutex<Option<Result<CommitInfo>>>,
    pub(super) ready: Condvar,
    pub(super) waker: Mutex<Option<Waker>>,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(super) struct WriteThreadWake {
    pub(super) thread: thread::Thread,
}

impl WriteRequest {
    pub(super) fn batch(batch: WriteBatch, write_options: WriteOptions) -> Self {
        Self {
            operations: batch.into_operations(),
            write_options,
            transaction_reads: None,
        }
    }

    pub(super) fn transaction(
        read_sequence: Sequence,
        read_set: TransactionReadSet,
        batch: WriteBatch,
        write_options: WriteOptions,
    ) -> Self {
        Self {
            operations: batch.into_operations(),
            write_options,
            transaction_reads: Some(TransactionReads {
                read_sequence,
                read_set,
            }),
        }
    }
}

impl WriterLocalWriteState {
    pub(super) const fn new(prepared: PreparedCommit, wal_accept: WalAcceptState) -> Self {
        Self {
            prepared,
            wal_accept,
        }
    }
}

impl SequencedWrite {
    pub(super) const fn new(
        prepared: PreparedCommit,
        slot: CommitSlot,
        durability: DurabilityMode,
        accept_wal: bool,
    ) -> Self {
        Self {
            prepared,
            slot,
            durability,
            accept_wal,
        }
    }
}

impl DurableSequencedWrite {
    pub(super) const fn new(prepared: PreparedCommit, slot: CommitSlot) -> Self {
        Self { prepared, slot }
    }
}

impl PreparedCommit {
    pub(super) fn new(
        write_options: WriteOptions,
        transaction_reads: Option<TransactionReads>,
        wal_operations: Vec<BatchOperation>,
        deltas: Vec<PreparedShardDelta>,
    ) -> Self {
        let touched_states = unique_lsm_trees(deltas.iter().map(|delta| Arc::clone(&delta.state)));
        let estimated_bytes = deltas.iter().fold(0_u64, |bytes, delta| {
            bytes.saturating_add(delta.estimated_bytes)
        });
        Self {
            write_options,
            transaction_reads,
            wal_operations,
            deltas,
            touched_states,
            estimated_bytes,
        }
    }

    #[must_use]
    pub(super) fn operation_count(&self) -> usize {
        self.wal_operations.len()
    }
}

impl PreparedShardDelta {
    pub(super) fn new(bucket: String, shard: PreparedShardId, state: Arc<LsmTree>) -> Self {
        Self {
            bucket,
            shard,
            state,
            operations: Vec::new(),
            key_bounds: PreparedDeltaKeyBounds::default(),
            estimated_bytes: 0,
        }
    }

    pub(super) fn matches(&self, state: &Arc<LsmTree>, shard: PreparedShardId) -> bool {
        self.shard == shard && Arc::ptr_eq(&self.state, state)
    }

    pub(super) fn push_operation(&mut self, batch_index: u32, operation: BatchOperation) {
        self.key_bounds.include_operation(&operation);
        self.estimated_bytes = self
            .estimated_bytes
            .saturating_add(operation_estimated_bytes(&operation));
        self.operations.push(PreparedDeltaOperation {
            batch_index,
            operation,
        });
    }
}

impl PreparedShardId {
    pub(super) const CURRENT_SINGLE_SHARD: Self = Self(0);

    #[must_use]
    pub(super) const fn for_operation(_operation: &BatchOperation) -> Self {
        Self::CURRENT_SINGLE_SHARD
    }
}

impl PreparedDeltaKeyBounds {
    pub(super) fn include_operation(&mut self, operation: &BatchOperation) {
        match operation {
            BatchOperation::Put { key, .. } | BatchOperation::Delete { key, .. } => {
                self.include_point_key(key);
            }
            BatchOperation::DeleteRange { range, .. } => {
                self.include_lower(&range.start);
                self.include_upper(&range.end);
            }
        }
    }

    pub(super) fn include_point_key(&mut self, key: &[u8]) {
        if !self.lower_unbounded {
            include_min_key(&mut self.lower, key);
        }
        if !self.upper_unbounded {
            include_max_key(&mut self.upper, key);
        }
    }

    pub(super) fn include_lower(&mut self, bound: &Bound<Vec<u8>>) {
        match bound {
            Bound::Included(key) | Bound::Excluded(key) => {
                if !self.lower_unbounded {
                    include_min_key(&mut self.lower, key);
                }
            }
            Bound::Unbounded => {
                self.lower = None;
                self.lower_unbounded = true;
            }
        }
    }

    pub(super) fn include_upper(&mut self, bound: &Bound<Vec<u8>>) {
        match bound {
            Bound::Included(key) | Bound::Excluded(key) => {
                if !self.upper_unbounded {
                    include_max_key(&mut self.upper, key);
                }
            }
            Bound::Unbounded => {
                self.upper = None;
                self.upper_unbounded = true;
            }
        }
    }
}

impl PublishedWrite {
    pub(super) const fn new(
        commit_info: CommitInfo,
        request_flush: bool,
        visible_slot: Option<CommitSlot>,
    ) -> Self {
        Self {
            commit_info,
            request_flush,
            visible_slot,
        }
    }
}

impl AcceptedWrite {
    pub(super) fn accept(request: WriteRequest) -> (Self, WriteWaiter) {
        let completion = Arc::new(WriteCompletion::new());
        (
            Self {
                request,
                completion: Arc::clone(&completion),
            },
            WriteWaiter { completion },
        )
    }

    pub(super) fn execute(self, db: &Db) {
        let result = db.commit_write_request(self.request);
        self.completion.complete(result);
    }
}

impl WriteCompletion {
    pub(super) fn new() -> Self {
        Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
            waker: Mutex::new(None),
        }
    }

    pub(super) fn complete(&self, result: Result<CommitInfo>) {
        {
            let mut slot = match self.result.lock() {
                Ok(slot) => slot,
                Err(poisoned) => poisoned.into_inner(),
            };
            *slot = Some(result);
        }
        self.ready.notify_all();

        let waker = match self.waker.lock() {
            Ok(mut waker) => waker.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl WriteWaiter {
    pub(super) fn wait(self) -> Result<CommitInfo> {
        let mut result = self
            .completion
            .result
            .lock()
            .map_err(|_| lock_poisoned("write completion"))?;
        loop {
            if let Some(result) = result.take() {
                return result;
            }
            result = self
                .completion
                .ready
                .wait(result)
                .map_err(|_| lock_poisoned("write completion"))?;
        }
    }

    pub(super) fn poll_result(&self, context: &mut Context<'_>) -> Poll<Result<CommitInfo>> {
        match self.take_result() {
            Ok(Some(result)) => return Poll::Ready(result),
            Ok(None) => {}
            Err(error) => return Poll::Ready(Err(error)),
        }

        if let Err(error) = self.register_waker(context) {
            return Poll::Ready(Err(error));
        }

        match self.take_result() {
            Ok(Some(result)) => Poll::Ready(result),
            Ok(None) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    pub(super) fn register_waker(&self, context: &Context<'_>) -> Result<()> {
        let mut waker = self
            .completion
            .waker
            .lock()
            .map_err(|_| lock_poisoned("write completion waker"))?;
        let replace = match waker.as_ref() {
            Some(registered) => !registered.will_wake(context.waker()),
            None => true,
        };
        if replace {
            *waker = Some(context.waker().clone());
        }
        Ok(())
    }

    pub(super) fn take_result(&self) -> Result<Option<Result<CommitInfo>>> {
        self.completion
            .result
            .lock()
            .map(|mut result| result.take())
            .map_err(|_| lock_poisoned("write completion"))
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl BackgroundWriteFuture {
    pub(super) fn new(db: Db, request: WriteRequest) -> Self {
        Self {
            state: BackgroundWriteFutureState::Start { db, request },
        }
    }

    pub(super) fn start(
        db: &Db,
        request: WriteRequest,
        context: &mut Context<'_>,
    ) -> BackgroundWriteStart {
        let completion = Arc::new(WriteCompletion::new());
        let waiter = WriteWaiter {
            completion: Arc::clone(&completion),
        };
        if let Err(error) = waiter.register_waker(context) {
            return BackgroundWriteStart::Ready(Err(error));
        }

        let task_db = db.clone();
        let task = move || {
            let result = wait_for_engine_write_future(task_db.commit_write_request_async(request));
            drop(task_db);
            completion.complete(result);
        };

        if db.inner.runtime.capabilities().background_threads() {
            let spawn = db
                .inner
                .runtime
                .spawn_background("trine-async-write".to_owned(), task);
            return match spawn {
                Ok(_task) => BackgroundWriteStart::Pending(waiter),
                Err(error) => BackgroundWriteStart::Ready(Err(error)),
            };
        }

        task();
        match waiter.poll_result(context) {
            Poll::Ready(result) => BackgroundWriteStart::Ready(result),
            Poll::Pending => BackgroundWriteStart::Pending(waiter),
        }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl Future for BackgroundWriteFuture {
    type Output = Result<CommitInfo>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let state = std::mem::replace(&mut self.state, BackgroundWriteFutureState::Done);
        match state {
            BackgroundWriteFutureState::Start { db, request } => {
                match Self::start(&db, request, context) {
                    BackgroundWriteStart::Ready(result) => Poll::Ready(result),
                    BackgroundWriteStart::Pending(waiter) => {
                        self.state = BackgroundWriteFutureState::Waiting { waiter };
                        Poll::Pending
                    }
                }
            }
            BackgroundWriteFutureState::Waiting { waiter } => match waiter.poll_result(context) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    self.state = BackgroundWriteFutureState::Waiting { waiter };
                    Poll::Pending
                }
            },
            BackgroundWriteFutureState::Done => {
                panic!("write future polled after completion");
            }
        }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl std::task::Wake for WriteThreadWake {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn wait_for_engine_write_future<T>(future: impl Future<Output = Result<T>>) -> Result<T> {
    let waker = Waker::from(Arc::new(WriteThreadWake {
        thread: thread::current(),
    }));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => thread::park(),
        }
    }
}
