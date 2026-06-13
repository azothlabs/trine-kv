use std::{
    future::Future,
    ops::Bound,
    pin::Pin,
    sync::{Arc, Condvar, Mutex, atomic::Ordering},
    task::{Context, Poll, Waker},
};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use std::path::Path;

use crate::{
    error::{Error, Result},
    lsm::LsmTree,
    options::{DurabilityMode, StorageMode, WriteOptions},
    storage::{StorageCapability, StorageReadBackend},
    transaction::TransactionReadSet,
    types::{CommitInfo, Sequence},
    wal::WalBatch,
    write_batch::{BatchOperation, WriteBatch},
};

use super::{
    CommitSlot, Db, PublishBarrierGuard, lock_poisoned, usize_to_u64_saturating, validate_batch_len,
};

#[derive(Debug)]
struct WriteRequest {
    operations: Vec<BatchOperation>,
    write_options: WriteOptions,
    transaction_reads: Option<TransactionReads>,
}

#[derive(Debug)]
enum AcceptedWriteState {
    Noop(CommitInfo),
    Pending(WriterLocalWriteState),
}

#[derive(Debug)]
struct WriterLocalWriteState {
    prepared: PreparedCommit,
    wal_accept: WalAcceptState,
}

#[derive(Debug)]
enum SequencedWriteState {
    Noop(CommitInfo),
    Pending(SequencedWrite),
}

#[derive(Debug)]
struct SequencedWrite {
    prepared: PreparedCommit,
    slot: CommitSlot,
    durability: DurabilityMode,
    accept_wal: bool,
}

#[derive(Debug)]
struct DurableSequencedWrite {
    prepared: PreparedCommit,
    slot: CommitSlot,
}

#[derive(Debug)]
struct PreparedCommit {
    write_options: WriteOptions,
    transaction_reads: Option<TransactionReads>,
    wal_operations: Vec<BatchOperation>,
    deltas: Vec<PreparedShardDelta>,
    touched_states: Vec<Arc<LsmTree>>,
    estimated_bytes: u64,
}

#[derive(Debug)]
struct PreparedShardDelta {
    bucket: String,
    shard: PreparedShardId,
    state: Arc<LsmTree>,
    operations: Vec<PreparedDeltaOperation>,
    key_bounds: PreparedDeltaKeyBounds,
    estimated_bytes: u64,
}

#[derive(Debug)]
struct PreparedDeltaOperation {
    batch_index: u32,
    operation: BatchOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PreparedShardId(u32);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PreparedDeltaKeyBounds {
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    lower_unbounded: bool,
    upper_unbounded: bool,
}

#[derive(Debug)]
struct PublishedWrite {
    commit_info: CommitInfo,
    request_flush: bool,
    visible_slot: Option<CommitSlot>,
}

#[derive(Debug)]
struct TransactionReads {
    read_sequence: Sequence,
    read_set: TransactionReadSet,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalAcceptState {
    Deferred,
    Accepted(CommitSlot),
}

#[derive(Debug)]
struct AcceptedWrite {
    request: WriteRequest,
    completion: Arc<WriteCompletion>,
}

#[derive(Debug)]
struct WriteWaiter {
    completion: Arc<WriteCompletion>,
}

#[derive(Debug)]
struct WriteCompletion {
    result: Mutex<Option<Result<CommitInfo>>>,
    ready: Condvar,
    waker: Mutex<Option<Waker>>,
}

#[derive(Debug)]
struct WriteFuture {
    state: WriteFutureState,
}

#[derive(Debug)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
enum WriteFutureState {
    Start { db: Db, request: WriteRequest },
    Waiting { waiter: WriteWaiter },
    Done,
}

#[derive(Debug)]
enum WriteStart {
    Ready(Result<CommitInfo>),
    Pending(WriteWaiter),
}

impl WriteRequest {
    fn batch(batch: WriteBatch, write_options: WriteOptions) -> Self {
        Self {
            operations: batch.into_operations(),
            write_options,
            transaction_reads: None,
        }
    }

    fn transaction(
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
    const fn new(prepared: PreparedCommit, wal_accept: WalAcceptState) -> Self {
        Self {
            prepared,
            wal_accept,
        }
    }
}

impl SequencedWrite {
    const fn new(
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
    const fn new(prepared: PreparedCommit, slot: CommitSlot) -> Self {
        Self { prepared, slot }
    }
}

impl PreparedCommit {
    fn new(
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
    fn operation_count(&self) -> usize {
        self.wal_operations.len()
    }
}

impl PreparedShardDelta {
    fn new(bucket: String, shard: PreparedShardId, state: Arc<LsmTree>) -> Self {
        Self {
            bucket,
            shard,
            state,
            operations: Vec::new(),
            key_bounds: PreparedDeltaKeyBounds::default(),
            estimated_bytes: 0,
        }
    }

    fn matches(&self, state: &Arc<LsmTree>, shard: PreparedShardId) -> bool {
        self.shard == shard && Arc::ptr_eq(&self.state, state)
    }

    fn push_operation(&mut self, batch_index: u32, operation: BatchOperation) {
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
    const CURRENT_SINGLE_SHARD: Self = Self(0);

    #[must_use]
    const fn for_operation(_operation: &BatchOperation) -> Self {
        Self::CURRENT_SINGLE_SHARD
    }
}

impl PreparedDeltaKeyBounds {
    fn include_operation(&mut self, operation: &BatchOperation) {
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

    fn include_point_key(&mut self, key: &[u8]) {
        if !self.lower_unbounded {
            include_min_key(&mut self.lower, key);
        }
        if !self.upper_unbounded {
            include_max_key(&mut self.upper, key);
        }
    }

    fn include_lower(&mut self, bound: &Bound<Vec<u8>>) {
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

    fn include_upper(&mut self, bound: &Bound<Vec<u8>>) {
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
    const fn new(
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
    fn accept(request: WriteRequest) -> (Self, WriteWaiter) {
        let completion = Arc::new(WriteCompletion::new());
        (
            Self {
                request,
                completion: Arc::clone(&completion),
            },
            WriteWaiter { completion },
        )
    }

    fn execute(self, db: &Db) {
        let result = db.commit_write_request(self.request);
        self.completion.complete(result);
    }
}

impl WriteCompletion {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
            waker: Mutex::new(None),
        }
    }

    fn complete(&self, result: Result<CommitInfo>) {
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
    fn wait(self) -> Result<CommitInfo> {
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

    fn poll_result(&self, context: &mut Context<'_>) -> Poll<Result<CommitInfo>> {
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

    fn register_waker(&self, context: &Context<'_>) -> Result<()> {
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

    fn take_result(&self) -> Result<Option<Result<CommitInfo>>> {
        self.completion
            .result
            .lock()
            .map(|mut result| result.take())
            .map_err(|_| lock_poisoned("write completion"))
    }
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
impl WriteFuture {
    fn new(db: Db, request: WriteRequest) -> Self {
        Self {
            state: WriteFutureState::Start { db, request },
        }
    }

    fn start(db: &Db, request: WriteRequest, context: &mut Context<'_>) -> WriteStart {
        let (accepted_write, waiter) = AcceptedWrite::accept(request);
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        if db.inner.runtime.capabilities().blocking_adapter() {
            if let Err(error) = waiter.register_waker(context) {
                return WriteStart::Ready(Err(error));
            }
            let task_db = db.clone();
            let spawn_result = db.inner.runtime.spawn_blocking(move || {
                accepted_write.execute(&task_db);
            });
            return match spawn_result {
                Ok(()) => WriteStart::Pending(waiter),
                Err(error) => WriteStart::Ready(Err(error)),
            };
        }

        accepted_write.execute(db);
        match waiter.poll_result(context) {
            Poll::Ready(result) => WriteStart::Ready(result),
            Poll::Pending => WriteStart::Pending(waiter),
        }
    }
}

impl Future for WriteFuture {
    type Output = Result<CommitInfo>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let state = std::mem::replace(&mut self.state, WriteFutureState::Done);
        match state {
            WriteFutureState::Start { db, request } => match Self::start(&db, request, context) {
                WriteStart::Ready(result) => Poll::Ready(result),
                WriteStart::Pending(waiter) => {
                    self.state = WriteFutureState::Waiting { waiter };
                    Poll::Pending
                }
            },
            WriteFutureState::Waiting { waiter } => match waiter.poll_result(context) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    self.state = WriteFutureState::Waiting { waiter };
                    Poll::Pending
                }
            },
            WriteFutureState::Done => {
                panic!("write future polled after completion");
            }
        }
    }
}

impl Db {
    /// Commits an atomic write batch synchronously with explicit write options.
    pub fn write_sync(&self, batch: WriteBatch, options: WriteOptions) -> Result<CommitInfo> {
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent writes require async API",
            ));
        }
        self.run_accepted_write(WriteRequest::batch(batch, options))
    }

    #[must_use = "write futures do nothing unless polled"]
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    /// Commits an atomic write batch asynchronously with explicit write options.
    pub fn write(
        &self,
        batch: WriteBatch,
        options: WriteOptions,
    ) -> impl Future<Output = Result<CommitInfo>> + Send + 'static {
        self.run_accepted_write_async(WriteRequest::batch(batch, options))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    /// Commits an atomic write batch asynchronously with explicit write options.
    pub fn write(
        &self,
        batch: WriteBatch,
        options: WriteOptions,
    ) -> impl Future<Output = Result<CommitInfo>> + 'static {
        let db = self.clone();
        async move {
            db.run_owned_write_request_async(WriteRequest::batch(batch, options))
                .await
        }
    }

    pub(crate) fn commit_transaction(
        &self,
        read_sequence: Sequence,
        read_set: TransactionReadSet,
        batch: WriteBatch,
        write_options: WriteOptions,
    ) -> Result<CommitInfo> {
        if self.inner.options.storage_mode.is_browser_persistent() {
            return Err(Error::unsupported_backend(
                "browser persistent transactions require async API",
            ));
        }
        self.run_accepted_write(WriteRequest::transaction(
            read_sequence,
            read_set,
            batch,
            write_options,
        ))
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(crate) fn commit_transaction_async(
        &self,
        read_sequence: Sequence,
        read_set: TransactionReadSet,
        batch: WriteBatch,
        write_options: WriteOptions,
    ) -> impl Future<Output = Result<CommitInfo>> + Send + 'static {
        self.run_accepted_write_async(WriteRequest::transaction(
            read_sequence,
            read_set,
            batch,
            write_options,
        ))
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub(crate) fn commit_transaction_async(
        &self,
        read_sequence: Sequence,
        read_set: TransactionReadSet,
        batch: WriteBatch,
        write_options: WriteOptions,
    ) -> impl Future<Output = Result<CommitInfo>> + 'static {
        let db = self.clone();
        async move {
            db.run_owned_write_request_async(WriteRequest::transaction(
                read_sequence,
                read_set,
                batch,
                write_options,
            ))
            .await
        }
    }

    fn run_accepted_write(&self, request: WriteRequest) -> Result<CommitInfo> {
        let (accepted_write, waiter) = AcceptedWrite::accept(request);
        accepted_write.execute(self);
        waiter.wait()
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn run_accepted_write_async(
        &self,
        request: WriteRequest,
    ) -> impl Future<Output = Result<CommitInfo>> + Send + 'static {
        let db = self.clone();
        async move {
            if db.uses_native_platform_async_write_path() {
                db.commit_write_request_async(request).await
            } else {
                WriteFuture::new(db, request).await
            }
        }
    }

    fn commit_write_request(&self, request: WriteRequest) -> Result<CommitInfo> {
        let accepted_state = self.accept_write_request(request)?;
        self.publish_accepted_write_state(accepted_state)
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    async fn commit_write_request_async(&self, request: WriteRequest) -> Result<CommitInfo> {
        let accepted_state = self.accept_write_request_with_wal_preaccept(request, false)?;
        self.publish_accepted_write_state_async(accepted_state)
            .await
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn uses_native_platform_async_write_path(&self) -> bool {
        self.inner.options.storage_mode.persistent_path().is_some()
            && self.inner.substrate.wal_is_present()
            && self
                .inner
                .native_storage
                .capabilities()
                .supports(StorageCapability::PlatformAsyncIo)
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn run_owned_write_request_async(&self, request: WriteRequest) -> Result<CommitInfo> {
        if !self.inner.options.storage_mode.is_browser_persistent() {
            return self.commit_write_request(request);
        }

        let completion = Arc::new(WriteCompletion::new());
        let waiter = WriteWaiter {
            completion: Arc::clone(&completion),
        };
        let db = self.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let result = db.commit_write_request_async(request).await;
            completion.complete(result);
        });
        std::future::poll_fn(move |context| waiter.poll_result(context)).await
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn commit_write_request_async(&self, request: WriteRequest) -> Result<CommitInfo> {
        if !self.inner.options.storage_mode.is_browser_persistent() {
            return self.commit_write_request(request);
        }

        let accepted_state = self.accept_write_request(request)?;
        self.publish_accepted_write_state_async(accepted_state)
            .await
    }

    fn accept_write_request(&self, request: WriteRequest) -> Result<AcceptedWriteState> {
        self.accept_write_request_with_wal_preaccept(request, true)
    }

    fn accept_write_request_with_wal_preaccept(
        &self,
        request: WriteRequest,
        preaccept_wal: bool,
    ) -> Result<AcceptedWriteState> {
        let WriteRequest {
            operations,
            write_options,
            transaction_reads,
        } = request;
        self.ensure_open()?;

        if operations.is_empty() && transaction_reads.is_none() {
            return Ok(AcceptedWriteState::Noop(CommitInfo::new(
                self.last_committed_sequence(),
            )));
        }

        if self.inner.options.read_only && !operations.is_empty() {
            return Err(Error::ReadOnly);
        }
        self.take_background_maintenance_error()?;

        // Check every batch-wide precondition before entering the publish
        // barrier or touching memtables, so a rejected batch cannot leave
        // partial state.
        validate_batch_len(operations.len())?;
        if !operations.is_empty() {
            self.apply_write_backpressure()?;
        }

        let prepared =
            self.prepare_writer_local_commit(operations, write_options, transaction_reads)?;
        let wal_accept = if preaccept_wal {
            self.preaccept_wal_front_door_if_ready(&prepared)?
        } else {
            WalAcceptState::Deferred
        };
        Ok(AcceptedWriteState::Pending(WriterLocalWriteState::new(
            prepared, wal_accept,
        )))
    }

    fn publish_accepted_write_state(
        &self,
        accepted_state: AcceptedWriteState,
    ) -> Result<CommitInfo> {
        match accepted_state {
            AcceptedWriteState::Noop(commit_info) => Ok(commit_info),
            AcceptedWriteState::Pending(writer_state) => {
                let sequenced = {
                    let publish = self.inner.publish_barrier.enter()?;
                    self.sequence_writer_local_state_under_barrier(writer_state, &publish)?
                };
                let sequenced = match sequenced {
                    SequencedWriteState::Noop(commit_info) => return Ok(commit_info),
                    SequencedWriteState::Pending(sequenced) => sequenced,
                };
                let durable = self.accept_deferred_wal_for_sequenced_write(sequenced)?;
                let published = {
                    let _memtable_publish = self
                        .inner
                        .memtable_publish_lock
                        .lock()
                        .map_err(|_| lock_poisoned("memtable publish lock"))?;
                    let published =
                        self.publish_durable_writer_local_state_under_memtable_lock(durable)?;
                    if let Some(slot) = published.visible_slot {
                        self.inner.commit_tracker.mark_visible(slot)?;
                    }
                    published
                };
                if published.request_flush {
                    self.request_background_flush();
                }
                Ok(published.commit_info)
            }
        }
    }

    fn sequence_writer_local_state_under_barrier(
        &self,
        writer_state: WriterLocalWriteState,
        _publish: &PublishBarrierGuard<'_>,
    ) -> Result<SequencedWriteState> {
        let WriterLocalWriteState {
            prepared,
            wal_accept,
        } = writer_state;

        // Transaction read validation stays serialized with sequence
        // assignment. Once the slot is reserved, WAL append can happen outside
        // this global barrier without letting later commits take an earlier
        // sequence.
        if let Some(TransactionReads {
            read_sequence,
            read_set,
        }) = &prepared.transaction_reads
        {
            self.validate_transaction_reads(*read_sequence, read_set)?;
        }
        if prepared.operation_count() == 0 {
            return Ok(SequencedWriteState::Noop(CommitInfo::new(
                self.last_committed_sequence(),
            )));
        }
        debug_assert!(prepared.estimated_bytes > 0);

        let durability = effective_durability(
            self.inner.options.durability,
            prepared.write_options.durability,
        );
        self.validate_storage_durability(durability)?;
        let slot = match wal_accept {
            WalAcceptState::Deferred => self.inner.commit_tracker.reserve_slot()?,
            WalAcceptState::Accepted(slot) => {
                debug_assert!(prepared.transaction_reads.is_none());
                slot
            }
        };

        Ok(SequencedWriteState::Pending(SequencedWrite::new(
            prepared,
            slot,
            durability,
            matches!(wal_accept, WalAcceptState::Deferred) && self.has_wal_front_door(),
        )))
    }

    fn accept_deferred_wal_for_sequenced_write(
        &self,
        sequenced: SequencedWrite,
    ) -> Result<DurableSequencedWrite> {
        let SequencedWrite {
            prepared,
            slot,
            durability,
            accept_wal,
        } = sequenced;

        if accept_wal {
            if let Err(error) =
                self.accept_wal_front_door(slot.sequence(), &prepared.wal_operations, durability)
            {
                self.inner.commit_tracker.mark_skipped(slot)?;
                return Err(error);
            }
        }

        Ok(DurableSequencedWrite::new(prepared, slot))
    }

    async fn publish_accepted_write_state_async(
        &self,
        accepted_state: AcceptedWriteState,
    ) -> Result<CommitInfo> {
        match accepted_state {
            AcceptedWriteState::Noop(commit_info) => Ok(commit_info),
            AcceptedWriteState::Pending(writer_state) => {
                let sequenced = {
                    let publish = self.inner.publish_barrier.enter()?;
                    self.sequence_writer_local_state_under_barrier(writer_state, &publish)?
                };
                let sequenced = match sequenced {
                    SequencedWriteState::Noop(commit_info) => return Ok(commit_info),
                    SequencedWriteState::Pending(sequenced) => sequenced,
                };
                let durable = self
                    .accept_deferred_wal_for_sequenced_write_async(sequenced)
                    .await?;
                let published = {
                    let _memtable_publish = self
                        .inner
                        .memtable_publish_lock
                        .lock()
                        .map_err(|_| lock_poisoned("memtable publish lock"))?;
                    let published =
                        self.publish_durable_writer_local_state_under_memtable_lock(durable)?;
                    if let Some(slot) = published.visible_slot {
                        self.inner.commit_tracker.mark_visible(slot)?;
                    }
                    published
                };
                if published.request_flush {
                    self.request_background_flush();
                }
                Ok(published.commit_info)
            }
        }
    }

    async fn accept_deferred_wal_for_sequenced_write_async(
        &self,
        sequenced: SequencedWrite,
    ) -> Result<DurableSequencedWrite> {
        let SequencedWrite {
            prepared,
            slot,
            durability,
            accept_wal,
        } = sequenced;

        if accept_wal {
            if let Err(error) = self
                .accept_wal_front_door_async(slot.sequence(), &prepared.wal_operations, durability)
                .await
            {
                self.inner.commit_tracker.mark_skipped(slot)?;
                return Err(error);
            }
        }

        Ok(DurableSequencedWrite::new(prepared, slot))
    }

    fn publish_durable_writer_local_state_under_memtable_lock(
        &self,
        sequenced: DurableSequencedWrite,
    ) -> Result<PublishedWrite> {
        let DurableSequencedWrite { prepared, slot } = sequenced;
        let sequence = slot.sequence();

        let mut delta_publication_started = false;
        let publish_in_memory_deltas =
            matches!(self.inner.options.storage_mode, StorageMode::InMemory);
        let delta_epoch_max_bytes = usize_to_u64_saturating(self.inner.options.write_buffer_bytes);
        for delta in prepared.deltas {
            debug_assert!(!delta.bucket.is_empty());
            if publish_in_memory_deltas {
                let delta_operations = delta
                    .operations
                    .iter()
                    .map(|operation| (operation.operation.clone(), operation.batch_index));
                if let Err(error) = delta.state.publish_delta_operations_with_budget(
                    delta_operations,
                    sequence,
                    delta_epoch_max_bytes,
                ) {
                    return self.finish_failed_memtable_publication(
                        slot,
                        delta_publication_started,
                        error,
                    );
                }
                delta_publication_started = true;
                continue;
            }
            for operation in delta.operations {
                if let Err(error) = delta.state.apply_operation(
                    operation.operation,
                    sequence,
                    operation.batch_index,
                ) {
                    return self.finish_failed_memtable_publication(
                        slot,
                        delta_publication_started,
                        error,
                    );
                }
                delta_publication_started = true;
            }
        }

        let request_flush = match self
            .freeze_large_active_memtables_after_commit(sequence, &prepared.touched_states)
        {
            Ok(request_flush) => request_flush,
            Err(error) => {
                self.inner.maintenance.record_error(&Error::Corruption {
                    message: format!("post-commit memtable freeze failed: {error}"),
                });
                false
            }
        };
        Ok(PublishedWrite::new(
            CommitInfo::new(sequence),
            request_flush,
            Some(slot),
        ))
    }

    fn finish_failed_memtable_publication(
        &self,
        slot: CommitSlot,
        publication_started: bool,
        error: Error,
    ) -> Result<PublishedWrite> {
        if !publication_started {
            self.inner.commit_tracker.mark_skipped(slot)?;
            return Err(error);
        }

        let error = Error::Corruption {
            message: format!(
                "commit {} failed after partially publishing in-memory state: {error}; \
                 database handle closed; reopen persistent databases to replay WAL",
                slot.sequence().get()
            ),
        };
        self.inner.closed.store(true, Ordering::Release);
        self.inner.maintenance.record_error(&error);
        self.inner.maintenance.shutdown();
        Err(error)
    }

    fn preaccept_wal_front_door_if_ready(
        &self,
        prepared: &PreparedCommit,
    ) -> Result<WalAcceptState> {
        if !self.can_preaccept_wal_front_door(prepared) {
            return Ok(WalAcceptState::Deferred);
        }

        let durability = effective_durability(
            self.inner.options.durability,
            prepared.write_options.durability,
        );
        self.validate_storage_durability(durability)?;
        let slot = self.inner.commit_tracker.reserve_slot()?;
        if let Err(error) =
            self.accept_wal_front_door(slot.sequence(), &prepared.wal_operations, durability)
        {
            self.inner.commit_tracker.mark_skipped(slot)?;
            return Err(error);
        }

        Ok(WalAcceptState::Accepted(slot))
    }

    fn can_preaccept_wal_front_door(&self, prepared: &PreparedCommit) -> bool {
        prepared.operation_count() != 0
            && prepared.transaction_reads.is_none()
            && self.inner.substrate.wal_is_present()
            && self.inner.options.storage_mode.persistent_path().is_some()
    }

    fn has_wal_front_door(&self) -> bool {
        self.inner.substrate.wal_is_present() || {
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            {
                self.inner.browser_wal.is_some()
            }
            #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
            {
                false
            }
        }
    }

    fn validate_storage_durability(&self, durability: DurabilityMode) -> Result<()> {
        if (self.inner.options.storage_mode.is_wasi_persistent()
            || self.inner.options.storage_mode.is_browser_persistent())
            && matches!(
                durability,
                DurabilityMode::SyncData | DurabilityMode::SyncAll
            )
        {
            return Err(Error::unsupported_durability(durability));
        }
        Ok(())
    }

    fn prepare_writer_local_commit(
        &self,
        operations: Vec<BatchOperation>,
        write_options: WriteOptions,
        transaction_reads: Option<TransactionReads>,
    ) -> Result<PreparedCommit> {
        let states = self.resolve_batch_buckets(&operations)?;
        let wal_operations = operations.clone();
        let mut deltas = Vec::new();

        for (batch_index, (operation, state)) in operations.into_iter().zip(states).enumerate() {
            let batch_index = u32::try_from(batch_index).map_err(|_| {
                Error::invalid_options("write batch operation count exceeds u32::MAX")
            })?;
            let shard = PreparedShardId::for_operation(&operation);
            let delta_index = deltas
                .iter()
                .position(|delta: &PreparedShardDelta| delta.matches(&state, shard));
            if let Some(index) = delta_index {
                deltas[index].push_operation(batch_index, operation);
            } else {
                let bucket = operation.bucket().to_owned();
                let mut delta = PreparedShardDelta::new(bucket, shard, state);
                delta.push_operation(batch_index, operation);
                deltas.push(delta);
            }
        }

        Ok(PreparedCommit::new(
            write_options,
            transaction_reads,
            wal_operations,
            deltas,
        ))
    }

    fn validate_transaction_reads(
        &self,
        read_sequence: Sequence,
        read_set: &TransactionReadSet,
    ) -> Result<()> {
        for read in &read_set.point_reads {
            let state = self.bucket_state(&read.bucket)?;
            if state.point_key_modified_after(&read.key, read_sequence)? {
                return Err(Error::Conflict {
                    message: format!("point read conflict in bucket {}", read.bucket),
                });
            }
        }

        for read in &read_set.range_reads {
            let state = self.bucket_state(&read.bucket)?;
            if state.key_range_modified_after(&read.range, read_sequence)? {
                return Err(Error::Conflict {
                    message: format!("range read conflict in bucket {}", read.bucket),
                });
            }
        }

        Ok(())
    }

    fn accept_wal_front_door(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        self.inner
            .substrate
            .accept_commit(sequence, operations, durability)
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    async fn accept_wal_front_door_async(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        if let Some(wal) = &self.inner.browser_wal {
            let storage = self
                .inner
                .browser_storage
                .as_ref()
                .ok_or_else(|| Error::Corruption {
                    message: "browser persistent database is missing storage backend".to_owned(),
                })?;
            let accepted = wal
                .accept_commit(storage, Path::new(""), sequence, operations, durability)
                .await?;
            debug_assert_eq!(accepted.sequence(), sequence);
            return Ok(());
        }

        self.accept_wal_front_door(sequence, operations, durability)
    }

    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    async fn accept_wal_front_door_async(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        self.inner
            .substrate
            .accept_commit_async(sequence, operations, durability)
            .await
    }

    pub(super) fn replay_wal_batches(
        &self,
        batches: Vec<WalBatch>,
        replay_floor: Sequence,
    ) -> Result<()> {
        let mut last_seen = Sequence::ZERO;
        let mut last_committed = replay_floor;

        for batch in batches {
            if batch.sequence <= last_seen {
                return Err(Error::Corruption {
                    message: "WAL sequence did not increase".to_owned(),
                });
            }
            last_seen = batch.sequence;
            validate_batch_len(batch.operations.len())?;

            if batch.sequence <= replay_floor {
                continue;
            }

            for (batch_index, operation) in batch.operations.into_iter().enumerate() {
                let batch_index = u32::try_from(batch_index)
                    .map_err(|_| Error::invalid_options("WAL operation count exceeds u32::MAX"))?;
                let state = self.bucket_state(operation.bucket()).map_err(|error| {
                    if let Error::BucketMissing { name } = error {
                        Error::Corruption {
                            message: format!("WAL references bucket missing from manifest: {name}"),
                        }
                    } else {
                        error
                    }
                })?;
                state.apply_operation(operation, batch.sequence, batch_index)?;
            }

            last_committed = batch.sequence;
        }

        self.inner
            .commit_tracker
            .reset_visible_boundary(last_committed)?;
        Ok(())
    }
}

fn include_min_key(slot: &mut Option<Vec<u8>>, key: &[u8]) {
    if slot
        .as_ref()
        .is_none_or(|existing| key < existing.as_slice())
    {
        *slot = Some(key.to_vec());
    }
}

fn include_max_key(slot: &mut Option<Vec<u8>>, key: &[u8]) {
    if slot
        .as_ref()
        .is_none_or(|existing| key > existing.as_slice())
    {
        *slot = Some(key.to_vec());
    }
}

fn operation_estimated_bytes(operation: &BatchOperation) -> u64 {
    const PREPARED_OPERATION_OVERHEAD_BYTES: u64 = 32;

    let payload_bytes = match operation {
        BatchOperation::Put { bucket, key, value } => usize_to_u64_saturating(bucket.len())
            .saturating_add(usize_to_u64_saturating(key.len()))
            .saturating_add(usize_to_u64_saturating(value.len())),
        BatchOperation::Delete { bucket, key } => {
            usize_to_u64_saturating(bucket.len()).saturating_add(usize_to_u64_saturating(key.len()))
        }
        BatchOperation::DeleteRange { bucket, range } => usize_to_u64_saturating(bucket.len())
            .saturating_add(bound_estimated_bytes(&range.start))
            .saturating_add(bound_estimated_bytes(&range.end)),
    };

    PREPARED_OPERATION_OVERHEAD_BYTES.saturating_add(payload_bytes)
}

fn bound_estimated_bytes(bound: &Bound<Vec<u8>>) -> u64 {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => usize_to_u64_saturating(key.len()),
        Bound::Unbounded => 0,
    }
}

fn unique_lsm_trees(states: impl IntoIterator<Item = Arc<LsmTree>>) -> Vec<Arc<LsmTree>> {
    let mut unique = Vec::<Arc<LsmTree>>::new();
    for state in states {
        if unique.iter().any(|existing| Arc::ptr_eq(existing, &state)) {
            continue;
        }
        unique.push(state);
    }
    unique
}

fn effective_durability(default: DurabilityMode, requested: DurabilityMode) -> DurabilityMode {
    // The database option is a safety floor for all writes. Per-write options
    // can ask for a stronger WAL sync, but cannot quietly weaken the database
    // level selected at open time.
    if durability_rank(requested) >= durability_rank(default) {
        requested
    } else {
        default
    }
}

const fn durability_rank(mode: DurabilityMode) -> u8 {
    match mode {
        DurabilityMode::Buffered => 0,
        DurabilityMode::Flush => 1,
        DurabilityMode::SyncData => 2,
        DurabilityMode::SyncAll => 3,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        panic::{AssertUnwindSafe, catch_unwind},
        path::PathBuf,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::{
        Db, WriteBatch,
        db::CommitTracker,
        error::Error,
        lsm::LsmTree,
        options::{DbOptions, WriteOptions},
        transaction::{ReadKey, TransactionReadSet},
        types::Sequence,
        wal,
        write_batch::BatchOperation,
    };

    use super::{
        AcceptedWrite, AcceptedWriteState, DurabilityMode, PreparedShardId, WalAcceptState,
        WriteRequest, effective_durability,
    };

    fn publish_writer_state_for_test(
        db: &Db,
        writer_state: super::WriterLocalWriteState,
        publish: &super::PublishBarrierGuard<'_>,
    ) -> super::PublishedWrite {
        let sequenced = db
            .sequence_writer_local_state_under_barrier(writer_state, publish)
            .expect("sequence writer-local state");
        let sequenced = match sequenced {
            super::SequencedWriteState::Noop(_) => {
                panic!("test write should need a commit sequence")
            }
            super::SequencedWriteState::Pending(sequenced) => sequenced,
        };
        let durable = db
            .accept_deferred_wal_for_sequenced_write(sequenced)
            .expect("accept deferred WAL if needed");
        let _memtable_publish = db
            .inner
            .memtable_publish_lock
            .lock()
            .expect("memtable publish lock");
        db.publish_durable_writer_local_state_under_memtable_lock(durable)
            .expect("publish durable writer-local state")
    }

    #[test]
    fn database_durability_is_a_write_floor() {
        assert_eq!(
            effective_durability(DurabilityMode::Buffered, DurabilityMode::SyncData),
            DurabilityMode::SyncData
        );
        assert_eq!(
            effective_durability(DurabilityMode::SyncAll, DurabilityMode::Buffered),
            DurabilityMode::SyncAll
        );
    }

    #[test]
    fn commit_tracker_waits_for_prior_terminal_slot() {
        let tracker = CommitTracker::new(Sequence::ZERO);

        let first = tracker.reserve_slot().expect("reserve first slot");
        let second = tracker.reserve_slot().expect("reserve second slot");
        assert_eq!(first.sequence(), Sequence::new(1));
        assert_eq!(second.sequence(), Sequence::new(2));

        tracker.mark_visible(second).expect("mark second visible");
        assert_eq!(tracker.visible_sequence(), Sequence::ZERO);

        tracker.mark_skipped(first).expect("mark first skipped");
        assert_eq!(tracker.visible_sequence(), Sequence::new(2));

        let third = tracker.reserve_slot().expect("reserve third slot");
        assert_eq!(third.sequence(), Sequence::new(3));
    }

    #[test]
    fn commit_tracker_rejects_second_terminal_transition() {
        let tracker = CommitTracker::new(Sequence::ZERO);
        let slot = tracker.reserve_slot().expect("reserve slot");

        tracker.mark_visible(slot).expect("mark slot visible");

        assert!(tracker.mark_skipped(slot).is_err());
        assert_eq!(tracker.visible_sequence(), Sequence::new(1));
    }

    #[test]
    fn accepted_write_completion_delivers_success_result() {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), b"v".to_vec());
        let request = WriteRequest::batch(batch, WriteOptions::default());
        let (accepted_write, waiter) = AcceptedWrite::accept(request);

        accepted_write.execute(&db);
        let commit = waiter.wait().expect("waiter receives commit result");

        assert_eq!(commit.sequence(), db.last_committed_sequence());
        assert_eq!(
            db.get_sync(b"k").expect("read committed key"),
            Some(b"v".to_vec())
        );
    }

    #[test]
    fn accepted_write_completion_delivers_error_result() {
        let mut options = DbOptions::memory();
        options.read_only = true;
        let db = Db::open_sync(options).expect("read-only memory db opens");
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), b"v".to_vec());
        let request = WriteRequest::batch(batch, WriteOptions::default());
        let (accepted_write, waiter) = AcceptedWrite::accept(request);

        accepted_write.execute(&db);
        let error = waiter.wait().expect_err("waiter receives commit error");

        assert!(matches!(error, Error::ReadOnly));
        assert_eq!(db.get_sync(b"k").expect("read missing key"), None);
    }

    #[test]
    fn accepted_write_preflight_creates_writer_local_state_without_publication() {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        db.bucket_sync("events").expect("named bucket opens");
        let mut batch = WriteBatch::new();
        batch.put(b"default".to_vec(), b"v1".to_vec());
        batch
            .put_bucket("events", b"event".to_vec(), b"v2".to_vec())
            .expect("stage named bucket write");
        let request = WriteRequest::batch(batch, WriteOptions::default());

        let accepted_state = db
            .accept_write_request(request)
            .expect("write request is accepted");
        let AcceptedWriteState::Pending(writer_state) = accepted_state else {
            panic!("non-empty write must produce writer-local state");
        };

        let prepared = writer_state.prepared;
        assert_eq!(prepared.operation_count(), 2);
        assert!(prepared.transaction_reads.is_none());
        assert_eq!(
            prepared
                .wal_operations
                .iter()
                .map(BatchOperation::bucket)
                .collect::<Vec<_>>(),
            ["default", "events"]
        );
        assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
        assert_eq!(
            db.get_sync(b"default").expect("preflight is not visible"),
            None
        );

        assert_eq!(prepared.deltas.len(), 2);
        assert_eq!(prepared.touched_states.len(), 2);
        assert_eq!(prepared.deltas[0].bucket, "default");
        assert_eq!(
            prepared.deltas[0].shard,
            PreparedShardId::CURRENT_SINGLE_SHARD
        );
        assert_eq!(prepared.deltas[0].operations[0].batch_index, 0);
        assert_eq!(
            prepared.deltas[0].operations[0].operation.bucket(),
            "default"
        );
        assert_eq!(
            prepared.deltas[0].key_bounds.lower.as_deref(),
            Some(b"default".as_slice())
        );
        assert_eq!(
            prepared.deltas[0].key_bounds.upper.as_deref(),
            Some(b"default".as_slice())
        );
        assert_eq!(prepared.deltas[1].bucket, "events");
        assert_eq!(prepared.deltas[1].operations[0].batch_index, 1);
        assert_eq!(
            prepared.deltas[1].operations[0].operation.bucket(),
            "events"
        );
        assert!(prepared.estimated_bytes > 0);
        assert!(
            prepared
                .deltas
                .iter()
                .all(|delta| delta.estimated_bytes > 0)
        );
        assert!(!Arc::ptr_eq(
            &prepared.deltas[0].state,
            &prepared.deltas[1].state
        ));
    }

    #[test]
    fn partial_memtable_publication_failure_closes_database_handle() {
        let path = temp_db_path("partial-publish-failure");
        let db = Db::open_sync(DbOptions::new(&path)).expect("persistent db opens");
        db.bucket_sync("events").expect("named bucket opens");
        let events_state = db.bucket_state("events").expect("events bucket state");
        poison_active_memtable_entries(&events_state);

        let mut batch = WriteBatch::new();
        batch.put(b"default".to_vec(), b"v1".to_vec());
        batch
            .put_bucket("events", b"event".to_vec(), b"v2".to_vec())
            .expect("stage named bucket write");

        let error = db
            .write_sync(batch, WriteOptions::default())
            .expect_err("second bucket publish should fail");
        match error {
            Error::Corruption { message } => {
                assert!(message.contains("partially publishing in-memory state"));
                assert!(message.contains("database handle closed"));
            }
            other => panic!("expected corruption error, got {other:?}"),
        }

        assert!(matches!(db.get_sync(b"default"), Err(Error::Closed)));

        drop(db);
        cleanup_dir(&path);
    }

    #[test]
    fn writer_local_preparation_groups_same_bucket_delta_with_bounds() {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        let mut batch = WriteBatch::new();
        batch.put(b"b".to_vec(), b"v".to_vec());
        batch.delete(b"a".to_vec());
        batch.delete_range(crate::types::KeyRange::half_open(
            b"c".to_vec(),
            b"e".to_vec(),
        ));
        let request = WriteRequest::batch(batch, WriteOptions::default());

        let accepted_state = db
            .accept_write_request(request)
            .expect("write request is accepted");
        let AcceptedWriteState::Pending(writer_state) = accepted_state else {
            panic!("non-empty write must produce writer-local state");
        };
        let prepared = writer_state.prepared;

        assert_eq!(prepared.operation_count(), 3);
        assert_eq!(prepared.deltas.len(), 1);
        assert_eq!(prepared.touched_states.len(), 1);
        assert_eq!(prepared.deltas[0].bucket, "default");
        assert_eq!(
            prepared.deltas[0].shard,
            PreparedShardId::CURRENT_SINGLE_SHARD
        );
        assert_eq!(
            prepared.deltas[0]
                .operations
                .iter()
                .map(|operation| operation.batch_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            prepared.deltas[0].key_bounds.lower.as_deref(),
            Some(b"a".as_slice())
        );
        assert_eq!(
            prepared.deltas[0].key_bounds.upper.as_deref(),
            Some(b"e".as_slice())
        );
        assert!(!prepared.deltas[0].key_bounds.lower_unbounded);
        assert!(!prepared.deltas[0].key_bounds.upper_unbounded);
        assert!(prepared.deltas[0].estimated_bytes > 0);
    }

    #[test]
    fn persistent_blind_write_accepts_wal_before_publish_barrier() {
        let path = temp_db_path("preaccept-wal");
        let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
        options.background_worker_count = 0;
        let db = Db::open_sync(options).expect("persistent db opens");
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), b"v".to_vec());
        let request = WriteRequest::batch(batch, WriteOptions::flush());

        let blocked_publish = db
            .inner
            .publish_barrier
            .enter()
            .expect("enter publish barrier");
        let accepted_state = db
            .accept_write_request(request)
            .expect("write request is accepted");
        let AcceptedWriteState::Pending(writer_state) = accepted_state else {
            panic!("non-empty write must produce writer-local state");
        };

        assert!(matches!(
            writer_state.wal_accept,
            WalAcceptState::Accepted(_)
        ));
        assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
        assert_eq!(
            db.get_sync(b"k").expect("preaccepted write is not visible"),
            None
        );
        let wal_batches = wal::read_all_batches(&path).expect("WAL reads");
        assert_eq!(wal_batches.len(), 1);
        assert_eq!(wal_batches[0].sequence, Sequence::new(1));

        drop(blocked_publish);
        let publish = db
            .inner
            .publish_barrier
            .enter()
            .expect("enter publish barrier");
        let published = publish_writer_state_for_test(&db, writer_state, &publish);
        assert_eq!(published.commit_info.sequence(), Sequence::new(1));
        let visible_slot = published.visible_slot.expect("preaccepted commit has slot");
        db.inner
            .commit_tracker
            .mark_visible(visible_slot)
            .expect("mark preaccepted commit visible");
        assert_eq!(db.last_committed_sequence(), Sequence::new(1));
        assert_eq!(
            db.get_sync(b"k").expect("published write reads"),
            Some(b"v".to_vec())
        );

        drop(publish);
        drop(db);
        cleanup_dir(&path);
    }

    #[test]
    fn persistent_transaction_accepts_wal_after_sequence_barrier_before_memory_publish() {
        let path = temp_db_path("transaction-wal-after-sequence");
        let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
        options.background_worker_count = 0;
        let db = Db::open_sync(options).expect("persistent db opens");
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), b"txn".to_vec());
        let request = WriteRequest::transaction(
            Sequence::ZERO,
            TransactionReadSet {
                point_reads: vec![ReadKey {
                    bucket: "default".to_owned(),
                    key: b"k".to_vec(),
                }],
                range_reads: Vec::new(),
            },
            batch,
            WriteOptions::flush(),
        );
        let accepted_state = db
            .accept_write_request(request)
            .expect("transaction write request is accepted");
        let AcceptedWriteState::Pending(writer_state) = accepted_state else {
            panic!("transaction write must produce writer-local state");
        };
        assert!(matches!(writer_state.wal_accept, WalAcceptState::Deferred));
        assert!(
            wal::read_all_batches(&path).expect("WAL reads").is_empty(),
            "transaction WAL should wait for serialized read validation"
        );

        let publish = db
            .inner
            .publish_barrier
            .enter()
            .expect("enter publish barrier");
        let sequenced = db
            .sequence_writer_local_state_under_barrier(writer_state, &publish)
            .expect("transaction read set validates and slot reserves");
        let super::SequencedWriteState::Pending(sequenced) = sequenced else {
            panic!("transaction write should reserve a slot");
        };
        assert_eq!(db.stats().commit_open_slots, 1);
        assert!(
            wal::read_all_batches(&path).expect("WAL reads").is_empty(),
            "sequence reservation should not append WAL while barrier is held"
        );
        drop(publish);

        let durable = db
            .accept_deferred_wal_for_sequenced_write(sequenced)
            .expect("transaction WAL accepts outside publish barrier");
        let wal_batches = wal::read_all_batches(&path).expect("WAL reads");
        assert_eq!(wal_batches.len(), 1);
        assert_eq!(wal_batches[0].sequence, Sequence::new(1));
        assert_eq!(
            db.get_sync(b"k")
                .expect("WAL-accepted transaction is not visible"),
            None
        );

        let memtable_publish = db
            .inner
            .memtable_publish_lock
            .lock()
            .expect("memtable publish lock");
        let published = db
            .publish_durable_writer_local_state_under_memtable_lock(durable)
            .expect("publish transaction state");
        let visible_slot = published
            .visible_slot
            .expect("transaction has visible slot");
        db.inner
            .commit_tracker
            .mark_visible(visible_slot)
            .expect("mark transaction visible");
        assert_eq!(
            db.get_sync(b"k").expect("transaction write is visible"),
            Some(b"txn".to_vec())
        );

        drop(memtable_publish);
        drop(db);
        cleanup_dir(&path);
    }

    #[test]
    fn writer_local_state_publishes_under_memtable_lock_after_sequence() {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), b"v".to_vec());
        let request = WriteRequest::batch(batch, WriteOptions::default());
        let accepted_state = db
            .accept_write_request(request)
            .expect("write request is accepted");
        let AcceptedWriteState::Pending(writer_state) = accepted_state else {
            panic!("non-empty write must produce writer-local state");
        };

        let publish = db
            .inner
            .publish_barrier
            .enter()
            .expect("enter publish barrier");
        let published = publish_writer_state_for_test(&db, writer_state, &publish);

        assert!(!published.request_flush);
        assert_eq!(published.commit_info.sequence(), Sequence::new(1));
        assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
        assert_eq!(
            db.get_sync(b"k").expect("published slot is not visible"),
            None
        );
        let visible_slot = published.visible_slot.expect("published write has slot");
        db.inner
            .commit_tracker
            .mark_visible(visible_slot)
            .expect("mark published write visible");
        assert_eq!(db.last_committed_sequence(), Sequence::new(1));
        assert_eq!(
            db.get_sync(b"k").expect("read committed key"),
            Some(b"v".to_vec())
        );
        let state = db.bucket_state("default").expect("default bucket state");
        assert_eq!(
            state
                .active_memtable_bytes()
                .expect("active memtable bytes"),
            0
        );
        let (delta_count, point_delta_count, range_tombstone_count) =
            state.delta_debug_counts().expect("delta counts");
        assert_eq!(delta_count, 1);
        assert_eq!(point_delta_count, 1);
        assert_eq!(range_tombstone_count, 0);
        let db_stats = db.stats();
        assert!(db_stats.memtable_bytes > 0);
        assert_eq!(db_stats.immutable_memtables, 0);
    }

    #[test]
    fn visible_sequence_waits_for_earlier_published_slot_completion() {
        let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
        let mut first_batch = WriteBatch::new();
        first_batch.put(b"k".to_vec(), b"v1".to_vec());
        let first_request = WriteRequest::batch(first_batch, WriteOptions::default());
        let AcceptedWriteState::Pending(first_state) = db
            .accept_write_request(first_request)
            .expect("first write request is accepted")
        else {
            panic!("first write must produce writer-local state");
        };

        let mut second_batch = WriteBatch::new();
        second_batch.put(b"k".to_vec(), b"v2".to_vec());
        let second_request = WriteRequest::batch(second_batch, WriteOptions::default());
        let AcceptedWriteState::Pending(second_state) = db
            .accept_write_request(second_request)
            .expect("second write request is accepted")
        else {
            panic!("second write must produce writer-local state");
        };

        let publish = db
            .inner
            .publish_barrier
            .enter()
            .expect("enter publish barrier");
        let first_published = publish_writer_state_for_test(&db, first_state, &publish);
        let second_published = publish_writer_state_for_test(&db, second_state, &publish);
        assert_eq!(first_published.commit_info.sequence(), Sequence::new(1));
        assert_eq!(second_published.commit_info.sequence(), Sequence::new(2));

        assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
        assert_eq!(db.get_sync(b"k").expect("published writes are gated"), None);

        db.inner
            .commit_tracker
            .mark_visible(second_published.visible_slot.expect("second slot"))
            .expect("mark second slot visible");
        assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
        assert_eq!(
            db.get_sync(b"k").expect("visible sequence waits for first"),
            None
        );

        db.inner
            .commit_tracker
            .mark_visible(first_published.visible_slot.expect("first slot"))
            .expect("mark first slot visible");
        assert_eq!(db.last_committed_sequence(), Sequence::new(2));
        assert_eq!(
            db.get_sync(b"k").expect("latest visible key"),
            Some(b"v2".to_vec())
        );
    }

    #[test]
    fn in_memory_write_budget_merges_deltas_without_active_mirror() {
        let mut options = DbOptions::memory();
        options.write_buffer_bytes = 1;
        let db = Db::open_sync(options).expect("memory db opens");

        db.put_sync(b"k", b"v1").expect("first write");
        let snapshot = db.snapshot();
        db.put_sync(b"k", b"v2").expect("second write");

        assert_eq!(
            db.get_sync(b"k").expect("current read"),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            snapshot
                .get_sync(&db.default_bucket_sync().expect("default bucket"), b"k")
                .expect("snapshot read"),
            Some(b"v1".to_vec())
        );

        let state = db.bucket_state("default").expect("default bucket state");
        assert_eq!(
            state
                .active_memtable_bytes()
                .expect("active memtable bytes"),
            0
        );
        let delta_stats = state.delta_debug_stats().expect("delta stats");
        assert_eq!(delta_stats.merged_epoch_count, 1);
        assert_eq!(delta_stats.max_shard_chain_len, 1);
        assert!(delta_stats.open_epoch_bytes > 0);
    }

    fn poison_active_memtable_entries(state: &LsmTree) {
        let active_memtable = state
            .active_memtable
            .read()
            .expect("active memtable pointer lock is not poisoned")
            .clone();
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _entries = active_memtable
                .write_entries()
                .expect("memtable entries lock starts healthy");
            panic!("poison memtable entries for commit failure test");
        }));
        assert!(result.is_err());
    }

    fn temp_db_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "trine-kv-commit-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn cleanup_dir(path: &std::path::Path) {
        match fs::remove_dir_all(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("failed to remove {}: {error}", path.display()),
        }
    }
}
