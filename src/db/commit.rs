use std::{
    future::Future,
    ops::Bound,
    sync::{Arc, Mutex, atomic::Ordering},
};
#[cfg(not(target_os = "wasi"))]
use std::{
    sync::Condvar,
    task::{Context, Poll, Waker},
};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use std::path::Path;
#[cfg(all(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    not(target_os = "wasi")
))]
use std::{pin::Pin, thread};

use crate::{
    error::{Error, Result},
    lsm::LsmTree,
    options::{DbOptions, DurabilityMode, StorageMode, WriteOptions},
    transaction::TransactionReadSet,
    types::{CommitInfo, Sequence},
    wal::WalBatch,
    write_batch::{BatchOperation, WriteBatch},
};

use super::open_helpers::{usize_to_u64_saturating, validate_batch_len};
use super::{CommitSlot, Db, PublishSequenceGuard, lock_poisoned};

mod helpers;
mod state;

pub(super) use helpers::replay_wal_batches_into_buckets;
use helpers::{
    effective_durability, include_max_key, include_min_key, operation_estimated_bytes,
    unique_lsm_trees, validate_operation_resource_bounds,
};
#[cfg(all(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    not(target_os = "wasi")
))]
use state::BackgroundWriteFuture;
use state::{
    AcceptedWrite, AcceptedWriteState, DurableSequencedWrite, PreparedCommit, PreparedShardDelta,
    PreparedShardId, PublishedWrite, SequencedWrite, SequencedWriteState, TransactionReads,
    WalAcceptState, WriteRequest, WriterLocalWriteState,
};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use state::{WriteCompletion, WriteWaiter};

#[cfg(test)]
mod tests;

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
        #[cfg(target_os = "wasi")]
        {
            let db = self.clone();
            return async move { db.run_accepted_write(request) };
        }

        #[cfg(not(target_os = "wasi"))]
        {
            let db = self.clone();
            BackgroundWriteFuture::new(db, request)
        }
    }

    fn commit_write_request(&self, request: WriteRequest) -> Result<CommitInfo> {
        let _publish_activity = self.inner.publish_barrier.begin_activity()?;
        let accepted_state = self.accept_write_request(request)?;
        self.publish_accepted_write_state(accepted_state)
    }

    #[cfg(all(
        not(all(target_arch = "wasm32", target_os = "unknown")),
        not(target_os = "wasi")
    ))]
    async fn commit_write_request_async(&self, request: WriteRequest) -> Result<CommitInfo> {
        let _publish_activity = self.inner.publish_barrier.begin_activity()?;
        let accepted_state = self.accept_write_request_with_wal_preaccept(request, false)?;
        self.publish_accepted_write_state_async(accepted_state)
            .await
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

        let _publish_activity = self.inner.publish_barrier.begin_activity()?;
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
        validate_operation_resource_bounds(
            &operations,
            self.inner.options.max_key_bytes,
            self.inner.options.max_value_bytes,
        )?;
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
                    let publish = self.inner.publish_barrier.enter_sequence()?;
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
        _publish: &PublishSequenceGuard<'_>,
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

    #[cfg(not(target_os = "wasi"))]
    async fn publish_accepted_write_state_async(
        &self,
        accepted_state: AcceptedWriteState,
    ) -> Result<CommitInfo> {
        match accepted_state {
            AcceptedWriteState::Noop(commit_info) => Ok(commit_info),
            AcceptedWriteState::Pending(writer_state) => {
                let sequenced = {
                    let publish = self.inner.publish_barrier.enter_sequence()?;
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

    #[cfg(not(target_os = "wasi"))]
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

    #[cfg(all(
        not(all(target_arch = "wasm32", target_os = "unknown")),
        not(target_os = "wasi")
    ))]
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
        let buckets = self
            .inner
            .buckets
            .read()
            .map_err(|_| lock_poisoned("bucket registry"))?;
        let last_committed = replay_wal_batches_into_buckets(&buckets, batches, replay_floor)?;
        drop(buckets);
        self.inner
            .commit_tracker
            .reset_visible_boundary(last_committed)?;
        Ok(())
    }
}
