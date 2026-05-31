use std::sync::Arc;

use crate::{
    error::{Error, Result},
    lsm::LsmTree,
    options::{DurabilityMode, WriteOptions},
    transaction::TransactionReadSet,
    types::{CommitInfo, Sequence},
    wal::WalBatch,
    write_batch::{BatchOperation, WriteBatch},
};

use super::{Db, lock_poisoned, validate_batch_len};

impl Db {
    pub fn write(&self, batch: WriteBatch, options: WriteOptions) -> Result<CommitInfo> {
        self.commit_operations(batch.into_operations(), options, None)
    }

    pub(crate) fn commit_transaction(
        &self,
        read_sequence: Sequence,
        read_set: TransactionReadSet,
        batch: WriteBatch,
        write_options: WriteOptions,
    ) -> Result<CommitInfo> {
        self.commit_operations(
            batch.into_operations(),
            write_options,
            Some((read_sequence, read_set)),
        )
    }

    fn commit_operations(
        &self,
        operations: Vec<BatchOperation>,
        write_options: WriteOptions,
        transaction_reads: Option<(Sequence, TransactionReadSet)>,
    ) -> Result<CommitInfo> {
        self.ensure_open()?;

        if operations.is_empty() && transaction_reads.is_none() {
            return Ok(CommitInfo::new(self.last_committed_sequence()));
        }

        if self.inner.options.read_only && !operations.is_empty() {
            return Err(Error::ReadOnly);
        }
        self.take_background_maintenance_error()?;

        // Check every batch-wide precondition before taking the writer lock or
        // touching memtables, so a rejected batch cannot leave partial state.
        validate_batch_len(operations.len())?;
        if !operations.is_empty() {
            self.apply_write_backpressure()?;
        }

        // The writer lock serializes commit sequence assignment and memtable
        // updates. Reads only take bucket/table read locks and do not enter
        // this coordinator.
        let _writer = self
            .inner
            .writer
            .lock()
            .map_err(|_| lock_poisoned("writer coordinator"))?;

        // Read validation and writes share one commit slot. Once validation
        // succeeds, no other writer can sneak in before this batch lands.
        if let Some((read_sequence, read_set)) = transaction_reads {
            self.validate_transaction_reads(read_sequence, &read_set)?;
        }
        if operations.is_empty() {
            return Ok(CommitInfo::new(self.last_committed_sequence()));
        }

        let states = self.resolve_batch_buckets(&operations)?;

        let indexed_operations = operations
            .into_iter()
            .zip(states)
            .enumerate()
            .map(|(batch_index, (operation, state))| {
                let batch_index = u32::try_from(batch_index).map_err(|_| {
                    Error::invalid_options("write batch operation count exceeds u32::MAX")
                })?;
                Ok((batch_index, operation, state))
            })
            .collect::<Result<Vec<_>>>()?;
        let wal_operations = indexed_operations
            .iter()
            .map(|(_, operation, _)| operation.clone())
            .collect::<Vec<_>>();

        let durability =
            effective_durability(self.inner.options.durability, write_options.durability);
        let slot = self.inner.commit_tracker.reserve_slot()?;
        let sequence = slot.sequence();
        if let Err(error) = self.append_wal(sequence, &wal_operations, durability) {
            self.inner.commit_tracker.mark_skipped(slot)?;
            return Err(error);
        }

        let touched_states = unique_lsm_trees(
            indexed_operations
                .iter()
                .map(|(_, _, state)| Arc::clone(state)),
        );
        let mut delta_publication_started = false;
        for (batch_index, operation, state) in indexed_operations {
            if let Err(error) = state.apply_operation(operation, sequence, batch_index) {
                if !delta_publication_started {
                    self.inner.commit_tracker.mark_skipped(slot)?;
                }
                return Err(error);
            }
            delta_publication_started = true;
        }

        self.inner.commit_tracker.mark_visible(slot)?;
        if self.freeze_large_active_memtables_after_commit_locked(sequence, &touched_states)? {
            self.request_background_flush();
        }
        Ok(CommitInfo::new(sequence))
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

    fn append_wal(
        &self,
        sequence: Sequence,
        operations: &[BatchOperation],
        durability: DurabilityMode,
    ) -> Result<()> {
        if let Some(wal) = &self.inner.wal {
            wal.lock()
                .map_err(|_| lock_poisoned("WAL writer"))?
                .append_batch(sequence, operations, durability)?;
        }

        Ok(())
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
    use crate::{db::CommitTracker, types::Sequence};

    use super::{DurabilityMode, effective_durability};

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
}
