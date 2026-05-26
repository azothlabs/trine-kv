use std::{
    ops::Bound,
    sync::{Arc, atomic::Ordering},
};

use crate::{
    blob::ValueRef,
    error::Result,
    internal_key::{InternalKey, ValueKind},
    memtable::Memtable,
    range_tombstone,
    types::{KeyRange, Sequence},
    write_batch::BatchOperation,
};

use super::tree::{ImmutableMemtable, LsmTree, RangeTombstone, lock_poisoned};

impl LsmTree {
    pub(crate) fn apply_operation(
        &self,
        operation: BatchOperation,
        sequence: Sequence,
        batch_index: u32,
    ) -> Result<()> {
        let active_memtable = self
            .active_memtable
            .read()
            .map_err(|_| lock_poisoned("active memtable"))?
            .clone();

        match operation {
            BatchOperation::Insert { key, value, .. } => {
                active_memtable
                    .insert(
                        InternalKey::new(key, sequence, ValueKind::Put, batch_index),
                        Some(ValueRef::Inline(value)),
                    )
                    .map_err(|()| lock_poisoned("memtable entries"))?;
            }
            BatchOperation::Remove { key, .. } => {
                active_memtable
                    .insert(
                        InternalKey::new(key, sequence, ValueKind::PointDelete, batch_index),
                        None,
                    )
                    .map_err(|()| lock_poisoned("memtable entries"))?;
            }
            BatchOperation::RemoveRange { range, .. } => {
                // Range tombstones share the same commit sequence and batch
                // order as point records. Keep the tombstone byte counter in
                // the same step so write-buffer checks stay O(1).
                let tombstone_bytes = range_tombstone_bytes(&range);
                let mut range_tombstones = self
                    .range_tombstones
                    .write()
                    .map_err(|_| lock_poisoned("range tombstones"))?;
                range_tombstone::insert_sorted(
                    &mut range_tombstones,
                    RangeTombstone {
                        range,
                        sequence,
                        batch_index,
                    },
                );
                self.range_tombstone_bytes
                    .fetch_add(tombstone_bytes, Ordering::AcqRel);
            }
        }

        Ok(())
    }

    pub(crate) fn active_memtable_bytes(&self) -> Result<u64> {
        let active_memtable = self
            .active_memtable
            .read()
            .map_err(|_| lock_poisoned("active memtable"))?
            .clone();
        Ok(active_memtable
            .estimated_bytes()
            .saturating_add(self.range_tombstone_bytes.load(Ordering::Acquire)))
    }

    pub(crate) fn memtable_bytes(&self) -> Result<u64> {
        let mut bytes = self.active_memtable_bytes()?;
        let immutable_memtables = self
            .immutable_memtables
            .read()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?
            .clone();

        for immutable in immutable_memtables {
            bytes = bytes.saturating_add(immutable.estimated_bytes);
        }

        Ok(bytes)
    }

    pub(crate) fn immutable_memtable_count(&self) -> Result<usize> {
        self.immutable_memtables
            .read()
            .map_err(|_| lock_poisoned("immutable memtable queue"))
            .map(|immutable_memtables| immutable_memtables.len())
    }

    pub(crate) fn has_immutable_memtables(&self) -> Result<bool> {
        self.immutable_memtable_count()
            .map(|immutable_memtables| immutable_memtables != 0)
    }

    pub(crate) fn freeze_active_memtable(&self, freeze_sequence: Sequence) -> Result<bool> {
        // Lock order is active pointer -> active tombstones -> immutable queue.
        // The new active memtable is installed only after the frozen point
        // records and tombstones are queued together.
        let mut active_memtable = self
            .active_memtable
            .write()
            .map_err(|_| lock_poisoned("active memtable"))?;
        let active = Arc::clone(&active_memtable);
        let entries_empty = active
            .is_empty()
            .map_err(|()| lock_poisoned("memtable entries"))?;
        let mut range_tombstones = self
            .range_tombstones
            .write()
            .map_err(|_| lock_poisoned("range tombstones"))?;

        if entries_empty && range_tombstones.is_empty() {
            return Ok(false);
        }

        let immutable = ImmutableMemtable {
            estimated_bytes: active
                .estimated_bytes()
                .saturating_add(self.range_tombstone_bytes.load(Ordering::Acquire)),
            memtable: active,
            range_tombstones: Arc::new(range_tombstones.clone()),
            freeze_sequence,
        };
        self.immutable_memtables
            .write()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?
            .push(immutable);

        *active_memtable = Arc::new(Memtable::default());
        range_tombstones.clear();
        self.range_tombstone_bytes.store(0, Ordering::Release);

        Ok(true)
    }
}

fn range_tombstone_bytes(range: &KeyRange) -> u64 {
    key_range_bytes(range)
        .saturating_add(usize_to_u64_saturating(std::mem::size_of::<Sequence>()))
        .saturating_add(usize_to_u64_saturating(std::mem::size_of::<u32>()))
}

fn key_range_bytes(range: &KeyRange) -> u64 {
    bound_bytes(&range.start).saturating_add(bound_bytes(&range.end))
}

fn bound_bytes(bound: &Bound<Vec<u8>>) -> u64 {
    match bound {
        Bound::Included(bytes) | Bound::Excluded(bytes) => usize_to_u64_saturating(bytes.len()),
        Bound::Unbounded => 0,
    }
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}
