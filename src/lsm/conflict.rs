use std::{
    ops::Bound,
    sync::{Arc, atomic::Ordering},
};

use crate::{
    blob::ValueRef,
    error::Result,
    internal_key::{InternalKey, first_internal_key_for_user, last_internal_key_for_user},
    memtable::Memtable,
    range_tombstone,
    range_tombstone::RangeTombstoneIndex,
    types::{KeyRange, Sequence},
};

use super::{
    LsmVersion,
    delta::DeltaSnapshot,
    tree::{ImmutableMemtable, LsmTree, RangeTombstone, lock_poisoned},
};

#[derive(Debug, Clone)]
struct ConflictSourceSnapshot {
    version: Arc<LsmVersion>,
    delta_snapshot: DeltaSnapshot,
    active_memtable: Arc<Memtable>,
    active_range_tombstones: Vec<RangeTombstone>,
    immutable_memtables: Vec<ImmutableMemtable>,
}

impl LsmTree {
    pub(crate) fn point_key_modified_after(
        &self,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<bool> {
        // A point read is invalidated by either a newer point record for that
        // user key or a newer range tombstone covering it.
        let snapshot = self.point_conflict_snapshot(key, read_sequence)?;
        self.point_key_modified_after_in_snapshot(&snapshot, key, read_sequence)
    }

    fn point_key_modified_after_in_snapshot(
        &self,
        snapshot: &ConflictSourceSnapshot,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<bool> {
        for (internal_key, _) in self.collect_point_key_records(snapshot, key)? {
            if internal_key.sequence() > read_sequence {
                return Ok(true);
            }
        }

        Self::range_tombstone_modified_after_key(snapshot, key, read_sequence)
    }

    pub(crate) fn key_range_modified_after(
        &self,
        range: &KeyRange,
        read_sequence: Sequence,
    ) -> Result<bool> {
        // A range read is invalidated by any newer point record inside the
        // range or any newer range tombstone whose bounds overlap the read.
        let snapshot = self.range_conflict_snapshot(read_sequence)?;
        self.key_range_modified_after_in_snapshot(&snapshot, range, read_sequence)
    }

    fn key_range_modified_after_in_snapshot(
        &self,
        snapshot: &ConflictSourceSnapshot,
        range: &KeyRange,
        read_sequence: Sequence,
    ) -> Result<bool> {
        for (internal_key, _) in self.collect_range_point_records(snapshot, range)? {
            if internal_key.sequence() > read_sequence {
                return Ok(true);
            }
        }

        Self::range_tombstone_modified_after_range(snapshot, range, read_sequence)
    }

    fn point_conflict_snapshot(
        &self,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<ConflictSourceSnapshot> {
        let delta_snapshot = if self.delta_mirror_covers(read_sequence) {
            DeltaSnapshot::default()
        } else {
            self.delta_snapshot_for_key(key)?
        };
        self.conflict_source_snapshot_with_deltas(delta_snapshot)
    }

    fn range_conflict_snapshot(&self, read_sequence: Sequence) -> Result<ConflictSourceSnapshot> {
        let delta_snapshot = if self.delta_mirror_covers(read_sequence) {
            DeltaSnapshot::default()
        } else {
            self.delta_snapshot()?
        };
        self.conflict_source_snapshot_with_deltas(delta_snapshot)
    }

    fn conflict_source_snapshot_with_deltas(
        &self,
        delta_snapshot: DeltaSnapshot,
    ) -> Result<ConflictSourceSnapshot> {
        // Match the ordinary read/scan source order: capture memtable sources
        // before the table version. Flush publishes the L0 table before removing
        // the immutable memtable, so this order may see both copies but cannot
        // miss a write that a transaction must treat as a conflict.
        let active_memtable = self
            .active_memtable
            .read()
            .map_err(|_| lock_poisoned("active memtable"))?
            .clone();
        let active_range_tombstones = if self.range_tombstone_bytes.load(Ordering::Acquire) == 0 {
            Vec::new()
        } else {
            self.range_tombstones
                .read()
                .map_err(|_| lock_poisoned("range tombstones"))?
                .clone()
        };
        let immutable_memtables = if self.has_immutable_memtable_fast() {
            self.immutable_memtables
                .read()
                .map_err(|_| lock_poisoned("immutable memtable queue"))?
                .clone()
        } else {
            Vec::new()
        };
        let version = self.current_version()?;

        Ok(ConflictSourceSnapshot {
            version,
            delta_snapshot,
            active_memtable,
            active_range_tombstones,
            immutable_memtables,
        })
    }

    fn collect_point_key_records(
        &self,
        snapshot: &ConflictSourceSnapshot,
        key: &[u8],
    ) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
        let mut records = collect_memtable_point_records(&snapshot.active_memtable, key)?;
        for delta in snapshot.delta_snapshot.deltas() {
            records.extend(collect_memtable_point_records(&delta.memtable, key)?);
        }
        for immutable in &snapshot.immutable_memtables {
            records.extend(collect_memtable_point_records(&immutable.memtable, key)?);
        }

        for table in snapshot.version.point_lookup_tables(key) {
            records.extend(
                table
                    .point_records_for_key_with_cache(key, self.options.index_search_policy, None)?
                    .into_iter()
                    .map(|record| (record.internal_key, record.value)),
            );
        }
        records.sort_by(|left, right| left.0.cmp(&right.0));

        Ok(records)
    }

    fn collect_range_point_records(
        &self,
        snapshot: &ConflictSourceSnapshot,
        range: &KeyRange,
    ) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
        let mut records = collect_memtable_range_records(&snapshot.active_memtable, range)?;
        for delta in snapshot.delta_snapshot.deltas() {
            records.extend(collect_memtable_range_records(&delta.memtable, range)?);
        }
        for immutable in &snapshot.immutable_memtables {
            records.extend(collect_memtable_range_records(&immutable.memtable, range)?);
        }

        for table in snapshot.version.table_handles() {
            records.extend(
                table
                    .point_records_in_range_with_cache(
                        range,
                        self.options.index_search_policy,
                        None,
                    )?
                    .into_iter()
                    .map(|record| (record.internal_key, record.value)),
            );
        }
        records.sort_by(|left, right| left.0.cmp(&right.0));

        Ok(records)
    }

    fn range_tombstone_modified_after_key(
        snapshot: &ConflictSourceSnapshot,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<bool> {
        let memtable_tombstones = conflict_memtable_range_tombstones(snapshot);
        if memtable_tombstones
            .covering_key(key)
            .any(|tombstone| tombstone.sequence > read_sequence)
        {
            return Ok(true);
        }

        for table in snapshot.version.table_handles() {
            let tombstones = table.range_tombstones()?;
            if tombstones
                .covering_key(key)
                .any(|tombstone| tombstone.sequence > read_sequence)
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn range_tombstone_modified_after_range(
        snapshot: &ConflictSourceSnapshot,
        range: &KeyRange,
        read_sequence: Sequence,
    ) -> Result<bool> {
        let memtable_tombstones = conflict_memtable_range_tombstones(snapshot);
        if memtable_tombstones
            .overlapping_range(range)
            .any(|tombstone| tombstone.sequence > read_sequence)
        {
            return Ok(true);
        }

        for table in snapshot.version.table_handles() {
            if table
                .range_tombstones_overlapping_range(range)?
                .into_iter()
                .any(|tombstone| tombstone.sequence > read_sequence)
            {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

fn conflict_memtable_range_tombstones(
    snapshot: &ConflictSourceSnapshot,
) -> RangeTombstoneIndex<RangeTombstone> {
    let mut tombstones = Vec::new();
    for delta in snapshot.delta_snapshot.deltas() {
        tombstones.extend(delta.range_tombstones.iter().cloned());
    }
    tombstones.extend(snapshot.active_range_tombstones.clone());
    for immutable in &snapshot.immutable_memtables {
        tombstones.extend(immutable.range_tombstones.iter().cloned());
    }
    RangeTombstoneIndex::new(tombstones)
}

fn collect_memtable_point_records(
    memtable: &crate::memtable::Memtable,
    key: &[u8],
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let entries = memtable
        .read_entries()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let start = Bound::Included(first_internal_key_for_user(key));
    let end = Bound::Included(last_internal_key_for_user(key));
    Ok(entries
        .range((start, end))
        .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::{
        options::BucketOptions,
        types::{KeyRange, Sequence},
        write_batch::BatchOperation,
    };

    use super::*;

    #[test]
    fn conflict_snapshot_keeps_flushed_immutable_point_after_queue_removal() -> Result<()> {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new())?;
        let read_sequence = Sequence::new(1);
        let write_sequence = Sequence::new(2);

        tree.apply_operation(
            BatchOperation::Put {
                bucket: "default".to_owned(),
                key: b"a".to_vec(),
                value: b"v2".to_vec(),
            },
            write_sequence,
            0,
        )?;
        assert!(tree.freeze_active_memtable(write_sequence)?);

        let snapshot = tree.point_conflict_snapshot(b"a", read_sequence)?;
        remove_immutable_queue(&tree);

        assert!(tree.point_key_modified_after_in_snapshot(&snapshot, b"a", read_sequence)?);

        Ok(())
    }

    #[test]
    fn conflict_snapshot_keeps_flushed_immutable_range_point_after_queue_removal() -> Result<()> {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new())?;
        let read_sequence = Sequence::new(1);
        let write_sequence = Sequence::new(2);

        tree.apply_operation(
            BatchOperation::Put {
                bucket: "default".to_owned(),
                key: b"b".to_vec(),
                value: b"v2".to_vec(),
            },
            write_sequence,
            0,
        )?;
        assert!(tree.freeze_active_memtable(write_sequence)?);

        let snapshot = tree.range_conflict_snapshot(read_sequence)?;
        remove_immutable_queue(&tree);

        assert!(tree.key_range_modified_after_in_snapshot(
            &snapshot,
            &KeyRange::half_open(b"a", b"z"),
            read_sequence
        )?);

        Ok(())
    }

    #[test]
    fn conflict_snapshot_keeps_flushed_immutable_tombstone_after_queue_removal() -> Result<()> {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new())?;
        let read_sequence = Sequence::new(1);
        let write_sequence = Sequence::new(2);

        tree.apply_operation(
            BatchOperation::DeleteRange {
                bucket: "default".to_owned(),
                range: KeyRange::half_open(b"a", b"z"),
            },
            write_sequence,
            0,
        )?;
        assert!(tree.freeze_active_memtable(write_sequence)?);

        let point_snapshot = tree.point_conflict_snapshot(b"b", read_sequence)?;
        let range_snapshot = tree.range_conflict_snapshot(read_sequence)?;
        remove_immutable_queue(&tree);

        assert!(tree.point_key_modified_after_in_snapshot(&point_snapshot, b"b", read_sequence)?);
        assert!(tree.key_range_modified_after_in_snapshot(
            &range_snapshot,
            &KeyRange::half_open(b"b", b"c"),
            read_sequence
        )?);

        Ok(())
    }

    fn remove_immutable_queue(tree: &LsmTree) {
        tree.immutable_memtables
            .write()
            .expect("immutable memtable queue lock is not poisoned")
            .clear();
        tree.immutable_memtable_count.store(0, Ordering::Release);
    }
}

fn collect_memtable_range_records(
    memtable: &crate::memtable::Memtable,
    range: &KeyRange,
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let entries = memtable
        .read_entries()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    Ok(entries
        .iter()
        .filter(|(internal_key, _)| {
            range_tombstone::key_is_in_range(internal_key.user_key(), range)
        })
        .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
        .collect())
}
