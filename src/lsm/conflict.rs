use std::ops::Bound;

use crate::{
    blob::ValueRef,
    error::Result,
    internal_key::{InternalKey, first_internal_key_for_user, last_internal_key_for_user},
    range_tombstone,
    types::{KeyRange, Sequence},
};

use super::{
    LsmVersion,
    tree::{LsmTree, lock_poisoned},
};

impl LsmTree {
    pub(crate) fn point_key_modified_after(
        &self,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<bool> {
        // A point read is invalidated by either a newer point record for that
        // user key or a newer range tombstone covering it.
        let version = self.current_version()?;
        for (internal_key, _) in self.collect_point_key_records(&version, key, read_sequence)? {
            if internal_key.sequence() > read_sequence {
                return Ok(true);
            }
        }

        self.range_tombstone_modified_after_key(&version, key, read_sequence)
    }

    pub(crate) fn key_range_modified_after(
        &self,
        range: &KeyRange,
        read_sequence: Sequence,
    ) -> Result<bool> {
        // A range read is invalidated by any newer point record inside the
        // range or any newer range tombstone whose bounds overlap the read.
        let version = self.current_version()?;
        for (internal_key, _) in self.collect_range_point_records(&version, range, read_sequence)? {
            if internal_key.sequence() > read_sequence {
                return Ok(true);
            }
        }

        self.range_tombstone_modified_after_range(&version, range, read_sequence)
    }

    fn collect_point_key_records(
        &self,
        version: &LsmVersion,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
        let active_memtable = self
            .active_memtable
            .read()
            .map_err(|_| lock_poisoned("active memtable"))?
            .clone();
        let mut records = collect_memtable_point_records(&active_memtable, key)?;
        if !self.delta_mirror_covers(read_sequence) {
            let delta_snapshot = self.delta_snapshot_for_key(key)?;
            for delta in delta_snapshot.deltas() {
                records.extend(collect_memtable_point_records(&delta.memtable, key)?);
            }
        }

        let immutable_memtables = self
            .immutable_memtables
            .read()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?
            .clone();
        for immutable in immutable_memtables {
            records.extend(collect_memtable_point_records(&immutable.memtable, key)?);
        }

        for table in version.point_lookup_tables(key) {
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
        version: &LsmVersion,
        range: &KeyRange,
        read_sequence: Sequence,
    ) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
        let active_memtable = self
            .active_memtable
            .read()
            .map_err(|_| lock_poisoned("active memtable"))?
            .clone();
        let mut records = collect_memtable_range_records(&active_memtable, range)?;
        if !self.delta_mirror_covers(read_sequence) {
            let delta_snapshot = self.delta_snapshot()?;
            for delta in delta_snapshot.deltas() {
                records.extend(collect_memtable_range_records(&delta.memtable, range)?);
            }
        }

        let immutable_memtables = self
            .immutable_memtables
            .read()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?
            .clone();
        for immutable in immutable_memtables {
            records.extend(collect_memtable_range_records(&immutable.memtable, range)?);
        }

        for table in version.table_handles() {
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
        &self,
        version: &LsmVersion,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<bool> {
        let memtable_tombstones =
            self.memtable_range_tombstones_for_read_sequence(read_sequence)?;
        if memtable_tombstones
            .covering_key(key)
            .any(|tombstone| tombstone.sequence > read_sequence)
        {
            return Ok(true);
        }

        for table in version.table_handles() {
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
        &self,
        version: &LsmVersion,
        range: &KeyRange,
        read_sequence: Sequence,
    ) -> Result<bool> {
        let memtable_tombstones =
            self.memtable_range_tombstones_for_read_sequence(read_sequence)?;
        if memtable_tombstones
            .overlapping_range(range)
            .any(|tombstone| tombstone.sequence > read_sequence)
        {
            return Ok(true);
        }

        for table in version.table_handles() {
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
