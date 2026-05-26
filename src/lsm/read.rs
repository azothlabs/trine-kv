use std::{ops::Bound, path::Path};

use crate::{
    blob::{self, ValueRef},
    cache,
    error::{Error, Result},
    internal_key::{
        InternalKey, ValueKind, first_internal_key_for_user, last_internal_key_for_user,
    },
    memtable::Memtable,
    range_tombstone::RangeTombstoneIndex,
    types::Sequence,
};

use super::tree::{LsmTree, RangeTombstone};

#[derive(Debug, Clone)]
struct PointRecordCandidate {
    internal_key: InternalKey,
    value: Option<ValueRef>,
}

impl LsmTree {
    pub(crate) fn read_visible_point(
        &self,
        key: &[u8],
        read_sequence: Sequence,
        db_path: Option<&Path>,
        block_cache: Option<&cache::BlockCache>,
    ) -> Result<Option<Vec<u8>>> {
        // A point read needs exactly one newest visible internal record for the
        // user key, then a tombstone coverage check for that candidate.
        let mut candidate = self.newest_visible_memtable_point_candidate(key, read_sequence)?;
        let memtable_range_tombstones = self.memtable_range_tombstones()?;
        let tables = self
            .tables
            .read()
            .map_err(|_| lock_poisoned("table list"))?;

        for table in tables.iter() {
            if !table.may_contain_key(key) {
                continue;
            }
            if let Some(record) = table.newest_visible_point_record_for_key_with_cache(
                key,
                read_sequence,
                self.options.index_search_policy,
                block_cache,
            )? {
                keep_newer_point_candidate(
                    &mut candidate,
                    &record.internal_key,
                    record.value.as_ref(),
                );
            }
        }

        let Some(candidate) = candidate else {
            return Ok(None);
        };

        match candidate.internal_key.kind() {
            ValueKind::Put => {
                let covered_by_memtable_tombstone = range_tombstones_cover(
                    &memtable_range_tombstones,
                    key,
                    candidate.internal_key.sequence(),
                    candidate.internal_key.batch_index(),
                    read_sequence,
                );
                let mut covered_by_table_tombstone = false;
                if !covered_by_memtable_tombstone {
                    for table in tables.iter() {
                        covered_by_table_tombstone = table.range_tombstone_covers_visible_point(
                            key,
                            candidate.internal_key.sequence(),
                            candidate.internal_key.batch_index(),
                            read_sequence,
                        )?;
                        if covered_by_table_tombstone {
                            break;
                        }
                    }
                }
                if covered_by_memtable_tombstone || covered_by_table_tombstone {
                    Ok(None)
                } else {
                    drop(tables);
                    value_bytes(candidate.value.as_ref(), db_path).map(Some)
                }
            }
            ValueKind::PointDelete | ValueKind::RangeDelete => Ok(None),
        }
    }

    pub(crate) fn memtable_range_tombstones(&self) -> Result<RangeTombstoneIndex<RangeTombstone>> {
        let active_tombstones = self
            .range_tombstones
            .read()
            .map_err(|_| lock_poisoned("range tombstones"))?;
        let mut tombstones = active_tombstones.clone();
        drop(active_tombstones);

        let immutable_memtables = self
            .immutable_memtables
            .read()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?
            .clone();
        for immutable in immutable_memtables {
            tombstones.extend(immutable.range_tombstones.iter().cloned());
        }

        Ok(RangeTombstoneIndex::new(tombstones))
    }

    fn newest_visible_memtable_point_candidate(
        &self,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<Option<PointRecordCandidate>> {
        let active_memtable = self
            .active_memtable
            .read()
            .map_err(|_| lock_poisoned("active memtable"))?
            .clone();
        let mut candidate = None;
        keep_newest_visible_memtable_point_candidate(
            &mut candidate,
            &active_memtable,
            key,
            read_sequence,
        )?;

        let immutable_memtables = self
            .immutable_memtables
            .read()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?
            .clone();
        for immutable in immutable_memtables {
            keep_newest_visible_memtable_point_candidate(
                &mut candidate,
                &immutable.memtable,
                key,
                read_sequence,
            )?;
        }

        Ok(candidate)
    }
}

fn keep_newest_visible_memtable_point_candidate(
    candidate: &mut Option<PointRecordCandidate>,
    memtable: &Memtable,
    key: &[u8],
    read_sequence: Sequence,
) -> Result<()> {
    let entries = memtable
        .read_entries()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let start = Bound::Included(first_internal_key_for_user(key));
    let end = Bound::Included(last_internal_key_for_user(key));

    for (internal_key, value) in entries.range((start, end)) {
        if internal_key.sequence() > read_sequence {
            continue;
        }
        keep_newer_point_candidate(candidate, internal_key, value.as_ref());
        break;
    }

    Ok(())
}

fn keep_newer_point_candidate(
    candidate: &mut Option<PointRecordCandidate>,
    internal_key: &InternalKey,
    value: Option<&ValueRef>,
) {
    let replace = candidate
        .as_ref()
        .is_none_or(|current| internal_key < &current.internal_key);
    if replace {
        *candidate = Some(PointRecordCandidate {
            internal_key: internal_key.clone(),
            value: value.cloned(),
        });
    }
}

fn range_tombstones_cover(
    range_tombstones: &RangeTombstoneIndex<RangeTombstone>,
    key: &[u8],
    point_sequence: Sequence,
    point_batch_index: u32,
    read_sequence: Sequence,
) -> bool {
    range_tombstones.covering_key(key).any(|tombstone| {
        tombstone.covers_visible_point(key, point_sequence, point_batch_index, read_sequence)
    })
}

fn value_bytes(value: Option<&ValueRef>, db_path: Option<&Path>) -> Result<Vec<u8>> {
    let value = value.ok_or_else(|| Error::Corruption {
        message: "put record is missing value bytes".to_owned(),
    })?;

    match value {
        ValueRef::Inline(bytes) => Ok(bytes.clone()),
        ValueRef::Blob { .. } => {
            let db_path = db_path.ok_or_else(|| Error::Corruption {
                message: "in-memory database cannot read blob value references".to_owned(),
            })?;
            blob::read_value(db_path, value)
        }
    }
}

fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}
