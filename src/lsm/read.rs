use std::{
    cell::Cell,
    collections::BTreeMap,
    ops::Bound,
    path::Path,
    sync::{Arc, atomic::Ordering},
};

use crate::{
    cache,
    error::{Error, Result},
    internal_key::{
        InternalKey, ValueKind, first_internal_key_for_user, last_internal_key_for_user,
    },
    memtable::Memtable,
    point_value::{PointValue, PointValueSource},
    range_tombstone::RangeTombstoneIndex,
    stats::BlobReadMetrics,
    storage::StorageReadBackend,
    types::Sequence,
};

use super::{
    LsmVersion,
    delta::DeltaSnapshot,
    tree::{ImmutableMemtable, LsmTree, RangeTombstone, lock_poisoned},
};

#[derive(Debug, Clone)]
struct PointRecordCandidate {
    internal_key: InternalKey,
    value: Option<PointValueSource>,
}

#[derive(Debug, Clone)]
pub(crate) struct LsmPointReadSnapshot {
    version: Arc<LsmVersion>,
    delta_snapshot: DeltaSnapshot,
    active_memtable: Arc<Memtable>,
    active_range_tombstones: Vec<RangeTombstone>,
    immutable_memtables: Vec<ImmutableMemtable>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AsyncPointReadIo<'io, B>
where
    B: StorageReadBackend,
{
    backend: &'io B,
    db_path: Option<&'io Path>,
    block_cache: Option<&'io cache::BlockCache>,
    blob_reads: Option<&'io BlobReadMetrics>,
}

impl<'io, B> AsyncPointReadIo<'io, B>
where
    B: StorageReadBackend,
{
    pub(crate) const fn new(
        backend: &'io B,
        db_path: Option<&'io Path>,
        block_cache: Option<&'io cache::BlockCache>,
        blob_reads: Option<&'io BlobReadMetrics>,
    ) -> Self {
        Self {
            backend,
            db_path,
            block_cache,
            blob_reads,
        }
    }
}

impl LsmTree {
    pub(crate) fn point_read_snapshot(
        &self,
        read_sequence: Sequence,
    ) -> Result<LsmPointReadSnapshot> {
        let delta_snapshot = if self.delta_mirror_covers(read_sequence) {
            DeltaSnapshot::default()
        } else {
            self.delta_snapshot()?
        };
        self.point_read_snapshot_with_deltas(delta_snapshot)
    }

    fn point_read_snapshot_for_key(
        &self,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<LsmPointReadSnapshot> {
        let delta_snapshot = if self.delta_mirror_covers(read_sequence) {
            DeltaSnapshot::default()
        } else {
            self.delta_snapshot_for_key(key)?
        };
        self.point_read_snapshot_with_deltas(delta_snapshot)
    }

    pub(crate) fn point_read_snapshot_for_keys<K>(
        &self,
        keys: &[K],
        read_sequence: Sequence,
    ) -> Result<LsmPointReadSnapshot>
    where
        K: AsRef<[u8]>,
    {
        let delta_snapshot = if self.delta_mirror_covers(read_sequence) {
            DeltaSnapshot::default()
        } else {
            self.delta_snapshot_for_keys(keys.iter().map(AsRef::as_ref))?
        };
        self.point_read_snapshot_with_deltas(delta_snapshot)
    }

    fn point_read_snapshot_with_deltas(
        &self,
        delta_snapshot: DeltaSnapshot,
    ) -> Result<LsmPointReadSnapshot> {
        // Capture memtable sources before the version. Flush publishes the new
        // table version before removing the immutable memtable, so this order
        // can see a duplicate source but cannot miss a committed record.
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

        Ok(LsmPointReadSnapshot {
            version,
            delta_snapshot,
            active_memtable,
            active_range_tombstones,
            immutable_memtables,
        })
    }

    pub(crate) fn read_visible_point(
        &self,
        key: &[u8],
        read_sequence: Sequence,
        db_path: Option<&Path>,
        block_cache: Option<&cache::BlockCache>,
        blob_reads: Option<&BlobReadMetrics>,
    ) -> Result<Option<Vec<u8>>> {
        self.read_visible_point_value(key, read_sequence, db_path, block_cache, blob_reads)?
            .map(|value| Ok(value.into_value()))
            .transpose()
    }

    pub(crate) async fn read_visible_point_async<B>(
        &self,
        backend: &B,
        key: &[u8],
        read_sequence: Sequence,
        db_path: Option<&Path>,
        block_cache: Option<&cache::BlockCache>,
        blob_reads: Option<&BlobReadMetrics>,
    ) -> Result<Option<Vec<u8>>>
    where
        B: StorageReadBackend,
    {
        self.read_visible_point_value_async(
            backend,
            key,
            read_sequence,
            db_path,
            block_cache,
            blob_reads,
        )
        .await?
        .map(|value| Ok(value.into_value()))
        .transpose()
    }

    pub(crate) fn read_visible_point_value(
        &self,
        key: &[u8],
        read_sequence: Sequence,
        db_path: Option<&Path>,
        block_cache: Option<&cache::BlockCache>,
        blob_reads: Option<&BlobReadMetrics>,
    ) -> Result<Option<PointValue>> {
        let snapshot = self.point_read_snapshot_for_key(key, read_sequence)?;
        self.read_visible_point_value_in_snapshot(
            &snapshot,
            key,
            read_sequence,
            db_path,
            block_cache,
            blob_reads,
        )
    }

    pub(crate) async fn read_visible_point_value_async<B>(
        &self,
        backend: &B,
        key: &[u8],
        read_sequence: Sequence,
        db_path: Option<&Path>,
        block_cache: Option<&cache::BlockCache>,
        blob_reads: Option<&BlobReadMetrics>,
    ) -> Result<Option<PointValue>>
    where
        B: StorageReadBackend,
    {
        let snapshot = self.point_read_snapshot_for_key(key, read_sequence)?;
        self.read_visible_point_value_in_snapshot_async(
            &snapshot,
            key,
            read_sequence,
            AsyncPointReadIo::new(backend, db_path, block_cache, blob_reads),
        )
        .await
    }

    pub(crate) fn read_visible_point_value_in_snapshot(
        &self,
        snapshot: &LsmPointReadSnapshot,
        key: &[u8],
        read_sequence: Sequence,
        db_path: Option<&Path>,
        block_cache: Option<&cache::BlockCache>,
        blob_reads: Option<&BlobReadMetrics>,
    ) -> Result<Option<PointValue>> {
        // A point read needs exactly one newest visible internal record for the
        // user key, then a tombstone coverage check for that candidate.
        let mut candidate = Self::newest_visible_memtable_point_candidate_in_snapshot(
            snapshot,
            key,
            read_sequence,
        )?;
        let newest_candidate_sequence = Cell::new(
            candidate
                .as_ref()
                .map(|candidate| candidate.internal_key.sequence()),
        );

        snapshot.version.for_each_point_lookup_table(
            key,
            |table| table_may_have_newer_point_record(table, newest_candidate_sequence.get()),
            |table| {
                if let Some(record) = table.newest_visible_point_value_record_for_key_with_cache(
                    key,
                    read_sequence,
                    self.options.index_search_policy,
                    block_cache,
                )? {
                    keep_newer_point_candidate_owned(
                        &mut candidate,
                        record.internal_key,
                        record.value,
                    );
                    newest_candidate_sequence.set(
                        candidate
                            .as_ref()
                            .map(|candidate| candidate.internal_key.sequence()),
                    );
                }
                Ok(())
            },
        )?;

        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let PointRecordCandidate {
            internal_key,
            value,
        } = candidate;

        match internal_key.kind() {
            ValueKind::Put => {
                let memtable_range_tombstones = memtable_range_tombstones_in_snapshot(snapshot);
                let covered_by_memtable_tombstone = range_tombstones_cover(
                    &memtable_range_tombstones,
                    key,
                    internal_key.sequence(),
                    internal_key.batch_index(),
                    read_sequence,
                );
                let mut covered_by_table_tombstone = false;
                if !covered_by_memtable_tombstone {
                    for table in snapshot.version.range_tombstone_tables_for_key(key) {
                        covered_by_table_tombstone = table.range_tombstone_covers_visible_point(
                            key,
                            internal_key.sequence(),
                            internal_key.batch_index(),
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
                    point_value(value, &internal_key, db_path, blob_reads).map(Some)
                }
            }
            ValueKind::PointDelete | ValueKind::RangeDelete => Ok(None),
        }
    }

    pub(crate) fn read_visible_point_values_in_snapshot<K>(
        &self,
        snapshot: &LsmPointReadSnapshot,
        keys: &[K],
        read_sequence: Sequence,
        db_path: Option<&Path>,
        block_cache: Option<&cache::BlockCache>,
        blob_reads: Option<&BlobReadMetrics>,
    ) -> Result<Vec<Option<PointValue>>>
    where
        K: AsRef<[u8]>,
    {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if point_read_batch_prefers_single_key_path(keys) {
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(self.read_visible_point_value_in_snapshot(
                    snapshot,
                    key.as_ref(),
                    read_sequence,
                    db_path,
                    block_cache,
                    blob_reads,
                )?);
            }
            return Ok(values);
        }

        let batch = PointReadBatch::from_keys(keys);
        snapshot
            .version
            .record_batch_point_shape(keys.len(), batch.unique_keys.len());
        let mut candidates = Vec::with_capacity(batch.unique_keys.len());
        candidates.resize_with(batch.unique_keys.len(), || None);
        for (index, key) in batch.unique_keys.iter().enumerate() {
            candidates[index] = Self::newest_visible_memtable_point_candidate_in_snapshot(
                snapshot,
                key,
                read_sequence,
            )?;
        }

        let newest_candidate_sequences = candidates
            .iter()
            .map(|candidate| {
                Cell::new(
                    candidate
                        .as_ref()
                        .map(|candidate| candidate.internal_key.sequence()),
                )
            })
            .collect::<Vec<_>>();

        snapshot.version.for_each_point_lookup_table_for_keys(
            &batch.unique_keys,
            |table, key_index| {
                table_may_have_newer_point_record(
                    table,
                    newest_candidate_sequences[key_index].get(),
                )
            },
            |table, key_indices| {
                let table_keys = key_indices
                    .iter()
                    .map(|key_index| batch.unique_keys[*key_index])
                    .collect::<Vec<_>>();
                let records = table.newest_visible_point_value_records_for_keys_with_cache(
                    &table_keys,
                    read_sequence,
                    self.options.index_search_policy,
                    block_cache,
                )?;
                for (local_index, record) in records.into_iter().enumerate() {
                    let Some(record) = record else {
                        continue;
                    };
                    let key_index = key_indices[local_index];
                    keep_newer_point_candidate_owned(
                        &mut candidates[key_index],
                        record.internal_key,
                        record.value,
                    );
                    newest_candidate_sequences[key_index].set(
                        candidates[key_index]
                            .as_ref()
                            .map(|candidate| candidate.internal_key.sequence()),
                    );
                }
                Ok(())
            },
        )?;

        if candidates.iter().all(Option::is_none) {
            let unique_values = vec![None; batch.unique_keys.len()];
            return Ok(batch.scatter(&unique_values));
        }

        let memtable_range_tombstones = memtable_range_tombstones_in_snapshot(snapshot);
        let mut unique_values = Vec::with_capacity(batch.unique_keys.len());
        for (key_index, candidate) in candidates.into_iter().enumerate() {
            unique_values.push(resolve_point_candidate(
                snapshot,
                &memtable_range_tombstones,
                batch.unique_keys[key_index],
                candidate,
                read_sequence,
                db_path,
                blob_reads,
            )?);
        }

        Ok(batch.scatter(&unique_values))
    }

    pub(crate) async fn read_visible_point_value_in_snapshot_async<B>(
        &self,
        snapshot: &LsmPointReadSnapshot,
        key: &[u8],
        read_sequence: Sequence,
        io: AsyncPointReadIo<'_, B>,
    ) -> Result<Option<PointValue>>
    where
        B: StorageReadBackend,
    {
        let mut candidate = Self::newest_visible_memtable_point_candidate_in_snapshot(
            snapshot,
            key,
            read_sequence,
        )?;
        let newest_candidate_sequence = Cell::new(
            candidate
                .as_ref()
                .map(|candidate| candidate.internal_key.sequence()),
        );

        for table in snapshot.version.point_lookup_tables(key) {
            if !table_may_have_newer_point_record(&table, newest_candidate_sequence.get()) {
                continue;
            }
            if let Some(record) = table
                .newest_visible_point_value_record_for_key_with_cache_async(
                    key,
                    read_sequence,
                    self.options.index_search_policy,
                    io.block_cache,
                )
                .await?
            {
                keep_newer_point_candidate_owned(&mut candidate, record.internal_key, record.value);
                newest_candidate_sequence.set(
                    candidate
                        .as_ref()
                        .map(|candidate| candidate.internal_key.sequence()),
                );
            }
        }

        let Some(candidate) = candidate else {
            return Ok(None);
        };
        let PointRecordCandidate {
            internal_key,
            value,
        } = candidate;

        match internal_key.kind() {
            ValueKind::Put => {
                let memtable_range_tombstones = memtable_range_tombstones_in_snapshot(snapshot);
                let covered_by_memtable_tombstone = range_tombstones_cover(
                    &memtable_range_tombstones,
                    key,
                    internal_key.sequence(),
                    internal_key.batch_index(),
                    read_sequence,
                );
                let mut covered_by_table_tombstone = false;
                if !covered_by_memtable_tombstone {
                    for table in snapshot.version.range_tombstone_tables_for_key(key) {
                        covered_by_table_tombstone = table
                            .range_tombstone_covers_visible_point_async(
                                key,
                                internal_key.sequence(),
                                internal_key.batch_index(),
                                read_sequence,
                            )
                            .await?;
                        if covered_by_table_tombstone {
                            break;
                        }
                    }
                }
                if covered_by_memtable_tombstone || covered_by_table_tombstone {
                    Ok(None)
                } else {
                    point_value_async(io.backend, value, &internal_key, io.db_path, io.blob_reads)
                        .await
                        .map(Some)
                }
            }
            ValueKind::PointDelete | ValueKind::RangeDelete => Ok(None),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn read_visible_point_values_in_snapshot_async<B, K>(
        &self,
        snapshot: &LsmPointReadSnapshot,
        keys: &[K],
        read_sequence: Sequence,
        io: AsyncPointReadIo<'_, B>,
    ) -> Result<Vec<Option<PointValue>>>
    where
        B: StorageReadBackend,
        K: AsRef<[u8]>,
    {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if point_read_batch_prefers_single_key_path(keys) {
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(
                    self.read_visible_point_value_in_snapshot_async(
                        snapshot,
                        key.as_ref(),
                        read_sequence,
                        AsyncPointReadIo::new(
                            io.backend,
                            io.db_path,
                            io.block_cache,
                            io.blob_reads,
                        ),
                    )
                    .await?,
                );
            }
            return Ok(values);
        }

        let batch = PointReadBatch::from_keys(keys);
        snapshot
            .version
            .record_batch_point_shape(keys.len(), batch.unique_keys.len());
        let mut candidates = Vec::with_capacity(batch.unique_keys.len());
        candidates.resize_with(batch.unique_keys.len(), || None);
        for (index, key) in batch.unique_keys.iter().enumerate() {
            candidates[index] = Self::newest_visible_memtable_point_candidate_in_snapshot(
                snapshot,
                key,
                read_sequence,
            )?;
        }

        let newest_candidate_sequences = candidates
            .iter()
            .map(|candidate| {
                Cell::new(
                    candidate
                        .as_ref()
                        .map(|candidate| candidate.internal_key.sequence()),
                )
            })
            .collect::<Vec<_>>();

        let mut table_groups = Vec::new();
        snapshot.version.for_each_point_lookup_table_for_keys(
            &batch.unique_keys,
            |table, key_index| {
                table_may_have_newer_point_record(
                    table,
                    newest_candidate_sequences[key_index].get(),
                )
            },
            |table, key_indices| {
                table_groups.push((Arc::clone(table), key_indices.to_vec()));
                Ok(())
            },
        )?;

        for (table, key_indices) in table_groups {
            let table_keys = key_indices
                .iter()
                .map(|key_index| batch.unique_keys[*key_index])
                .collect::<Vec<_>>();
            let records = table
                .newest_visible_point_value_records_for_keys_with_cache_async(
                    &table_keys,
                    read_sequence,
                    self.options.index_search_policy,
                    io.block_cache,
                )
                .await?;
            for (local_index, record) in records.into_iter().enumerate() {
                let Some(record) = record else {
                    continue;
                };
                let key_index = key_indices[local_index];
                keep_newer_point_candidate_owned(
                    &mut candidates[key_index],
                    record.internal_key,
                    record.value,
                );
                newest_candidate_sequences[key_index].set(
                    candidates[key_index]
                        .as_ref()
                        .map(|candidate| candidate.internal_key.sequence()),
                );
            }
        }

        if candidates.iter().all(Option::is_none) {
            let unique_values = vec![None; batch.unique_keys.len()];
            return Ok(batch.scatter(&unique_values));
        }

        let memtable_range_tombstones = memtable_range_tombstones_in_snapshot(snapshot);
        let mut unique_values = Vec::with_capacity(batch.unique_keys.len());
        for (key_index, candidate) in candidates.into_iter().enumerate() {
            unique_values.push(
                resolve_point_candidate_async(
                    &io,
                    snapshot,
                    &memtable_range_tombstones,
                    batch.unique_keys[key_index],
                    candidate,
                    read_sequence,
                )
                .await?,
            );
        }

        Ok(batch.scatter(&unique_values))
    }

    pub(crate) fn memtable_range_tombstones_for_read_sequence(
        &self,
        read_sequence: Sequence,
    ) -> Result<RangeTombstoneIndex<RangeTombstone>> {
        self.memtable_range_tombstones_with_deltas(!self.delta_mirror_covers(read_sequence))
    }

    fn memtable_range_tombstones_with_deltas(
        &self,
        include_deltas: bool,
    ) -> Result<RangeTombstoneIndex<RangeTombstone>> {
        let mut tombstones = Vec::new();

        if include_deltas {
            let delta_snapshot = self.delta_snapshot()?;
            for delta in delta_snapshot.deltas() {
                tombstones.extend(delta.range_tombstones.iter().cloned());
            }
        }

        if self.range_tombstone_bytes.load(Ordering::Acquire) != 0 {
            let active_tombstones = self
                .range_tombstones
                .read()
                .map_err(|_| lock_poisoned("range tombstones"))?;
            tombstones.extend(active_tombstones.iter().cloned());
        }

        if !self.has_immutable_memtable_fast() {
            return Ok(RangeTombstoneIndex::new(tombstones));
        }

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

    fn newest_visible_memtable_point_candidate_in_snapshot(
        snapshot: &LsmPointReadSnapshot,
        key: &[u8],
        read_sequence: Sequence,
    ) -> Result<Option<PointRecordCandidate>> {
        let mut candidate = None;
        keep_newest_visible_memtable_point_candidate(
            &mut candidate,
            &snapshot.active_memtable,
            key,
            read_sequence,
        )?;

        for immutable in &snapshot.immutable_memtables {
            keep_newest_visible_memtable_point_candidate(
                &mut candidate,
                &immutable.memtable,
                key,
                read_sequence,
            )?;
        }
        for delta in snapshot.delta_snapshot.deltas() {
            if candidate
                .as_ref()
                .is_some_and(|current| delta.sequence <= current.internal_key.sequence())
            {
                continue;
            }
            keep_newest_visible_memtable_point_candidate(
                &mut candidate,
                &delta.memtable,
                key,
                read_sequence,
            )?;
        }

        Ok(candidate)
    }
}

#[derive(Debug)]
struct PointReadBatch<'key> {
    unique_keys: Vec<&'key [u8]>,
    positions: Vec<Vec<usize>>,
    input_len: usize,
}

const POINT_READ_BATCH_GROUPING_MIN_KEYS: usize = 8;
const POINT_READ_BATCH_LINEAR_DEDUP_MAX_KEYS: usize = 32;

impl<'key> PointReadBatch<'key> {
    fn from_keys<K>(keys: &'key [K]) -> Self
    where
        K: AsRef<[u8]>,
    {
        if keys.len() <= POINT_READ_BATCH_LINEAR_DEDUP_MAX_KEYS {
            return Self::from_keys_linear(keys);
        }

        let mut unique_indices = BTreeMap::<Vec<u8>, usize>::new();
        let mut unique_keys = Vec::new();
        let mut positions: Vec<Vec<usize>> = Vec::new();

        for (position, key) in keys.iter().enumerate() {
            let key = key.as_ref();
            if let Some(&index) = unique_indices.get(key) {
                positions[index].push(position);
                continue;
            }

            let index = unique_keys.len();
            unique_indices.insert(key.to_vec(), index);
            unique_keys.push(key);
            positions.push(vec![position]);
        }

        Self {
            unique_keys,
            positions,
            input_len: keys.len(),
        }
    }

    fn from_keys_linear<K>(keys: &'key [K]) -> Self
    where
        K: AsRef<[u8]>,
    {
        let mut unique_keys: Vec<&[u8]> = Vec::new();
        let mut positions: Vec<Vec<usize>> = Vec::new();

        for (position, key) in keys.iter().enumerate() {
            let key = key.as_ref();
            if let Some(index) = unique_keys.iter().position(|unique_key| *unique_key == key) {
                positions[index].push(position);
                continue;
            }

            unique_keys.push(key);
            positions.push(vec![position]);
        }

        Self {
            unique_keys,
            positions,
            input_len: keys.len(),
        }
    }

    fn scatter(&self, unique_values: &[Option<PointValue>]) -> Vec<Option<PointValue>> {
        debug_assert_eq!(unique_values.len(), self.unique_keys.len());
        let mut values = Vec::with_capacity(self.input_len);
        values.resize_with(self.input_len, || None);
        for (unique_index, positions) in self.positions.iter().enumerate() {
            for position in positions {
                values[*position].clone_from(&unique_values[unique_index]);
            }
        }
        values
    }
}

fn point_read_batch_prefers_single_key_path<K>(keys: &[K]) -> bool
where
    K: AsRef<[u8]>,
{
    keys.len() < POINT_READ_BATCH_GROUPING_MIN_KEYS && point_read_batch_keys_are_unique(keys)
}

fn point_read_batch_keys_are_unique<K>(keys: &[K]) -> bool
where
    K: AsRef<[u8]>,
{
    for (index, key) in keys.iter().enumerate() {
        let key = key.as_ref();
        if keys[..index]
            .iter()
            .any(|existing| existing.as_ref() == key)
        {
            return false;
        }
    }
    true
}

fn resolve_point_candidate(
    snapshot: &LsmPointReadSnapshot,
    memtable_range_tombstones: &RangeTombstoneIndex<RangeTombstone>,
    key: &[u8],
    candidate: Option<PointRecordCandidate>,
    read_sequence: Sequence,
    db_path: Option<&Path>,
    blob_reads: Option<&BlobReadMetrics>,
) -> Result<Option<PointValue>> {
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let PointRecordCandidate {
        internal_key,
        value,
    } = candidate;

    match internal_key.kind() {
        ValueKind::Put => {
            let covered_by_memtable_tombstone = range_tombstones_cover(
                memtable_range_tombstones,
                key,
                internal_key.sequence(),
                internal_key.batch_index(),
                read_sequence,
            );
            let mut covered_by_table_tombstone = false;
            if !covered_by_memtable_tombstone {
                for table in snapshot.version.range_tombstone_tables_for_key(key) {
                    covered_by_table_tombstone = table.range_tombstone_covers_visible_point(
                        key,
                        internal_key.sequence(),
                        internal_key.batch_index(),
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
                point_value(value, &internal_key, db_path, blob_reads).map(Some)
            }
        }
        ValueKind::PointDelete | ValueKind::RangeDelete => Ok(None),
    }
}

async fn resolve_point_candidate_async<B>(
    io: &AsyncPointReadIo<'_, B>,
    snapshot: &LsmPointReadSnapshot,
    memtable_range_tombstones: &RangeTombstoneIndex<RangeTombstone>,
    key: &[u8],
    candidate: Option<PointRecordCandidate>,
    read_sequence: Sequence,
) -> Result<Option<PointValue>>
where
    B: StorageReadBackend,
{
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let PointRecordCandidate {
        internal_key,
        value,
    } = candidate;

    match internal_key.kind() {
        ValueKind::Put => {
            let covered_by_memtable_tombstone = range_tombstones_cover(
                memtable_range_tombstones,
                key,
                internal_key.sequence(),
                internal_key.batch_index(),
                read_sequence,
            );
            let mut covered_by_table_tombstone = false;
            if !covered_by_memtable_tombstone {
                for table in snapshot.version.range_tombstone_tables_for_key(key) {
                    covered_by_table_tombstone = table
                        .range_tombstone_covers_visible_point_async(
                            key,
                            internal_key.sequence(),
                            internal_key.batch_index(),
                            read_sequence,
                        )
                        .await?;
                    if covered_by_table_tombstone {
                        break;
                    }
                }
            }
            if covered_by_memtable_tombstone || covered_by_table_tombstone {
                Ok(None)
            } else {
                point_value_async(io.backend, value, &internal_key, io.db_path, io.blob_reads)
                    .await
                    .map(Some)
            }
        }
        ValueKind::PointDelete | ValueKind::RangeDelete => Ok(None),
    }
}

fn memtable_range_tombstones_in_snapshot(
    snapshot: &LsmPointReadSnapshot,
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

fn keep_newest_visible_memtable_point_candidate(
    candidate: &mut Option<PointRecordCandidate>,
    memtable: &Memtable,
    key: &[u8],
    read_sequence: Sequence,
) -> Result<()> {
    if memtable.estimated_bytes() == 0 {
        return Ok(());
    }

    let entries = memtable
        .read_entries()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    let Some((smallest, _)) = entries.first_key_value() else {
        return Ok(());
    };
    let Some((largest, _)) = entries.last_key_value() else {
        return Ok(());
    };
    if key < smallest.user_key() || key > largest.user_key() {
        return Ok(());
    }

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
    value: Option<&crate::blob::ValueRef>,
) {
    let replace = candidate
        .as_ref()
        .is_none_or(|current| internal_key < &current.internal_key);
    if replace {
        *candidate = Some(PointRecordCandidate {
            internal_key: internal_key.clone(),
            value: value.cloned().map(PointValueSource::from_value_ref),
        });
    }
}

fn keep_newer_point_candidate_owned(
    candidate: &mut Option<PointRecordCandidate>,
    internal_key: InternalKey,
    value: Option<PointValueSource>,
) {
    let replace = candidate
        .as_ref()
        .is_none_or(|current| internal_key < current.internal_key);
    if replace {
        *candidate = Some(PointRecordCandidate {
            internal_key,
            value,
        });
    }
}

fn table_may_have_newer_point_record(
    table: &crate::table::Table,
    newest_candidate_sequence: Option<Sequence>,
) -> bool {
    newest_candidate_sequence.is_none_or(|sequence| table.properties().largest_sequence >= sequence)
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

fn point_value(
    value: Option<PointValueSource>,
    internal_key: &InternalKey,
    db_path: Option<&Path>,
    blob_reads: Option<&BlobReadMetrics>,
) -> Result<PointValue> {
    let value = value.ok_or_else(|| Error::Corruption {
        message: "put record is missing value bytes".to_owned(),
    })?;

    value.into_point_value(internal_key, db_path, blob_reads)
}

async fn point_value_async<B>(
    backend: &B,
    value: Option<PointValueSource>,
    internal_key: &InternalKey,
    db_path: Option<&Path>,
    blob_reads: Option<&BlobReadMetrics>,
) -> Result<PointValue>
where
    B: StorageReadBackend,
{
    let value = value.ok_or_else(|| Error::Corruption {
        message: "put record is missing value bytes".to_owned(),
    })?;

    value
        .into_point_value_with_backend_async(backend, internal_key, db_path, blob_reads)
        .await
}

#[cfg(test)]
mod tests {
    use super::{POINT_READ_BATCH_GROUPING_MIN_KEYS, point_read_batch_prefers_single_key_path};

    #[test]
    fn small_unique_point_batches_use_single_key_path() {
        let keys = [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()];

        assert!(point_read_batch_prefers_single_key_path(&keys));
    }

    #[test]
    fn small_duplicate_point_batches_keep_batch_path() {
        let keys = [b"a".as_slice(), b"b".as_slice(), b"a".as_slice()];

        assert!(!point_read_batch_prefers_single_key_path(&keys));
    }

    #[test]
    fn grouping_threshold_uses_batch_path() {
        let keys = (0..POINT_READ_BATCH_GROUPING_MIN_KEYS)
            .map(|index| format!("key-{index:08}").into_bytes())
            .collect::<Vec<_>>();

        assert!(!point_read_batch_prefers_single_key_path(&keys));
    }
}
