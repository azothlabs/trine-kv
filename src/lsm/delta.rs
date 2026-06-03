use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use crate::{
    blob::ValueRef,
    error::Result,
    internal_key::{InternalKey, ValueKind},
    memtable::Memtable,
    range_tombstone,
    types::{KeyRange, Sequence},
    write_batch::BatchOperation,
};

use super::tree::{LsmTree, RangeTombstone, lock_poisoned};

const DELTA_SHARD_COUNT: usize = 16;
const DELTA_EPOCH_MAX_DELTAS: usize = 8;

#[derive(Debug)]
pub(crate) struct DeltaShardSet {
    shards: Vec<DeltaShard>,
}

#[derive(Debug, Default)]
struct DeltaShard {
    state: RwLock<DeltaShardState>,
}

#[derive(Debug, Default)]
struct DeltaShardState {
    open_epoch: DeltaEpoch,
    sealed_epoch_count: u64,
    merged_epoch_count: u64,
    retired_delta_count: u64,
}

#[derive(Debug, Default)]
struct DeltaEpoch {
    deltas: Vec<Arc<InMemoryDelta>>,
    estimated_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct InMemoryDelta {
    pub(crate) memtable: Arc<Memtable>,
    pub(crate) range_tombstones: Arc<Vec<RangeTombstone>>,
    pub(crate) estimated_bytes: u64,
    pub(crate) sequence: Sequence,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DeltaSnapshot {
    deltas: Vec<Arc<InMemoryDelta>>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DeltaDebugStats {
    pub(crate) delta_count: usize,
    pub(crate) point_delta_count: usize,
    pub(crate) range_tombstone_count: usize,
    pub(crate) max_shard_chain_len: usize,
    pub(crate) open_epoch_bytes: u64,
    pub(crate) sealed_epoch_count: u64,
    pub(crate) merged_epoch_count: u64,
    pub(crate) retired_delta_count: u64,
}

#[derive(Debug)]
struct DeltaBuilder {
    memtable: Arc<Memtable>,
    range_tombstones: Vec<RangeTombstone>,
    range_tombstone_bytes: u64,
}

impl DeltaShardSet {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..DELTA_SHARD_COUNT)
                .map(|_| DeltaShard::default())
                .collect(),
        }
    }

    #[cfg(test)]
    fn publish_operations(
        &self,
        operations: impl IntoIterator<Item = (BatchOperation, u32)>,
        sequence: Sequence,
    ) -> Result<()> {
        self.publish_operations_with_budget(operations, sequence, u64::MAX)
    }

    fn publish_operations_with_budget(
        &self,
        operations: impl IntoIterator<Item = (BatchOperation, u32)>,
        sequence: Sequence,
        max_epoch_bytes: u64,
    ) -> Result<()> {
        let mut builders = (0..self.shards.len())
            .map(|_| DeltaBuilder::new())
            .collect::<Vec<_>>();

        for (operation, batch_index) in operations {
            match operation {
                BatchOperation::Put { key, value, .. } => {
                    let shard_index = self.shard_index_for_key(&key);
                    builders[shard_index].put(key, value, sequence, batch_index)?;
                }
                BatchOperation::Delete { key, .. } => {
                    let shard_index = self.shard_index_for_key(&key);
                    builders[shard_index].delete(key, sequence, batch_index)?;
                }
                BatchOperation::DeleteRange { range, .. } => {
                    for builder in &mut builders {
                        builder.delete_range(range.clone(), sequence, batch_index);
                    }
                }
            }
        }

        for (shard, builder) in self.shards.iter().zip(builders) {
            if let Some(delta) = builder.finish(sequence) {
                shard.publish(delta, max_epoch_bytes)?;
            }
        }

        Ok(())
    }

    fn snapshot_all(&self) -> Result<DeltaSnapshot> {
        let mut deltas = Vec::new();
        for shard in &self.shards {
            deltas.extend(shard.snapshot()?);
        }
        Ok(DeltaSnapshot { deltas })
    }

    fn snapshot_for_key(&self, key: &[u8]) -> Result<DeltaSnapshot> {
        let shard_index = self.shard_index_for_key(key);
        Ok(DeltaSnapshot {
            deltas: self.shards[shard_index].snapshot()?,
        })
    }

    fn snapshot_for_keys<'key, I>(&self, keys: I) -> Result<DeltaSnapshot>
    where
        I: IntoIterator<Item = &'key [u8]>,
    {
        let mut selected = vec![false; self.shards.len()];
        for key in keys {
            let shard_index = self.shard_index_for_key(key);
            selected[shard_index] = true;
        }

        let mut deltas = Vec::new();
        for (shard, selected) in self.shards.iter().zip(selected) {
            if selected {
                deltas.extend(shard.snapshot()?);
            }
        }
        Ok(DeltaSnapshot { deltas })
    }

    fn estimated_bytes(&self) -> Result<u64> {
        let mut bytes = 0_u64;
        for shard in &self.shards {
            bytes = bytes.saturating_add(shard.estimated_bytes()?);
        }
        Ok(bytes)
    }

    fn shard_index_for_key(&self, key: &[u8]) -> usize {
        debug_assert!(!self.shards.is_empty());
        let shard_count = u64::try_from(self.shards.len()).expect("delta shard count fits u64");
        let index = stable_key_hash(key) % shard_count;
        usize::try_from(index).expect("delta shard index fits usize")
    }

    #[cfg(test)]
    fn debug_counts(&self) -> Result<(usize, usize, usize)> {
        let stats = self.debug_stats()?;

        Ok((
            stats.delta_count,
            stats.point_delta_count,
            stats.range_tombstone_count,
        ))
    }

    #[cfg(test)]
    fn debug_stats(&self) -> Result<DeltaDebugStats> {
        let mut aggregate = DeltaDebugStats::default();

        for shard in &self.shards {
            let shard_state = shard
                .state
                .read()
                .map_err(|_| lock_poisoned("delta shard"))?;
            aggregate.delta_count = aggregate
                .delta_count
                .saturating_add(shard_state.open_epoch.deltas.len());
            aggregate.max_shard_chain_len = aggregate
                .max_shard_chain_len
                .max(shard_state.open_epoch.deltas.len());
            aggregate.open_epoch_bytes = aggregate
                .open_epoch_bytes
                .saturating_add(shard_state.open_epoch.estimated_bytes);
            aggregate.sealed_epoch_count = aggregate
                .sealed_epoch_count
                .saturating_add(shard_state.sealed_epoch_count);
            aggregate.merged_epoch_count = aggregate
                .merged_epoch_count
                .saturating_add(shard_state.merged_epoch_count);
            aggregate.retired_delta_count = aggregate
                .retired_delta_count
                .saturating_add(shard_state.retired_delta_count);
            for delta in &shard_state.open_epoch.deltas {
                if delta.memtable.estimated_bytes() != 0 {
                    aggregate.point_delta_count = aggregate.point_delta_count.saturating_add(1);
                }
                aggregate.range_tombstone_count = aggregate
                    .range_tombstone_count
                    .saturating_add(delta.range_tombstones.len());
            }
        }

        Ok(aggregate)
    }
}

impl DeltaShard {
    fn publish(&self, delta: Arc<InMemoryDelta>, max_epoch_bytes: u64) -> Result<()> {
        self.state
            .write()
            .map_err(|_| lock_poisoned("delta shard"))?
            .publish(delta, max_epoch_bytes)
    }

    fn snapshot(&self) -> Result<Vec<Arc<InMemoryDelta>>> {
        self.state
            .read()
            .map_err(|_| lock_poisoned("delta shard"))
            .map(|state| state.open_epoch.deltas.clone())
    }

    fn estimated_bytes(&self) -> Result<u64> {
        self.state
            .read()
            .map_err(|_| lock_poisoned("delta shard"))
            .map(|state| state.open_epoch.estimated_bytes)
    }
}

impl DeltaShardState {
    fn publish(&mut self, delta: Arc<InMemoryDelta>, max_epoch_bytes: u64) -> Result<()> {
        self.open_epoch.push(delta);
        if self.open_epoch_exceeds_budget(max_epoch_bytes) {
            self.seal_and_merge_open_epoch()?;
        }
        Ok(())
    }

    fn open_epoch_exceeds_budget(&self, max_epoch_bytes: u64) -> bool {
        self.open_epoch.deltas.len() >= DELTA_EPOCH_MAX_DELTAS
            || self.open_epoch.estimated_bytes >= max_epoch_bytes
    }

    fn seal_and_merge_open_epoch(&mut self) -> Result<()> {
        if self.open_epoch.deltas.len() <= 1 {
            return Ok(());
        }

        let sealed = std::mem::take(&mut self.open_epoch);
        let retired_count = usize_to_u64_saturating(sealed.deltas.len());
        let merged = merge_deltas(&sealed.deltas)?;
        self.sealed_epoch_count = self.sealed_epoch_count.saturating_add(1);
        self.merged_epoch_count = self.merged_epoch_count.saturating_add(1);
        self.retired_delta_count = self.retired_delta_count.saturating_add(retired_count);
        self.open_epoch.push(merged);
        Ok(())
    }
}

impl DeltaEpoch {
    fn push(&mut self, delta: Arc<InMemoryDelta>) {
        self.estimated_bytes = self.estimated_bytes.saturating_add(delta.estimated_bytes);
        self.deltas.push(delta);
    }
}

impl DeltaSnapshot {
    pub(crate) fn deltas(&self) -> &[Arc<InMemoryDelta>] {
        &self.deltas
    }
}

impl DeltaBuilder {
    fn new() -> Self {
        Self {
            memtable: Arc::new(Memtable::default()),
            range_tombstones: Vec::new(),
            range_tombstone_bytes: 0,
        }
    }

    fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        sequence: Sequence,
        batch_index: u32,
    ) -> Result<()> {
        self.memtable
            .insert(
                InternalKey::new(key, sequence, ValueKind::Put, batch_index),
                Some(ValueRef::Inline(value)),
            )
            .map_err(|()| lock_poisoned("delta memtable entries"))
    }

    fn delete(&self, key: Vec<u8>, sequence: Sequence, batch_index: u32) -> Result<()> {
        self.memtable
            .insert(
                InternalKey::new(key, sequence, ValueKind::PointDelete, batch_index),
                None,
            )
            .map_err(|()| lock_poisoned("delta memtable entries"))
    }

    fn delete_range(&mut self, range: KeyRange, sequence: Sequence, batch_index: u32) {
        self.range_tombstone_bytes = self
            .range_tombstone_bytes
            .saturating_add(range_tombstone_bytes(&range));
        range_tombstone::insert_sorted(
            &mut self.range_tombstones,
            RangeTombstone {
                range,
                sequence,
                batch_index,
            },
        );
    }

    fn finish(self, sequence: Sequence) -> Option<Arc<InMemoryDelta>> {
        let estimated_bytes = self
            .memtable
            .estimated_bytes()
            .saturating_add(self.range_tombstone_bytes);
        if estimated_bytes == 0 {
            return None;
        }

        Some(Arc::new(InMemoryDelta {
            memtable: self.memtable,
            range_tombstones: Arc::new(self.range_tombstones),
            estimated_bytes,
            sequence,
        }))
    }
}

fn merge_deltas(deltas: &[Arc<InMemoryDelta>]) -> Result<Arc<InMemoryDelta>> {
    debug_assert!(!deltas.is_empty());
    let memtable = Arc::new(Memtable::default());
    let mut range_tombstones = Vec::new();
    let mut total_range_tombstone_bytes = 0_u64;
    let mut sequence = Sequence::ZERO;

    for delta in deltas {
        sequence = sequence.max(delta.sequence);

        let entries = delta
            .memtable
            .read_entries()
            .map_err(|_| lock_poisoned("delta memtable entries"))?;
        for (internal_key, value) in entries.iter() {
            memtable
                .insert(internal_key.clone(), value.clone())
                .map_err(|()| lock_poisoned("delta merge memtable entries"))?;
        }

        for tombstone in delta.range_tombstones.iter() {
            let tombstone_bytes = range_tombstone_bytes(&tombstone.range);
            total_range_tombstone_bytes =
                total_range_tombstone_bytes.saturating_add(tombstone_bytes);
            range_tombstones.push(tombstone.clone());
        }
    }
    range_tombstone::sort_tombstones(&mut range_tombstones);

    let estimated_bytes = memtable
        .estimated_bytes()
        .saturating_add(total_range_tombstone_bytes);
    Ok(Arc::new(InMemoryDelta {
        memtable,
        range_tombstones: Arc::new(range_tombstones),
        estimated_bytes,
        sequence,
    }))
}

impl LsmTree {
    #[cfg(test)]
    pub(crate) fn publish_delta_operations(
        &self,
        operations: impl IntoIterator<Item = (BatchOperation, u32)>,
        sequence: Sequence,
    ) -> Result<()> {
        self.delta_shards.publish_operations(operations, sequence)
    }

    pub(crate) fn publish_delta_operations_with_budget(
        &self,
        operations: impl IntoIterator<Item = (BatchOperation, u32)>,
        sequence: Sequence,
        max_epoch_bytes: u64,
    ) -> Result<()> {
        self.delta_shards
            .publish_operations_with_budget(operations, sequence, max_epoch_bytes)
    }

    pub(crate) fn delta_snapshot(&self) -> Result<DeltaSnapshot> {
        self.delta_shards.snapshot_all()
    }

    pub(crate) fn delta_snapshot_for_key(&self, key: &[u8]) -> Result<DeltaSnapshot> {
        self.delta_shards.snapshot_for_key(key)
    }

    pub(crate) fn delta_snapshot_for_keys<'key, I>(&self, keys: I) -> Result<DeltaSnapshot>
    where
        I: IntoIterator<Item = &'key [u8]>,
    {
        self.delta_shards.snapshot_for_keys(keys)
    }

    pub(crate) fn delta_mirror_covers(&self, read_sequence: Sequence) -> bool {
        let mirror_sequence = self.delta_mirror_sequence.load(Ordering::Acquire);
        mirror_sequence != Sequence::ZERO.get() && mirror_sequence >= read_sequence.get()
    }

    pub(crate) fn mark_delta_mirror_sequence(&self, sequence: Sequence) {
        self.delta_mirror_sequence
            .fetch_max(sequence.get(), Ordering::AcqRel);
    }

    pub(crate) fn delta_estimated_bytes(&self) -> Result<u64> {
        self.delta_shards.estimated_bytes()
    }

    #[cfg(test)]
    pub(crate) fn delta_debug_counts(&self) -> Result<(usize, usize, usize)> {
        self.delta_shards.debug_counts()
    }

    #[cfg(test)]
    pub(crate) fn delta_debug_stats(&self) -> Result<DeltaDebugStats> {
        self.delta_shards.debug_stats()
    }
}

fn stable_key_hash(key: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn range_tombstone_bytes(range: &KeyRange) -> u64 {
    bound_bytes(&range.start)
        .saturating_add(bound_bytes(&range.end))
        .saturating_add(32)
}

fn bound_bytes(bound: &std::ops::Bound<Vec<u8>>) -> u64 {
    match bound {
        std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) => {
            usize_to_u64_saturating(key.len())
        }
        std::ops::Bound::Unbounded => 0,
    }
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        bucket::DEFAULT_BUCKET_NAME,
        iterator::{Direction, Iter, ScanSelector, ScanSourceInput},
        options::BucketOptions,
        snapshot::Snapshot,
        types::{KeyRange, Sequence},
        write_batch::BatchOperation,
    };

    use super::{DELTA_EPOCH_MAX_DELTAS, LsmTree};

    #[test]
    fn delta_heads_feed_point_reads_without_active_memtable() {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new()).expect("tree builds");
        tree.publish_delta_operations(
            vec![(
                BatchOperation::Put {
                    bucket: DEFAULT_BUCKET_NAME.to_owned(),
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
                0,
            )],
            Sequence::new(1),
        )
        .expect("publish delta");

        assert_eq!(
            tree.read_visible_point(b"k", Sequence::new(1), None, None, None)
                .expect("read delta point"),
            Some(b"v".to_vec())
        );
        assert_eq!(
            tree.read_visible_point(b"k", Sequence::ZERO, None, None, None)
                .expect("old read ignores delta"),
            None
        );
    }

    #[test]
    fn delta_heads_feed_range_tombstone_reads_without_active_memtable() {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new()).expect("tree builds");
        tree.publish_delta_operations(
            vec![(
                BatchOperation::Put {
                    bucket: DEFAULT_BUCKET_NAME.to_owned(),
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
                0,
            )],
            Sequence::new(1),
        )
        .expect("publish put delta");
        tree.publish_delta_operations(
            vec![(
                BatchOperation::DeleteRange {
                    bucket: DEFAULT_BUCKET_NAME.to_owned(),
                    range: KeyRange::half_open(b"a".to_vec(), b"z".to_vec()),
                },
                0,
            )],
            Sequence::new(2),
        )
        .expect("publish range delta");

        assert_eq!(
            tree.read_visible_point(b"k", Sequence::new(1), None, None, None)
                .expect("snapshot before range tombstone"),
            Some(b"v".to_vec())
        );
        assert_eq!(
            tree.read_visible_point(b"k", Sequence::new(2), None, None, None)
                .expect("range tombstone covers delta point"),
            None
        );

        let (delta_count, point_delta_count, range_tombstone_count) =
            tree.delta_debug_counts().expect("delta counts");
        assert_eq!(delta_count, 17);
        assert_eq!(point_delta_count, 1);
        assert_eq!(range_tombstone_count, 16);
    }

    #[test]
    fn delta_heads_feed_range_scans_without_active_memtable() {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new()).expect("tree builds");
        tree.publish_delta_operations(
            vec![
                (
                    BatchOperation::Put {
                        bucket: DEFAULT_BUCKET_NAME.to_owned(),
                        key: b"a".to_vec(),
                        value: b"one".to_vec(),
                    },
                    0,
                ),
                (
                    BatchOperation::Put {
                        bucket: DEFAULT_BUCKET_NAME.to_owned(),
                        key: b"b".to_vec(),
                        value: b"two".to_vec(),
                    },
                    1,
                ),
            ],
            Sequence::new(1),
        )
        .expect("publish delta");

        let scan = tree
            .scan(
                &ScanSelector::Range(KeyRange::all()),
                Direction::Forward,
                Sequence::new(1),
                None,
            )
            .expect("scan delta sources");
        let mut iter = Iter::from_sources(
            Direction::Forward,
            ScanSourceInput {
                read_sequence: Sequence::new(1),
                read_pin: Snapshot::new(Sequence::new(1)),
                db_path: None,
                native_storage: None,
                blob_reads: None,
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        );

        let first = iter
            .next_sync()
            .expect("first row exists")
            .expect("first row reads");
        let second = iter
            .next_sync()
            .expect("second row exists")
            .expect("second row reads");
        assert_eq!((first.key, first.value), (b"a".to_vec(), b"one".to_vec()));
        assert_eq!((second.key, second.value), (b"b".to_vec(), b"two".to_vec()));
        assert!(iter.next_sync().is_none());
    }

    #[test]
    fn delta_epoch_merge_bounds_chain_and_preserves_point_reads() {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new()).expect("tree builds");
        let delta_budget = u64::try_from(DELTA_EPOCH_MAX_DELTAS).expect("delta budget fits u64");
        let write_count = delta_budget + 2;

        for sequence in 1..=write_count {
            tree.publish_delta_operations(
                vec![(
                    BatchOperation::Put {
                        bucket: DEFAULT_BUCKET_NAME.to_owned(),
                        key: b"k".to_vec(),
                        value: format!("v{sequence}").into_bytes(),
                    },
                    0,
                )],
                Sequence::new(sequence),
            )
            .expect("publish delta");
        }

        assert_eq!(
            tree.read_visible_point(b"k", Sequence::new(write_count), None, None, None)
                .expect("latest point survives merge"),
            Some(format!("v{write_count}").into_bytes())
        );
        assert_eq!(
            tree.read_visible_point(b"k", Sequence::new(3), None, None, None)
                .expect("older point survives merge"),
            Some(b"v3".to_vec())
        );

        let stats = tree.delta_debug_stats().expect("delta stats");
        assert_eq!(stats.merged_epoch_count, 1);
        assert_eq!(stats.sealed_epoch_count, 1);
        assert_eq!(stats.retired_delta_count, delta_budget);
        assert!(stats.max_shard_chain_len < DELTA_EPOCH_MAX_DELTAS);
        assert!(stats.open_epoch_bytes > 0);
    }

    #[test]
    fn delta_epoch_merge_preserves_range_tombstone_visibility() {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new()).expect("tree builds");
        tree.publish_delta_operations(
            vec![(
                BatchOperation::Put {
                    bucket: DEFAULT_BUCKET_NAME.to_owned(),
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
                0,
            )],
            Sequence::new(1),
        )
        .expect("publish put delta");

        let delete_count = DELTA_EPOCH_MAX_DELTAS;
        for index in 0..delete_count {
            let sequence = u64::try_from(index).expect("delete index fits u64") + 2;
            tree.publish_delta_operations(
                vec![(
                    BatchOperation::DeleteRange {
                        bucket: DEFAULT_BUCKET_NAME.to_owned(),
                        range: KeyRange::half_open(b"a".to_vec(), b"z".to_vec()),
                    },
                    0,
                )],
                Sequence::new(sequence),
            )
            .expect("publish range delta");
        }

        assert_eq!(
            tree.read_visible_point(b"k", Sequence::new(1), None, None, None)
                .expect("snapshot before range tombstones"),
            Some(b"v".to_vec())
        );
        assert_eq!(
            tree.read_visible_point(
                b"k",
                Sequence::new(u64::try_from(delete_count).expect("delete count fits u64") + 1),
                None,
                None,
                None
            )
            .expect("range tombstones survive merge"),
            None
        );

        let stats = tree.delta_debug_stats().expect("delta stats");
        assert!(stats.merged_epoch_count >= 1);
        assert_eq!(stats.sealed_epoch_count, stats.merged_epoch_count);
        assert!(stats.range_tombstone_count >= delete_count);
    }
}
