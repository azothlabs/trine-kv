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

#[derive(Debug)]
pub(crate) struct DeltaShardSet {
    shards: Vec<DeltaShard>,
}

#[derive(Debug, Default)]
struct DeltaShard {
    deltas: RwLock<Vec<Arc<InMemoryDelta>>>,
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

    fn publish_operations(
        &self,
        operations: impl IntoIterator<Item = (BatchOperation, u32)>,
        sequence: Sequence,
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
                shard.publish(delta)?;
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

    fn shard_index_for_key(&self, key: &[u8]) -> usize {
        debug_assert!(!self.shards.is_empty());
        let shard_count = u64::try_from(self.shards.len()).expect("delta shard count fits u64");
        let index = stable_key_hash(key) % shard_count;
        usize::try_from(index).expect("delta shard index fits usize")
    }

    #[cfg(test)]
    fn debug_counts(&self) -> Result<(usize, usize, usize)> {
        let mut delta_count = 0_usize;
        let mut point_delta_count = 0_usize;
        let mut range_tombstone_count = 0_usize;

        for shard in &self.shards {
            let deltas = shard
                .deltas
                .read()
                .map_err(|_| lock_poisoned("delta shard"))?;
            delta_count = delta_count.saturating_add(deltas.len());
            for delta in deltas.iter() {
                if delta.memtable.estimated_bytes() != 0 {
                    point_delta_count = point_delta_count.saturating_add(1);
                }
                range_tombstone_count =
                    range_tombstone_count.saturating_add(delta.range_tombstones.len());
            }
        }

        Ok((delta_count, point_delta_count, range_tombstone_count))
    }
}

impl DeltaShard {
    fn publish(&self, delta: Arc<InMemoryDelta>) -> Result<()> {
        self.deltas
            .write()
            .map_err(|_| lock_poisoned("delta shard"))?
            .push(delta);
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<Arc<InMemoryDelta>>> {
        self.deltas
            .read()
            .map_err(|_| lock_poisoned("delta shard"))
            .map(|deltas| deltas.clone())
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

impl LsmTree {
    pub(crate) fn publish_delta_operations(
        &self,
        operations: impl IntoIterator<Item = (BatchOperation, u32)>,
        sequence: Sequence,
    ) -> Result<()> {
        self.delta_shards.publish_operations(operations, sequence)
    }

    pub(crate) fn delta_snapshot(&self) -> Result<DeltaSnapshot> {
        self.delta_shards.snapshot_all()
    }

    pub(crate) fn delta_snapshot_for_key(&self, key: &[u8]) -> Result<DeltaSnapshot> {
        self.delta_shards.snapshot_for_key(key)
    }

    pub(crate) fn delta_mirror_covers(&self, read_sequence: Sequence) -> bool {
        self.delta_mirror_sequence.load(Ordering::Acquire) >= read_sequence.get()
    }

    pub(crate) fn mark_delta_mirror_sequence(&self, sequence: Sequence) {
        self.delta_mirror_sequence
            .fetch_max(sequence.get(), Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(crate) fn delta_debug_counts(&self) -> Result<(usize, usize, usize)> {
        self.delta_shards.debug_counts()
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
        iterator::{Direction, Iter, ScanSelector},
        options::BucketOptions,
        snapshot::Snapshot,
        types::{KeyRange, Sequence},
        write_batch::BatchOperation,
    };

    use super::LsmTree;

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
            Sequence::new(1),
            Snapshot::new(Sequence::new(1)),
            None,
            None,
            scan.range_tombstones,
            scan.sources,
        );

        let first = iter
            .next()
            .expect("first row exists")
            .expect("first row reads");
        let second = iter
            .next()
            .expect("second row exists")
            .expect("second row reads");
        assert_eq!((first.key, first.value), (b"a".to_vec(), b"one".to_vec()));
        assert_eq!((second.key, second.value), (b"b".to_vec(), b"two".to_vec()));
        assert!(iter.next().is_none());
    }
}
