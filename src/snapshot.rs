use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::{
    bucket::Bucket,
    error::{Error, Result},
    invariants::{SnapshotReachability, classify_snapshot_reachability},
    iterator::{Iter, LazyIter},
    types::{KeyRange, ReadVersion, Sequence, Value},
};

#[derive(Debug, Default)]
pub(crate) struct SnapshotTracker {
    active: Mutex<BTreeMap<Sequence, usize>>,
    compaction_floors: Mutex<BTreeMap<Sequence, usize>>,
}

impl SnapshotTracker {
    pub(crate) fn pinned_snapshot(self: &Arc<Self>, read_sequence: Sequence) -> Snapshot {
        self.pin(read_sequence);
        Snapshot {
            read_sequence,
            pin: Some(SnapshotPin {
                tracker: Arc::clone(self),
            }),
        }
    }

    pub(crate) fn pinned_retained_snapshot(
        self: &Arc<Self>,
        read_sequence: Sequence,
        latest_sequence: Sequence,
        retained_floor: Sequence,
    ) -> Result<Snapshot> {
        let compaction_floors = self
            .compaction_floors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let compaction_floor = compaction_floors.keys().next_back().copied();
        if compaction_floor.is_some_and(|floor| read_sequence < floor) {
            return Err(Error::runtime_busy(
                "requested snapshot is older than an admitted compaction; retry after maintenance",
            ));
        }
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let oldest_retained = active
            .keys()
            .next()
            .copied()
            .unwrap_or(latest_sequence)
            .min(retained_floor);
        let requested = ReadVersion::from_sequence(read_sequence);
        match classify_snapshot_reachability(
            read_sequence.get(),
            oldest_retained.get(),
            latest_sequence.get(),
        ) {
            SnapshotReachability::TooNew => {
                return Err(Error::ReadVersionTooNew {
                    requested,
                    latest: ReadVersion::from_sequence(latest_sequence),
                });
            }
            SnapshotReachability::TooOld => {
                return Err(Error::ReadVersionExpired {
                    requested,
                    oldest_retained: ReadVersion::from_sequence(oldest_retained),
                });
            }
            SnapshotReachability::Reachable => {}
        }

        *active.entry(read_sequence).or_default() += 1;
        drop(active);
        drop(compaction_floors);
        Ok(Snapshot {
            read_sequence,
            pin: Some(SnapshotPin {
                tracker: Arc::clone(self),
            }),
        })
    }

    pub(crate) fn oldest_active_or(&self, fallback: Sequence) -> Sequence {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .next()
            .copied()
            .unwrap_or(fallback)
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .sum()
    }

    pub(crate) fn begin_compaction(
        self: &Arc<Self>,
        retained_floor_without_snapshots: Sequence,
    ) -> (Sequence, CompactionSnapshotGuard) {
        let mut floors = self
            .compaction_floors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Snapshot admission holds the floor lock until it has inserted into
        // `active`; taking the locks in the same order makes admission atomic.
        let active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let floor = active
            .keys()
            .next()
            .copied()
            .unwrap_or(retained_floor_without_snapshots)
            .min(retained_floor_without_snapshots);
        *floors.entry(floor).or_default() += 1;
        drop(active);
        drop(floors);
        (
            floor,
            CompactionSnapshotGuard {
                tracker: Arc::clone(self),
                floor,
            },
        )
    }

    fn pin(&self, read_sequence: Sequence) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active.entry(read_sequence).or_default() += 1;
    }

    fn unpin(&self, read_sequence: Sequence) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = active.get_mut(&read_sequence) {
            *count -= 1;
            if *count == 0 {
                active.remove(&read_sequence);
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct CompactionSnapshotGuard {
    tracker: Arc<SnapshotTracker>,
    floor: Sequence,
}

impl Drop for CompactionSnapshotGuard {
    fn drop(&mut self) {
        let mut floors = self
            .tracker
            .compaction_floors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = floors.get_mut(&self.floor) {
            if *count <= 1 {
                floors.remove(&self.floor);
            } else {
                *count -= 1;
            }
        }
    }
}

#[derive(Debug)]
struct SnapshotPin {
    tracker: Arc<SnapshotTracker>,
}

/// Repeatable-read handle pinned to a committed read version.
#[derive(Debug)]
pub struct Snapshot {
    read_sequence: Sequence,
    pin: Option<SnapshotPin>,
}

impl Snapshot {
    #[must_use]
    pub(crate) const fn new(read_sequence: Sequence) -> Self {
        Self {
            read_sequence,
            pin: None,
        }
    }

    pub(crate) fn read_sequence_for(
        &self,
        expected_tracker: &Arc<SnapshotTracker>,
    ) -> Result<Sequence> {
        let belongs_to_database = self
            .pin
            .as_ref()
            .is_some_and(|pin| Arc::ptr_eq(&pin.tracker, expected_tracker));
        if !belongs_to_database {
            return Err(Error::SnapshotDatabaseMismatch);
        }
        Ok(self.read_sequence)
    }

    /// Returns the public read version visible through this snapshot.
    ///
    /// All point, range, and prefix reads made through this snapshot use this
    /// same database-wide read boundary, even when newer writes commit before
    /// the reads run. The snapshot keeps that retained version pinned until all
    /// snapshot clones are dropped.
    #[must_use]
    pub const fn read_version(&self) -> ReadVersion {
        ReadVersion::from_sequence(self.read_sequence)
    }

    #[must_use]
    pub(crate) fn is_pinned(&self) -> bool {
        self.pin.is_some()
    }

    /// Synchronously reads `key` from `bucket` at this snapshot.
    pub fn get_sync(&self, bucket: &Bucket, key: &[u8]) -> Result<Option<Value>> {
        bucket.get_at_sync(self, key)
    }

    /// Synchronously scans `range` forward at this snapshot.
    pub fn range_sync(&self, bucket: &Bucket, range: &KeyRange) -> Result<Iter> {
        bucket.range_at_sync(self, range)
    }

    /// Synchronously scans `range` forward with lazy value reads at this snapshot.
    pub fn range_lazy_sync(&self, bucket: &Bucket, range: &KeyRange) -> Result<LazyIter> {
        bucket.range_lazy_at_sync(self, range)
    }

    /// Synchronously scans `range` in reverse at this snapshot.
    pub fn range_reverse_sync(&self, bucket: &Bucket, range: &KeyRange) -> Result<Iter> {
        bucket.range_reverse_at_sync(self, range)
    }

    /// Synchronously scans `range` in reverse with lazy value reads at this snapshot.
    pub fn range_lazy_reverse_sync(&self, bucket: &Bucket, range: &KeyRange) -> Result<LazyIter> {
        bucket.range_lazy_reverse_at_sync(self, range)
    }

    /// Synchronously scans keys beginning with `prefix` at this snapshot.
    pub fn prefix_sync(&self, bucket: &Bucket, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        bucket.prefix_at_sync(self, prefix)
    }

    /// Synchronously scans keys beginning with `prefix` with lazy value reads.
    pub fn prefix_lazy_sync(
        &self,
        bucket: &Bucket,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        bucket.prefix_lazy_at_sync(self, prefix)
    }

    /// Synchronously scans keys beginning with `prefix` in reverse.
    pub fn prefix_reverse_sync(&self, bucket: &Bucket, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        bucket.prefix_reverse_at_sync(self, prefix)
    }

    /// Synchronously scans keys beginning with `prefix` in reverse with lazy value reads.
    pub fn prefix_lazy_reverse_sync(
        &self,
        bucket: &Bucket,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        bucket.prefix_lazy_reverse_at_sync(self, prefix)
    }

    /// Asynchronously reads `key` from `bucket` at this snapshot.
    pub async fn get(&self, bucket: &Bucket, key: &[u8]) -> Result<Option<Value>> {
        bucket.get_at(self, key).await
    }

    /// Asynchronously scans `range` forward at this snapshot.
    pub async fn range(&self, bucket: &Bucket, range: &KeyRange) -> Result<Iter> {
        bucket.range_at(self, range).await
    }

    /// Asynchronously scans `range` forward with lazy value reads at this snapshot.
    pub async fn range_lazy(&self, bucket: &Bucket, range: &KeyRange) -> Result<LazyIter> {
        bucket.range_lazy_at(self, range).await
    }

    /// Asynchronously scans `range` in reverse at this snapshot.
    pub async fn range_reverse(&self, bucket: &Bucket, range: &KeyRange) -> Result<Iter> {
        bucket.range_reverse_at(self, range).await
    }

    /// Asynchronously scans `range` in reverse with lazy value reads at this snapshot.
    pub async fn range_lazy_reverse(&self, bucket: &Bucket, range: &KeyRange) -> Result<LazyIter> {
        bucket.range_lazy_reverse_at(self, range).await
    }

    /// Asynchronously scans keys beginning with `prefix` at this snapshot.
    pub async fn prefix(&self, bucket: &Bucket, prefix: impl Into<Vec<u8>>) -> Result<Iter> {
        bucket.prefix_at(self, prefix).await
    }

    /// Asynchronously scans keys beginning with `prefix` with lazy value reads.
    pub async fn prefix_lazy(
        &self,
        bucket: &Bucket,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        bucket.prefix_lazy_at(self, prefix).await
    }

    /// Asynchronously scans keys beginning with `prefix` in reverse.
    pub async fn prefix_reverse(
        &self,
        bucket: &Bucket,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<Iter> {
        bucket.prefix_reverse_at(self, prefix).await
    }

    /// Asynchronously scans keys beginning with `prefix` in reverse with lazy value reads.
    pub async fn prefix_lazy_reverse(
        &self,
        bucket: &Bucket,
        prefix: impl Into<Vec<u8>>,
    ) -> Result<LazyIter> {
        bucket.prefix_lazy_reverse_at(self, prefix).await
    }
}

impl Clone for Snapshot {
    fn clone(&self) -> Self {
        if let Some(pin) = &self.pin {
            pin.tracker.pin(self.read_sequence);
            Self {
                read_sequence: self.read_sequence,
                pin: Some(SnapshotPin {
                    tracker: Arc::clone(&pin.tracker),
                }),
            }
        } else {
            Self::new(self.read_sequence)
        }
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        if let Some(pin) = &self.pin {
            pin.tracker.unpin(self.read_sequence);
        }
    }
}

impl PartialEq for Snapshot {
    fn eq(&self, other: &Self) -> bool {
        self.read_sequence == other.read_sequence
    }
}

impl Eq for Snapshot {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_compaction_blocks_new_older_snapshot_until_install_finishes() {
        let tracker = Arc::new(SnapshotTracker::default());
        let (floor, guard) = tracker.begin_compaction(Sequence::new(10));
        assert_eq!(floor, Sequence::new(10));

        let error = tracker
            .pinned_retained_snapshot(Sequence::new(5), Sequence::new(20), Sequence::new(0))
            .expect_err("older snapshot cannot enter an admitted compaction");
        assert!(matches!(error, Error::RuntimeBusy { .. }));

        drop(guard);
        tracker
            .pinned_retained_snapshot(Sequence::new(5), Sequence::new(20), Sequence::new(0))
            .expect("snapshot admission resumes after compaction guard drops");
    }

    #[test]
    fn already_pinned_snapshot_lowers_new_compaction_floor() {
        let tracker = Arc::new(SnapshotTracker::default());
        let snapshot = tracker.pinned_snapshot(Sequence::new(4));
        let (floor, _guard) = tracker.begin_compaction(Sequence::new(10));
        assert_eq!(floor, Sequence::new(4));
        drop(snapshot);
    }
}
