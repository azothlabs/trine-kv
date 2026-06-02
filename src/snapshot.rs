use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use crate::{
    bucket::Bucket,
    error::Result,
    iterator::{Iter, LazyIter},
    types::{KeyRange, Sequence, Value},
};

#[derive(Debug, Default)]
pub(crate) struct SnapshotTracker {
    active: Mutex<BTreeMap<Sequence, usize>>,
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
struct SnapshotPin {
    tracker: Arc<SnapshotTracker>,
}

/// Repeatable-read handle pinned to a committed sequence.
#[derive(Debug)]
pub struct Snapshot {
    read_sequence: Sequence,
    pin: Option<SnapshotPin>,
}

impl Snapshot {
    /// Creates an untracked snapshot at `read_sequence`.
    #[must_use]
    pub const fn new(read_sequence: Sequence) -> Self {
        Self {
            read_sequence,
            pin: None,
        }
    }

    /// Returns the sequence visible through this snapshot.
    #[must_use]
    pub const fn read_sequence(&self) -> Sequence {
        self.read_sequence
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
