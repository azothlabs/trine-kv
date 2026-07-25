use std::{
    future::poll_fn,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    sync::{Arc, Condvar, Mutex, RwLock},
    task::{Poll, Waker},
};

use crate::{
    error::{Error, Result},
    memtable::Memtable,
    options::BucketOptions,
    range_tombstone::RangeTombstoneLike,
    table::Table,
    types::{KeyRange, Sequence},
};

use super::{LsmVersion, delta::DeltaShardSet};

#[derive(Debug)]
pub(crate) struct LsmTree {
    pub(crate) options: BucketOptions,
    pub(crate) active_memtable: RwLock<Arc<Memtable>>,
    pub(crate) range_tombstones: RwLock<Vec<RangeTombstone>>,
    pub(crate) range_tombstone_bytes: AtomicU64,
    pub(crate) delta_shards: DeltaShardSet,
    pub(crate) delta_mirror_sequence: AtomicU64,
    pub(crate) immutable_memtables: RwLock<Vec<ImmutableMemtable>>,
    pub(crate) immutable_memtable_count: AtomicUsize,
    pub(crate) current_version: RwLock<Arc<LsmVersion>>,
    lifecycle: Mutex<LsmLifecycle>,
    lifecycle_idle: Condvar,
}

impl LsmTree {
    pub(crate) fn new(options: BucketOptions, tables: Vec<Arc<Table>>) -> Result<Self> {
        let current_version = Arc::new(LsmVersion::new(tables)?);
        Ok(Self {
            options,
            active_memtable: RwLock::new(Arc::new(Memtable::default())),
            range_tombstones: RwLock::new(Vec::new()),
            range_tombstone_bytes: AtomicU64::new(0),
            delta_shards: DeltaShardSet::new(),
            delta_mirror_sequence: AtomicU64::new(Sequence::ZERO.get()),
            immutable_memtables: RwLock::new(Vec::new()),
            immutable_memtable_count: AtomicUsize::new(0),
            current_version: RwLock::new(current_version),
            lifecycle: Mutex::new(LsmLifecycle::default()),
            lifecycle_idle: Condvar::new(),
        })
    }

    /// Admits one write for the full prepare -> WAL -> memtable publication
    /// interval. Bucket deletion first closes this admission gate and then waits
    /// for all returned guards to drop, so no accepted WAL record can outlive
    /// the bucket recorded in the manifest.
    pub(crate) fn admit_write(self: &Arc<Self>) -> Result<LsmWriteAdmission> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| lock_poisoned("LSM lifecycle"))?;
        if lifecycle.dropping {
            return Err(Error::runtime_busy("bucket deletion is in progress"));
        }
        lifecycle.active_writes =
            lifecycle
                .active_writes
                .checked_add(1)
                .ok_or_else(|| Error::Corruption {
                    message: "LSM active write count overflow".to_owned(),
                })?;
        Ok(LsmWriteAdmission {
            tree: Arc::clone(self),
        })
    }

    pub(crate) fn begin_drop(self: &Arc<Self>) -> Result<LsmDropGuard> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| lock_poisoned("LSM lifecycle"))?;
        if lifecycle.dropping {
            return Err(Error::runtime_busy(
                "bucket deletion is already in progress",
            ));
        }
        lifecycle.dropping = true;
        Ok(LsmDropGuard {
            tree: Arc::clone(self),
            committed: false,
        })
    }

    pub(crate) fn ensure_available(&self) -> Result<()> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| lock_poisoned("LSM lifecycle"))?;
        if lifecycle.dropping {
            Err(Error::runtime_busy("bucket deletion is in progress"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn current_version(&self) -> Result<Arc<LsmVersion>> {
        self.current_version
            .read()
            .map_err(|_| lock_poisoned("LSM version"))
            .map(|version| Arc::clone(&version))
    }

    pub(crate) fn install_version(&self, version: LsmVersion) -> Result<()> {
        *self
            .current_version
            .write()
            .map_err(|_| lock_poisoned("LSM version"))? = Arc::new(version);
        Ok(())
    }

    pub(crate) fn tables_snapshot(&self) -> Result<Vec<Arc<Table>>> {
        Ok(self.current_version()?.table_handles())
    }

    pub(crate) fn l0_table_count(&self) -> Result<usize> {
        Ok(self.current_version()?.l0_table_count())
    }

    pub(crate) fn l0_has_overlapping_tables(&self) -> Result<bool> {
        Ok(self.current_version()?.l0_has_overlapping_tables())
    }

    pub(crate) fn has_immutable_memtable_fast(&self) -> bool {
        self.immutable_memtable_count.load(Ordering::Acquire) != 0
    }
}

#[derive(Debug, Default)]
struct LsmLifecycle {
    dropping: bool,
    active_writes: usize,
    drain_wakers: Vec<Waker>,
}

#[derive(Debug)]
pub(crate) struct LsmWriteAdmission {
    tree: Arc<LsmTree>,
}

impl Drop for LsmWriteAdmission {
    fn drop(&mut self) {
        let wakers = {
            let Ok(mut lifecycle) = self.tree.lifecycle.lock() else {
                return;
            };
            lifecycle.active_writes = lifecycle.active_writes.saturating_sub(1);
            if lifecycle.active_writes == 0 {
                self.tree.lifecycle_idle.notify_all();
                std::mem::take(&mut lifecycle.drain_wakers)
            } else {
                Vec::new()
            }
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

#[derive(Debug)]
pub(crate) struct LsmDropGuard {
    tree: Arc<LsmTree>,
    committed: bool,
}

impl LsmDropGuard {
    pub(crate) fn wait_for_writes_sync(&self) -> Result<()> {
        let mut lifecycle = self
            .tree
            .lifecycle
            .lock()
            .map_err(|_| lock_poisoned("LSM lifecycle"))?;
        while lifecycle.active_writes != 0 {
            lifecycle = self
                .tree
                .lifecycle_idle
                .wait(lifecycle)
                .map_err(|_| lock_poisoned("LSM lifecycle"))?;
        }
        Ok(())
    }

    pub(crate) async fn wait_for_writes(&self) -> Result<()> {
        poll_fn(|context| {
            let Ok(mut lifecycle) = self.tree.lifecycle.lock() else {
                return Poll::Ready(Err(lock_poisoned("LSM lifecycle")));
            };
            if lifecycle.active_writes == 0 {
                return Poll::Ready(Ok(()));
            }
            if !lifecycle
                .drain_wakers
                .iter()
                .any(|waker| waker.will_wake(context.waker()))
            {
                lifecycle.drain_wakers.push(context.waker().clone());
            }
            Poll::Pending
        })
        .await
    }

    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for LsmDropGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let wakers = {
            let Ok(mut lifecycle) = self.tree.lifecycle.lock() else {
                return;
            };
            lifecycle.dropping = false;
            std::mem::take(&mut lifecycle.drain_wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RangeTombstone {
    pub(crate) range: KeyRange,
    pub(crate) sequence: Sequence,
    pub(crate) batch_index: u32,
}

impl RangeTombstone {
    pub(crate) fn covers_visible_point(
        &self,
        key: &[u8],
        point_sequence: Sequence,
        point_batch_index: u32,
        read_sequence: Sequence,
    ) -> bool {
        if self.sequence > read_sequence
            || !crate::range_tombstone::key_is_in_range(key, &self.range)
        {
            return false;
        }

        self.sequence > point_sequence
            || (self.sequence == point_sequence && self.batch_index > point_batch_index)
    }
}

impl RangeTombstoneLike for RangeTombstone {
    fn range(&self) -> &KeyRange {
        &self.range
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ImmutableMemtable {
    pub(crate) memtable: Arc<Memtable>,
    pub(crate) range_tombstones: Arc<Vec<RangeTombstone>>,
    pub(crate) estimated_bytes: u64,
    pub(crate) freeze_sequence: Sequence,
}

pub(super) fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use std::sync::Arc;

    use super::LsmTree;
    use crate::options::BucketOptions;

    #[test]
    fn drop_gate_rejects_new_writes_and_drains_existing_admissions() {
        let tree =
            Arc::new(LsmTree::new(BucketOptions::default(), Vec::new()).expect("create tree"));
        let admission = tree.admit_write().expect("admit existing write");
        let drop_guard = Arc::clone(&tree).begin_drop().expect("begin drop");

        assert!(
            tree.admit_write().is_err(),
            "drop gate must reject a writer arriving after drop starts"
        );
        drop(admission);
        drop_guard
            .wait_for_writes_sync()
            .expect("existing writer drains");
        drop_guard.commit();

        assert!(
            tree.admit_write().is_err(),
            "committed drop keeps stale handles fenced permanently"
        );
    }

    #[test]
    fn aborted_drop_reopens_admission() {
        let tree =
            Arc::new(LsmTree::new(BucketOptions::default(), Vec::new()).expect("create tree"));
        drop(Arc::clone(&tree).begin_drop().expect("begin drop"));
        tree.admit_write()
            .expect("failed durable drop must restore write admission");
    }
}
