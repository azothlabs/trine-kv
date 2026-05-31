use std::{
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    sync::{Arc, RwLock},
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
        })
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
