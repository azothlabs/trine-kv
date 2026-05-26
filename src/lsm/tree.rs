use std::sync::{Arc, RwLock};

use crate::{
    error::{Error, Result},
    memtable::Memtable,
    options::KeyspaceOptions,
    range_tombstone::RangeTombstoneLike,
    table::Table,
    types::{KeyRange, Sequence},
};

use super::LsmVersion;

#[derive(Debug)]
pub(crate) struct LsmTree {
    pub(crate) options: KeyspaceOptions,
    pub(crate) active_memtable: RwLock<Arc<Memtable>>,
    pub(crate) range_tombstones: RwLock<Vec<RangeTombstone>>,
    pub(crate) immutable_memtables: RwLock<Vec<ImmutableMemtable>>,
    pub(crate) current_version: RwLock<Arc<LsmVersion>>,
}

impl LsmTree {
    pub(crate) fn new(options: KeyspaceOptions, tables: Vec<Arc<Table>>) -> Result<Self> {
        let current_version = Arc::new(LsmVersion::new(tables)?);
        Ok(Self {
            options,
            active_memtable: RwLock::new(Arc::new(Memtable::default())),
            range_tombstones: RwLock::new(Vec::new()),
            immutable_memtables: RwLock::new(Vec::new()),
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
    pub(crate) freeze_sequence: Sequence,
}

pub(super) fn lock_poisoned(lock_name: &'static str) -> Error {
    Error::Corruption {
        message: format!("{lock_name} lock poisoned"),
    }
}
