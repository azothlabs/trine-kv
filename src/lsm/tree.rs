use std::{
    cmp::Ordering as CmpOrdering,
    sync::{Arc, RwLock},
};

use crate::{
    memtable::Memtable,
    options::KeyspaceOptions,
    range_tombstone::RangeTombstoneLike,
    table::Table,
    types::{KeyRange, Sequence},
};

#[derive(Debug)]
pub(crate) struct LsmTree {
    pub(crate) options: KeyspaceOptions,
    pub(crate) active_memtable: RwLock<Arc<Memtable>>,
    pub(crate) range_tombstones: RwLock<Vec<RangeTombstone>>,
    pub(crate) immutable_memtables: RwLock<Vec<ImmutableMemtable>>,
    pub(crate) tables: RwLock<Vec<Arc<Table>>>,
}

impl LsmTree {
    pub(crate) fn new(options: KeyspaceOptions, mut tables: Vec<Arc<Table>>) -> Self {
        Self::sort_tables_for_reads(&mut tables);
        Self {
            options,
            active_memtable: RwLock::new(Arc::new(Memtable::default())),
            range_tombstones: RwLock::new(Vec::new()),
            immutable_memtables: RwLock::new(Vec::new()),
            tables: RwLock::new(tables),
        }
    }

    pub(crate) fn sort_tables_for_reads(tables: &mut [Arc<Table>]) {
        // Keep table handles in level order. Reads still merge candidate
        // records defensively, but this invariant gives optimized point reads
        // and compaction picking one stable rule to share.
        tables.sort_by(compare_tables_for_reads);
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

fn compare_tables_for_reads(left: &Arc<Table>, right: &Arc<Table>) -> CmpOrdering {
    let left = left.properties();
    let right = right.properties();
    left.level
        .cmp(&right.level)
        .then_with(|| right.largest_sequence.cmp(&left.largest_sequence))
        .then_with(|| right.id.cmp(&left.id))
}
