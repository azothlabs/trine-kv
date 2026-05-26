use std::{ops::Bound, sync::Arc};

use crate::{
    cache,
    error::Result,
    iterator::{Direction, RecordSource, ScanRangeTombstone, ScanSelector, prefix_successor},
    range_tombstone::RangeTombstoneIndex,
    types::KeyRange,
};

use super::{
    LsmVersion,
    tree::{LsmTree, RangeTombstone, lock_poisoned},
};

#[derive(Debug)]
pub(crate) struct LsmScan {
    pub(crate) range_tombstones: Vec<ScanRangeTombstone>,
    pub(crate) sources: Vec<RecordSource>,
}

impl LsmTree {
    pub(crate) fn scan(
        &self,
        selector: &ScanSelector,
        direction: Direction,
        block_cache: Option<&Arc<cache::BlockCache>>,
    ) -> Result<LsmScan> {
        let version = self.current_version()?;
        Ok(LsmScan {
            range_tombstones: self.scan_range_tombstones(&version, selector)?,
            sources: self.scan_sources(&version, selector, direction, block_cache)?,
        })
    }

    fn scan_sources(
        &self,
        version: &LsmVersion,
        selector: &ScanSelector,
        direction: Direction,
        block_cache: Option<&Arc<cache::BlockCache>>,
    ) -> Result<Vec<RecordSource>> {
        let active_memtable = self
            .active_memtable
            .read()
            .map_err(|_| lock_poisoned("active memtable"))?
            .clone();

        let mut sources = vec![RecordSource::memtable(
            active_memtable,
            selector.clone(),
            direction,
        )];
        let immutable_memtables = self
            .immutable_memtables
            .read()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?
            .clone();
        sources.extend(immutable_memtables.into_iter().map(|immutable| {
            RecordSource::memtable(immutable.memtable, selector.clone(), direction)
        }));

        for table in version.table_handles() {
            if let Some(prefix) = selector.prefix() {
                if !table.may_contain_prefix(prefix, &self.options.prefix_extractor) {
                    continue;
                }
            }

            let cursor = table.point_cursor(
                selector.clone(),
                self.options.prefix_extractor.clone(),
                direction,
                self.options.index_search_policy,
                block_cache.cloned(),
            );
            sources.push(RecordSource::table(cursor));
        }

        Ok(sources)
    }

    fn scan_range_tombstones(
        &self,
        version: &LsmVersion,
        selector: &ScanSelector,
    ) -> Result<Vec<ScanRangeTombstone>> {
        let range = selector_query_range(selector);
        let memtable_tombstones = self.memtable_range_tombstones()?;
        let mut tombstones = memtable_tombstones
            .overlapping_range(&range)
            .cloned()
            .collect::<Vec<_>>();

        for table in version.table_handles() {
            tombstones.extend(
                table
                    .range_tombstones_overlapping_range(&range)?
                    .into_iter()
                    .map(|tombstone| RangeTombstone {
                        range: tombstone.range,
                        sequence: tombstone.sequence,
                        batch_index: tombstone.batch_index,
                    }),
            );
        }

        Ok(RangeTombstoneIndex::new(tombstones)
            .all()
            .iter()
            .cloned()
            .map(|tombstone| {
                ScanRangeTombstone::new(tombstone.range, tombstone.sequence, tombstone.batch_index)
            })
            .collect())
    }
}

fn selector_query_range(selector: &ScanSelector) -> KeyRange {
    match selector {
        ScanSelector::Range(range) => range.clone(),
        ScanSelector::Prefix(prefix) => KeyRange {
            start: Bound::Included(prefix.clone()),
            end: prefix_successor(prefix).map_or(Bound::Unbounded, Bound::Excluded),
        },
    }
}
