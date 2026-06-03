use std::{
    ops::Bound,
    sync::{Arc, atomic::Ordering},
};

use crate::{
    cache,
    error::Result,
    iterator::{Direction, RecordSource, ScanRangeTombstone, ScanSelector, prefix_successor},
    memtable::Memtable,
    range_tombstone::RangeTombstoneIndex,
    types::{KeyRange, Sequence},
};

use super::{
    LsmVersion,
    delta::DeltaSnapshot,
    tree::{ImmutableMemtable, LsmTree, RangeTombstone, lock_poisoned},
};

#[derive(Debug)]
pub(crate) struct LsmScan {
    pub(crate) range_tombstones: Vec<ScanRangeTombstone>,
    pub(crate) sources: Vec<RecordSource>,
}

#[derive(Debug, Clone)]
struct LsmScanSourceSnapshot {
    delta_snapshot: DeltaSnapshot,
    active_memtable: Arc<Memtable>,
    active_range_tombstones: Vec<RangeTombstone>,
    immutable_memtables: Vec<ImmutableMemtable>,
}

impl LsmTree {
    pub(crate) fn scan(
        &self,
        selector: &ScanSelector,
        direction: Direction,
        read_sequence: Sequence,
        block_cache: Option<&Arc<cache::BlockCache>>,
    ) -> Result<LsmScan> {
        let source_snapshot = self.scan_source_snapshot(read_sequence)?;
        let version = self.current_version()?;
        Ok(LsmScan {
            range_tombstones: Self::scan_range_tombstones(&source_snapshot, &version, selector)?,
            sources: self.scan_sources(
                &source_snapshot,
                &version,
                selector,
                direction,
                block_cache,
            ),
        })
    }

    pub(crate) async fn scan_async(
        &self,
        selector: &ScanSelector,
        direction: Direction,
        read_sequence: Sequence,
        block_cache: Option<&Arc<cache::BlockCache>>,
    ) -> Result<LsmScan> {
        let source_snapshot = self.scan_source_snapshot(read_sequence)?;
        let version = self.current_version()?;
        Ok(LsmScan {
            range_tombstones: Self::scan_range_tombstones_async(
                &source_snapshot,
                &version,
                selector,
            )
            .await?,
            sources: self.scan_sources(
                &source_snapshot,
                &version,
                selector,
                direction,
                block_cache,
            ),
        })
    }

    fn scan_source_snapshot(&self, read_sequence: Sequence) -> Result<LsmScanSourceSnapshot> {
        let delta_snapshot = if self.delta_mirror_covers(read_sequence) {
            DeltaSnapshot::default()
        } else {
            self.delta_snapshot()?
        };

        // Capture memtable sources before the table version. Flush publishes
        // the new L0 table before removing the immutable memtable; this order
        // can see both copies, but it cannot miss the flushed records.
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

        Ok(LsmScanSourceSnapshot {
            delta_snapshot,
            active_memtable,
            active_range_tombstones,
            immutable_memtables,
        })
    }

    fn scan_sources(
        &self,
        source_snapshot: &LsmScanSourceSnapshot,
        version: &LsmVersion,
        selector: &ScanSelector,
        direction: Direction,
        block_cache: Option<&Arc<cache::BlockCache>>,
    ) -> Vec<RecordSource> {
        let mut sources = Vec::new();
        for delta in source_snapshot.delta_snapshot.deltas() {
            if delta.estimated_bytes == 0 || delta.memtable.estimated_bytes() == 0 {
                continue;
            }
            sources.push(RecordSource::memtable(
                Arc::clone(&delta.memtable),
                selector.clone(),
                direction,
            ));
        }

        sources.push(RecordSource::memtable(
            Arc::clone(&source_snapshot.active_memtable),
            selector.clone(),
            direction,
        ));
        sources.extend(source_snapshot.immutable_memtables.iter().map(|immutable| {
            RecordSource::memtable(Arc::clone(&immutable.memtable), selector.clone(), direction)
        }));

        let query_range = selector_query_range(selector);
        for table in version.range_scan_tables(&query_range) {
            if let Some(prefix) = selector.prefix() {
                table.record_prefix_table_probe();
                if !table.may_contain_prefix(prefix, &self.options.prefix_extractor) {
                    table.record_prefix_filter_miss();
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

        sources
    }

    fn scan_range_tombstones(
        source_snapshot: &LsmScanSourceSnapshot,
        version: &LsmVersion,
        selector: &ScanSelector,
    ) -> Result<Vec<ScanRangeTombstone>> {
        let range = selector_query_range(selector);
        let mut tombstones = source_snapshot
            .memtable_range_tombstones()
            .overlapping_range(&range)
            .cloned()
            .collect::<Vec<_>>();

        for table in version.range_scan_tables(&range) {
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

    async fn scan_range_tombstones_async(
        source_snapshot: &LsmScanSourceSnapshot,
        version: &LsmVersion,
        selector: &ScanSelector,
    ) -> Result<Vec<ScanRangeTombstone>> {
        let range = selector_query_range(selector);
        let mut tombstones = source_snapshot
            .memtable_range_tombstones()
            .overlapping_range(&range)
            .cloned()
            .collect::<Vec<_>>();

        for table in version.range_scan_tables(&range) {
            tombstones.extend(
                table
                    .range_tombstones_overlapping_range_async(&range)
                    .await?
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

impl LsmScanSourceSnapshot {
    fn memtable_range_tombstones(&self) -> RangeTombstoneIndex<RangeTombstone> {
        let mut tombstones = Vec::new();
        for delta in self.delta_snapshot.deltas() {
            tombstones.extend(delta.range_tombstones.iter().cloned());
        }
        tombstones.extend(self.active_range_tombstones.clone());
        for immutable in &self.immutable_memtables {
            tombstones.extend(immutable.range_tombstones.iter().cloned());
        }
        RangeTombstoneIndex::new(tombstones)
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use crate::{
        error::Result,
        iterator::{Iter, ScanSourceInput},
        options::BucketOptions,
        range_tombstone::RangeTombstoneLike,
        snapshot::Snapshot,
        types::{KeyRange, Sequence},
        write_batch::BatchOperation,
    };

    use super::*;

    #[test]
    fn scan_snapshot_keeps_flushed_immutable_point_source_after_queue_removal() -> Result<()> {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new())?;
        let read_sequence = Sequence::new(1);

        tree.apply_operation(
            BatchOperation::Put {
                bucket: "default".to_owned(),
                key: b"a".to_vec(),
                value: b"v1".to_vec(),
            },
            read_sequence,
            0,
        )?;
        assert!(tree.freeze_active_memtable(read_sequence)?);

        let selector = ScanSelector::Range(KeyRange::all());
        let source_snapshot = tree.scan_source_snapshot(read_sequence)?;
        remove_immutable_queue(&tree);

        let scan = scan_from_snapshot(&tree, &source_snapshot, &selector)?;
        let mut iter = Iter::from_sources(
            Direction::Forward,
            ScanSourceInput {
                read_sequence,
                read_pin: Snapshot::new(read_sequence),
                db_path: None,
                native_storage: None,
                blob_reads: None,
                range_tombstones: scan.range_tombstones,
                sources: scan.sources,
            },
        );

        let row = std::iter::Iterator::next(&mut iter)
            .transpose()?
            .expect("scan should keep the pre-removal immutable source");
        assert_eq!(row.key, b"a".to_vec());
        assert_eq!(row.value, b"v1".to_vec());
        assert!(std::iter::Iterator::next(&mut iter).transpose()?.is_none());

        Ok(())
    }

    #[test]
    fn scan_snapshot_keeps_flushed_immutable_range_tombstones_after_queue_removal() -> Result<()> {
        let tree = LsmTree::new(BucketOptions::default(), Vec::new())?;
        let read_sequence = Sequence::new(1);

        tree.apply_operation(
            BatchOperation::DeleteRange {
                bucket: "default".to_owned(),
                range: KeyRange::half_open(b"a", b"z"),
            },
            read_sequence,
            0,
        )?;
        assert!(tree.freeze_active_memtable(read_sequence)?);

        let selector = ScanSelector::Range(KeyRange::all());
        let source_snapshot = tree.scan_source_snapshot(read_sequence)?;
        remove_immutable_queue(&tree);

        let scan = scan_from_snapshot(&tree, &source_snapshot, &selector)?;
        assert_eq!(scan.range_tombstones.len(), 1);
        assert_eq!(
            scan.range_tombstones[0].range(),
            &KeyRange::half_open(b"a", b"z")
        );

        Ok(())
    }

    fn scan_from_snapshot(
        tree: &LsmTree,
        source_snapshot: &LsmScanSourceSnapshot,
        selector: &ScanSelector,
    ) -> Result<LsmScan> {
        let version = tree.current_version()?;
        Ok(LsmScan {
            range_tombstones: LsmTree::scan_range_tombstones(source_snapshot, &version, selector)?,
            sources: tree.scan_sources(
                source_snapshot,
                &version,
                selector,
                Direction::Forward,
                None,
            ),
        })
    }

    fn remove_immutable_queue(tree: &LsmTree) {
        tree.immutable_memtables
            .write()
            .expect("immutable memtable queue lock is not poisoned")
            .clear();
        tree.immutable_memtable_count.store(0, Ordering::Release);
    }
}
