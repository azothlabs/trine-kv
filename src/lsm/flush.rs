use std::sync::Arc;

use crate::{
    blob::ValueRef,
    error::{Error, Result},
    internal_key::InternalKey,
    options::BucketOptions,
    table::{self, Table, TableRangeTombstone},
    types::Sequence,
};

use super::tree::{LsmTree, lock_poisoned};

#[derive(Debug)]
pub(crate) struct FlushInput {
    pub(crate) memtable: Arc<crate::memtable::Memtable>,
    pub(crate) freeze_sequence: Sequence,
    pub(crate) table_level: table::TableLevel,
    pub(crate) table_options: table::TableWriteOptions,
    pub(crate) point_records: Vec<(InternalKey, Option<ValueRef>)>,
    pub(crate) range_tombstones: Vec<TableRangeTombstone>,
}

impl LsmTree {
    /// Returns the oldest sequence that would still need WAL replay after the
    /// supplied immutable memtables have been durably published.
    ///
    /// Callers serialize this snapshot with memtable publication. Scanning the
    /// actual records (rather than using each immutable's freeze high-water
    /// mark) is required because a bucket can be idle across many global
    /// commits and therefore contain sequences far below its freeze boundary.
    pub(crate) fn oldest_unflushed_sequence_excluding(
        &self,
        flushed_memtables: &[Arc<crate::memtable::Memtable>],
    ) -> Result<Option<Sequence>> {
        let mut oldest = None;

        let active_memtable = self
            .active_memtable
            .read()
            .map_err(|_| lock_poisoned("active memtable"))?
            .clone();
        update_oldest_point_sequence(&mut oldest, &active_memtable)?;

        let active_tombstones = self
            .range_tombstones
            .read()
            .map_err(|_| lock_poisoned("range tombstones"))?;
        for tombstone in active_tombstones.iter() {
            update_oldest_sequence(&mut oldest, tombstone.sequence);
        }
        drop(active_tombstones);

        let immutable_memtables = self
            .immutable_memtables
            .read()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?;
        for immutable in immutable_memtables.iter() {
            if flushed_memtables
                .iter()
                .any(|flushed| Arc::ptr_eq(flushed, &immutable.memtable))
            {
                continue;
            }
            update_oldest_point_sequence(&mut oldest, &immutable.memtable)?;
            for tombstone in immutable.range_tombstones.iter() {
                update_oldest_sequence(&mut oldest, tombstone.sequence);
            }
        }

        Ok(oldest)
    }

    pub(crate) fn prepare_flush_inputs(&self) -> Result<Vec<FlushInput>> {
        let immutable_memtables = self
            .immutable_memtables
            .read()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?
            .clone();
        let mut inputs = Vec::new();

        for immutable in immutable_memtables {
            let point_records = {
                let entries = immutable
                    .memtable
                    .read_entries()
                    .map_err(|_| lock_poisoned("memtable entries"))?;
                entries
                    .iter()
                    .map(|(internal_key, value)| (internal_key.clone(), value.clone()))
                    .collect::<Vec<_>>()
            };
            let range_tombstones = immutable
                .range_tombstones
                .iter()
                .map(|tombstone| TableRangeTombstone {
                    range: tombstone.range.clone(),
                    sequence: tombstone.sequence,
                    batch_index: tombstone.batch_index,
                })
                .collect::<Vec<_>>();

            if point_records.is_empty() && range_tombstones.is_empty() {
                continue;
            }

            inputs.push(FlushInput {
                memtable: Arc::clone(&immutable.memtable),
                freeze_sequence: immutable.freeze_sequence,
                table_level: table::TableLevel::ZERO,
                table_options: table_write_options(&self.options),
                point_records,
                range_tombstones,
            });
        }

        Ok(inputs)
    }

    pub(crate) fn install_flush(&self, input: &FlushInput, table: Arc<Table>) -> Result<()> {
        let version = self.current_version()?;
        let version = version.with_added_l0_table(table)?;
        self.install_version(version)?;

        // Publish the L0 table before removing the immutable memtable. A
        // reader that starts between the two swaps may see both copies, but it
        // cannot miss committed data.
        let mut immutable_memtables = self
            .immutable_memtables
            .write()
            .map_err(|_| lock_poisoned("immutable memtable queue"))?;
        let Some(position) = immutable_memtables.iter().position(|immutable| {
            immutable.freeze_sequence == input.freeze_sequence
                && Arc::ptr_eq(&immutable.memtable, &input.memtable)
        }) else {
            return Err(Error::Corruption {
                message: "flushed immutable memtable is no longer queued".to_owned(),
            });
        };
        immutable_memtables.remove(position);
        self.immutable_memtable_count
            .fetch_sub(1, std::sync::atomic::Ordering::Release);

        Ok(())
    }
}

fn update_oldest_point_sequence(
    oldest: &mut Option<Sequence>,
    memtable: &crate::memtable::Memtable,
) -> Result<()> {
    let entries = memtable
        .read_entries()
        .map_err(|_| lock_poisoned("memtable entries"))?;
    for internal_key in entries.keys() {
        update_oldest_sequence(oldest, internal_key.sequence());
    }
    Ok(())
}

fn update_oldest_sequence(oldest: &mut Option<Sequence>, candidate: Sequence) {
    *oldest = Some(oldest.map_or(candidate, |current| current.min(candidate)));
}

fn table_write_options(options: &BucketOptions) -> table::TableWriteOptions {
    table::TableWriteOptions {
        codec: options.compression.codec_id(),
        block_bytes: options.block_bytes,
        filter_policy: options.filter_policy,
        prefix_extractor: options.prefix_extractor.clone(),
        prefix_filter_policy: options.prefix_filter_policy,
        filter_depth_curve: options.filter_depth_curve,
        blob_threshold_bytes: options.blob_threshold_bytes,
        rewrite_blob_indexes: false,
    }
}
