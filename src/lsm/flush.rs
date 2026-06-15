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
    pub(crate) table_id: table::TableId,
    pub(crate) table_level: table::TableLevel,
    pub(crate) table_options: table::TableWriteOptions,
    pub(crate) point_records: Vec<(InternalKey, Option<ValueRef>)>,
    pub(crate) range_tombstones: Vec<TableRangeTombstone>,
}

impl LsmTree {
    pub(crate) fn prepare_flush_inputs(
        &self,
        next_table_id: &mut table::TableId,
    ) -> Result<Vec<FlushInput>> {
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
                table_id: *next_table_id,
                table_level: table::TableLevel::ZERO,
                table_options: table_write_options(&self.options),
                point_records,
                range_tombstones,
            });
            *next_table_id = next_table_id.next().ok_or_else(|| Error::Corruption {
                message: "table id counter overflow".to_owned(),
            })?;
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
