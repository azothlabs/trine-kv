use std::{collections::BTreeSet, ops::Bound, sync::Arc};

use crate::{
    blob::ValueRef,
    compaction,
    error::Result,
    internal_key::{InternalKey, ValueKind},
    iterator::{Direction, RecordGroup, ScanSelector},
    options::KeyspaceOptions,
    range_tombstone,
    table::{self, Table, TablePointCursor, TableRangeTombstone},
    types::{KeyRange, Sequence},
};

use super::tree::LsmTree;

#[derive(Debug)]
pub(crate) struct CompactionInput {
    pub(crate) table_level: table::TableLevel,
    pub(crate) table_options: table::TableWriteOptions,
    pub(crate) input_table_ids: Vec<table::TableId>,
    input_tables: Vec<Arc<Table>>,
}

#[derive(Debug)]
pub(crate) struct CompactionOutput {
    pub(crate) input_table_ids: Vec<table::TableId>,
    pub(crate) tables: Vec<Arc<Table>>,
}

#[derive(Debug)]
pub(crate) struct CompactionTablePayload {
    pub(crate) point_records: Vec<(InternalKey, Option<ValueRef>)>,
    pub(crate) range_tombstones: Vec<TableRangeTombstone>,
}

#[derive(Debug, Default)]
struct CompactionChunk {
    point_records: Vec<(InternalKey, Option<ValueRef>)>,
    estimated_bytes: u64,
}

impl LsmTree {
    pub(crate) fn plan_compaction(
        &self,
        keyspace: &str,
        range: &KeyRange,
        oldest_active_snapshot: Sequence,
        options: compaction::CompactionOptions,
    ) -> Result<Option<CompactionInput>> {
        let version = self.current_version()?;
        let tables = version.table_handles();
        let plan_tables = tables
            .iter()
            .map(|table| {
                compaction::CompactionTable::from_properties_with_bytes(
                    table.properties(),
                    table.estimated_file_bytes(),
                )
            })
            .collect::<Vec<_>>();
        let Some(plan) = compaction::plan_compaction(
            keyspace,
            &plan_tables,
            range,
            oldest_active_snapshot,
            options,
        )?
        else {
            return Ok(None);
        };
        let input_table_ids = plan.input_tables.iter().copied().collect::<BTreeSet<_>>();
        let input_tables = tables
            .iter()
            .filter(|table| input_table_ids.contains(&table.properties().id))
            .cloned()
            .collect::<Vec<_>>();
        let input_table_ids = input_tables
            .iter()
            .map(|table| table.properties().id)
            .collect::<Vec<_>>();

        Ok(Some(CompactionInput {
            table_level: plan.output_level,
            table_options: table_write_options(&self.options),
            input_table_ids,
            input_tables,
        }))
    }

    pub(crate) fn build_compaction_table_payloads(
        &self,
        input: &CompactionInput,
        range: &KeyRange,
        oldest_active_snapshot: Sequence,
        target_table_bytes: usize,
    ) -> Result<Vec<CompactionTablePayload>> {
        let mut sources = input
            .input_tables
            .iter()
            .cloned()
            .map(|table| {
                CompactionSource::new(table.point_cursor(
                    ScanSelector::Range(range.clone()),
                    input.table_options.prefix_extractor.clone(),
                    Direction::Forward,
                    self.options.index_search_policy,
                    None,
                ))
            })
            .collect::<Vec<_>>();
        let range_tombstones = collect_compaction_range_tombstones(input, range)?;
        let mut tombstone_has_remaining_put = vec![false; range_tombstones.len()];
        let mut chunks = Vec::new();
        let mut current_chunk = CompactionChunk::default();
        let target_table_bytes = usize_to_u64_saturating(target_table_bytes).max(1);

        while let Some(user_key) = next_compaction_user_key(&mut sources)? {
            let mut records = Vec::new();
            for source in &mut sources {
                if source.current_key()? == Some(user_key.as_slice()) {
                    let group = source
                        .take_current_group()?
                        .expect("source current key must have a current group");
                    records.push(group.first);
                    records.extend(group.rest);
                }
            }

            let records = compact_point_record_group(records, oldest_active_snapshot);
            if records.is_empty() {
                continue;
            }
            mark_tombstones_covering_records(
                &range_tombstones,
                &mut tombstone_has_remaining_put,
                &records,
            );
            push_compaction_records_to_chunks(
                &mut chunks,
                &mut current_chunk,
                records,
                target_table_bytes,
            );
        }

        if !current_chunk.point_records.is_empty() {
            chunks.push(current_chunk);
        }

        let range_tombstones = cleanup_range_tombstones_by_coverage(
            range_tombstones,
            tombstone_has_remaining_put,
            range_is_all(range),
        );
        Ok(compaction_payloads_from_chunks(
            chunks,
            &range_tombstones,
            range_is_all(range),
            target_table_bytes,
        ))
    }

    pub(crate) fn install_compaction(&self, output: CompactionOutput) -> Result<()> {
        let version = self.current_version()?;
        let version = version.with_replaced_tables(&output.input_table_ids, output.tables)?;
        self.install_version(version)?;
        Ok(())
    }
}

fn collect_compaction_range_tombstones(
    input: &CompactionInput,
    range: &KeyRange,
) -> Result<Vec<TableRangeTombstone>> {
    let mut tombstones = Vec::new();
    for table in &input.input_tables {
        tombstones.extend(table.range_tombstones_overlapping_range(range)?);
    }
    range_tombstone::sort_tombstones(&mut tombstones);
    Ok(tombstones)
}

#[derive(Debug)]
struct CompactionSource {
    cursor: TablePointCursor,
    current: Option<RecordGroup>,
}

impl CompactionSource {
    fn new(cursor: TablePointCursor) -> Self {
        Self {
            cursor,
            current: None,
        }
    }

    fn current_key(&mut self) -> Result<Option<&[u8]>> {
        self.ensure_current()?;
        Ok(self.current.as_ref().map(|group| group.user_key.as_slice()))
    }

    fn take_current_group(&mut self) -> Result<Option<RecordGroup>> {
        self.ensure_current()?;
        Ok(self.current.take())
    }

    fn ensure_current(&mut self) -> Result<()> {
        if self.current.is_none() {
            self.current = self.cursor.next_group()?;
        }
        Ok(())
    }
}

fn next_compaction_user_key(sources: &mut [CompactionSource]) -> Result<Option<Vec<u8>>> {
    let mut selected: Option<Vec<u8>> = None;
    for source in sources {
        let Some(user_key) = source.current_key()? else {
            continue;
        };
        if selected
            .as_ref()
            .is_none_or(|selected| user_key < selected.as_slice())
        {
            selected = Some(user_key.to_vec());
        }
    }
    Ok(selected)
}

fn compact_point_record_group(
    records: Vec<(InternalKey, Option<ValueRef>)>,
    oldest_active_snapshot: Sequence,
) -> Vec<(InternalKey, Option<ValueRef>)> {
    let compacted = compact_point_records(records, oldest_active_snapshot);
    cleanup_point_tombstones(&compacted)
}

fn push_compaction_records_to_chunks(
    chunks: &mut Vec<CompactionChunk>,
    current_chunk: &mut CompactionChunk,
    records: Vec<(InternalKey, Option<ValueRef>)>,
    target_table_bytes: u64,
) {
    let record_bytes = records.iter().map(compaction_record_bytes).sum::<u64>();
    if !current_chunk.point_records.is_empty()
        && current_chunk.estimated_bytes.saturating_add(record_bytes) > target_table_bytes
    {
        chunks.push(std::mem::take(current_chunk));
    }

    current_chunk.estimated_bytes = current_chunk.estimated_bytes.saturating_add(record_bytes);
    current_chunk.point_records.extend(records);
}

fn mark_tombstones_covering_records(
    tombstones: &[TableRangeTombstone],
    tombstone_has_remaining_put: &mut [bool],
    records: &[(InternalKey, Option<ValueRef>)],
) {
    for (internal_key, _) in records {
        if !matches!(internal_key.kind(), ValueKind::Put) {
            continue;
        }
        for (index, tombstone) in tombstones.iter().enumerate() {
            if internal_key.sequence() <= tombstone.sequence
                && range_tombstone::key_is_in_range(internal_key.user_key(), &tombstone.range)
            {
                tombstone_has_remaining_put[index] = true;
            }
        }
    }
}

fn cleanup_range_tombstones_by_coverage(
    range_tombstones: Vec<TableRangeTombstone>,
    tombstone_has_remaining_put: Vec<bool>,
    full_keyspace_compaction: bool,
) -> Vec<TableRangeTombstone> {
    if !full_keyspace_compaction {
        return range_tombstones;
    }

    range_tombstones
        .into_iter()
        .zip(tombstone_has_remaining_put)
        .filter_map(|(tombstone, keep)| keep.then_some(tombstone))
        .collect()
}

fn compaction_payloads_from_chunks(
    chunks: Vec<CompactionChunk>,
    range_tombstones: &[TableRangeTombstone],
    full_keyspace_compaction: bool,
    target_table_bytes: u64,
) -> Vec<CompactionTablePayload> {
    let mut payloads = Vec::with_capacity(chunks.len());
    let mut assigned_tombstones = vec![false; range_tombstones.len()];

    for chunk in chunks {
        let Some(span) = chunk_range(&chunk.point_records) else {
            continue;
        };
        let mut chunk_tombstones = Vec::new();
        for (index, tombstone) in range_tombstones.iter().enumerate() {
            if let Some(tombstone) =
                tombstone_for_output_span(tombstone, &span, full_keyspace_compaction)
            {
                assigned_tombstones[index] = true;
                chunk_tombstones.push(tombstone);
            }
        }

        payloads.push(CompactionTablePayload {
            point_records: chunk.point_records,
            range_tombstones: chunk_tombstones,
        });
    }

    let mut tombstone_only = Vec::new();
    let mut tombstone_only_bytes = 0_u64;
    for (index, tombstone) in range_tombstones.iter().enumerate() {
        if assigned_tombstones[index] {
            continue;
        }
        let tombstone_bytes = range_tombstone_bytes(tombstone);
        if !tombstone_only.is_empty()
            && tombstone_only_bytes.saturating_add(tombstone_bytes) > target_table_bytes
        {
            payloads.push(CompactionTablePayload {
                point_records: Vec::new(),
                range_tombstones: std::mem::take(&mut tombstone_only),
            });
            tombstone_only_bytes = 0;
        }
        tombstone_only.push(tombstone.clone());
        tombstone_only_bytes = tombstone_only_bytes.saturating_add(tombstone_bytes);
    }
    if !tombstone_only.is_empty() {
        payloads.push(CompactionTablePayload {
            point_records: Vec::new(),
            range_tombstones: tombstone_only,
        });
    }

    payloads
}

fn tombstone_for_output_span(
    tombstone: &TableRangeTombstone,
    span: &KeyRange,
    full_keyspace_compaction: bool,
) -> Option<TableRangeTombstone> {
    if full_keyspace_compaction {
        range_tombstone::range_intersection(&tombstone.range, span).map(|range| {
            TableRangeTombstone {
                range,
                sequence: tombstone.sequence,
                batch_index: tombstone.batch_index,
            }
        })
    } else if range_tombstone::ranges_overlap(&tombstone.range, span) {
        Some(tombstone.clone())
    } else {
        None
    }
}

fn chunk_range(point_records: &[(InternalKey, Option<ValueRef>)]) -> Option<KeyRange> {
    let smallest = point_records.first()?.0.user_key();
    let largest = point_records.last()?.0.user_key();
    Some(range_tombstone::range_from_inclusive_span(
        smallest, largest,
    ))
}

fn compaction_record_bytes(record: &(InternalKey, Option<ValueRef>)) -> u64 {
    usize_to_u64_saturating(record.0.user_key().len())
        .saturating_add(record.1.as_ref().map_or(0, ValueRef::len))
        .saturating_add(32)
}

fn range_tombstone_bytes(tombstone: &TableRangeTombstone) -> u64 {
    key_range_bytes(&tombstone.range)
        .saturating_add(usize_to_u64_saturating(std::mem::size_of::<Sequence>()))
        .saturating_add(usize_to_u64_saturating(std::mem::size_of::<u32>()))
}

fn compact_point_records(
    mut point_records: Vec<(InternalKey, Option<ValueRef>)>,
    oldest_active_snapshot: Sequence,
) -> Vec<(InternalKey, Option<ValueRef>)> {
    point_records.sort_by(|left, right| left.0.cmp(&right.0));

    let mut compacted = Vec::with_capacity(point_records.len());
    let mut current_user_key: Option<Vec<u8>> = None;
    let mut kept_floor_version = false;

    for record in point_records {
        if current_user_key.as_deref() != Some(record.0.user_key()) {
            current_user_key = Some(record.0.user_key().to_vec());
            kept_floor_version = false;
        }

        // Keep all versions newer than the oldest active snapshot. At or
        // below that snapshot, only the newest record for the user key can
        // still be observed.
        if record.0.sequence() > oldest_active_snapshot {
            compacted.push(record);
        } else if !kept_floor_version {
            compacted.push(record);
            kept_floor_version = true;
        }
    }

    compacted
}

fn cleanup_point_tombstones(
    point_records: &[(InternalKey, Option<ValueRef>)],
) -> Vec<(InternalKey, Option<ValueRef>)> {
    let mut compacted = Vec::with_capacity(point_records.len());
    let mut index = 0;

    while index < point_records.len() {
        let user_key = point_records[index].0.user_key();
        let group_end = point_records[index..]
            .partition_point(|(internal_key, _)| internal_key.user_key() == user_key)
            + index;

        for record_index in index..group_end {
            let (internal_key, _) = &point_records[record_index];
            if matches!(internal_key.kind(), ValueKind::PointDelete)
                && !has_older_point_record(point_records, record_index, group_end)
            {
                continue;
            }
            compacted.push(point_records[record_index].clone());
        }

        index = group_end;
    }

    compacted
}

fn has_older_point_record(
    point_records: &[(InternalKey, Option<ValueRef>)],
    tombstone_index: usize,
    group_end: usize,
) -> bool {
    let tombstone_sequence = point_records[tombstone_index].0.sequence();
    point_records[tombstone_index + 1..group_end]
        .iter()
        .any(|(internal_key, _)| internal_key.sequence() <= tombstone_sequence)
}

fn key_range_bytes(range: &KeyRange) -> u64 {
    bound_bytes(&range.start).saturating_add(bound_bytes(&range.end))
}

fn bound_bytes(bound: &Bound<Vec<u8>>) -> u64 {
    match bound {
        Bound::Included(bytes) | Bound::Excluded(bytes) => usize_to_u64_saturating(bytes.len()),
        Bound::Unbounded => 0,
    }
}

fn range_is_all(range: &KeyRange) -> bool {
    matches!(
        (&range.start, &range.end),
        (Bound::Unbounded, Bound::Unbounded)
    )
}

fn table_write_options(options: &KeyspaceOptions) -> table::TableWriteOptions {
    table::TableWriteOptions {
        codec: options.compression.codec_id(),
        block_bytes: options.block_bytes,
        filter_policy: options.filter_policy,
        prefix_extractor: options.prefix_extractor.clone(),
        prefix_filter_policy: options.prefix_filter_policy,
        blob_threshold_bytes: options.blob_threshold_bytes,
    }
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    match u64::try_from(value) {
        Ok(value) => value,
        Err(_) => u64::MAX,
    }
}

#[cfg(test)]
fn cleanup_range_tombstones(
    range_tombstones: Vec<TableRangeTombstone>,
    point_records: &[(InternalKey, Option<ValueRef>)],
    full_keyspace_compaction: bool,
) -> Vec<TableRangeTombstone> {
    // Partial compaction cannot prove there is no older covered data just
    // outside its input tables. Keep range tombstones there and only clean them
    // when the whole keyspace participates in this compaction pass.
    if !full_keyspace_compaction {
        return range_tombstones;
    }

    range_tombstones
        .into_iter()
        .filter(|tombstone| range_tombstone_covers_remaining_put(tombstone, point_records))
        .collect()
}

#[cfg(test)]
fn range_tombstone_covers_remaining_put(
    tombstone: &TableRangeTombstone,
    point_records: &[(InternalKey, Option<ValueRef>)],
) -> bool {
    point_records.iter().any(|(internal_key, _)| {
        matches!(internal_key.kind(), ValueKind::Put)
            && internal_key.sequence() <= tombstone.sequence
            && range_tombstone::key_is_in_range(internal_key.user_key(), &tombstone.range)
    })
}

#[cfg(test)]
mod tests {
    use super::{
        InternalKey, TableRangeTombstone, ValueKind, ValueRef, cleanup_point_tombstones,
        cleanup_range_tombstones, compact_point_records,
    };
    use crate::types::{KeyRange, Sequence};

    #[test]
    fn compaction_keeps_newer_versions_and_snapshot_floor() {
        let compacted = compact_point_records(
            vec![
                record("a", 1),
                record("a", 3),
                record("a", 2),
                record("b", 1),
                record("b", 2),
            ],
            Sequence::new(2),
        );

        assert_eq!(
            record_sequences(&compacted),
            vec![("a", 3), ("a", 2), ("b", 2)]
        );
    }

    #[test]
    fn compaction_without_old_snapshot_keeps_only_newest_record_per_key() {
        let compacted = compact_point_records(
            vec![
                record("a", 1),
                record("a", 4),
                record("a", 3),
                tombstone("b", 2),
                record("b", 1),
            ],
            Sequence::new(4),
        );

        assert_eq!(record_sequences(&compacted), vec![("a", 4), ("b", 2)]);
        assert!(matches!(compacted[1].0.kind(), ValueKind::PointDelete));
    }

    #[test]
    fn point_tombstone_cleanup_drops_delete_after_older_records_are_removed() {
        let compacted =
            compact_point_records(vec![tombstone("a", 2), record("a", 1)], Sequence::new(2));

        assert!(cleanup_point_tombstones(&compacted).is_empty());
    }

    #[test]
    fn point_tombstone_cleanup_keeps_delete_while_older_record_remains() {
        let compacted =
            compact_point_records(vec![tombstone("a", 3), record("a", 1)], Sequence::new(1));

        assert_eq!(
            record_sequences(&cleanup_point_tombstones(&compacted)),
            vec![("a", 3), ("a", 1)]
        );
    }

    #[test]
    fn range_tombstone_cleanup_keeps_tombstone_covering_remaining_put() {
        let tombstones =
            cleanup_range_tombstones(vec![range_tombstone("a", "c", 2)], &[record("b", 1)], true);

        assert_eq!(tombstones.len(), 1);
    }

    #[test]
    fn range_tombstone_cleanup_drops_tombstone_without_remaining_put() {
        let tombstones = cleanup_range_tombstones(
            vec![range_tombstone("a", "c", 2)],
            &[record("b", 3), record("z", 1)],
            true,
        );

        assert!(tombstones.is_empty());
    }

    #[test]
    fn range_tombstone_cleanup_keeps_tombstone_for_partial_compaction() {
        let tombstones = cleanup_range_tombstones(vec![range_tombstone("a", "c", 2)], &[], false);

        assert_eq!(tombstones.len(), 1);
    }

    fn record(key: &str, sequence: u64) -> (InternalKey, Option<ValueRef>) {
        (
            InternalKey::new(key, Sequence::new(sequence), ValueKind::Put, 0),
            Some(ValueRef::Inline(format!("{key}-{sequence}").into_bytes())),
        )
    }

    fn tombstone(key: &str, sequence: u64) -> (InternalKey, Option<ValueRef>) {
        (
            InternalKey::new(key, Sequence::new(sequence), ValueKind::PointDelete, 0),
            None,
        )
    }

    fn range_tombstone(start: &str, end: &str, sequence: u64) -> TableRangeTombstone {
        TableRangeTombstone {
            range: KeyRange::half_open(start.as_bytes(), end.as_bytes()),
            sequence: Sequence::new(sequence),
            batch_index: 0,
        }
    }

    fn record_sequences(records: &[(InternalKey, Option<ValueRef>)]) -> Vec<(&str, u64)> {
        records
            .iter()
            .map(|(internal_key, _)| {
                (
                    std::str::from_utf8(internal_key.user_key()).expect("test key is UTF-8"),
                    internal_key.sequence().get(),
                )
            })
            .collect()
    }
}
