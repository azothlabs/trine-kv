use super::{
    BlobIndex, BlockHandle, BlockHashEntry, Bytes, CodecId, Cursor, DataBlockIndexEntry,
    DataBlockPointLookupIndex, DataBlockRecordHeader, DataBlockRecordView, DecodedBlock,
    DecodedDataBlock, Error, IndexPartitionEntry, InternalKey, KeyRange,
    MIN_DATA_BLOCK_HASH_ENTRY_BYTES, MIN_DATA_RECORD_BYTES, MIN_INDEX_ENTRY_BYTES,
    MIN_INDEX_PARTITION_ENTRY_BYTES, MIN_RANGE_TOMBSTONE_BYTES, POINT_KEY_FILTER_ABSENT,
    POINT_KEY_FILTER_PRESENT, PREFIX_FILTER_ABSENT, PREFIX_FILTER_PRESENT, PointKeyFilter,
    PrefixFilter, RESTART_POINT_BYTES, Range, Result, SectionHandle, Sequence, TablePointRecord,
    TableProperties, TableRangeTombstone, TableSection, VALUE_BLOB, VALUE_BLOB_INDEX, VALUE_INLINE,
    VALUE_NONE, ValueRefHeader, block_bounds, ensure_count_fits_remaining, invalid_table,
    range_tombstone, section_bounds, u32_to_usize, user_key_hash, usize_to_u32,
};

pub(in crate::table) fn validate_block_codec(
    actual: CodecId,
    expected: CodecId,
    section: TableSection,
) -> Result<()> {
    if actual == expected {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "table {section:?} block codec {} does not match table codec {}",
            actual.as_str(),
            expected.as_str()
        ),
    })
}

pub(in crate::table) fn validate_index_top_level_codec(
    actual: CodecId,
    expected: CodecId,
) -> Result<()> {
    if matches!(actual, CodecId::None) || actual == expected {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "table index top-level block codec {} does not match table codec {} or none",
            actual.as_str(),
            expected.as_str()
        ),
    })
}

pub(in crate::table) fn decode_properties_block(bytes: &[u8]) -> Result<TableProperties> {
    let mut cursor = Cursor::new(bytes);
    let properties = cursor.read_properties()?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing properties block bytes"));
    }
    Ok(properties)
}

pub(in crate::table) fn decode_index_block(bytes: &[u8]) -> Result<Vec<DataBlockIndexEntry>> {
    let mut cursor = Cursor::new(bytes);
    let entry_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        entry_count,
        cursor.remaining_len(),
        MIN_INDEX_ENTRY_BYTES,
        "index entry count exceeds block bytes",
    )?;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(DataBlockIndexEntry {
            smallest_internal_key: cursor.read_internal_key()?,
            largest_internal_key: cursor.read_internal_key()?,
            block: BlockHandle {
                offset: cursor.read_u64()?,
                len: cursor.read_u64()?,
            },
            point_key_filter: read_point_key_filter(&mut cursor)?,
            prefix_filter: read_prefix_filter(&mut cursor)?,
        });
    }
    if !cursor.is_finished() {
        return Err(invalid_table("trailing index block bytes"));
    }
    Ok(entries)
}

pub(in crate::table) fn decode_index_top_level(bytes: &[u8]) -> Result<Vec<IndexPartitionEntry>> {
    let mut cursor = Cursor::new(bytes);
    let partition_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        partition_count,
        cursor.remaining_len(),
        MIN_INDEX_PARTITION_ENTRY_BYTES,
        "index partition count exceeds block bytes",
    )?;
    let mut partitions = Vec::with_capacity(partition_count);
    for _ in 0..partition_count {
        partitions.push(IndexPartitionEntry {
            smallest_internal_key: cursor.read_internal_key()?,
            largest_internal_key: cursor.read_internal_key()?,
            block: BlockHandle {
                offset: cursor.read_u64()?,
                len: cursor.read_u64()?,
            },
            first_data_block_index: cursor.read_u32()? as usize,
            data_block_count: cursor.read_u32()? as usize,
        });
    }
    if !cursor.is_finished() {
        return Err(invalid_table("trailing index top-level block bytes"));
    }
    Ok(partitions)
}

pub(in crate::table) fn data_block_record_view_at<'block>(
    bytes: &'block [u8],
    record_headers: &[DataBlockRecordHeader],
    index: usize,
) -> Result<DataBlockRecordView<'block>> {
    record_headers
        .get(index)
        .ok_or_else(|| invalid_table("record index outside data block"))?
        .view(bytes)
}

pub(in crate::table) fn read_data_block_record_header(
    cursor: &mut Cursor<'_>,
) -> Result<DataBlockRecordHeader> {
    let record_offset = usize_to_u32(cursor.offset, "data block record offset")?;
    let user_key = cursor.read_bytes_range()?;
    let sequence = Sequence::new(cursor.read_u64()?);
    let kind = cursor.read_value_kind()?;
    let batch_index = cursor.read_u32()?;
    let value = read_value_ref_header(cursor)?;
    let record_end = usize_to_u32(cursor.offset, "data block record end")?;
    Ok(DataBlockRecordHeader {
        record_offset,
        record_end,
        user_key_offset: user_key.start,
        user_key_len: user_key.end.saturating_sub(user_key.start),
        sequence,
        kind,
        batch_index,
        value,
    })
}

pub(in crate::table) fn read_value_ref_header(
    cursor: &mut Cursor<'_>,
) -> Result<Option<ValueRefHeader>> {
    match cursor.read_u8()? {
        VALUE_NONE => Ok(None),
        VALUE_INLINE => {
            let bytes = cursor.read_bytes_range()?;
            Ok(Some(ValueRefHeader::Inline {
                offset: bytes.start,
                len: bytes.end.saturating_sub(bytes.start),
            }))
        }
        VALUE_BLOB => Ok(Some(ValueRefHeader::Blob {
            file_id: cursor.read_u64()?,
            offset: cursor.read_u64()?,
            len: cursor.read_u64()?,
            checksum: cursor.read_u32()?,
        })),
        VALUE_BLOB_INDEX => Ok(Some(ValueRefHeader::BlobIndex(BlobIndex {
            file_id: cursor.read_u64()?,
            offset: cursor.read_u64()?,
            encoded_len: cursor.read_u64()?,
            value_len: cursor.read_u64()?,
            value_checksum: cursor.read_u32()?,
            record_checksum: cursor.read_u32()?,
            compression: cursor.read_codec()?,
        }))),
        tag => Err(Error::InvalidFormat {
            message: format!("unknown table value reference {tag}"),
        }),
    }
}

pub(in crate::table) fn decode_data_block(bytes: Vec<u8>) -> Result<DecodedDataBlock> {
    let len = bytes.len();
    let bytes = Bytes::from(bytes);
    decode_data_block_shared(bytes, 0..len, true)
}

pub(in crate::table) fn decode_data_block_from_block(
    block: DecodedBlock,
    validate_full_hash_index: bool,
) -> Result<DecodedDataBlock> {
    let (bytes, payload_range) = block.into_shared_payload();
    decode_data_block_shared(bytes, payload_range, validate_full_hash_index)
}

pub(in crate::table) fn decode_data_block_shared(
    bytes: Bytes,
    payload_range: Range<usize>,
    validate_full_hash_index: bool,
) -> Result<DecodedDataBlock> {
    let payload = bytes
        .get(payload_range.clone())
        .ok_or_else(|| invalid_table("data block payload outside shared bytes"))?;
    let mut cursor = Cursor::new(payload);
    let record_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        record_count,
        cursor.remaining_len(),
        MIN_DATA_RECORD_BYTES,
        "data record count exceeds block bytes",
    )?;
    let mut record_headers = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        record_headers.push(read_data_block_record_header(&mut cursor)?);
    }
    let restart_indices = decode_restart_points(&mut cursor, &record_headers)?;
    let point_lookup_index = decode_data_block_point_lookup_index(
        &mut cursor,
        payload,
        &record_headers,
        validate_full_hash_index,
    )?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing data block bytes"));
    }
    Ok(DecodedDataBlock {
        bytes,
        payload_range,
        record_headers: record_headers.into_boxed_slice(),
        restart_indices,
        point_lookup_index,
    })
}

pub(in crate::table) fn decode_data_block_point_lookup_index(
    cursor: &mut Cursor<'_>,
    bytes: &[u8],
    record_headers: &[DataBlockRecordHeader],
    validate_full_hash_index: bool,
) -> Result<DataBlockPointLookupIndex> {
    let entry_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        entry_count,
        cursor.remaining_len(),
        MIN_DATA_BLOCK_HASH_ENTRY_BYTES,
        "data block hash index entry count exceeds block bytes",
    )?;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let entry = BlockHashEntry {
            key_hash: cursor.read_u64()?,
            start_record: cursor.read_u32()?,
            end_record: cursor.read_u32()?,
        };
        validate_data_block_hash_entry(entry, bytes, record_headers)?;
        entries.push(entry);
    }

    let decoded = DataBlockPointLookupIndex::from_entries(entries);
    if validate_full_hash_index {
        let expected = data_block_point_lookup_index_from_block(bytes, record_headers)?;
        if decoded != expected {
            return Err(invalid_table(
                "data block hash index does not match records",
            ));
        }
    }
    Ok(decoded)
}

pub(in crate::table) fn validate_data_block_hash_entry(
    entry: BlockHashEntry,
    bytes: &[u8],
    record_headers: &[DataBlockRecordHeader],
) -> Result<()> {
    if entry.start_record >= entry.end_record
        || u32_to_usize(entry.end_record) > record_headers.len()
    {
        return Err(invalid_table("data block hash index range is invalid"));
    }
    let first_record =
        data_block_record_view_at(bytes, record_headers, u32_to_usize(entry.start_record))?;
    if user_key_hash(first_record.user_key) != entry.key_hash {
        return Err(invalid_table("data block hash index hash mismatch"));
    }
    for record_index in u32_to_usize(entry.start_record)..u32_to_usize(entry.end_record) {
        let record = data_block_record_view_at(bytes, record_headers, record_index)?;
        if record.user_key != first_record.user_key {
            return Err(invalid_table("data block hash index range crosses keys"));
        }
    }
    Ok(())
}

pub(in crate::table) fn data_block_point_lookup_index_from_block(
    bytes: &[u8],
    record_headers: &[DataBlockRecordHeader],
) -> Result<DataBlockPointLookupIndex> {
    let mut entries = Vec::new();
    let mut start = 0;
    while start < record_headers.len() {
        let first_record = data_block_record_view_at(bytes, record_headers, start)?;
        let key = first_record.user_key;
        let mut end = start + 1;
        while end < record_headers.len()
            && data_block_record_view_at(bytes, record_headers, end)?.user_key == key
        {
            end += 1;
        }
        entries.push(BlockHashEntry {
            key_hash: user_key_hash(key),
            start_record: usize_to_u32(start, "data block hash range start")?,
            end_record: usize_to_u32(end, "data block hash range end")?,
        });
        start = end;
    }
    Ok(DataBlockPointLookupIndex::from_entries(entries))
}

pub(in crate::table) fn decode_range_tombstone_block(
    bytes: &[u8],
) -> Result<Vec<TableRangeTombstone>> {
    let mut cursor = Cursor::new(bytes);
    let tombstone_count = cursor.read_u32()? as usize;
    ensure_count_fits_remaining(
        tombstone_count,
        cursor.remaining_len(),
        MIN_RANGE_TOMBSTONE_BYTES,
        "range tombstone count exceeds block bytes",
    )?;
    let mut range_tombstones = Vec::with_capacity(tombstone_count);
    for _ in 0..tombstone_count {
        let start = cursor.read_bound()?;
        let end = cursor.read_bound()?;
        range_tombstones.push(TableRangeTombstone {
            range: KeyRange { start, end },
            sequence: Sequence::new(cursor.read_u64()?),
            batch_index: cursor.read_u32()?,
        });
    }
    if !cursor.is_finished() {
        return Err(invalid_table("trailing range tombstone block bytes"));
    }
    range_tombstone::sort_tombstones(&mut range_tombstones);
    Ok(range_tombstones)
}

pub(in crate::table) fn decode_filter_block(
    bytes: &[u8],
) -> Result<(Option<PointKeyFilter>, Option<PrefixFilter>)> {
    let mut cursor = Cursor::new(bytes);
    let point_key_filter = read_point_key_filter(&mut cursor)?;
    let prefix_filter = read_prefix_filter(&mut cursor)?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing filter block bytes"));
    }
    Ok((point_key_filter, prefix_filter))
}

pub(in crate::table) fn read_point_key_filter(
    cursor: &mut Cursor<'_>,
) -> Result<Option<PointKeyFilter>> {
    match cursor.read_u8()? {
        POINT_KEY_FILTER_ABSENT => Ok(None),
        POINT_KEY_FILTER_PRESENT => {
            let bit_count = cursor.read_u64()?;
            let hash_count = cursor.read_u8()?;
            let bytes = cursor.read_bytes()?.to_vec();
            Ok(Some(PointKeyFilter::from_parts(
                bit_count, hash_count, bytes,
            )?))
        }
        tag => Err(Error::InvalidFormat {
            message: format!("unknown table point-key filter tag {tag}"),
        }),
    }
}

pub(in crate::table) fn read_prefix_filter(
    cursor: &mut Cursor<'_>,
) -> Result<Option<PrefixFilter>> {
    match cursor.read_u8()? {
        PREFIX_FILTER_ABSENT => Ok(None),
        PREFIX_FILTER_PRESENT => {
            let extractor = cursor.read_prefix_extractor()?;
            let bit_count = cursor.read_u64()?;
            let hash_count = cursor.read_u8()?;
            let bytes = cursor.read_bytes()?.to_vec();
            Ok(Some(PrefixFilter::from_parts(
                extractor, bit_count, hash_count, bytes,
            )?))
        }
        tag => Err(Error::InvalidFormat {
            message: format!("unknown table prefix filter tag {tag}"),
        }),
    }
}

pub(in crate::table) fn decode_restart_points(
    cursor: &mut Cursor<'_>,
    record_headers: &[DataBlockRecordHeader],
) -> Result<Box<[u32]>> {
    // The on-disk restart list stores byte offsets. Convert them to record
    // indexes once during table open, and reject offsets that do not land
    // exactly on a decoded record boundary.
    let records_end = cursor.offset;
    let restart_count = cursor.read_u32()? as usize;
    if record_headers.is_empty() {
        if restart_count == 0 {
            return Ok(Box::default());
        }
        return Err(invalid_table("empty data block has restart points"));
    }
    if restart_count == 0 {
        return Err(invalid_table("data block is missing restart points"));
    }
    ensure_count_fits_remaining(
        restart_count,
        cursor.remaining_len(),
        RESTART_POINT_BYTES,
        "data block restart count exceeds block bytes",
    )?;

    let mut restart_indices = Vec::with_capacity(restart_count);
    let mut previous_restart = None;
    for _ in 0..restart_count {
        let restart = cursor.read_u32()?;
        if u32_to_usize(restart) >= records_end {
            return Err(invalid_table("data block restart outside record area"));
        }
        if previous_restart.is_some_and(|previous| restart <= previous) {
            return Err(invalid_table("data block restart points are not sorted"));
        }
        let record_index = record_headers
            .binary_search_by_key(&restart, |record| record.record_offset)
            .map_err(|_| invalid_table("data block restart is not a record start"))?;
        restart_indices.push(usize_to_u32(
            record_index,
            "data block restart record index",
        )?);
        previous_restart = Some(restart);
    }
    if restart_indices.first().copied() != Some(0) {
        return Err(invalid_table(
            "data block first restart is not first record",
        ));
    }

    Ok(restart_indices.into_boxed_slice())
}

pub(in crate::table) fn validate_index_top_level(
    top_level_block: BlockHandle,
    partitions: &[IndexPartitionEntry],
    indexes: SectionHandle,
) -> Result<usize> {
    let (section_start, section_end) = section_bounds(indexes)?;
    let (top_start, top_end) = block_bounds(top_level_block)?;
    if top_start != section_start || top_end > section_end {
        return Err(Error::Corruption {
            message: "index top-level block is outside index section".to_owned(),
        });
    }
    let mut expected_block_index = 0_usize;
    let mut expected_offset = top_end;
    let mut previous_largest = None;
    for partition in partitions {
        if partition.data_block_count == 0 {
            return Err(invalid_table("empty index partition"));
        }
        if partition.first_data_block_index != expected_block_index {
            return Err(invalid_table("index partitions are not contiguous"));
        }
        let (partition_start, partition_end) = block_bounds(partition.block)?;
        if partition_start != expected_offset || partition_end > section_end {
            return Err(Error::Corruption {
                message: "index partition layout is inconsistent".to_owned(),
            });
        }
        if partition.smallest_internal_key > partition.largest_internal_key {
            return Err(invalid_table("index partition key bounds are inverted"));
        }
        if previous_largest
            .as_ref()
            .is_some_and(|previous| previous >= &partition.smallest_internal_key)
        {
            return Err(invalid_table("index partitions are not sorted"));
        }
        expected_block_index = expected_block_index
            .checked_add(partition.data_block_count)
            .ok_or_else(|| invalid_table("index partition data block count overflow"))?;
        expected_offset = partition_end;
        previous_largest = Some(partition.largest_internal_key.clone());
    }
    if expected_offset != section_end {
        return Err(Error::Corruption {
            message: "index partitions do not cover index section".to_owned(),
        });
    }
    Ok(expected_block_index)
}

pub(in crate::table) fn validate_index_partition(
    partition: &IndexPartitionEntry,
    entries: &[DataBlockIndexEntry],
    data_blocks: SectionHandle,
) -> Result<()> {
    if entries.len() != partition.data_block_count {
        return Err(invalid_table("index partition entry count mismatch"));
    }
    let first = entries
        .first()
        .ok_or_else(|| invalid_table("empty index partition"))?;
    let last = entries
        .last()
        .ok_or_else(|| invalid_table("empty index partition"))?;
    if first.smallest_internal_key != partition.smallest_internal_key
        || last.largest_internal_key != partition.largest_internal_key
    {
        return Err(invalid_table("index partition key bounds mismatch"));
    }

    let (data_start, data_end) = section_bounds(data_blocks)?;
    let mut previous_end = None;
    let mut previous_largest = None;
    for entry in entries {
        let (block_start, block_end) = block_bounds(entry.block)?;
        if block_start < data_start || block_end > data_end {
            return Err(Error::Corruption {
                message: "data block index points outside data section".to_owned(),
            });
        }
        if previous_end.is_some_and(|end| end != block_start) {
            return Err(Error::Corruption {
                message: "data block index partition has a gap".to_owned(),
            });
        }
        if previous_largest
            .as_ref()
            .is_some_and(|previous| previous >= &entry.smallest_internal_key)
        {
            return Err(Error::Corruption {
                message: "data block index entries are not sorted".to_owned(),
            });
        }
        previous_end = Some(block_end);
        previous_largest = Some(entry.largest_internal_key.clone());
    }
    Ok(())
}

pub(in crate::table) fn validate_decoded_data_block_entry(
    entry: &DataBlockIndexEntry,
    block: &DecodedDataBlock,
) -> Result<()> {
    if block.record_count() == 0 {
        return Err(Error::Corruption {
            message: "data block index points to an empty block".to_owned(),
        });
    }
    let first = block.record_view(0)?;
    let last = block.record_view(block.record_count() - 1)?;
    if !record_view_matches_internal_key(first, &entry.smallest_internal_key)
        || !record_view_matches_internal_key(last, &entry.largest_internal_key)
    {
        return Err(Error::Corruption {
            message: "data block index key bounds do not match block records".to_owned(),
        });
    }

    validate_sorted_decoded_data_block(block)
}

pub(in crate::table) fn validate_table_filters(
    point_filter: Option<&PointKeyFilter>,
    prefix_filter: Option<&PrefixFilter>,
    records: &[TablePointRecord],
) -> Result<()> {
    for record in records {
        validate_table_filters_for_key(
            point_filter,
            prefix_filter,
            record.internal_key.user_key(),
        )?;
    }

    Ok(())
}

pub(in crate::table) fn validate_table_filters_for_key(
    point_filter: Option<&PointKeyFilter>,
    prefix_filter: Option<&PrefixFilter>,
    user_key: &[u8],
) -> Result<()> {
    if point_filter.is_some_and(|filter| !filter.may_contain_key(user_key)) {
        return Err(Error::Corruption {
            message: "table point-key filter misses a table key".to_owned(),
        });
    }

    if let Some(filter) = prefix_filter {
        if filter
            .extractor()
            .extract(user_key)
            .is_some_and(|prefix| !filter.may_contain_prefix(prefix))
        {
            return Err(Error::Corruption {
                message: "table prefix filter misses a table prefix".to_owned(),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
pub(in crate::table) fn validate_data_block_filters(
    entry: &DataBlockIndexEntry,
    records: &[TablePointRecord],
) -> Result<()> {
    // Index-level filters can only remove data-block candidates if every key in
    // the block remains represented. Rejecting false negatives keeps filters
    // advisory instead of letting them decide MVCC visibility.
    for record in records {
        let user_key = record.internal_key.user_key();
        if entry
            .point_key_filter
            .as_ref()
            .is_some_and(|filter| !filter.may_contain_key(user_key))
        {
            return Err(Error::Corruption {
                message: "data block point-key filter misses a block key".to_owned(),
            });
        }

        if let Some(filter) = &entry.prefix_filter {
            if filter
                .extractor()
                .extract(user_key)
                .is_some_and(|prefix| !filter.may_contain_prefix(prefix))
            {
                return Err(Error::Corruption {
                    message: "data block prefix filter misses a block prefix".to_owned(),
                });
            }
        }
    }

    Ok(())
}

pub(in crate::table) fn validate_decoded_data_block_filters(
    entry: &DataBlockIndexEntry,
    block: &DecodedDataBlock,
) -> Result<()> {
    for record_index in 0..block.record_count() {
        let record = block.record_view(record_index)?;
        if entry
            .point_key_filter
            .as_ref()
            .is_some_and(|filter| !filter.may_contain_key(record.user_key))
        {
            return Err(Error::Corruption {
                message: "data block point-key filter misses a block key".to_owned(),
            });
        }

        if let Some(filter) = &entry.prefix_filter {
            if filter
                .extractor()
                .extract(record.user_key)
                .is_some_and(|prefix| !filter.may_contain_prefix(prefix))
            {
                return Err(Error::Corruption {
                    message: "data block prefix filter misses a block prefix".to_owned(),
                });
            }
        }
    }

    Ok(())
}

pub(in crate::table) fn validate_sorted_decoded_data_block(block: &DecodedDataBlock) -> Result<()> {
    for record_index in 1..block.record_count() {
        let previous = block.record_view(record_index - 1)?;
        let current = block.record_view(record_index)?;
        if data_block_record_view_cmp(previous, current) != std::cmp::Ordering::Less {
            return Err(Error::Corruption {
                message: "table point records are not sorted by internal key".to_owned(),
            });
        }
    }

    Ok(())
}

pub(in crate::table) fn data_block_record_view_cmp(
    left: DataBlockRecordView<'_>,
    right: DataBlockRecordView<'_>,
) -> std::cmp::Ordering {
    left.user_key
        .cmp(right.user_key)
        .then_with(|| right.sequence.cmp(&left.sequence))
        .then_with(|| right.batch_index.cmp(&left.batch_index))
        .then_with(|| left.kind.cmp(&right.kind))
}

pub(in crate::table) fn record_view_matches_internal_key(
    record: DataBlockRecordView<'_>,
    key: &InternalKey,
) -> bool {
    record.user_key == key.user_key()
        && record.sequence == key.sequence()
        && record.kind == key.kind()
        && record.batch_index == key.batch_index()
}
