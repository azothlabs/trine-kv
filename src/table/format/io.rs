use super::{
    Arc, BTreeMap, BlockHandle, BlockManager, BlockReadSource, BlockingStorageReadBackend,
    BlockingStorageReadObject, CodecId, Cursor, DATA_BLOCK_RESTART_INTERVAL, DataBlockIndexEntry,
    DataBlockPointLookupIndex, DecodedBlock, DecodedDataBlock, EncodedTable, Error, FOOTER_LEN,
    FOOTER_MAGIC, HEADER_LEN, INDEX_PARTITION_TARGET_ENTRIES, IndexPartitionEntry,
    MemoryStorageBackend, NativeFileObject, POINT_KEY_FILTER_ABSENT, POINT_KEY_FILTER_PRESENT,
    PREFIX_FILTER_ABSENT, PREFIX_FILTER_PRESENT, Path, PointKeyFilter, PrefixFilter,
    RangeTombstoneIndex, Result, RwLock, SectionHandle, StorageCapability, StorageObjectId,
    StorageObjectKind, StorageReadBackend, StorageReadObject, StorageReadSource, TABLE_MAGIC,
    TABLE_VERSION, Table, TableDataBlock, TableFilterStats, TableFooter, TableLevel,
    TablePointRecord, TableProperties, TableReadPathStats, TableSection, block_bounds, checksum,
    decode_data_block_from_block, decode_filter_block, decode_index_block, decode_index_top_level,
    decode_properties_block, decode_range_tombstone_block, index_partitions_for_loaded_blocks,
    invalid_table, limits, put_bound, put_bytes, put_internal_key, put_prefix_extractor,
    put_properties, put_section_handle, put_u8, put_u16, put_u32, put_u64, put_value_ref,
    read_u16_at, read_u32_at, section_bounds, should_pin_read_metadata, table_properties,
    table_read_source, usize_to_u32, usize_to_u64, validate_block_codec,
    validate_decoded_data_block_entry, validate_decoded_data_block_filters,
    validate_index_partition, validate_index_top_level, validate_index_top_level_codec,
    validate_sorted_point_records, validate_table_filters,
};

#[cfg(test)]
pub(in crate::table) fn encode_table(table: &Table) -> Result<Vec<u8>> {
    Ok(encode_table_for_write(table)?.payload)
}

pub(in crate::table) fn encode_table_for_write(table: &Table) -> Result<EncodedTable> {
    let mut bytes = Vec::new();
    let codec = table.properties.codec;
    let point_records = table
        .point_records
        .as_deref()
        .ok_or_else(|| invalid_table("cannot encode table without point records"))?;
    let data_block_metadata = table
        .data_blocks
        .as_deref()
        .ok_or_else(|| invalid_table("cannot encode table without data block metadata"))?;
    let (data_blocks, index_entries) =
        append_data_blocks(&mut bytes, codec, point_records, data_block_metadata)?;
    let range_tombstones =
        append_single_block_section(&mut bytes, codec, &encode_range_tombstone_block(table)?)?;
    let filters = append_single_block_section(&mut bytes, codec, &encode_filter_block(table)?)?;
    let (indexes, index_partitions) = append_index_section(&mut bytes, codec, &index_entries)?;
    let properties = append_single_block_section(
        &mut bytes,
        codec,
        &encode_properties_block(&table.properties)?,
    )?;
    let footer = TableFooter {
        data_blocks,
        range_tombstones,
        filters,
        indexes,
        properties,
    };
    put_footer(&mut bytes, &footer);

    Ok(EncodedTable {
        payload_len: bytes.len(),
        payload: bytes,
        footer,
        data_block_count: index_entries.len(),
        index_partitions,
        pinned_index_partitions: pinned_index_partitions_from_entries(
            &index_entries,
            table.properties.level,
        )?,
    })
}

#[cfg(test)]
pub(in crate::table) fn decode_table(bytes: &[u8]) -> Result<Table> {
    decode_table_bytes(bytes)
}

pub(in crate::table) fn decode_table_bytes(bytes: &[u8]) -> Result<Table> {
    let backend = MemoryStorageBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::RandomRead)?;
    let object = StorageObjectId::memory(StorageObjectKind::Table, "test-table");
    backend.insert_read_object(object.clone(), bytes.to_vec())?;
    let table_object = backend.open_read_blocking(object)?;
    decode_table_from_storage_object(&table_object)
}

#[allow(clippy::too_many_lines)]
pub(in crate::table) fn decode_table_from_storage_object(
    table_object: &impl BlockingStorageReadObject,
) -> Result<Table> {
    let source = StorageReadSource::new(table_object);
    let file_len = table_object.len_blocking()?;
    if file_len < HEADER_LEN as u64 {
        return Err(invalid_table("short header"));
    }

    let mut header = [0_u8; HEADER_LEN];
    source.read_exact_at(0, &mut header)?;
    let magic = read_u32_at(&header, 0)?;
    let version = read_u16_at(&header, 4)?;
    let payload_len = read_u32_at(&header, 6)? as usize;
    limits::ensure_invalid_format_len(
        payload_len,
        limits::MAX_WHOLE_TABLE_DECODE_BYTES,
        "table payload length",
    )?;
    let payload_checksum = read_u32_at(&header, 10)?;
    if magic != TABLE_MAGIC {
        return Err(Error::Corruption {
            message: "table magic mismatch".to_owned(),
        });
    }
    if version != TABLE_VERSION {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported table version {version}"),
        });
    }
    let expected_len = usize_to_u64(
        limits::checked_add_invalid_format(HEADER_LEN, payload_len, "table length")?,
        "table file length",
    )?;
    if file_len != expected_len {
        return Err(Error::Corruption {
            message: "table length mismatch".to_owned(),
        });
    }

    let mut payload = vec![0_u8; payload_len];
    source.read_exact_at(HEADER_LEN, &mut payload)?;
    if checksum(&payload) != payload_checksum {
        return Err(Error::Corruption {
            message: "table checksum mismatch".to_owned(),
        });
    }

    let footer = read_footer_from_source(&source, payload_len)?;
    validate_footer_sections_by_len(payload_len, &footer)?;

    let properties_block =
        read_single_block_section_from_source_shared(&source, payload_len, footer.properties)?;
    let properties_codec = properties_block.codec();
    let properties = decode_properties_block(properties_block.payload())?;
    validate_block_codec(properties_codec, properties.codec, TableSection::Properties)?;

    let (top_level_block, index_block) =
        read_first_block_in_section_from_source_shared(&source, payload_len, footer.indexes)?;
    let index_codec = index_block.codec();
    validate_index_top_level_codec(index_codec, properties.codec)?;
    let index_partitions = decode_index_top_level(index_block.payload())?;
    let data_block_count =
        validate_index_top_level(top_level_block, &index_partitions, footer.indexes)?;

    let (point_records, data_blocks) = decode_point_records_and_blocks_from_source(
        &source,
        payload_len,
        &properties,
        &index_partitions,
        footer.data_blocks,
    )?;

    let tombstone_block = read_single_block_section_from_source_shared(
        &source,
        payload_len,
        footer.range_tombstones,
    )?;
    let tombstone_codec = tombstone_block.codec();
    validate_block_codec(
        tombstone_codec,
        properties.codec,
        TableSection::RangeTombstones,
    )?;
    let range_tombstones = decode_range_tombstone_block(tombstone_block.payload())?;
    let filter_block =
        read_single_block_section_from_source_shared(&source, payload_len, footer.filters)?;
    let filter_codec = filter_block.codec();
    validate_block_codec(filter_codec, properties.codec, TableSection::Filters)?;
    let (point_key_filter, prefix_filter) = decode_filter_block(filter_block.payload())?;
    validate_table_filters(
        point_key_filter.as_ref(),
        prefix_filter.as_ref(),
        &point_records,
    )?;
    if properties
        != table_properties(
            properties.id,
            properties.level,
            properties.codec,
            &point_records,
            &range_tombstones,
        )
    {
        return Err(Error::Corruption {
            message: "table properties do not match encoded records".to_owned(),
        });
    }

    let may_have_range_tombstones = !range_tombstones.is_empty();

    Ok(Table {
        path: None,
        file: None,
        payload_len,
        footer,
        properties,
        point_records: Some(point_records),
        data_block_count,
        index_partitions: index_partitions_for_loaded_blocks(&data_blocks),
        index_partition_cache: Arc::new(RwLock::new(BTreeMap::new())),
        data_blocks: Some(data_blocks),
        range_tombstones: Arc::new(RwLock::new(Some(Arc::new(RangeTombstoneIndex::new(
            range_tombstones,
        ))))),
        may_have_range_tombstones,
        point_key_filter,
        prefix_filter,
        filter_stats: Arc::new(TableFilterStats::default()),
        read_path_stats: Arc::new(TableReadPathStats::default()),
    })
}

pub(in crate::table) fn decode_point_records_and_blocks_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
    properties: &TableProperties,
    index_partitions: &[IndexPartitionEntry],
    data_blocks_section: SectionHandle,
) -> Result<(Vec<TablePointRecord>, Vec<TableDataBlock>)> {
    let mut point_records = Vec::new();
    let mut data_blocks = Vec::new();
    for partition in index_partitions {
        let index_block =
            read_checked_block_from_source_shared(source, payload_len, partition.block)?;
        let index_codec = index_block.codec();
        validate_block_codec(index_codec, properties.codec, TableSection::Indexes)?;
        let index_entries = decode_index_block(index_block.payload())?;
        validate_index_partition(partition, &index_entries, data_blocks_section)?;
        for entry in index_entries {
            let block = read_checked_block_from_source_shared(source, payload_len, entry.block)?;
            let block_codec = block.codec();
            validate_block_codec(block_codec, properties.codec, TableSection::DataBlocks)?;
            let decoded_block = decode_data_block_from_block(block, true)?;
            validate_decoded_data_block_entry(&entry, &decoded_block)?;
            validate_decoded_data_block_filters(&entry, &decoded_block)?;
            let block_records = decoded_block.records_owned()?;
            let record_start = point_records.len();
            point_records.extend(block_records);
            let record_end = point_records.len();
            data_blocks.push(TableDataBlock::from_record_range_and_block(
                &point_records,
                record_start..record_end,
                &decoded_block.restart_indices_with_base(record_start),
                entry.block,
                entry.point_key_filter.clone(),
                entry.prefix_filter.clone(),
            )?);
        }
    }
    validate_sorted_point_records(&point_records)?;
    Ok((point_records, data_blocks))
}

pub(in crate::table) fn append_data_blocks(
    bytes: &mut Vec<u8>,
    codec: CodecId,
    point_records: &[TablePointRecord],
    data_blocks: &[TableDataBlock],
) -> Result<(SectionHandle, Vec<DataBlockIndexEntry>)> {
    let section_start = bytes.len();
    let mut index_entries = Vec::new();

    for data_block in data_blocks {
        let records = point_records
            .get(data_block.record_range.clone())
            .ok_or_else(|| invalid_table("data block record range outside table"))?;
        let block_payload = encode_data_block(records)?;
        let block = BlockManager::append_checked(bytes, codec, &block_payload)?;
        index_entries.push(DataBlockIndexEntry {
            smallest_internal_key: data_block.smallest_internal_key.clone(),
            largest_internal_key: data_block.largest_internal_key.clone(),
            block,
            point_key_filter: data_block.point_key_filter.clone(),
            prefix_filter: data_block.prefix_filter.clone(),
        });
    }

    Ok((
        SectionHandle::from_span(section_start, bytes.len())?,
        index_entries,
    ))
}

pub(in crate::table) fn append_index_section(
    bytes: &mut Vec<u8>,
    codec: CodecId,
    index_entries: &[DataBlockIndexEntry],
) -> Result<(SectionHandle, Vec<IndexPartitionEntry>)> {
    let section_start = bytes.len();
    let partition_payloads = index_entries
        .chunks(INDEX_PARTITION_TARGET_ENTRIES)
        .enumerate()
        .map(|(partition_index, entries)| {
            let payload = encode_index_block(entries)?;
            let block = BlockManager::encode_checked(codec, &payload)?;
            let first_data_block_index = partition_index * INDEX_PARTITION_TARGET_ENTRIES;
            let first = entries
                .first()
                .ok_or_else(|| invalid_table("empty index partition"))?;
            let last = entries
                .last()
                .ok_or_else(|| invalid_table("empty index partition"))?;
            Ok((
                IndexPartitionEntry {
                    smallest_internal_key: first.smallest_internal_key.clone(),
                    largest_internal_key: last.largest_internal_key.clone(),
                    block: BlockHandle { offset: 0, len: 0 },
                    first_data_block_index,
                    data_block_count: entries.len(),
                },
                block,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut partitions = partition_payloads
        .iter()
        .map(|(partition, block)| {
            let mut partition = partition.clone();
            partition.block.len = usize_to_u64(block.len(), "index partition block length")?;
            Ok(partition)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut top_level =
        BlockManager::encode_checked(CodecId::None, &encode_index_top_level(&partitions)?)?;
    let mut next_offset = section_start
        .checked_add(top_level.len())
        .ok_or_else(|| invalid_table("index section offset overflow"))?;
    for (partition, (_, block)) in partitions.iter_mut().zip(&partition_payloads) {
        partition.block.offset = usize_to_u64(next_offset, "index partition block offset")?;
        next_offset = next_offset
            .checked_add(block.len())
            .ok_or_else(|| invalid_table("index section offset overflow"))?;
    }
    // The top-level partition index stores absolute offsets for the following
    // partition blocks. Keep it uncompressed so its byte length is stable after
    // those offsets are filled in; partition blocks still use the table codec.
    top_level = BlockManager::encode_checked(CodecId::None, &encode_index_top_level(&partitions)?)?;
    bytes.extend_from_slice(&top_level);
    for (_, block) in partition_payloads {
        bytes.extend_from_slice(&block);
    }

    Ok((
        SectionHandle::from_span(section_start, bytes.len())?,
        partitions,
    ))
}

pub(in crate::table) fn pinned_index_partitions_from_entries(
    index_entries: &[DataBlockIndexEntry],
    level: TableLevel,
) -> Result<BTreeMap<usize, Arc<Vec<TableDataBlock>>>> {
    let mut pinned = BTreeMap::new();
    if !should_pin_read_metadata(level) {
        return Ok(pinned);
    }

    for (partition_index, entries) in index_entries
        .chunks(INDEX_PARTITION_TARGET_ENTRIES)
        .enumerate()
    {
        let blocks = entries
            .iter()
            .cloned()
            .map(TableDataBlock::from_index_entry)
            .collect::<Result<Vec<_>>>()?;
        pinned.insert(partition_index, Arc::new(blocks));
    }
    Ok(pinned)
}

pub(in crate::table) fn append_single_block_section(
    bytes: &mut Vec<u8>,
    codec: CodecId,
    block_payload: &[u8],
) -> Result<SectionHandle> {
    let section_start = bytes.len();
    BlockManager::append_checked(bytes, codec, block_payload)?;
    SectionHandle::from_span(section_start, bytes.len())
}

pub(in crate::table) fn encode_data_block(records: &[TablePointRecord]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut restart_offsets = Vec::new();
    let point_lookup_index = DataBlockPointLookupIndex::from_records(records)?;
    put_u32(
        &mut bytes,
        usize_to_u32(records.len(), "data block record count")?,
    );

    for (index, record) in records.iter().enumerate() {
        if index % DATA_BLOCK_RESTART_INTERVAL == 0 {
            restart_offsets.push(usize_to_u32(bytes.len(), "data block restart offset")?);
        }
        put_internal_key(&mut bytes, &record.internal_key)?;
        put_value_ref(&mut bytes, record.value.as_ref())?;
    }

    put_u32(
        &mut bytes,
        usize_to_u32(restart_offsets.len(), "data block restart count")?,
    );
    for restart_offset in restart_offsets {
        put_u32(&mut bytes, restart_offset);
    }
    put_data_block_point_lookup_index(&mut bytes, &point_lookup_index)?;

    Ok(bytes)
}

pub(in crate::table) fn put_data_block_point_lookup_index(
    bytes: &mut Vec<u8>,
    index: &DataBlockPointLookupIndex,
) -> Result<()> {
    put_u32(
        bytes,
        usize_to_u32(index.entries.len(), "data block hash index entry count")?,
    );
    for entry in &index.entries {
        put_u64(bytes, entry.key_hash);
        put_u32(bytes, entry.start_record);
        put_u32(bytes, entry.end_record);
    }
    Ok(())
}

pub(in crate::table) fn encode_range_tombstone_block(table: &Table) -> Result<Vec<u8>> {
    let range_tombstones = table.range_tombstones()?;
    let mut bytes = Vec::new();
    put_u32(
        &mut bytes,
        usize_to_u32(
            range_tombstones.all().len(),
            "range tombstone block record count",
        )?,
    );
    for tombstone in range_tombstones.all() {
        put_bound(&mut bytes, &tombstone.range.start)?;
        put_bound(&mut bytes, &tombstone.range.end)?;
        put_u64(&mut bytes, tombstone.sequence.get());
        put_u32(&mut bytes, tombstone.batch_index);
    }
    Ok(bytes)
}

pub(in crate::table) fn encode_filter_block(table: &Table) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_point_key_filter(&mut bytes, table.point_key_filter.as_ref())?;
    put_prefix_filter(&mut bytes, table.prefix_filter.as_ref())?;
    Ok(bytes)
}

pub(in crate::table) fn encode_index_block(
    index_entries: &[DataBlockIndexEntry],
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_u32(
        &mut bytes,
        usize_to_u32(index_entries.len(), "data block index entry count")?,
    );
    for entry in index_entries {
        put_internal_key(&mut bytes, &entry.smallest_internal_key)?;
        put_internal_key(&mut bytes, &entry.largest_internal_key)?;
        put_u64(&mut bytes, entry.block.offset);
        put_u64(&mut bytes, entry.block.len);
        put_point_key_filter(&mut bytes, entry.point_key_filter.as_ref())?;
        put_prefix_filter(&mut bytes, entry.prefix_filter.as_ref())?;
    }
    Ok(bytes)
}

pub(in crate::table) fn encode_index_top_level(
    partitions: &[IndexPartitionEntry],
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_u32(
        &mut bytes,
        usize_to_u32(partitions.len(), "index partition count")?,
    );
    for partition in partitions {
        put_internal_key(&mut bytes, &partition.smallest_internal_key)?;
        put_internal_key(&mut bytes, &partition.largest_internal_key)?;
        put_u64(&mut bytes, partition.block.offset);
        put_u64(&mut bytes, partition.block.len);
        put_u32(
            &mut bytes,
            usize_to_u32(
                partition.first_data_block_index,
                "index partition first data block",
            )?,
        );
        put_u32(
            &mut bytes,
            usize_to_u32(
                partition.data_block_count,
                "index partition data block count",
            )?,
        );
    }
    Ok(bytes)
}

pub(in crate::table) fn put_point_key_filter(
    bytes: &mut Vec<u8>,
    filter: Option<&PointKeyFilter>,
) -> Result<()> {
    match filter {
        None => put_u8(bytes, POINT_KEY_FILTER_ABSENT),
        Some(filter) => {
            put_u8(bytes, POINT_KEY_FILTER_PRESENT);
            put_u64(bytes, filter.bit_count());
            put_u8(bytes, filter.hash_count());
            put_bytes(bytes, filter.bytes())?;
        }
    }

    Ok(())
}

pub(in crate::table) fn put_prefix_filter(
    bytes: &mut Vec<u8>,
    filter: Option<&PrefixFilter>,
) -> Result<()> {
    match filter {
        None => put_u8(bytes, PREFIX_FILTER_ABSENT),
        Some(filter) => {
            put_u8(bytes, PREFIX_FILTER_PRESENT);
            put_prefix_extractor(bytes, filter.extractor())?;
            put_u64(bytes, filter.bit_count());
            put_u8(bytes, filter.hash_count());
            put_bytes(bytes, filter.bytes())?;
        }
    }

    Ok(())
}

pub(in crate::table) fn encode_properties_block(properties: &TableProperties) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_properties(&mut bytes, properties)?;
    Ok(bytes)
}

#[cfg(test)]
pub(in crate::table) fn read_footer(payload: &[u8]) -> Result<TableFooter> {
    if payload.len() < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let footer_start = payload.len() - FOOTER_LEN;
    read_footer_bytes(&payload[footer_start..])
}

pub(in crate::table) fn read_footer_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
) -> Result<TableFooter> {
    if payload_len < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let footer_start = HEADER_LEN
        .checked_add(payload_len - FOOTER_LEN)
        .ok_or_else(|| invalid_table("footer offset overflow"))?;
    let footer = source.read_exact_at_owned(footer_start, FOOTER_LEN)?;
    read_footer_bytes(footer.as_slice())
}

pub(in crate::table) fn read_footer_bytes(footer: &[u8]) -> Result<TableFooter> {
    if footer.len() != FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let stored_checksum = read_u32_at(footer, FOOTER_LEN - 4)?;
    if checksum(&footer[..FOOTER_LEN - 4]) != stored_checksum {
        return Err(Error::Corruption {
            message: "table footer checksum mismatch".to_owned(),
        });
    }

    let mut cursor = Cursor::new(footer);
    let magic = cursor.read_u32()?;
    let version = cursor.read_u16()?;
    if magic != FOOTER_MAGIC {
        return Err(Error::Corruption {
            message: "table footer magic mismatch".to_owned(),
        });
    }
    if version != TABLE_VERSION {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported table footer version {version}"),
        });
    }

    let footer = TableFooter {
        data_blocks: cursor.read_section_handle()?,
        range_tombstones: cursor.read_section_handle()?,
        filters: cursor.read_section_handle()?,
        indexes: cursor.read_section_handle()?,
        properties: cursor.read_section_handle()?,
    };
    let _footer_checksum = cursor.read_u32()?;
    if !cursor.is_finished() {
        return Err(invalid_table("trailing footer bytes"));
    }

    Ok(footer)
}

pub(in crate::table) fn put_footer(bytes: &mut Vec<u8>, footer: &TableFooter) {
    let mut footer_bytes = Vec::with_capacity(FOOTER_LEN);
    put_u32(&mut footer_bytes, FOOTER_MAGIC);
    put_u16(&mut footer_bytes, TABLE_VERSION);
    put_section_handle(&mut footer_bytes, footer.data_blocks);
    put_section_handle(&mut footer_bytes, footer.range_tombstones);
    put_section_handle(&mut footer_bytes, footer.filters);
    put_section_handle(&mut footer_bytes, footer.indexes);
    put_section_handle(&mut footer_bytes, footer.properties);
    let footer_checksum = checksum(&footer_bytes);
    put_u32(&mut footer_bytes, footer_checksum);
    debug_assert_eq!(footer_bytes.len(), FOOTER_LEN);
    bytes.extend_from_slice(&footer_bytes);
}

pub(in crate::table) const fn empty_footer() -> TableFooter {
    let empty = SectionHandle { offset: 0, len: 0 };
    TableFooter {
        data_blocks: empty,
        range_tombstones: empty,
        filters: empty,
        indexes: empty,
        properties: empty,
    }
}

pub(in crate::table) fn validate_footer_sections_by_len(
    payload_len: usize,
    footer: &TableFooter,
) -> Result<()> {
    let footer_start = payload_len - FOOTER_LEN;
    let mut expected_start = 0_usize;
    for section in [
        footer.data_blocks,
        footer.range_tombstones,
        footer.filters,
        footer.indexes,
        footer.properties,
    ] {
        let (section_start, section_end) = section_bounds(section)?;
        if section_start != expected_start || section_end > footer_start {
            return Err(Error::Corruption {
                message: "table section layout is inconsistent".to_owned(),
            });
        }
        expected_start = section_end;
    }
    if expected_start != footer_start {
        return Err(Error::Corruption {
            message: "table footer does not cover all section bytes".to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
pub(in crate::table) fn read_first_block_in_section(
    payload: &[u8],
    section: SectionHandle,
) -> Result<(BlockHandle, CodecId, Vec<u8>)> {
    let (section_start, section_end) = section_bounds(section)?;
    if section.len == 0 {
        return Err(invalid_table("empty block section"));
    }
    let (block, codec, decoded) = read_checked_block_at_payload_offset(payload, section_start)?;
    let (_, block_end) = block_bounds(block)?;
    if block_end > section_end {
        return Err(Error::Corruption {
            message: "section block length mismatch".to_owned(),
        });
    }
    Ok((block, codec, decoded))
}

pub(in crate::table) fn read_single_block_section_from_file_shared(
    path: &Path,
    file: Option<&NativeFileObject>,
    payload_len: usize,
    section: SectionHandle,
) -> Result<DecodedBlock> {
    let source = table_read_source(path, file);
    read_single_block_section_from_source_shared(&source, payload_len, section)
}

pub(in crate::table) async fn read_single_block_section_from_file_shared_async(
    path: &Path,
    file: Option<&NativeFileObject>,
    payload_len: usize,
    section: SectionHandle,
) -> Result<DecodedBlock> {
    if let Some(file) = file {
        return read_single_block_section_from_storage_object_shared_async(
            file,
            payload_len,
            section,
        )
        .await;
    }

    read_single_block_section_from_file_shared(path, None, payload_len, section)
}

pub(in crate::table) fn read_single_block_section_from_source_shared(
    source: &impl BlockReadSource,
    payload_len: usize,
    section: SectionHandle,
) -> Result<DecodedBlock> {
    if payload_len < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let (_, section_end) = section_bounds(section)?;
    if section.len == 0 {
        return Err(invalid_table("empty single-block section"));
    }
    if section_end > payload_len - FOOTER_LEN {
        return Err(Error::Corruption {
            message: "table section layout is inconsistent".to_owned(),
        });
    }
    let block = BlockHandle {
        offset: section.offset,
        len: section.len,
    };
    let (_, block_end) = block_bounds(block)?;
    if block_end != section_end {
        return Err(Error::Corruption {
            message: "section block length mismatch".to_owned(),
        });
    }
    read_checked_block_from_source_shared(source, payload_len, block)
}

pub(in crate::table) async fn read_single_block_section_from_storage_object_shared_async(
    object: &impl StorageReadObject,
    payload_len: usize,
    section: SectionHandle,
) -> Result<DecodedBlock> {
    if payload_len < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let (_, section_end) = section_bounds(section)?;
    if section.len == 0 {
        return Err(invalid_table("empty single-block section"));
    }
    if section_end > payload_len - FOOTER_LEN {
        return Err(Error::Corruption {
            message: "table section layout is inconsistent".to_owned(),
        });
    }
    let block = BlockHandle {
        offset: section.offset,
        len: section.len,
    };
    let (_, block_end) = block_bounds(block)?;
    if block_end != section_end {
        return Err(Error::Corruption {
            message: "section block length mismatch".to_owned(),
        });
    }
    read_checked_block_from_storage_object_shared_async(object, payload_len, block).await
}

pub(in crate::table) fn read_first_block_in_section_from_source_shared(
    source: &impl BlockReadSource,
    payload_len: usize,
    section: SectionHandle,
) -> Result<(BlockHandle, DecodedBlock)> {
    if payload_len < FOOTER_LEN {
        return Err(invalid_table("short footer"));
    }
    let (section_start, section_end) = section_bounds(section)?;
    if section.len == 0 {
        return Err(invalid_table("empty block section"));
    }
    if section_end > payload_len - FOOTER_LEN {
        return Err(Error::Corruption {
            message: "table section layout is inconsistent".to_owned(),
        });
    }
    let (block, decoded_block) =
        read_checked_block_at_source_offset_shared(source, payload_len, section_start)?;
    let (_, block_end) = block_bounds(block)?;
    if block_end > section_end {
        return Err(Error::Corruption {
            message: "section block length mismatch".to_owned(),
        });
    }
    Ok((block, decoded_block))
}

#[cfg(test)]
pub(in crate::table) fn read_checked_block(
    payload: &[u8],
    block: BlockHandle,
) -> Result<(CodecId, Vec<u8>)> {
    BlockManager::read_checked(payload, block)
}

#[cfg(test)]
pub(in crate::table) fn read_checked_block_at_payload_offset(
    payload: &[u8],
    offset: usize,
) -> Result<(BlockHandle, CodecId, Vec<u8>)> {
    BlockManager::read_checked_at_payload_offset(payload, offset)
}

#[cfg(test)]
pub(in crate::table) fn read_checked_block_from_file(
    path: &Path,
    file: Option<&NativeFileObject>,
    payload_len: usize,
    block: BlockHandle,
) -> Result<(CodecId, Vec<u8>)> {
    let source = table_read_source(path, file);
    read_checked_block_from_source(&source, payload_len, block)
}

#[cfg(test)]
pub(in crate::table) fn read_checked_block_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
    block: BlockHandle,
) -> Result<(CodecId, Vec<u8>)> {
    BlockManager::read_checked_from_source(payload_len, HEADER_LEN, block, source)
}

pub(in crate::table) fn read_checked_block_from_source_shared(
    source: &impl BlockReadSource,
    payload_len: usize,
    block: BlockHandle,
) -> Result<DecodedBlock> {
    BlockManager::read_checked_from_source_shared(payload_len, HEADER_LEN, block, source)
}

pub(in crate::table) fn read_checked_block_at_source_offset_shared(
    source: &impl BlockReadSource,
    payload_len: usize,
    offset: usize,
) -> Result<(BlockHandle, DecodedBlock)> {
    BlockManager::read_checked_at_source_offset_shared(payload_len, HEADER_LEN, offset, source)
}

pub(in crate::table) fn read_data_block_from_file(
    path: &Path,
    file: Option<&NativeFileObject>,
    payload_len: usize,
    expected_codec: CodecId,
    entry: &DataBlockIndexEntry,
) -> Result<DecodedDataBlock> {
    let source = table_read_source(path, file);
    read_data_block_from_source(&source, payload_len, expected_codec, entry)
}

pub(in crate::table) async fn read_data_block_from_file_async(
    path: &Path,
    file: Option<&NativeFileObject>,
    payload_len: usize,
    expected_codec: CodecId,
    entry: &DataBlockIndexEntry,
) -> Result<DecodedDataBlock> {
    if let Some(file) = file {
        return read_data_block_from_storage_object_async(file, payload_len, expected_codec, entry)
            .await;
    }

    read_data_block_from_file(path, None, payload_len, expected_codec, entry)
}

pub(in crate::table) fn read_data_block_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
    expected_codec: CodecId,
    entry: &DataBlockIndexEntry,
) -> Result<DecodedDataBlock> {
    let block = read_checked_block_from_source_shared(source, payload_len, entry.block)?;
    let actual_codec = block.codec();
    validate_block_codec(actual_codec, expected_codec, TableSection::DataBlocks)?;
    let decoded = decode_data_block_from_block(block, true)?;
    validate_decoded_data_block_entry(entry, &decoded)?;
    validate_decoded_data_block_filters(entry, &decoded)?;
    Ok(decoded)
}

pub(in crate::table) async fn read_data_block_from_storage_object_async(
    object: &impl StorageReadObject,
    payload_len: usize,
    expected_codec: CodecId,
    entry: &DataBlockIndexEntry,
) -> Result<DecodedDataBlock> {
    let block =
        read_checked_block_from_storage_object_shared_async(object, payload_len, entry.block)
            .await?;
    let actual_codec = block.codec();
    validate_block_codec(actual_codec, expected_codec, TableSection::DataBlocks)?;
    let decoded = decode_data_block_from_block(block, true)?;
    validate_decoded_data_block_entry(entry, &decoded)?;
    validate_decoded_data_block_filters(entry, &decoded)?;
    Ok(decoded)
}

pub(in crate::table) async fn read_checked_block_from_storage_object_shared_async(
    object: &impl StorageReadObject,
    payload_len: usize,
    block: BlockHandle,
) -> Result<DecodedBlock> {
    let (start, end) = block_bounds(block)?;
    if end > payload_len {
        return Err(invalid_table("block outside table payload"));
    }

    let source_offset = HEADER_LEN
        .checked_add(start)
        .ok_or_else(|| invalid_table("block file offset overflow"))?;
    let block_bytes = object
        .read_exact_at_owned(source_offset, end - start)
        .await?;
    BlockManager::decode_checked_owned(block_bytes)
}
