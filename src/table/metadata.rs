use super::{
    Arc, BTreeMap, BTreeSet, BlockHandle, BlockReadSource, BlockingStorageObjectListBackend,
    BlockingStorageObjectWriteBackend, BlockingStorageReadBackend, BlockingStorageReadObject,
    Bound, BufferedBlockReadSource, CodecId, DATA_BLOCK_RESTART_INTERVAL, DurabilityMode,
    EncodedTable, Error, FilterDepthCurve, FilterPolicy, HEADER_LEN,
    INDEX_PARTITION_TARGET_ENTRIES, INLINE_VALUE_HEADER_BYTES, IndexPartitionEntry, InternalKey,
    MIN_INTERNAL_KEY_BYTES, NativeFileBackend, NativeFileObject, NativeFileReadSource,
    PINNED_READ_METADATA_MAX_LEVEL, Path, PathBuf, PointKeyFilter, PrefixFilter,
    PrefixFilterPolicy, RangeTombstoneIndex, Result, RwLock, SectionHandle, Sequence,
    StorageCapability, StorageObjectId, StorageObjectKind, StorageObjectListBackend,
    StorageObjectListRequest, StorageObjectWriteBackend, StorageReadBackend, StorageReadObject,
    StorageReadSource, TABLE_FILE_EXTENSION, TABLE_MAGIC, TABLE_VERSION, Table, TableBlobReference,
    TableDataBlock, TableFilterStats, TableId, TableLevel, TablePointRecord, TableProperties,
    TableRangeTombstone, TableReadPathStats, TableSection, TableWriteOptions, ValueRef,
    WHOLE_TABLE_SYNC_OPEN_MAX_BYTES, checksum, decode_filter_block, decode_index_block,
    decode_index_top_level, decode_properties_block, decode_table_bytes, empty_footer,
    encode_table_for_write, invalid_table, limits, point_record_encoded_len,
    read_checked_block_from_source_shared, read_checked_block_from_storage_object_shared_async,
    read_data_block_from_source, read_first_block_in_section_from_source_shared,
    read_footer_from_source, read_single_block_section_from_source_shared, read_u16_at,
    read_u32_at, sort_point_records_if_needed, usize_to_u64, validate_block_codec,
    validate_footer_sections_by_len, validate_index_partition, validate_index_top_level,
    validate_index_top_level_codec, validate_table_filters_for_key,
};

#[must_use]
pub fn table_path(db_path: &Path, table_id: TableId) -> PathBuf {
    db_path.join(format!(
        "table-{id:020}.{TABLE_FILE_EXTENSION}",
        id = table_id.get()
    ))
}

#[allow(dead_code)]
pub(crate) fn list_table_file_ids(db_path: &Path) -> Result<BTreeSet<TableId>> {
    let backend = table_storage_backend();
    list_table_file_ids_with_backend(&backend, db_path)
}

pub(crate) fn list_table_file_ids_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<BTreeSet<TableId>> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectListing)?;
    let request = StorageObjectListRequest::native_file(StorageObjectKind::Table, db_path)
        .with_file_extension(TABLE_FILE_EXTENSION);
    table_file_ids_from_objects(backend.list_objects_blocking(request)?)
}

#[allow(dead_code)]
pub(crate) async fn list_table_file_ids_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
) -> Result<BTreeSet<TableId>>
where
    B: StorageObjectListBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::ObjectListing)?;
    let request = StorageObjectListRequest::native_file(StorageObjectKind::Table, db_path)
        .with_file_extension(TABLE_FILE_EXTENSION);
    table_file_ids_from_objects(backend.list_objects(request).await?)
}

pub(super) fn table_file_ids_from_objects(
    objects: impl IntoIterator<Item = StorageObjectId>,
) -> Result<BTreeSet<TableId>> {
    let mut table_ids = BTreeSet::new();
    for object in objects {
        if let Some(table_id) = table_file_id_from_path(object.path())? {
            table_ids.insert(table_id);
        }
    }

    Ok(table_ids)
}

pub(crate) fn table_file_id_from_path(path: &Path) -> Result<Option<TableId>> {
    let has_table_extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case(TABLE_FILE_EXTENSION));
    if !has_table_extension {
        return Ok(None);
    }

    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return Ok(None);
    };
    let Some(table_id) = stem.strip_prefix("table-") else {
        return Ok(None);
    };
    table_id
        .parse::<u64>()
        .map(|id| Some(TableId(id)))
        .map_err(|_| Error::Corruption {
            message: format!("invalid table file name: {}", path.display()),
        })
}

#[allow(dead_code)]
pub(crate) fn write_table(
    path: &Path,
    table_id: TableId,
    level: TableLevel,
    options: &TableWriteOptions,
    point_records: &[(InternalKey, Option<ValueRef>)],
    range_tombstones: &[TableRangeTombstone],
) -> Result<Table> {
    let backend = table_storage_backend();
    write_table_with_backend(
        &backend,
        path,
        table_id,
        level,
        options,
        point_records,
        range_tombstones,
    )
}

pub(crate) fn write_table_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
    table_id: TableId,
    level: TableLevel,
    options: &TableWriteOptions,
    point_records: &[(InternalKey, Option<ValueRef>)],
    range_tombstones: &[TableRangeTombstone],
) -> Result<Table> {
    write_table_with_backend_with_durability(
        backend,
        path,
        table_id,
        level,
        options,
        point_records,
        range_tombstones,
        DurabilityMode::SyncAll,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_table_with_backend_with_durability(
    backend: &NativeFileBackend,
    path: &Path,
    table_id: TableId,
    level: TableLevel,
    options: &TableWriteOptions,
    point_records: &[(InternalKey, Option<ValueRef>)],
    range_tombstones: &[TableRangeTombstone],
    durability: DurabilityMode,
) -> Result<Table> {
    if point_records.is_empty() && range_tombstones.is_empty() {
        return Err(Error::invalid_options("cannot write an empty table"));
    }

    // The caller batches the parent-directory sync after one or more table
    // writes and before publishing the manifest. That keeps table/blob file
    // names durable without forcing one directory sync per output file.
    let mut point_records = point_records.to_vec();
    sort_point_records_if_needed(&mut point_records);
    let db_path = path
        .parent()
        .ok_or_else(|| Error::invalid_options("table path has no parent"))?;
    let point_records = if options.rewrite_blob_indexes {
        // Level Merge keeps the same MVCC records but gives retained large
        // values a fresh blob layout beside the output table.
        crate::blob::inline_blob_values_with_backend(backend, db_path, &point_records)?
    } else {
        point_records
    };
    let point_records = crate::blob::write_large_values_with_backend_with_durability(
        backend,
        db_path,
        table_id.get(),
        effective_blob_threshold_bytes(options.blob_threshold_bytes),
        CodecId::None,
        &point_records,
        durability,
    )?
    .into_iter()
    .map(|(internal_key, value)| TablePointRecord {
        internal_key,
        value,
    })
    .collect::<Vec<_>>();
    let data_blocks = build_data_blocks(&point_records, options, level)?;
    let (point_key_filter, prefix_filter) = if should_pin_read_metadata(level) {
        (
            build_point_key_filter(options, &point_records, level),
            build_prefix_filter(options, &point_records, level),
        )
    } else {
        (None, None)
    };

    let table = Table {
        path: None,
        file: None,
        payload_len: 0,
        footer: empty_footer(),
        properties: table_properties(
            table_id,
            level,
            options.codec,
            &point_records,
            range_tombstones,
        ),
        point_key_filter,
        prefix_filter,
        filter_stats: Arc::new(TableFilterStats::default()),
        read_path_stats: Arc::new(TableReadPathStats::default()),
        point_records: Some(point_records),
        data_block_count: data_blocks.len(),
        index_partitions: index_partitions_for_loaded_blocks(&data_blocks),
        index_partition_cache: Arc::new(RwLock::new(BTreeMap::new())),
        data_blocks: Some(data_blocks),
        range_tombstones: Arc::new(RwLock::new(Some(Arc::new(RangeTombstoneIndex::new(
            range_tombstones.to_vec(),
        ))))),
        may_have_range_tombstones: !range_tombstones.is_empty(),
    };
    let encoded = encode_table_for_write(&table)?;
    let payload_len = u32::try_from(encoded.payload_len)
        .map_err(|_| Error::invalid_options("table payload exceeds u32::MAX"))?;
    let payload_checksum = checksum(&encoded.payload);
    let mut bytes = Vec::with_capacity(HEADER_LEN + encoded.payload_len);

    bytes.extend_from_slice(&TABLE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&TABLE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    bytes.extend_from_slice(&encoded.payload);

    backend
        .capabilities()
        .require(StorageCapability::ObjectWrite)?;
    let object = table_storage_object(path);
    backend.write_object_blocking(
        object.clone(),
        Arc::from(bytes.into_boxed_slice()),
        durability,
    )?;
    backend
        .capabilities()
        .require(StorageCapability::RandomRead)?;
    let file = Arc::new(backend.open_read_blocking(object)?);

    Ok(written_table_metadata(path, Some(file), table, encoded))
}

pub(super) fn written_table_metadata(
    path: &Path,
    file: Option<Arc<NativeFileObject>>,
    table: Table,
    encoded: EncodedTable,
) -> Table {
    Table {
        path: Some(path.to_path_buf()),
        file,
        payload_len: encoded.payload_len,
        footer: encoded.footer,
        properties: table.properties,
        point_records: None,
        data_blocks: None,
        data_block_count: encoded.data_block_count,
        index_partitions: encoded.index_partitions,
        index_partition_cache: Arc::new(RwLock::new(encoded.pinned_index_partitions)),
        range_tombstones: table.range_tombstones,
        may_have_range_tombstones: table.may_have_range_tombstones,
        point_key_filter: table.point_key_filter,
        prefix_filter: table.prefix_filter,
        filter_stats: table.filter_stats,
        read_path_stats: table.read_path_stats,
    }
}

#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_table_with_backend_async<B>(
    backend: &B,
    path: &Path,
    table_id: TableId,
    level: TableLevel,
    options: &TableWriteOptions,
    point_records: &[(InternalKey, Option<ValueRef>)],
    range_tombstones: &[TableRangeTombstone],
    durability: DurabilityMode,
) -> Result<Table>
where
    B: StorageObjectWriteBackend,
{
    if point_records.is_empty() && range_tombstones.is_empty() {
        return Err(Error::invalid_options("cannot write an empty table"));
    }

    let mut point_records = point_records.to_vec();
    sort_point_records_if_needed(&mut point_records);
    let db_path = path
        .parent()
        .ok_or_else(|| Error::invalid_options("table path has no parent"))?;
    let point_records = if options.rewrite_blob_indexes {
        crate::blob::inline_blob_values_with_backend_async(backend, db_path, &point_records).await?
    } else {
        point_records
    };
    let point_records = crate::blob::write_large_values_with_backend_async(
        backend,
        db_path,
        table_id.get(),
        effective_blob_threshold_bytes(options.blob_threshold_bytes),
        CodecId::None,
        &point_records,
        durability,
    )
    .await?
    .into_iter()
    .map(|(internal_key, value)| TablePointRecord {
        internal_key,
        value,
    })
    .collect::<Vec<_>>();
    let data_blocks = build_data_blocks(&point_records, options, level)?;
    let (point_key_filter, prefix_filter) = if should_pin_read_metadata(level) {
        (
            build_point_key_filter(options, &point_records, level),
            build_prefix_filter(options, &point_records, level),
        )
    } else {
        (None, None)
    };

    let mut table = Table {
        path: None,
        file: None,
        payload_len: 0,
        footer: empty_footer(),
        properties: table_properties(
            table_id,
            level,
            options.codec,
            &point_records,
            range_tombstones,
        ),
        point_key_filter,
        prefix_filter,
        filter_stats: Arc::new(TableFilterStats::default()),
        read_path_stats: Arc::new(TableReadPathStats::default()),
        point_records: Some(point_records),
        data_block_count: data_blocks.len(),
        index_partitions: index_partitions_for_loaded_blocks(&data_blocks),
        index_partition_cache: Arc::new(RwLock::new(BTreeMap::new())),
        data_blocks: Some(data_blocks),
        range_tombstones: Arc::new(RwLock::new(Some(Arc::new(RangeTombstoneIndex::new(
            range_tombstones.to_vec(),
        ))))),
        may_have_range_tombstones: !range_tombstones.is_empty(),
    };
    let encoded = encode_table_for_write(&table)?;
    table.payload_len = encoded.payload_len;
    table.footer = encoded.footer.clone();
    let payload_len = u32::try_from(encoded.payload_len)
        .map_err(|_| Error::invalid_options("table payload exceeds u32::MAX"))?;
    let payload_checksum = checksum(&encoded.payload);
    let mut bytes = Vec::with_capacity(HEADER_LEN + encoded.payload_len);

    bytes.extend_from_slice(&TABLE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&TABLE_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    bytes.extend_from_slice(&encoded.payload);

    backend
        .capabilities()
        .require(StorageCapability::ObjectWrite)?;
    let object = table_storage_object(path);
    backend
        .write_object(object, Arc::from(bytes.into_boxed_slice()), durability)
        .await?;

    Ok(table)
}

#[allow(dead_code)]
pub(crate) fn read_table(path: &Path) -> Result<Table> {
    let backend = table_storage_backend();
    read_table_with_backend(&backend, path)
}

pub(crate) fn read_table_with_backend(backend: &NativeFileBackend, path: &Path) -> Result<Table> {
    backend
        .capabilities()
        .require(StorageCapability::RandomRead)?;
    let object = table_storage_object(path);
    let table_file = Arc::new(backend.open_read_blocking(object)?);
    let file_len = table_file.len_blocking()?;
    if file_len <= WHOLE_TABLE_SYNC_OPEN_MAX_BYTES {
        let len = usize::try_from(file_len)
            .map_err(|_| Error::invalid_options("table file length exceeds usize"))?;
        let bytes = table_file
            .read_exact_at_owned_blocking(0, len)
            .map_err(|error| Error::Corruption {
                message: format!(
                    "referenced table {} cannot be read: {error}",
                    path.display()
                ),
            })?;
        let source = BufferedBlockReadSource {
            bytes: bytes.as_slice(),
        };
        return read_table_metadata_from_source(path, &source, Some(table_file), file_len);
    }

    let source = StorageReadSource::new(table_file.as_ref());
    read_table_metadata_from_source(path, &source, Some(Arc::clone(&table_file)), file_len)
}

pub(super) fn read_table_metadata_from_source(
    path: &Path,
    source: &impl BlockReadSource,
    file: Option<Arc<NativeFileObject>>,
    file_len: u64,
) -> Result<Table> {
    let header = read_table_header(path, source)?;
    let magic = read_u32_at(&header, 0)?;
    let version = read_u16_at(&header, 4)?;
    let payload_len = read_u32_at(&header, 6)? as usize;
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
        HEADER_LEN
            .checked_add(payload_len)
            .ok_or_else(|| invalid_table("table length overflow"))?,
        "table file length",
    )?;
    if file_len != expected_len {
        return Err(Error::Corruption {
            message: "table length mismatch".to_owned(),
        });
    }

    let footer = read_footer_from_source(source, payload_len)?;
    validate_footer_sections_by_len(payload_len, &footer)?;

    let properties_block =
        read_single_block_section_from_source_shared(source, payload_len, footer.properties)?;
    let properties_codec = properties_block.codec();
    let properties = decode_properties_block(properties_block.payload())?;
    validate_block_codec(properties_codec, properties.codec, TableSection::Properties)?;

    let (top_level_block, index_block) =
        read_first_block_in_section_from_source_shared(source, payload_len, footer.indexes)?;
    let index_codec = index_block.codec();
    validate_index_top_level_codec(index_codec, properties.codec)?;
    let index_partitions = decode_index_top_level(index_block.payload())?;
    let data_block_count =
        validate_index_top_level(top_level_block, &index_partitions, footer.indexes)?;
    let (point_key_filter, prefix_filter) =
        read_pinned_table_filters(source, payload_len, footer.filters, &properties)?;
    validate_pinned_table_filter_coverage(
        source,
        payload_len,
        footer.data_blocks,
        properties.codec,
        &index_partitions,
        point_key_filter.as_ref(),
        prefix_filter.as_ref(),
    )?;
    let index_partition_cache = Arc::new(RwLock::new(read_pinned_index_partitions(
        source,
        payload_len,
        footer.data_blocks,
        &index_partitions,
        properties.codec,
        properties.level,
    )?));
    Ok(Table {
        path: Some(path.to_path_buf()),
        file,
        payload_len,
        footer,
        properties,
        point_records: None,
        data_blocks: None,
        data_block_count,
        index_partitions,
        index_partition_cache,
        range_tombstones: Arc::new(RwLock::new(None)),
        may_have_range_tombstones: true,
        point_key_filter,
        prefix_filter,
        filter_stats: Arc::new(TableFilterStats::default()),
        read_path_stats: Arc::new(TableReadPathStats::default()),
    })
}

#[allow(dead_code)]
pub(crate) async fn read_table_with_backend_async<B>(backend: &B, path: &Path) -> Result<Table>
where
    B: StorageReadBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::RandomRead)?;
    let object = table_storage_object(path);
    let table_object = backend
        .open_read(object)
        .await
        .map_err(|error| Error::Corruption {
            message: format!(
                "referenced table {} cannot be opened: {error}",
                path.display()
            ),
        })?;
    let file_len = table_object
        .len()
        .await
        .map_err(|error| Error::Corruption {
            message: format!(
                "referenced table {} metadata cannot be read: {error}",
                path.display()
            ),
        })?;
    let file_len = usize::try_from(file_len)
        .map_err(|_| Error::invalid_options("table file length exceeds usize"))?;
    limits::ensure_corruption_len(
        file_len,
        HEADER_LEN + limits::MAX_WHOLE_TABLE_DECODE_BYTES,
        "table file length",
    )?;
    let bytes = table_object
        .read_exact_at_owned(0, file_len)
        .await
        .map_err(|error| Error::Corruption {
            message: format!(
                "referenced table {} cannot be read: {error}",
                path.display()
            ),
        })?;
    decode_table_bytes(bytes.as_slice())
}

#[allow(dead_code)]
pub(crate) async fn inline_blob_values_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    mut table: Table,
) -> Result<Table>
where
    B: StorageReadBackend,
{
    if table.properties.blob_file_ids().is_empty() {
        return Ok(table);
    }

    let point_records = table
        .point_records
        .take()
        .ok_or_else(|| Error::Corruption {
            message: "table point records must be loaded before async blob inline".to_owned(),
        })?;
    let mut rewritten = Vec::with_capacity(point_records.len());
    for mut record in point_records {
        if let Some(value @ (ValueRef::BlobIndex(_) | ValueRef::Blob { .. })) =
            record.value.as_ref()
        {
            let bytes = crate::blob::read_value_for_internal_key_with_backend_async(
                backend,
                db_path,
                value,
                Some(&record.internal_key),
            )
            .await?;
            record.value = Some(ValueRef::Inline(bytes));
        }
        rewritten.push(record);
    }
    table.point_records = Some(rewritten);
    Ok(table)
}

pub(super) fn read_table_header(
    path: &Path,
    source: &impl BlockReadSource,
) -> Result<[u8; HEADER_LEN]> {
    let header = source
        .read_exact_at_owned(0, HEADER_LEN)
        .map_err(|error| Error::Corruption {
            message: format!(
                "referenced table {} header cannot be read: {error}",
                path.display()
            ),
        })?
        .into_bytes();
    header
        .as_ref()
        .try_into()
        .map_err(|_| invalid_table("short table header"))
}

pub(super) fn table_read_source<'src>(
    path: &Path,
    file: Option<&'src NativeFileObject>,
) -> NativeFileReadSource<'src, NativeFileObject> {
    NativeFileReadSource::new(table_storage_object(path), file)
}

pub(super) fn table_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::Table, path)
}

pub(super) fn table_storage_backend() -> NativeFileBackend {
    NativeFileBackend::new()
}

pub(super) fn read_pinned_table_filters(
    source: &impl BlockReadSource,
    payload_len: usize,
    filter_section: SectionHandle,
    properties: &TableProperties,
) -> Result<(Option<PointKeyFilter>, Option<PrefixFilter>)> {
    if !should_pin_read_metadata(properties.level) {
        return Ok((None, None));
    }

    let filter_block =
        read_single_block_section_from_source_shared(source, payload_len, filter_section)?;
    let filter_codec = filter_block.codec();
    validate_block_codec(filter_codec, properties.codec, TableSection::Filters)?;
    decode_filter_block(filter_block.payload())
}

pub(super) fn validate_pinned_table_filter_coverage(
    source: &impl BlockReadSource,
    payload_len: usize,
    data_blocks_section: SectionHandle,
    expected_codec: CodecId,
    partitions: &[IndexPartitionEntry],
    point_filter: Option<&PointKeyFilter>,
    prefix_filter: Option<&PrefixFilter>,
) -> Result<()> {
    if point_filter.is_none() && prefix_filter.is_none() {
        return Ok(());
    }

    for partition in partitions {
        let index_block =
            read_checked_block_from_source_shared(source, payload_len, partition.block)?;
        let index_codec = index_block.codec();
        validate_block_codec(index_codec, expected_codec, TableSection::Indexes)?;
        let index_entries = decode_index_block(index_block.payload())?;
        validate_index_partition(partition, &index_entries, data_blocks_section)?;
        for entry in index_entries {
            let block = read_data_block_from_source(source, payload_len, expected_codec, &entry)?;
            for record_index in 0..block.record_count() {
                let record = block.record_view(record_index)?;
                validate_table_filters_for_key(point_filter, prefix_filter, record.user_key)?;
            }
        }
    }

    Ok(())
}

pub(super) fn read_pinned_index_partitions(
    source: &impl BlockReadSource,
    payload_len: usize,
    data_blocks_section: SectionHandle,
    partitions: &[IndexPartitionEntry],
    expected_codec: CodecId,
    level: TableLevel,
) -> Result<BTreeMap<usize, Arc<Vec<TableDataBlock>>>> {
    let mut pinned = BTreeMap::new();
    if !should_pin_read_metadata(level) {
        return Ok(pinned);
    }

    for (partition_index, partition) in partitions.iter().enumerate() {
        let entries = read_index_partition_from_source(
            source,
            payload_len,
            data_blocks_section,
            expected_codec,
            partition,
        )?;
        pinned.insert(partition_index, Arc::new(entries));
    }
    Ok(pinned)
}

pub(super) fn read_index_partition_from_source(
    source: &impl BlockReadSource,
    payload_len: usize,
    data_blocks_section: SectionHandle,
    expected_codec: CodecId,
    partition: &IndexPartitionEntry,
) -> Result<Vec<TableDataBlock>> {
    let block = read_checked_block_from_source_shared(source, payload_len, partition.block)?;
    let codec = block.codec();
    validate_block_codec(codec, expected_codec, TableSection::Indexes)?;
    let entries = decode_index_block(block.payload())?;
    validate_index_partition(partition, &entries, data_blocks_section)?;
    entries
        .into_iter()
        .map(TableDataBlock::from_index_entry)
        .collect()
}

pub(super) async fn read_index_partition_from_file_async(
    path: &Path,
    file: Option<&NativeFileObject>,
    payload_len: usize,
    data_blocks_section: SectionHandle,
    expected_codec: CodecId,
    partition: &IndexPartitionEntry,
) -> Result<Vec<TableDataBlock>> {
    if let Some(file) = file {
        return read_index_partition_from_storage_object_async(
            file,
            payload_len,
            data_blocks_section,
            expected_codec,
            partition,
        )
        .await;
    }

    let source = table_read_source(path, None);
    read_index_partition_from_source(
        &source,
        payload_len,
        data_blocks_section,
        expected_codec,
        partition,
    )
}

pub(super) async fn read_index_partition_from_storage_object_async(
    object: &impl StorageReadObject,
    payload_len: usize,
    data_blocks_section: SectionHandle,
    expected_codec: CodecId,
    partition: &IndexPartitionEntry,
) -> Result<Vec<TableDataBlock>> {
    let block =
        read_checked_block_from_storage_object_shared_async(object, payload_len, partition.block)
            .await?;
    let codec = block.codec();
    validate_block_codec(codec, expected_codec, TableSection::Indexes)?;
    let entries = decode_index_block(block.payload())?;
    validate_index_partition(partition, &entries, data_blocks_section)?;
    entries
        .into_iter()
        .map(TableDataBlock::from_index_entry)
        .collect()
}

pub(super) const fn should_pin_read_metadata(level: TableLevel) -> bool {
    level.get() <= PINNED_READ_METADATA_MAX_LEVEL
}

pub(super) fn table_properties(
    table_id: TableId,
    level: TableLevel,
    codec: CodecId,
    point_records: &[TablePointRecord],
    range_tombstones: &[TableRangeTombstone],
) -> TableProperties {
    let mut smallest_sequence: Option<Sequence> = None;
    let mut largest_sequence: Option<Sequence> = None;

    for sequence in point_records
        .iter()
        .map(|record| record.internal_key.sequence())
        .chain(range_tombstones.iter().map(|tombstone| tombstone.sequence))
    {
        smallest_sequence =
            Some(smallest_sequence.map_or(sequence, |current| std::cmp::min(current, sequence)));
        largest_sequence =
            Some(largest_sequence.map_or(sequence, |current| std::cmp::max(current, sequence)));
    }

    let blob_references = table_blob_references(point_records);
    let blob_file_ids = blob_references
        .iter()
        .map(|reference| reference.file_id)
        .collect();

    let (smallest_user_key, largest_user_key) = table_key_bounds(point_records, range_tombstones);

    TableProperties {
        id: table_id,
        level,
        smallest_user_key,
        largest_user_key,
        smallest_sequence: smallest_sequence.unwrap_or(Sequence::ZERO),
        largest_sequence: largest_sequence.unwrap_or(Sequence::ZERO),
        codec,
        blob_file_ids,
        blob_references,
    }
}

pub(super) fn table_blob_references(point_records: &[TablePointRecord]) -> Vec<TableBlobReference> {
    let mut references = BTreeMap::<u64, TableBlobReference>::new();

    for record in point_records {
        let Some(value) = record.value.as_ref() else {
            continue;
        };
        let (file_id, referenced_bytes) = match value {
            ValueRef::BlobIndex(index) => (index.file_id, index.encoded_len),
            ValueRef::Blob { file_id, len, .. } => (*file_id, *len),
            ValueRef::Inline(_) => continue,
        };

        references
            .entry(file_id)
            .and_modify(|reference| {
                reference.referenced_bytes =
                    reference.referenced_bytes.saturating_add(referenced_bytes);
                reference.referenced_record_count =
                    reference.referenced_record_count.saturating_add(1);
                if record.internal_key < reference.smallest_internal_key {
                    reference.smallest_internal_key = record.internal_key.clone();
                }
                if record.internal_key > reference.largest_internal_key {
                    reference.largest_internal_key = record.internal_key.clone();
                }
            })
            .or_insert_with(|| TableBlobReference {
                file_id,
                referenced_bytes,
                referenced_record_count: 1,
                smallest_internal_key: record.internal_key.clone(),
                largest_internal_key: record.internal_key.clone(),
            });
    }

    references.into_values().collect()
}

pub(super) fn table_key_bounds(
    point_records: &[TablePointRecord],
    range_tombstones: &[TableRangeTombstone],
) -> (Vec<u8>, Vec<u8>) {
    let mut smallest = point_records
        .first()
        .map(|record| record.internal_key.user_key().to_vec());
    let mut largest = point_records
        .last()
        .map(|record| record.internal_key.user_key().to_vec());

    for tombstone in range_tombstones {
        let (Some(start), Some(end)) = (
            finite_bound_bytes(&tombstone.range.start),
            finite_bound_bytes(&tombstone.range.end),
        ) else {
            continue;
        };
        update_smallest(&mut smallest, start);
        update_largest(&mut largest, end);
    }

    match (smallest, largest) {
        (Some(smallest), Some(largest)) => (smallest, largest),
        _ => (Vec::new(), Vec::new()),
    }
}

pub(super) fn finite_bound_bytes(bound: &Bound<Vec<u8>>) -> Option<Vec<u8>> {
    match bound {
        Bound::Included(bytes) | Bound::Excluded(bytes) => Some(bytes.clone()),
        Bound::Unbounded => None,
    }
}

pub(super) fn update_smallest(current: &mut Option<Vec<u8>>, candidate: Vec<u8>) {
    if current
        .as_ref()
        .is_none_or(|current| candidate.as_slice() < current.as_slice())
    {
        *current = Some(candidate);
    }
}

pub(super) fn update_largest(current: &mut Option<Vec<u8>>, candidate: Vec<u8>) {
    if current
        .as_ref()
        .is_none_or(|current| candidate.as_slice() > current.as_slice())
    {
        *current = Some(candidate);
    }
}

pub(super) fn build_prefix_filter(
    options: &TableWriteOptions,
    point_records: &[TablePointRecord],
    level: TableLevel,
) -> Option<PrefixFilter> {
    match options.prefix_filter_policy {
        PrefixFilterPolicy::Disabled => None,
        PrefixFilterPolicy::Bloom { bits_per_prefix } => PrefixFilter::from_keys(
            options.prefix_extractor.clone(),
            point_records
                .iter()
                .map(|record| record.internal_key.user_key()),
            level_adjusted_filter_bits(options.filter_depth_curve, bits_per_prefix, level),
        ),
    }
}

pub(super) fn build_point_key_filter(
    options: &TableWriteOptions,
    point_records: &[TablePointRecord],
    level: TableLevel,
) -> Option<PointKeyFilter> {
    match options.filter_policy {
        FilterPolicy::Disabled => None,
        FilterPolicy::Bloom { bits_per_key } => Some(PointKeyFilter::from_keys(
            point_records
                .iter()
                .map(|record| record.internal_key.user_key()),
            level_adjusted_filter_bits(options.filter_depth_curve, bits_per_key, level),
        )),
    }
}

/// Depth-scaled bits per element for layered (Monkey-style) filter allocation,
/// shared by the point filter (`bits_per_key`) and the prefix filter
/// (`bits_per_prefix`).
///
/// Pinned shallow levels (L0/L1, see `PINNED_READ_METADATA_MAX_LEVEL`) keep the
/// configured base so hot, recent data stays accurately filtered. Deeper levels
/// hold exponentially more keys and dominate filter memory, so they get
/// progressively fewer bits per element. The result never exceeds the base, so
/// total filter memory cannot regress versus uniform allocation - important for
/// memory-constrained embedded use. A deeper false positive costs at most one
/// extra block-filter / data-block probe, which is the classic Monkey trade of
/// memory for worst-case lookup cost. Both filters are self-describing
/// (`from_parts` carries bit and hash counts), so per-level bits need no
/// storage-format change. The curve shape is configured per bucket via
/// [`FilterDepthCurve`].
pub(super) fn level_adjusted_filter_bits(
    curve: FilterDepthCurve,
    base: u8,
    level: TableLevel,
) -> u8 {
    const AUTO_BITS_PER_LEVEL_STEP: u8 = 2;
    const AUTO_MIN_FILTER_BITS: u8 = 4;
    let depth = level.get().saturating_sub(PINNED_READ_METADATA_MAX_LEVEL);
    let depth = u8::try_from(depth).unwrap_or(u8::MAX);
    let (step, floor) = match curve {
        FilterDepthCurve::Uniform => return base,
        FilterDepthCurve::Auto => (AUTO_BITS_PER_LEVEL_STEP, AUTO_MIN_FILTER_BITS),
        FilterDepthCurve::Custom { step, floor } => (step, floor),
        // Ascending (cost-weighted) curve: deeper levels gain bits up to `ceil`.
        // The pinned shallow levels (depth 0) keep the base; below them each level
        // adds `step`, clamped so the deepest never exceeds `ceil` (and `ceil`
        // never drops below the base, so the curve only ever raises deep bits).
        FilterDepthCurve::CostWeighted { step, ceil } => {
            let gain = step.saturating_mul(depth);
            let ceil = ceil.max(base);
            return base.saturating_add(gain).min(ceil);
        }
    };
    let reduction = step.saturating_mul(depth);
    let floor = floor.min(base);
    base.saturating_sub(reduction).max(floor)
}

pub(super) fn build_data_blocks(
    point_records: &[TablePointRecord],
    options: &TableWriteOptions,
    level: TableLevel,
) -> Result<Vec<TableDataBlock>> {
    let mut data_blocks = Vec::new();
    let mut block_start = 0;

    while block_start < point_records.len() {
        let mut block_end = block_start;
        let mut estimated_len = 0_usize;
        while block_end < point_records.len() {
            let next_len = point_record_encoded_len(&point_records[block_end]);
            if block_end > block_start && estimated_len + next_len > options.block_bytes {
                break;
            }
            estimated_len += next_len;
            block_end += 1;
        }

        let restart_indices = (block_start..block_end)
            .step_by(DATA_BLOCK_RESTART_INTERVAL)
            .collect::<Vec<_>>();
        let records = &point_records[block_start..block_end];
        data_blocks.push(TableDataBlock::from_record_range(
            point_records,
            block_start..block_end,
            &restart_indices,
            build_point_key_filter(options, records, level),
            build_prefix_filter(options, records, level),
        )?);
        block_start = block_end;
    }

    Ok(data_blocks)
}

pub(super) fn effective_blob_threshold_bytes(configured_threshold: usize) -> usize {
    configured_threshold.min(max_inline_value_bytes())
}

pub(super) fn max_inline_value_bytes() -> usize {
    limits::MAX_DECODED_BLOCK_BYTES
        .saturating_sub(MIN_INTERNAL_KEY_BYTES + INLINE_VALUE_HEADER_BYTES)
}

pub(super) fn index_partitions_for_loaded_blocks(
    data_blocks: &[TableDataBlock],
) -> Vec<IndexPartitionEntry> {
    data_blocks
        .chunks(INDEX_PARTITION_TARGET_ENTRIES)
        .enumerate()
        .filter_map(|(partition_index, blocks)| {
            let first = blocks.first()?;
            let last = blocks.last()?;
            Some(IndexPartitionEntry {
                smallest_internal_key: first.smallest_internal_key.clone(),
                largest_internal_key: last.largest_internal_key.clone(),
                block: BlockHandle { offset: 0, len: 0 },
                first_data_block_index: partition_index * INDEX_PARTITION_TARGET_ENTRIES,
                data_block_count: blocks.len(),
            })
        })
        .collect()
}
