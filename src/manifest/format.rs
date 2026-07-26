use super::{
    Arc, BTreeMap, BlobLevelMergePolicy, BlockingStorageManifestPublishBackend,
    BlockingStorageManifestReadBackend, BucketOptions, CodecId, CompressionProfile, DurabilityMode,
    Error, FilterDepthCurve, FilterPolicy, HEADER_LEN, IndexSearchPolicy, InternalKey,
    MANIFEST_MAGIC, MANIFEST_VERSION, MIN_TABLE_PROPERTY_BYTES, ManifestState, NativeFileBackend,
    Path, PrefixExtractor, PrefixFilterPolicy, PublishOutcome, Result, Sequence,
    StorageManifestPublishBackend, StorageManifestReadBackend, StorageObjectId, StorageObjectKind,
    TableBlobReference, TableId, TableLevel, TableProperties, ValueKind, io, limits,
};

#[cfg(test)]
pub(crate) fn read_manifest(path: &Path) -> Result<ManifestState> {
    let bytes = read_manifest_bytes(path)?.ok_or_else(|| {
        Error::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("manifest {} not found", path.display()),
        ))
    })?;
    decode_manifest(&bytes)
}

#[allow(dead_code)]
pub(crate) async fn read_manifest_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<ManifestState>
where
    B: StorageManifestReadBackend,
{
    let bytes = read_manifest_bytes_with_backend_async(backend, path)
        .await?
        .ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("manifest {} not found", path.display()),
            ))
        })?;
    decode_manifest(&bytes)
}

#[cfg(test)]
pub(super) fn read_manifest_bytes(path: &Path) -> Result<Option<Arc<[u8]>>> {
    let backend = NativeFileBackend::new();
    read_manifest_bytes_with_backend(&backend, path)
}

pub(super) fn read_manifest_bytes_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<Option<Arc<[u8]>>> {
    let object = manifest_storage_object(path);
    backend.read_current_manifest_blocking(object)
}

pub(super) async fn read_manifest_bytes_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<Option<Arc<[u8]>>>
where
    B: StorageManifestReadBackend,
{
    let object = manifest_storage_object(path);
    backend.read_current_manifest(object).await
}

pub(super) fn publish_manifest_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
    state: &ManifestState,
) -> Result<PublishOutcome> {
    let bytes = encode_manifest_bytes(state)?;
    let object = manifest_storage_object(path);
    if let Err(error) =
        backend.publish_manifest_blocking(object, bytes, native_manifest_publish_durability())
    {
        return if error.manifest_was_published() {
            Ok(PublishOutcome::PublishedDurabilityUnknown { error })
        } else {
            Err(error)
        };
    }
    // Temp-write + atomic rename cannot lose a CAS race, so the filesystem
    // manifest always advances.
    Ok(PublishOutcome::Published)
}

pub(super) const fn native_manifest_publish_durability() -> DurabilityMode {
    #[cfg(target_os = "wasi")]
    {
        DurabilityMode::Flush
    }
    #[cfg(not(target_os = "wasi"))]
    {
        DurabilityMode::SyncAll
    }
}

pub(super) async fn publish_manifest_with_backend_async<B>(
    backend: &B,
    path: &Path,
    state: &ManifestState,
    durability: DurabilityMode,
) -> Result<PublishOutcome>
where
    B: StorageManifestPublishBackend,
{
    let bytes = encode_manifest_bytes(state)?;
    let object = manifest_storage_object(path);
    if let Err(error) = backend.publish_manifest(object, bytes, durability).await {
        return if error.manifest_was_published() {
            Ok(PublishOutcome::PublishedDurabilityUnknown { error })
        } else {
            Err(error)
        };
    }
    Ok(PublishOutcome::Published)
}

pub(super) fn encode_manifest_bytes(state: &ManifestState) -> Result<Arc<[u8]>> {
    let payload = encode_state(state)?;
    if payload.len() > limits::MAX_MANIFEST_PAYLOAD_BYTES {
        return Err(Error::invalid_options(format!(
            "manifest payload length {} exceeds maximum {}",
            payload.len(),
            limits::MAX_MANIFEST_PAYLOAD_BYTES
        )));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::invalid_options("manifest payload exceeds u32::MAX"))?;
    let payload_checksum = checksum(&payload);
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());

    bytes.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    bytes.extend_from_slice(&payload);

    Ok(bytes.into())
}

pub(super) fn manifest_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::Manifest, path)
}

pub(super) fn encode_state(state: &ManifestState) -> Result<Vec<u8>> {
    state.validate_next_file_id()?;
    state.validate_bucket_generations()?;
    let mut bytes = Vec::new();
    let bucket_count = u32::try_from(state.buckets.len())
        .map_err(|_| Error::invalid_options("too many buckets for manifest"))?;

    put_u64(&mut bytes, state.wal_replay_floor.get());
    put_u32(&mut bytes, bucket_count);
    for (name, options) in &state.buckets {
        put_bytes(&mut bytes, name.as_bytes())?;
        put_bucket_options(&mut bytes, options)?;
        put_u64(
            &mut bytes,
            state
                .bucket_generation(name)
                .ok_or_else(|| Error::Corruption {
                    message: format!("bucket {name:?} is missing its generation"),
                })?,
        );
    }
    put_tables(&mut bytes, &state.tables)?;
    put_pending_blob_deletions(&mut bytes, &state.pending_blob_deletions)?;
    put_checkpoints(&mut bytes, &state.checkpoints)?;
    put_u64(&mut bytes, state.writer_epoch);
    put_u64(&mut bytes, state.next_file_id);
    put_u64(&mut bytes, state.next_bucket_generation);

    Ok(bytes)
}

pub(super) fn decode_manifest(bytes: &[u8]) -> Result<ManifestState> {
    if bytes.len() < HEADER_LEN {
        return Err(invalid_manifest("short header"));
    }

    let magic = read_u32_at(bytes, 0)?;
    let version = read_u16_at(bytes, 4)?;
    let payload_len = read_u32_at(bytes, 6)? as usize;
    limits::ensure_invalid_format_len(
        payload_len,
        limits::MAX_MANIFEST_PAYLOAD_BYTES,
        "manifest payload length",
    )?;
    let payload_checksum = read_u32_at(bytes, 10)?;
    if magic != MANIFEST_MAGIC {
        return Err(Error::Corruption {
            message: "manifest magic mismatch".to_owned(),
        });
    }
    if version != MANIFEST_VERSION {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported manifest version {version}"),
        });
    }
    let expected_len =
        limits::checked_add_invalid_format(HEADER_LEN, payload_len, "manifest length")?;
    if bytes.len() != expected_len {
        return Err(Error::Corruption {
            message: "manifest length mismatch".to_owned(),
        });
    }

    let payload = &bytes[HEADER_LEN..];
    if checksum(payload) != payload_checksum {
        return Err(Error::Corruption {
            message: "manifest checksum mismatch".to_owned(),
        });
    }

    decode_state(payload)
}

pub(super) fn decode_state(payload: &[u8]) -> Result<ManifestState> {
    let mut cursor = Cursor::new(payload);
    let wal_replay_floor = Sequence::new(cursor.read_u64()?);
    let bucket_count = cursor.read_u32()? as usize;
    let mut buckets = BTreeMap::new();
    let mut bucket_generations = BTreeMap::new();
    let mut previous_bucket = None::<String>;

    for _ in 0..bucket_count {
        let name =
            String::from_utf8(cursor.read_bytes()?.to_vec()).map_err(|_| Error::InvalidFormat {
                message: "manifest bucket name is not valid UTF-8".to_owned(),
            })?;
        let options = cursor.read_bucket_options()?;
        let generation = cursor.read_u64()?;
        if previous_bucket
            .as_ref()
            .is_some_and(|previous| previous >= &name)
        {
            return Err(invalid_manifest("manifest buckets are not strictly sorted"));
        }
        bucket_generations.insert(name.clone(), generation);
        buckets.insert(name.clone(), options);
        previous_bucket = Some(name);
    }
    let tables = cursor.read_tables()?;
    let pending_blob_deletions = cursor.read_pending_blob_deletions()?;
    let checkpoints = cursor.read_checkpoints()?;
    let writer_epoch = cursor.read_u64()?;
    let next_file_id = cursor.read_u64()?;
    let next_bucket_generation = cursor.read_u64()?;

    if !cursor.is_finished() {
        return Err(invalid_manifest("trailing payload bytes"));
    }

    let state = ManifestState {
        wal_replay_floor,
        buckets,
        bucket_generations,
        tables,
        pending_blob_deletions,
        checkpoints,
        next_file_id,
        next_bucket_generation,
        writer_epoch,
    };
    state.validate_next_file_id()?;
    state.validate_bucket_generations()?;
    Ok(state)
}

pub(super) fn put_bucket_options(bytes: &mut Vec<u8>, options: &BucketOptions) -> Result<()> {
    put_bool(bytes, options.allow_empty_keys);
    put_compression_profile(bytes, options.compression);
    put_usize(bytes, options.block_bytes)?;
    put_filter_policy(bytes, options.filter_policy);
    put_prefix_extractor(bytes, &options.prefix_extractor)?;
    put_prefix_filter_policy(bytes, options.prefix_filter_policy);
    put_index_search_policy(bytes, options.index_search_policy);
    put_usize(bytes, options.blob_threshold_bytes)?;
    put_blob_level_merge_policy(bytes, options.blob_level_merge_policy);
    put_filter_depth_curve(bytes, options.filter_depth_curve);
    Ok(())
}

pub(super) fn put_filter_depth_curve(bytes: &mut Vec<u8>, value: FilterDepthCurve) {
    match value {
        FilterDepthCurve::Auto => put_u8(bytes, 0),
        FilterDepthCurve::Uniform => put_u8(bytes, 1),
        FilterDepthCurve::Custom { step, floor } => {
            put_u8(bytes, 2);
            put_u8(bytes, step);
            put_u8(bytes, floor);
        }
        FilterDepthCurve::CostWeighted { step, ceil } => {
            put_u8(bytes, 3);
            put_u8(bytes, step);
            put_u8(bytes, ceil);
        }
    }
}

pub(super) fn put_bool(bytes: &mut Vec<u8>, value: bool) {
    put_u8(bytes, u8::from(value));
}

pub(super) fn put_compression_profile(bytes: &mut Vec<u8>, value: CompressionProfile) {
    put_u8(
        bytes,
        match value {
            CompressionProfile::None => 0,
            CompressionProfile::Fast => 1,
        },
    );
}

pub(super) fn put_filter_policy(bytes: &mut Vec<u8>, value: FilterPolicy) {
    match value {
        FilterPolicy::Disabled => put_u8(bytes, 0),
        FilterPolicy::Bloom { bits_per_key } => {
            put_u8(bytes, 1);
            put_u8(bytes, bits_per_key);
        }
    }
}

pub(super) fn put_prefix_extractor(bytes: &mut Vec<u8>, value: &PrefixExtractor) -> Result<()> {
    match value {
        PrefixExtractor::FixedLen(len) => {
            put_u8(bytes, 0);
            put_usize(bytes, *len)?;
        }
        PrefixExtractor::Separator(separator) => {
            put_u8(bytes, 1);
            put_u8(bytes, *separator);
        }
        PrefixExtractor::Custom(name) => {
            put_u8(bytes, 2);
            put_bytes(bytes, name.as_bytes())?;
        }
        PrefixExtractor::Disabled => put_u8(bytes, 3),
    }
    Ok(())
}

pub(super) fn put_prefix_filter_policy(bytes: &mut Vec<u8>, value: PrefixFilterPolicy) {
    match value {
        PrefixFilterPolicy::Disabled => put_u8(bytes, 0),
        PrefixFilterPolicy::Bloom { bits_per_prefix } => {
            put_u8(bytes, 1);
            put_u8(bytes, bits_per_prefix);
        }
    }
}

pub(super) fn put_index_search_policy(bytes: &mut Vec<u8>, value: IndexSearchPolicy) {
    put_u8(
        bytes,
        match value {
            IndexSearchPolicy::Linear => 0,
            IndexSearchPolicy::Binary => 1,
            IndexSearchPolicy::Auto => 4,
        },
    );
}

pub(super) fn put_blob_level_merge_policy(bytes: &mut Vec<u8>, value: BlobLevelMergePolicy) {
    put_u8(
        bytes,
        match value {
            BlobLevelMergePolicy::Disabled => 0,
            BlobLevelMergePolicy::Auto => 1,
            BlobLevelMergePolicy::Always => 2,
        },
    );
}

pub(super) fn put_tables(
    bytes: &mut Vec<u8>,
    tables: &BTreeMap<String, Vec<TableProperties>>,
) -> Result<()> {
    let table_bucket_count = u32::try_from(tables.len())
        .map_err(|_| Error::invalid_options("too many table buckets for manifest"))?;
    put_u32(bytes, table_bucket_count);

    for (bucket, table_list) in tables {
        put_bytes(bytes, bucket.as_bytes())?;
        let table_count = u32::try_from(table_list.len())
            .map_err(|_| Error::invalid_options("too many tables for manifest bucket"))?;
        put_u32(bytes, table_count);
        for properties in table_list {
            put_table_properties(bytes, properties)?;
        }
    }

    Ok(())
}

pub(super) fn put_pending_blob_deletions(
    bytes: &mut Vec<u8>,
    pending_blob_deletions: &BTreeMap<u64, Sequence>,
) -> Result<()> {
    let count = u32::try_from(pending_blob_deletions.len())
        .map_err(|_| Error::invalid_options("too many pending blob deletions for manifest"))?;
    put_u32(bytes, count);
    for (file_id, sequence) in pending_blob_deletions {
        put_u64(bytes, *file_id);
        put_u64(bytes, sequence.get());
    }
    Ok(())
}

pub(super) fn put_checkpoints(
    bytes: &mut Vec<u8>,
    checkpoints: &BTreeMap<String, Sequence>,
) -> Result<()> {
    let count = u32::try_from(checkpoints.len())
        .map_err(|_| Error::invalid_options("too many checkpoints for manifest"))?;
    put_u32(bytes, count);
    for (name, sequence) in checkpoints {
        put_bytes(bytes, name.as_bytes())?;
        put_u64(bytes, sequence.get());
    }
    Ok(())
}

pub(super) fn put_table_properties(
    bytes: &mut Vec<u8>,
    properties: &TableProperties,
) -> Result<()> {
    put_u64(bytes, properties.id.get());
    put_u32(bytes, properties.level.get());
    put_bytes(bytes, &properties.smallest_user_key)?;
    put_bytes(bytes, &properties.largest_user_key)?;
    put_u64(bytes, properties.smallest_sequence.get());
    put_u64(bytes, properties.largest_sequence.get());
    put_codec(bytes, properties.codec);
    put_u32(
        bytes,
        u32::try_from(properties.blob_file_ids.len())
            .map_err(|_| Error::invalid_options("too many blob file ids for table properties"))?,
    );
    for file_id in &properties.blob_file_ids {
        put_u64(bytes, *file_id);
    }
    put_u32(
        bytes,
        u32::try_from(properties.blob_references.len())
            .map_err(|_| Error::invalid_options("too many blob references for table properties"))?,
    );
    for reference in &properties.blob_references {
        put_u64(bytes, reference.file_id);
        put_u64(bytes, reference.referenced_bytes);
        put_u64(bytes, reference.referenced_record_count);
        put_internal_key(bytes, &reference.smallest_internal_key)?;
        put_internal_key(bytes, &reference.largest_internal_key)?;
    }
    Ok(())
}

pub(super) fn put_internal_key(bytes: &mut Vec<u8>, internal_key: &InternalKey) -> Result<()> {
    put_bytes(bytes, internal_key.user_key())?;
    put_u64(bytes, internal_key.sequence().get());
    put_u8(
        bytes,
        match internal_key.kind() {
            ValueKind::Put => 1,
            ValueKind::PointDelete => 2,
            ValueKind::RangeDelete => 3,
        },
    );
    put_u32(bytes, internal_key.batch_index());
    Ok(())
}

pub(super) fn put_codec(bytes: &mut Vec<u8>, codec: CodecId) {
    put_u8(
        bytes,
        match codec {
            CodecId::None => 0,
            CodecId::FastLz4Block => 1,
        },
    );
}

pub(super) fn put_usize(bytes: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u64::try_from(value)
        .map_err(|_| Error::invalid_options("manifest usize field exceeds u64::MAX"))?;
    put_u64(bytes, value);
    Ok(())
}

pub(super) fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

pub(super) fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("manifest byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

pub(super) fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16> {
    let end = limits::checked_add_invalid_format(offset, 2, "u16 offset")?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_manifest("short u16"))?;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

pub(super) fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = limits::checked_add_invalid_format(offset, 4, "u32 offset")?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_manifest("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

pub(super) fn checksum(bytes: &[u8]) -> u32 {
    crate::checksum::crc32c(bytes)
}

pub(super) fn invalid_manifest(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid manifest: {message}"),
    }
}

pub(super) struct Cursor<'payload> {
    payload: &'payload [u8],
    offset: usize,
}

impl<'payload> Cursor<'payload> {
    const fn new(payload: &'payload [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .payload
            .get(self.offset)
            .ok_or_else(|| invalid_manifest("short u8"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_bool(&mut self) -> Result<bool> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(Error::InvalidFormat {
                message: format!("invalid manifest bool {value}"),
            }),
        }
    }

    fn read_u32(&mut self) -> Result<u32> {
        let value = read_u32_at(self.payload, self.offset)?;
        self.offset += 4;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64> {
        let end = limits::checked_add_invalid_format(self.offset, 8, "u64 offset")?;
        let value = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| invalid_manifest("short u64"))?;
        self.offset = end;
        Ok(u64::from_le_bytes([
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
        ]))
    }

    fn read_usize(&mut self) -> Result<usize> {
        usize::try_from(self.read_u64()?).map_err(|_| Error::UnsupportedFormat {
            message: "manifest usize field does not fit this platform".to_owned(),
        })
    }

    fn read_bytes(&mut self) -> Result<&'payload [u8]> {
        let len = self.read_u32()? as usize;
        let end =
            limits::checked_add_invalid_format(self.offset, len, "manifest byte field length")?;
        let value = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| invalid_manifest("short bytes"))?;
        self.offset = end;
        Ok(value)
    }

    fn read_bucket_options(&mut self) -> Result<BucketOptions> {
        Ok(BucketOptions {
            allow_empty_keys: self.read_bool()?,
            compression: self.read_compression_profile()?,
            block_bytes: self.read_usize()?,
            filter_policy: self.read_filter_policy()?,
            prefix_extractor: self.read_prefix_extractor()?,
            prefix_filter_policy: self.read_prefix_filter_policy()?,
            index_search_policy: self.read_index_search_policy()?,
            blob_threshold_bytes: self.read_usize()?,
            blob_level_merge_policy: self.read_blob_level_merge_policy()?,
            filter_depth_curve: self.read_filter_depth_curve()?,
        })
    }

    fn read_filter_depth_curve(&mut self) -> Result<FilterDepthCurve> {
        match self.read_u8()? {
            0 => Ok(FilterDepthCurve::Auto),
            1 => Ok(FilterDepthCurve::Uniform),
            2 => Ok(FilterDepthCurve::Custom {
                step: self.read_u8()?,
                floor: self.read_u8()?,
            }),
            3 => Ok(FilterDepthCurve::CostWeighted {
                step: self.read_u8()?,
                ceil: self.read_u8()?,
            }),
            other => Err(Error::Corruption {
                message: format!("unknown filter depth curve tag {other}"),
            }),
        }
    }

    fn read_tables(&mut self) -> Result<BTreeMap<String, Vec<TableProperties>>> {
        let table_bucket_count = self.read_u32()? as usize;
        let mut tables = BTreeMap::new();
        let mut previous_bucket = None::<String>;

        for _ in 0..table_bucket_count {
            let bucket = String::from_utf8(self.read_bytes()?.to_vec()).map_err(|_| {
                Error::InvalidFormat {
                    message: "manifest table bucket is not valid UTF-8".to_owned(),
                }
            })?;
            if previous_bucket
                .as_ref()
                .is_some_and(|previous| previous >= &bucket)
            {
                return Err(invalid_manifest(
                    "manifest table buckets are not strictly sorted",
                ));
            }
            let table_count = self.read_u32()? as usize;
            if table_count > self.remaining_len() / MIN_TABLE_PROPERTY_BYTES {
                return Err(invalid_manifest("table count exceeds payload bytes"));
            }
            let mut table_list = Vec::with_capacity(table_count);
            for _ in 0..table_count {
                table_list.push(self.read_table_properties()?);
            }
            tables.insert(bucket.clone(), table_list);
            previous_bucket = Some(bucket);
        }

        Ok(tables)
    }

    fn read_pending_blob_deletions(&mut self) -> Result<BTreeMap<u64, Sequence>> {
        let pending_count = self.read_u32()? as usize;
        if pending_count > self.remaining_len() / 16 {
            return Err(invalid_manifest(
                "pending blob deletion count exceeds payload bytes",
            ));
        }

        let mut pending = BTreeMap::new();
        let mut previous = None;
        for _ in 0..pending_count {
            let file_id = self.read_u64()?;
            if previous.is_some_and(|previous| previous >= file_id) {
                return Err(invalid_manifest("pending blob deletions are not sorted"));
            }
            let sequence = Sequence::new(self.read_u64()?);
            pending.insert(file_id, sequence);
            previous = Some(file_id);
        }
        Ok(pending)
    }

    fn read_checkpoints(&mut self) -> Result<BTreeMap<String, Sequence>> {
        let checkpoint_count = self.read_u32()? as usize;
        if checkpoint_count > self.remaining_len() / 12 {
            return Err(invalid_manifest("checkpoint count exceeds payload bytes"));
        }

        let mut checkpoints = BTreeMap::new();
        let mut previous = None::<String>;
        for _ in 0..checkpoint_count {
            let name = String::from_utf8(self.read_bytes()?.to_vec()).map_err(|_| {
                Error::InvalidFormat {
                    message: "manifest checkpoint name is not valid UTF-8".to_owned(),
                }
            })?;
            if name.is_empty() {
                return Err(invalid_manifest("checkpoint name is empty"));
            }
            if previous.as_ref().is_some_and(|previous| previous >= &name) {
                return Err(invalid_manifest("checkpoints are not sorted"));
            }
            let sequence = Sequence::new(self.read_u64()?);
            checkpoints.insert(name.clone(), sequence);
            previous = Some(name);
        }
        Ok(checkpoints)
    }

    fn read_table_properties(&mut self) -> Result<TableProperties> {
        Ok(TableProperties {
            id: TableId(self.read_u64()?),
            level: TableLevel(self.read_u32()?),
            smallest_user_key: self.read_bytes()?.to_vec(),
            largest_user_key: self.read_bytes()?.to_vec(),
            smallest_sequence: Sequence::new(self.read_u64()?),
            largest_sequence: Sequence::new(self.read_u64()?),
            codec: self.read_codec()?,
            blob_file_ids: self.read_blob_file_ids()?,
            blob_references: self.read_blob_references()?,
        })
    }

    fn read_blob_file_ids(&mut self) -> Result<Vec<u64>> {
        let file_id_count = self.read_u32()? as usize;
        if file_id_count > self.remaining_len() / 8 {
            return Err(invalid_manifest("blob file id count exceeds payload bytes"));
        }
        let mut file_ids = Vec::with_capacity(file_id_count);
        let mut previous = None;
        for _ in 0..file_id_count {
            let file_id = self.read_u64()?;
            if previous.is_some_and(|previous| previous >= file_id) {
                return Err(invalid_manifest("blob file ids are not sorted"));
            }
            file_ids.push(file_id);
            previous = Some(file_id);
        }
        Ok(file_ids)
    }

    fn read_blob_references(&mut self) -> Result<Vec<TableBlobReference>> {
        let reference_count = self.read_u32()? as usize;
        if reference_count > self.remaining_len() / 58 {
            return Err(invalid_manifest(
                "blob reference count exceeds payload bytes",
            ));
        }

        let mut references = Vec::with_capacity(reference_count);
        let mut previous = None;
        for _ in 0..reference_count {
            let file_id = self.read_u64()?;
            if previous.is_some_and(|previous| previous >= file_id) {
                return Err(invalid_manifest("blob references are not sorted"));
            }
            let referenced_bytes = self.read_u64()?;
            let referenced_record_count = self.read_u64()?;
            let smallest_internal_key = self.read_internal_key()?;
            let largest_internal_key = self.read_internal_key()?;
            if smallest_internal_key > largest_internal_key {
                return Err(invalid_manifest("blob reference key bounds are invalid"));
            }
            references.push(TableBlobReference {
                file_id,
                referenced_bytes,
                referenced_record_count,
                smallest_internal_key,
                largest_internal_key,
            });
            previous = Some(file_id);
        }
        Ok(references)
    }

    fn read_internal_key(&mut self) -> Result<InternalKey> {
        let user_key = self.read_bytes()?.to_vec();
        let sequence = Sequence::new(self.read_u64()?);
        let kind = self.read_value_kind()?;
        let batch_index = self.read_u32()?;
        Ok(InternalKey::new(user_key, sequence, kind, batch_index))
    }

    fn read_value_kind(&mut self) -> Result<ValueKind> {
        match self.read_u8()? {
            1 => Ok(ValueKind::Put),
            2 => Ok(ValueKind::PointDelete),
            3 => Ok(ValueKind::RangeDelete),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest internal value kind {tag}"),
            }),
        }
    }

    fn read_compression_profile(&mut self) -> Result<CompressionProfile> {
        match self.read_u8()? {
            0 => Ok(CompressionProfile::None),
            1 => Ok(CompressionProfile::Fast),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest compression profile {tag}"),
            }),
        }
    }

    fn read_filter_policy(&mut self) -> Result<FilterPolicy> {
        match self.read_u8()? {
            0 => Ok(FilterPolicy::Disabled),
            1 => Ok(FilterPolicy::Bloom {
                bits_per_key: self.read_u8()?,
            }),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest filter policy {tag}"),
            }),
        }
    }

    fn read_prefix_extractor(&mut self) -> Result<PrefixExtractor> {
        match self.read_u8()? {
            0 => Ok(PrefixExtractor::FixedLen(self.read_usize()?)),
            1 => Ok(PrefixExtractor::Separator(self.read_u8()?)),
            2 => {
                let name = String::from_utf8(self.read_bytes()?.to_vec()).map_err(|_| {
                    Error::InvalidFormat {
                        message: "manifest custom prefix extractor is not UTF-8".to_owned(),
                    }
                })?;
                Ok(PrefixExtractor::Custom(name))
            }
            3 => Ok(PrefixExtractor::Disabled),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest prefix extractor {tag}"),
            }),
        }
    }

    fn read_prefix_filter_policy(&mut self) -> Result<PrefixFilterPolicy> {
        match self.read_u8()? {
            0 => Ok(PrefixFilterPolicy::Disabled),
            1 => Ok(PrefixFilterPolicy::Bloom {
                bits_per_prefix: self.read_u8()?,
            }),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest prefix filter policy {tag}"),
            }),
        }
    }

    fn read_index_search_policy(&mut self) -> Result<IndexSearchPolicy> {
        match self.read_u8()? {
            0 => Ok(IndexSearchPolicy::Linear),
            1 => Ok(IndexSearchPolicy::Binary),
            4 => Ok(IndexSearchPolicy::Auto),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest index search policy {tag}"),
            }),
        }
    }

    fn read_blob_level_merge_policy(&mut self) -> Result<BlobLevelMergePolicy> {
        match self.read_u8()? {
            0 => Ok(BlobLevelMergePolicy::Disabled),
            1 => Ok(BlobLevelMergePolicy::Auto),
            2 => Ok(BlobLevelMergePolicy::Always),
            tag => Err(Error::InvalidFormat {
                message: format!("unknown manifest blob level merge policy {tag}"),
            }),
        }
    }

    fn read_codec(&mut self) -> Result<CodecId> {
        match self.read_u8()? {
            0 => Ok(CodecId::None),
            1 => Ok(CodecId::FastLz4Block),
            tag => Err(Error::UnsupportedFormat {
                message: format!("unknown manifest table codec {tag}"),
            }),
        }
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.payload.len()
    }

    const fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}
