use super::{
    BLOB_FILE_FORMAT_VERSION, BLOB_FOOTER_LEN, BLOB_FOOTER_MAGIC, BLOB_HEADER_LEN,
    BLOB_HEADER_WITHOUT_CHECKSUM_LEN, BLOB_MAGIC, BlobFile, BlobFileHeader, BlobFileProperties,
    BlobFileRecord, BlobIndex, BlobRecord, BlockingStorageReadObject, CodecId, Error, InternalKey,
    MIN_BLOB_RECORD_FRAME_BYTES, NativeFileBackend, Path, Result, Sequence, StorageReadBackend,
    StorageReadObject, ValueKind, blob_object_len, blob_object_len_async, blob_storage_backend,
    codec, limits, open_blob_read_object_with_backend, open_blob_read_object_with_backend_async,
    read_blob_exact_at, read_blob_exact_at_async,
};

pub fn encode_blob_file(
    header: BlobFileHeader,
    records: &[BlobRecord],
) -> Result<(Vec<u8>, Vec<BlobIndex>)> {
    if records.is_empty() {
        return Err(Error::invalid_options("cannot write an empty blob file"));
    }
    validate_blob_record_order(records)?;

    let mut bytes = Vec::new();
    put_header(&mut bytes, header);
    let mut indexed_records = Vec::with_capacity(records.len());

    for record in records {
        if record.internal_key.kind() != ValueKind::Put {
            return Err(Error::invalid_options(
                "blob records can only store put values",
            ));
        }
        ensure_blob_value_len(record.value.len())?;
        let offset = usize_to_u64(bytes.len(), "blob record offset")?;
        let encoded_value = codec::encode_block(record.compression, &record.value)?;
        let value_len = usize_to_u64(record.value.len(), "blob value length")?;
        let encoded_len = usize_to_u64(encoded_value.len(), "encoded blob value length")?;
        let value_checksum = checksum(&record.value);

        let mut body = Vec::new();
        put_internal_key(&mut body, &record.internal_key)?;
        put_u64(&mut body, value_len);
        put_u64(&mut body, encoded_len);
        put_codec(&mut body, record.compression);
        put_u32(&mut body, value_checksum);
        body.extend_from_slice(&encoded_value);

        let record_checksum = checksum(&body);
        put_u64(&mut bytes, usize_to_u64(body.len(), "blob record length")?);
        put_u32(&mut bytes, record_checksum);
        bytes.extend_from_slice(&body);

        indexed_records.push(BlobFileRecord {
            index: BlobIndex {
                file_id: header.file_id,
                offset,
                encoded_len,
                value_len,
                value_checksum,
                record_checksum,
                compression: record.compression,
            },
            record: record.clone(),
        });
    }

    let properties = properties_from_records(&indexed_records)?;
    let properties_offset = usize_to_u64(bytes.len(), "blob properties offset")?;
    let properties_bytes = encode_properties(&properties)?;
    let properties_len = usize_to_u64(properties_bytes.len(), "blob properties length")?;
    bytes.extend_from_slice(&properties_bytes);
    put_footer(&mut bytes, properties_offset, properties_len);
    ensure_blob_file_len(bytes.len())?;

    let indexes = indexed_records
        .into_iter()
        .map(|record| record.index)
        .collect();
    Ok((bytes, indexes))
}

pub fn decode_blob_file(bytes: &[u8]) -> Result<BlobFile> {
    if bytes.len() < BLOB_HEADER_LEN + BLOB_FOOTER_LEN {
        return Err(invalid_blob("file is too short"));
    }
    limits::ensure_invalid_format_len(
        bytes.len(),
        limits::MAX_WHOLE_BLOB_DECODE_BYTES,
        "blob file length",
    )?;

    let header = decode_header(bytes)?;
    let footer_start = bytes.len() - BLOB_FOOTER_LEN;
    let (properties_offset, properties_len) = decode_footer(&bytes[footer_start..])?;
    let properties_start = u64_to_usize(properties_offset, "blob properties offset")?;
    let properties_len = u64_to_usize(
        checked_blob_properties_len(properties_len)?,
        "blob properties length",
    )?;
    let properties_end = properties_start
        .checked_add(properties_len)
        .ok_or_else(|| invalid_blob("properties bounds overflow"))?;
    if properties_start < BLOB_HEADER_LEN || properties_end > footer_start {
        return Err(invalid_blob("properties bounds are outside the blob file"));
    }

    let properties = decode_properties(&bytes[properties_start..properties_end])?;
    let records = decode_records(header.file_id, &bytes[BLOB_HEADER_LEN..properties_start])?;
    let computed_properties = properties_from_records(&records)?;
    if properties != computed_properties {
        return Err(Error::Corruption {
            message: "blob properties do not match records".to_owned(),
        });
    }

    Ok(BlobFile {
        header,
        properties,
        records,
    })
}

#[allow(dead_code)]
pub(super) fn read_indexed_value(
    db_path: &Path,
    index: &BlobIndex,
    expected_internal_key: Option<&InternalKey>,
) -> Result<Vec<u8>> {
    let backend = blob_storage_backend();
    read_indexed_value_with_backend(&backend, db_path, index, expected_internal_key)
}

pub(super) fn read_indexed_value_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    index: &BlobIndex,
    expected_internal_key: Option<&InternalKey>,
) -> Result<Vec<u8>> {
    Ok(
        read_record_for_index_with_backend(backend, db_path, index, expected_internal_key)?
            .record
            .value,
    )
}

#[allow(dead_code)]
pub(super) async fn read_indexed_value_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    index: &BlobIndex,
    expected_internal_key: Option<&InternalKey>,
) -> Result<Vec<u8>>
where
    B: StorageReadBackend,
{
    Ok(
        read_record_for_index_with_backend_async(backend, db_path, index, expected_internal_key)
            .await?
            .record
            .value,
    )
}

#[allow(dead_code)]
pub(crate) fn read_record_for_index(
    db_path: &Path,
    index: &BlobIndex,
    expected_internal_key: Option<&InternalKey>,
) -> Result<BlobFileRecord> {
    let backend = blob_storage_backend();
    read_record_for_index_with_backend(&backend, db_path, index, expected_internal_key)
}

pub(crate) fn read_record_for_index_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    index: &BlobIndex,
    expected_internal_key: Option<&InternalKey>,
) -> Result<BlobFileRecord> {
    let object = open_blob_read_object_with_backend(backend, db_path, index.file_id)?;
    let file_len = blob_object_len(&object, "referenced blob file metadata cannot be read")?;
    validate_indexed_blob_header(&object, index.file_id)?;
    let record = read_indexed_blob_record(&object, file_len, index)?;

    if record.index != *index {
        return Err(Error::Corruption {
            message: "blob index metadata mismatch".to_owned(),
        });
    }
    if expected_internal_key.is_some_and(|expected| record.record.internal_key != *expected) {
        return Err(Error::Corruption {
            message: "blob record internal key mismatch".to_owned(),
        });
    }
    Ok(record)
}

#[allow(dead_code)]
pub(crate) async fn read_record_for_index_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    index: &BlobIndex,
    expected_internal_key: Option<&InternalKey>,
) -> Result<BlobFileRecord>
where
    B: StorageReadBackend,
{
    let object = open_blob_read_object_with_backend_async(backend, db_path, index.file_id).await?;
    let file_len =
        blob_object_len_async(&object, "referenced blob file metadata cannot be read").await?;
    validate_indexed_blob_header_async(&object, index.file_id).await?;
    let record = read_indexed_blob_record_async(&object, file_len, index).await?;
    if record.index != *index {
        return Err(Error::Corruption {
            message: "blob index metadata mismatch".to_owned(),
        });
    }
    if expected_internal_key.is_some_and(|expected| record.record.internal_key != *expected) {
        return Err(Error::Corruption {
            message: "blob record internal key mismatch".to_owned(),
        });
    }
    Ok(record)
}

pub(super) fn validate_indexed_blob_header(
    object: &impl BlockingStorageReadObject,
    expected_file_id: u64,
) -> Result<()> {
    let mut header_bytes = [0_u8; BLOB_HEADER_LEN];
    read_blob_exact_at(
        object,
        0,
        &mut header_bytes,
        "referenced blob header cannot be read",
    )?;
    let header = decode_header(&header_bytes)?;
    if header.file_id != expected_file_id {
        return Err(Error::Corruption {
            message: format!(
                "blob file id mismatch: path has {expected_file_id}, header has {}",
                header.file_id
            ),
        });
    }
    Ok(())
}

pub(super) async fn validate_indexed_blob_header_async(
    object: &impl StorageReadObject,
    expected_file_id: u64,
) -> Result<()> {
    let mut header_bytes = [0_u8; BLOB_HEADER_LEN];
    read_blob_exact_at_async(
        object,
        0,
        &mut header_bytes,
        "referenced blob header cannot be read",
    )
    .await?;
    let header = decode_header(&header_bytes)?;
    if header.file_id != expected_file_id {
        return Err(Error::Corruption {
            message: format!(
                "blob file id mismatch: path has {expected_file_id}, header has {}",
                header.file_id
            ),
        });
    }
    Ok(())
}

pub(super) fn read_indexed_blob_record(
    object: &impl BlockingStorageReadObject,
    file_len: u64,
    index: &BlobIndex,
) -> Result<BlobFileRecord> {
    if index.offset < BLOB_HEADER_LEN as u64 {
        return Err(invalid_blob("blob index offset points before records"));
    }

    let frame_end = checked_blob_offset_add(
        index.offset,
        MIN_BLOB_RECORD_FRAME_BYTES as u64,
        "blob record frame bounds",
    )?;
    if frame_end > file_len {
        return Err(invalid_blob("blob index frame is outside the blob file"));
    }

    let mut frame = [0_u8; MIN_BLOB_RECORD_FRAME_BYTES];
    read_blob_exact_at(
        object,
        index.offset,
        &mut frame,
        "referenced blob record frame cannot be read",
    )?;
    let body_len = read_u64_at(&frame, 0)?;
    let body_len = checked_blob_record_body_len(body_len)?;
    let record_checksum = read_u32_at(&frame, 8)?;
    let body_end = checked_blob_offset_add(frame_end, body_len, "blob record body bounds")?;
    if body_end > file_len {
        return Err(invalid_blob("blob index body is outside the blob file"));
    }

    let mut body = vec![0_u8; u64_to_usize(body_len, "blob record length")?];
    read_blob_exact_at(
        object,
        frame_end,
        &mut body,
        "referenced blob record body cannot be read",
    )?;
    if checksum(&body) != record_checksum {
        return Err(Error::Corruption {
            message: "blob record checksum mismatch".to_owned(),
        });
    }

    decode_record_body(index.file_id, index.offset, record_checksum, &body)
}

pub(super) async fn read_indexed_blob_record_async(
    object: &impl StorageReadObject,
    file_len: u64,
    index: &BlobIndex,
) -> Result<BlobFileRecord> {
    if index.offset < BLOB_HEADER_LEN as u64 {
        return Err(invalid_blob("blob index offset points before records"));
    }

    let frame_end = checked_blob_offset_add(
        index.offset,
        MIN_BLOB_RECORD_FRAME_BYTES as u64,
        "blob record frame bounds",
    )?;
    if frame_end > file_len {
        return Err(invalid_blob("blob index frame is outside the blob file"));
    }

    let mut frame = [0_u8; MIN_BLOB_RECORD_FRAME_BYTES];
    read_blob_exact_at_async(
        object,
        index.offset,
        &mut frame,
        "referenced blob record frame cannot be read",
    )
    .await?;
    let body_len = checked_blob_record_body_len(read_u64_at(&frame, 0)?)?;
    let record_checksum = read_u32_at(&frame, 8)?;
    let body_end = checked_blob_offset_add(frame_end, body_len, "blob record body bounds")?;
    if body_end > file_len {
        return Err(invalid_blob("blob index body is outside the blob file"));
    }

    let mut body = vec![0_u8; u64_to_usize(body_len, "blob record length")?];
    read_blob_exact_at_async(
        object,
        frame_end,
        &mut body,
        "referenced blob record body cannot be read",
    )
    .await?;
    if checksum(&body) != record_checksum {
        return Err(Error::Corruption {
            message: "blob record checksum mismatch".to_owned(),
        });
    }

    decode_record_body(index.file_id, index.offset, record_checksum, &body)
}

pub(super) fn validate_blob_record_order(records: &[BlobRecord]) -> Result<()> {
    for pair in records.windows(2) {
        if pair[0].internal_key > pair[1].internal_key {
            return Err(Error::invalid_options(
                "blob records must be sorted by internal key",
            ));
        }
    }
    Ok(())
}

pub(super) fn decode_records(file_id: u64, bytes: &[u8]) -> Result<Vec<BlobFileRecord>> {
    decode_records_with_budget(file_id, bytes, limits::MAX_WHOLE_BLOB_DECODE_BYTES)
}

pub(super) fn decode_records_with_budget(
    file_id: u64,
    bytes: &[u8],
    max_decoded_value_bytes: usize,
) -> Result<Vec<BlobFileRecord>> {
    let mut cursor = Cursor::new(bytes);
    let mut records = Vec::new();
    let mut decoded_value_bytes = 0_usize;
    while cursor.remaining_len() != 0 {
        if cursor.remaining_len() < MIN_BLOB_RECORD_FRAME_BYTES {
            return Err(invalid_blob("short blob record frame"));
        }
        let offset = usize_to_u64(cursor.offset, "blob record offset")?
            .checked_add(BLOB_HEADER_LEN as u64)
            .ok_or_else(|| invalid_blob("blob record offset overflow"))?;
        let body_len = checked_blob_record_body_len(cursor.read_u64()?)?;
        let body_len = u64_to_usize(body_len, "blob record length")?;
        let record_checksum = cursor.read_u32()?;
        let body = cursor.read_exact(body_len)?;
        if checksum(body) != record_checksum {
            return Err(Error::Corruption {
                message: "blob record checksum mismatch".to_owned(),
            });
        }
        let remaining_budget = max_decoded_value_bytes
            .checked_sub(decoded_value_bytes)
            .ok_or_else(|| invalid_blob("aggregate decoded blob value budget underflow"))?;
        let record = decode_record_body_with_budget(
            file_id,
            offset,
            record_checksum,
            body,
            remaining_budget,
            max_decoded_value_bytes,
        )?;
        decoded_value_bytes = decoded_value_bytes
            .checked_add(record.record.value.len())
            .ok_or_else(|| invalid_blob("aggregate decoded blob value length overflow"))?;
        records.push(record);
    }

    for pair in records.windows(2) {
        if pair[0].record.internal_key > pair[1].record.internal_key {
            return Err(Error::Corruption {
                message: "blob records are not ordered by internal key".to_owned(),
            });
        }
    }
    Ok(records)
}

pub(super) fn decode_record_body(
    file_id: u64,
    offset: u64,
    record_checksum: u32,
    body: &[u8],
) -> Result<BlobFileRecord> {
    decode_record_body_with_budget(
        file_id,
        offset,
        record_checksum,
        body,
        limits::MAX_DECODED_BLOCK_BYTES,
        limits::MAX_DECODED_BLOCK_BYTES,
    )
}

fn decode_record_body_with_budget(
    file_id: u64,
    offset: u64,
    record_checksum: u32,
    body: &[u8],
    remaining_decoded_budget: usize,
    max_decoded_value_bytes: usize,
) -> Result<BlobFileRecord> {
    let mut cursor = Cursor::new(body);
    let internal_key = cursor.read_internal_key()?;
    if internal_key.kind() != ValueKind::Put {
        return Err(invalid_blob("blob record internal key is not a put"));
    }
    let value_len = cursor.read_u64()?;
    let decoded_value_len = checked_blob_read_len(value_len)?;
    if decoded_value_len > remaining_decoded_budget {
        return Err(Error::InvalidFormat {
            message: format!(
                "aggregate decoded blob values exceed maximum {max_decoded_value_bytes}"
            ),
        });
    }
    let encoded_len = cursor.read_u64()?;
    let compression = cursor.read_codec()?;
    let value_checksum = cursor.read_u32()?;
    let encoded_value = cursor.read_exact(u64_to_usize(encoded_len, "encoded blob length")?)?;
    if cursor.remaining_len() != 0 {
        return Err(invalid_blob("blob record has trailing bytes"));
    }

    let value =
        codec::decode_block(compression, encoded_value, decoded_value_len).map_err(|error| {
            Error::Corruption {
                message: format!("blob value cannot be decoded: {error}"),
            }
        })?;
    if checksum(&value) != value_checksum {
        return Err(Error::Corruption {
            message: "blob value checksum mismatch".to_owned(),
        });
    }

    Ok(BlobFileRecord {
        index: BlobIndex {
            file_id,
            offset,
            encoded_len,
            value_len,
            value_checksum,
            record_checksum,
            compression,
        },
        record: BlobRecord {
            internal_key,
            value,
            compression,
        },
    })
}

pub(super) fn properties_from_records(records: &[BlobFileRecord]) -> Result<BlobFileProperties> {
    let first = records
        .first()
        .ok_or_else(|| Error::invalid_options("cannot build blob properties without records"))?;
    let last = records
        .last()
        .ok_or_else(|| Error::invalid_options("cannot build blob properties without records"))?;
    let mut smallest_sequence = first.record.internal_key.sequence();
    let mut largest_sequence = first.record.internal_key.sequence();
    let mut value_bytes = 0_u64;
    let mut encoded_bytes = 0_u64;
    let mut compression_saved_bytes = 0_u64;

    for record in records {
        let sequence = record.record.internal_key.sequence();
        smallest_sequence = smallest_sequence.min(sequence);
        largest_sequence = largest_sequence.max(sequence);
        value_bytes = value_bytes.saturating_add(record.index.value_len);
        encoded_bytes = encoded_bytes.saturating_add(record.index.encoded_len);
        compression_saved_bytes = compression_saved_bytes.saturating_add(
            record
                .index
                .value_len
                .saturating_sub(record.index.encoded_len),
        );
    }

    Ok(BlobFileProperties {
        record_count: usize_to_u64(records.len(), "blob record count")?,
        value_bytes,
        encoded_bytes,
        compression_saved_bytes,
        smallest_internal_key: first.record.internal_key.clone(),
        largest_internal_key: last.record.internal_key.clone(),
        smallest_sequence,
        largest_sequence,
    })
}

pub(super) fn put_header(bytes: &mut Vec<u8>, header: BlobFileHeader) {
    let start = bytes.len();
    put_u32(bytes, BLOB_MAGIC);
    put_u16(bytes, BLOB_FILE_FORMAT_VERSION);
    put_u64(bytes, header.file_id);
    put_u64(bytes, header.creation_sequence.get());
    put_u64(bytes, header.bucket_options_digest);
    put_u64(bytes, header.blob_threshold_bytes);
    put_codec(bytes, header.default_compression);
    let header_checksum = checksum(&bytes[start..]);
    put_u32(bytes, header_checksum);
}

pub(super) fn decode_header(bytes: &[u8]) -> Result<BlobFileHeader> {
    let header_bytes = bytes
        .get(..BLOB_HEADER_LEN)
        .ok_or_else(|| invalid_blob("short header"))?;
    let expected_checksum = read_u32_at(header_bytes, BLOB_HEADER_WITHOUT_CHECKSUM_LEN)?;
    if checksum(&header_bytes[..BLOB_HEADER_WITHOUT_CHECKSUM_LEN]) != expected_checksum {
        return Err(Error::Corruption {
            message: "blob header checksum mismatch".to_owned(),
        });
    }

    let mut cursor = Cursor::new(header_bytes);
    let magic = cursor.read_u32()?;
    if magic != BLOB_MAGIC {
        return Err(invalid_blob("magic mismatch"));
    }
    let version = cursor.read_u16()?;
    if version != BLOB_FILE_FORMAT_VERSION {
        return Err(Error::UnsupportedFormat {
            message: format!("unsupported blob file version {version}"),
        });
    }
    Ok(BlobFileHeader {
        file_id: cursor.read_u64()?,
        creation_sequence: Sequence::new(cursor.read_u64()?),
        bucket_options_digest: cursor.read_u64()?,
        blob_threshold_bytes: cursor.read_u64()?,
        default_compression: cursor.read_codec()?,
    })
}

pub(super) fn encode_properties(properties: &BlobFileProperties) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    put_u64(&mut bytes, properties.record_count);
    put_u64(&mut bytes, properties.value_bytes);
    put_u64(&mut bytes, properties.encoded_bytes);
    put_u64(&mut bytes, properties.compression_saved_bytes);
    put_internal_key(&mut bytes, &properties.smallest_internal_key)?;
    put_internal_key(&mut bytes, &properties.largest_internal_key)?;
    put_u64(&mut bytes, properties.smallest_sequence.get());
    put_u64(&mut bytes, properties.largest_sequence.get());
    let properties_checksum = checksum(&bytes);
    put_u32(&mut bytes, properties_checksum);
    Ok(bytes)
}

pub(super) fn decode_properties(bytes: &[u8]) -> Result<BlobFileProperties> {
    if bytes.len() < 4 {
        return Err(invalid_blob("short properties block"));
    }
    let checksum_offset = bytes.len() - 4;
    let stored_checksum = read_u32_at(bytes, checksum_offset)?;
    if checksum(&bytes[..checksum_offset]) != stored_checksum {
        return Err(Error::Corruption {
            message: "blob properties checksum mismatch".to_owned(),
        });
    }

    let mut cursor = Cursor::new(&bytes[..checksum_offset]);
    let properties = BlobFileProperties {
        record_count: cursor.read_u64()?,
        value_bytes: cursor.read_u64()?,
        encoded_bytes: cursor.read_u64()?,
        compression_saved_bytes: cursor.read_u64()?,
        smallest_internal_key: cursor.read_internal_key()?,
        largest_internal_key: cursor.read_internal_key()?,
        smallest_sequence: Sequence::new(cursor.read_u64()?),
        largest_sequence: Sequence::new(cursor.read_u64()?),
    };
    if cursor.remaining_len() != 0 {
        return Err(invalid_blob("blob properties have trailing bytes"));
    }
    Ok(properties)
}

pub(super) fn put_footer(bytes: &mut Vec<u8>, properties_offset: u64, properties_len: u64) {
    let mut footer = Vec::with_capacity(BLOB_FOOTER_LEN);
    put_u64(&mut footer, properties_offset);
    put_u64(&mut footer, properties_len);
    let footer_checksum = checksum(&footer);
    put_u32(&mut footer, footer_checksum);
    put_u32(&mut footer, BLOB_FOOTER_MAGIC);
    bytes.extend_from_slice(&footer);
}

pub(super) fn decode_footer(footer: &[u8]) -> Result<(u64, u64)> {
    if footer.len() != BLOB_FOOTER_LEN {
        return Err(invalid_blob("short footer"));
    }
    let magic = read_u32_at(footer, BLOB_FOOTER_LEN - 4)?;
    if magic != BLOB_FOOTER_MAGIC {
        return Err(invalid_blob("footer magic mismatch"));
    }
    let expected_checksum = read_u32_at(footer, 16)?;
    if checksum(&footer[..16]) != expected_checksum {
        return Err(Error::Corruption {
            message: "blob footer checksum mismatch".to_owned(),
        });
    }
    Ok((read_u64_at(footer, 0)?, read_u64_at(footer, 8)?))
}

pub(super) fn put_internal_key(bytes: &mut Vec<u8>, internal_key: &InternalKey) -> Result<()> {
    put_bytes(bytes, internal_key.user_key())?;
    put_u64(bytes, internal_key.sequence().get());
    put_value_kind(bytes, internal_key.kind());
    put_u32(bytes, internal_key.batch_index());
    Ok(())
}

pub(super) fn put_value_kind(bytes: &mut Vec<u8>, kind: ValueKind) {
    put_u8(
        bytes,
        match kind {
            ValueKind::Put => 1,
            ValueKind::PointDelete => 2,
            ValueKind::RangeDelete => 3,
        },
    );
}

pub(super) fn value_kind_from_tag(tag: u8) -> Result<ValueKind> {
    match tag {
        1 => Ok(ValueKind::Put),
        2 => Ok(ValueKind::PointDelete),
        3 => Ok(ValueKind::RangeDelete),
        tag => Err(Error::InvalidFormat {
            message: format!("unknown blob value kind {tag}"),
        }),
    }
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

pub(super) fn codec_from_tag(tag: u8) -> Result<CodecId> {
    match tag {
        0 => Ok(CodecId::None),
        1 => Ok(CodecId::FastLz4Block),
        tag => Err(Error::UnsupportedFormat {
            message: format!("unknown blob codec {tag}"),
        }),
    }
}

pub(super) fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

pub(super) fn put_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("blob byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

pub(super) fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = limits::checked_add_invalid_format(offset, 4, "u32 offset")?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_blob("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

pub(super) fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = limits::checked_add_invalid_format(offset, 8, "u64 offset")?;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| invalid_blob("short u64"))?;
    Ok(u64::from_le_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

pub(super) fn checked_blob_offset_add(left: u64, right: u64, field: &'static str) -> Result<u64> {
    left.checked_add(right).ok_or_else(|| Error::Corruption {
        message: format!("{field} overflow"),
    })
}

pub(super) fn usize_to_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u64::MAX")))
}

pub(super) fn u64_to_usize(value: u64, field: &'static str) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::Corruption {
        message: format!("{field} exceeds usize"),
    })
}

pub(super) fn checked_blob_read_len(len: u64) -> Result<usize> {
    let len = u64_to_usize(len, "blob length")?;
    limits::ensure_corruption_len(len, limits::MAX_DECODED_BLOCK_BYTES, "blob length")?;
    Ok(len)
}

pub(super) fn checked_whole_blob_file_len(len: u64) -> Result<u64> {
    let usize_len = u64_to_usize(len, "blob file length")?;
    limits::ensure_corruption_len(
        usize_len,
        limits::MAX_WHOLE_BLOB_DECODE_BYTES,
        "blob file length",
    )?;
    Ok(len)
}

pub(super) fn checked_blob_record_body_len(len: u64) -> Result<u64> {
    let usize_len = u64_to_usize(len, "blob record length")?;
    limits::ensure_invalid_format_len(
        usize_len,
        limits::MAX_BLOB_RECORD_BODY_BYTES,
        "blob record length",
    )?;
    Ok(len)
}

pub(super) fn checked_blob_properties_len(len: u64) -> Result<u64> {
    let usize_len = u64_to_usize(len, "blob properties length")?;
    limits::ensure_invalid_format_len(
        usize_len,
        limits::MAX_BLOB_PROPERTIES_BYTES,
        "blob properties length",
    )?;
    Ok(len)
}

pub(super) fn ensure_blob_value_len(len: usize) -> Result<()> {
    if len <= limits::MAX_DECODED_BLOCK_BYTES {
        return Ok(());
    }

    Err(Error::invalid_options(format!(
        "blob value length {len} exceeds maximum {}",
        limits::MAX_DECODED_BLOCK_BYTES
    )))
}

pub(super) fn ensure_blob_file_len(len: usize) -> Result<()> {
    if len <= limits::MAX_WHOLE_BLOB_DECODE_BYTES {
        return Ok(());
    }

    Err(Error::invalid_options(format!(
        "blob file length {len} exceeds maximum {}",
        limits::MAX_WHOLE_BLOB_DECODE_BYTES
    )))
}

pub(super) fn invalid_blob(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid blob file: {message}"),
    }
}

pub(super) fn checksum(bytes: &[u8]) -> u32 {
    crate::checksum::crc32c(bytes)
}

pub(super) struct Cursor<'payload> {
    payload: &'payload [u8],
    offset: usize,
}

impl<'payload> Cursor<'payload> {
    const fn new(payload: &'payload [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    const fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }

    fn read_exact(&mut self, len: usize) -> Result<&'payload [u8]> {
        let end = limits::checked_add_invalid_format(self.offset, len, "byte field length")?;
        let value = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| invalid_blob("short byte field"))?;
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .payload
            .get(self.offset)
            .ok_or_else(|| invalid_blob("short u8"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let value = self.read_exact(2)?;
        Ok(u16::from_le_bytes([value[0], value[1]]))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let value = self.read_exact(4)?;
        Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let value = self.read_exact(8)?;
        Ok(u64::from_le_bytes([
            value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
        ]))
    }

    fn read_bytes(&mut self) -> Result<&'payload [u8]> {
        let len = self.read_u32()? as usize;
        self.read_exact(len)
    }

    fn read_internal_key(&mut self) -> Result<InternalKey> {
        let user_key = self.read_bytes()?.to_vec();
        let sequence = Sequence::new(self.read_u64()?);
        let kind = value_kind_from_tag(self.read_u8()?)?;
        let batch_index = self.read_u32()?;
        Ok(InternalKey::new(user_key, sequence, kind, batch_index))
    }

    fn read_codec(&mut self) -> Result<CodecId> {
        codec_from_tag(self.read_u8()?)
    }
}
