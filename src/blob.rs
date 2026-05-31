use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    codec::{self, CodecId},
    error::{Error, Result},
    internal_key::{InternalKey, ValueKind},
    options::DurabilityMode,
    storage::{
        BlockingStorageObjectWriteBackend, NativeFileBackend, StorageCapability, StorageObjectId,
        StorageObjectKind, StorageReadBackend,
    },
    types::Sequence,
};

pub const BLOB_FILE_EXTENSION: &str = "trineb";
pub const BLOB_FILE_FORMAT_VERSION: u16 = 2;

const BLOB_MAGIC: u32 = 0x5452_424c;
const BLOB_FOOTER_MAGIC: u32 = 0x5452_4246;
const BLOB_HEADER_WITHOUT_CHECKSUM_LEN: usize = 39;
const BLOB_HEADER_LEN: usize = BLOB_HEADER_WITHOUT_CHECKSUM_LEN + 4;
const BLOB_FOOTER_LEN: usize = 24;
const MIN_BLOB_RECORD_FRAME_BYTES: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobIndex {
    pub file_id: u64,
    pub offset: u64,
    pub encoded_len: u64,
    pub value_len: u64,
    pub value_checksum: u32,
    pub record_checksum: u32,
    pub compression: CodecId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobFileHeader {
    pub file_id: u64,
    pub creation_sequence: Sequence,
    pub bucket_options_digest: u64,
    pub blob_threshold_bytes: u64,
    pub default_compression: CodecId,
}

impl BlobFileHeader {
    #[must_use]
    pub const fn new(
        file_id: u64,
        creation_sequence: Sequence,
        blob_threshold_bytes: u64,
        default_compression: CodecId,
    ) -> Self {
        Self {
            file_id,
            creation_sequence,
            bucket_options_digest: 0,
            blob_threshold_bytes,
            default_compression,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRecord {
    pub internal_key: InternalKey,
    pub value: Vec<u8>,
    pub compression: CodecId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobFileRecord {
    pub index: BlobIndex,
    pub record: BlobRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobFileProperties {
    pub record_count: u64,
    pub value_bytes: u64,
    pub encoded_bytes: u64,
    pub compression_saved_bytes: u64,
    pub smallest_internal_key: InternalKey,
    pub largest_internal_key: InternalKey,
    pub smallest_sequence: Sequence,
    pub largest_sequence: Sequence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobFile {
    pub header: BlobFileHeader,
    pub properties: BlobFileProperties,
    pub records: Vec<BlobFileRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueRef {
    Inline(Vec<u8>),
    BlobIndex(BlobIndex),
    Blob {
        file_id: u64,
        offset: u64,
        len: u64,
        checksum: u32,
    },
}

impl ValueRef {
    #[must_use]
    pub fn len(&self) -> u64 {
        match self {
            Self::Inline(bytes) => bytes.len() as u64,
            Self::BlobIndex(index) => index.value_len,
            Self::Blob { len, .. } => *len,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline(bytes) => Some(bytes),
            Self::BlobIndex(_) | Self::Blob { .. } => None,
        }
    }
}

#[must_use]
pub fn blob_path(db_path: &Path, file_id: u64) -> PathBuf {
    db_path.join(format!("blob-{file_id:020}.{BLOB_FILE_EXTENSION}",))
}

pub(crate) fn list_blob_file_ids(db_path: &Path) -> Result<BTreeSet<u64>> {
    let mut file_ids = BTreeSet::new();

    for entry in fs::read_dir(db_path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let path = entry.path();
        let has_blob_extension = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(BLOB_FILE_EXTENSION));
        if !has_blob_extension {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Some(file_id) = stem.strip_prefix("blob-") else {
            continue;
        };
        let file_id = file_id.parse::<u64>().map_err(|_| Error::Corruption {
            message: format!("invalid blob file name: {}", path.display()),
        })?;
        file_ids.insert(file_id);
    }

    Ok(file_ids)
}

pub(crate) fn write_large_values(
    db_path: &Path,
    file_id: u64,
    threshold: usize,
    compression: CodecId,
    records: &[(InternalKey, Option<ValueRef>)],
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let needs_blob_file = records.iter().any(
        |(_, value)| matches!(value, Some(ValueRef::Inline(bytes)) if bytes.len() >= threshold),
    );
    if !needs_blob_file {
        return Ok(records.to_vec());
    }

    let mut blob_records = Vec::new();
    for (internal_key, value) in records {
        if let Some(ValueRef::Inline(bytes)) = value {
            if bytes.len() >= threshold {
                blob_records.push(BlobRecord {
                    internal_key: internal_key.clone(),
                    value: bytes.clone(),
                    compression,
                });
            }
        }
    }

    let creation_sequence = records
        .iter()
        .map(|(internal_key, _)| internal_key.sequence())
        .max()
        .unwrap_or(Sequence::ZERO);
    let threshold_bytes = usize_to_u64(threshold, "blob threshold")?;
    let header = BlobFileHeader::new(file_id, creation_sequence, threshold_bytes, compression);
    let indexes = write_blob_file(db_path, file_id, header, &blob_records)?;
    let mut index_iter = indexes.into_iter();

    let mut rewritten = Vec::with_capacity(records.len());

    for (internal_key, value) in records {
        let value = match value {
            Some(ValueRef::Inline(bytes)) if bytes.len() >= threshold => {
                let index = index_iter.next().ok_or_else(|| Error::Corruption {
                    message: "missing blob index for separated value".to_owned(),
                })?;
                Some(ValueRef::BlobIndex(index))
            }
            value => value.clone(),
        };
        rewritten.push((internal_key.clone(), value));
    }
    if index_iter.next().is_some() {
        return Err(Error::Corruption {
            message: "unused blob index after rewriting large values".to_owned(),
        });
    }

    Ok(rewritten)
}

pub(crate) fn inline_blob_values(
    db_path: &Path,
    records: &[(InternalKey, Option<ValueRef>)],
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let mut rewritten = Vec::with_capacity(records.len());
    for (internal_key, value) in records {
        let value = match value {
            Some(ValueRef::Inline(bytes)) => Some(ValueRef::Inline(bytes.clone())),
            Some(value @ (ValueRef::BlobIndex(_) | ValueRef::Blob { .. })) => {
                Some(ValueRef::Inline(read_value_for_internal_key(
                    db_path,
                    value,
                    Some(internal_key),
                )?))
            }
            None => None,
        };
        rewritten.push((internal_key.clone(), value));
    }
    Ok(rewritten)
}

pub(crate) fn read_value_for_internal_key(
    db_path: &Path,
    value: &ValueRef,
    expected_internal_key: Option<&InternalKey>,
) -> Result<Vec<u8>> {
    match value {
        ValueRef::Inline(bytes) => Ok(bytes.clone()),
        ValueRef::BlobIndex(index) => read_indexed_value(db_path, index, expected_internal_key),
        ValueRef::Blob {
            file_id,
            offset,
            len,
            checksum: expected_checksum,
        } => {
            let len = usize::try_from(*len).map_err(|_| Error::Corruption {
                message: "blob length exceeds usize".to_owned(),
            })?;
            let mut file =
                File::open(blob_path(db_path, *file_id)).map_err(|error| Error::Corruption {
                    message: format!("referenced blob file cannot be opened: {error}"),
                })?;
            file.seek(SeekFrom::Start(*offset))?;
            let mut bytes = vec![0_u8; len];
            file.read_exact(&mut bytes)
                .map_err(|error| Error::Corruption {
                    message: format!("referenced blob bytes cannot be read: {error}"),
                })?;
            if checksum(&bytes) != *expected_checksum {
                return Err(Error::Corruption {
                    message: "blob checksum mismatch".to_owned(),
                });
            }
            Ok(bytes)
        }
    }
}

pub(crate) fn validate_blob_file(db_path: &Path, file_id: u64) -> Result<BlobFileProperties> {
    let blob_file = read_blob_file(db_path, file_id)?;
    Ok(blob_file.properties)
}

pub(crate) fn read_blob_file_properties(
    db_path: &Path,
    file_id: u64,
) -> Result<BlobFileProperties> {
    // GC candidate selection needs byte estimates, not value payloads. Recovery
    // still uses `read_blob_file` so every referenced record is fully checked.
    let mut file = File::open(blob_path(db_path, file_id)).map_err(|error| Error::Corruption {
        message: format!("referenced blob file cannot be opened: {error}"),
    })?;
    let file_len = file
        .metadata()
        .map_err(|error| Error::Corruption {
            message: format!("referenced blob file metadata cannot be read: {error}"),
        })?
        .len();
    if file_len < (BLOB_HEADER_LEN + BLOB_FOOTER_LEN) as u64 {
        return Err(invalid_blob("file is too short"));
    }

    validate_indexed_blob_header(&mut file, file_id)?;
    let footer_start = file_len - BLOB_FOOTER_LEN as u64;
    file.seek(SeekFrom::Start(footer_start))?;
    let mut footer = [0_u8; BLOB_FOOTER_LEN];
    file.read_exact(&mut footer)
        .map_err(|error| Error::Corruption {
            message: format!("referenced blob footer cannot be read: {error}"),
        })?;
    let (properties_offset, properties_len) = decode_footer(&footer)?;
    let properties_end =
        checked_blob_offset_add(properties_offset, properties_len, "blob properties bounds")?;
    if properties_offset < BLOB_HEADER_LEN as u64 || properties_end > footer_start {
        return Err(invalid_blob("properties bounds are outside the blob file"));
    }

    file.seek(SeekFrom::Start(properties_offset))?;
    let mut properties_bytes = vec![0_u8; u64_to_usize(properties_len, "blob properties length")?];
    file.read_exact(&mut properties_bytes)
        .map_err(|error| Error::Corruption {
            message: format!("referenced blob properties cannot be read: {error}"),
        })?;
    decode_properties(&properties_bytes)
}

pub(crate) fn read_blob_file(db_path: &Path, file_id: u64) -> Result<BlobFile> {
    let mut bytes = Vec::new();
    File::open(blob_path(db_path, file_id))
        .map_err(|error| Error::Corruption {
            message: format!("referenced blob file cannot be opened: {error}"),
        })?
        .read_to_end(&mut bytes)
        .map_err(|error| Error::Corruption {
            message: format!("referenced blob file cannot be read: {error}"),
        })?;
    let blob_file = decode_blob_file(&bytes)?;
    if blob_file.header.file_id != file_id {
        return Err(Error::Corruption {
            message: format!(
                "blob file id mismatch: path has {file_id}, header has {}",
                blob_file.header.file_id
            ),
        });
    }
    Ok(blob_file)
}

pub(crate) fn write_blob_file(
    db_path: &Path,
    file_id: u64,
    header: BlobFileHeader,
    records: &[BlobRecord],
) -> Result<Vec<BlobIndex>> {
    if header.file_id != file_id {
        return Err(Error::invalid_options(
            "blob header file id must match the output file id",
        ));
    }
    let (blob_bytes, indexes) = encode_blob_file(header, records)?;
    let path = blob_path(db_path, file_id);
    let backend = blob_storage_backend();
    backend
        .capabilities()
        .require(StorageCapability::ObjectWrite)?;
    backend.write_object_blocking(
        blob_storage_object(&path),
        Arc::from(blob_bytes.into_boxed_slice()),
        DurabilityMode::SyncAll,
    )?;
    Ok(indexes)
}

fn blob_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::Blob, path)
}

fn blob_storage_backend() -> NativeFileBackend {
    NativeFileBackend::new()
}

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

    let header = decode_header(bytes)?;
    let footer_start = bytes.len() - BLOB_FOOTER_LEN;
    let (properties_offset, properties_len) = decode_footer(&bytes[footer_start..])?;
    let properties_start = u64_to_usize(properties_offset, "blob properties offset")?;
    let properties_len = u64_to_usize(properties_len, "blob properties length")?;
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

fn read_indexed_value(
    db_path: &Path,
    index: &BlobIndex,
    expected_internal_key: Option<&InternalKey>,
) -> Result<Vec<u8>> {
    Ok(
        read_record_for_index(db_path, index, expected_internal_key)?
            .record
            .value,
    )
}

pub(crate) fn read_record_for_index(
    db_path: &Path,
    index: &BlobIndex,
    expected_internal_key: Option<&InternalKey>,
) -> Result<BlobFileRecord> {
    let mut file =
        File::open(blob_path(db_path, index.file_id)).map_err(|error| Error::Corruption {
            message: format!("referenced blob file cannot be opened: {error}"),
        })?;
    let file_len = file
        .metadata()
        .map_err(|error| Error::Corruption {
            message: format!("referenced blob file metadata cannot be read: {error}"),
        })?
        .len();
    validate_indexed_blob_header(&mut file, index.file_id)?;
    let record = read_indexed_blob_record(&mut file, file_len, index)?;

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

fn validate_indexed_blob_header(file: &mut File, expected_file_id: u64) -> Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut header_bytes = [0_u8; BLOB_HEADER_LEN];
    file.read_exact(&mut header_bytes)
        .map_err(|error| Error::Corruption {
            message: format!("referenced blob header cannot be read: {error}"),
        })?;
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

fn read_indexed_blob_record(
    file: &mut File,
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

    file.seek(SeekFrom::Start(index.offset))?;
    let mut frame = [0_u8; MIN_BLOB_RECORD_FRAME_BYTES];
    file.read_exact(&mut frame)
        .map_err(|error| Error::Corruption {
            message: format!("referenced blob record frame cannot be read: {error}"),
        })?;
    let body_len = read_u64_at(&frame, 0)?;
    let record_checksum = read_u32_at(&frame, 8)?;
    let body_end = checked_blob_offset_add(frame_end, body_len, "blob record body bounds")?;
    if body_end > file_len {
        return Err(invalid_blob("blob index body is outside the blob file"));
    }

    let mut body = vec![0_u8; u64_to_usize(body_len, "blob record length")?];
    file.read_exact(&mut body)
        .map_err(|error| Error::Corruption {
            message: format!("referenced blob record body cannot be read: {error}"),
        })?;
    if checksum(&body) != record_checksum {
        return Err(Error::Corruption {
            message: "blob record checksum mismatch".to_owned(),
        });
    }

    decode_record_body(index.file_id, index.offset, record_checksum, &body)
}

fn validate_blob_record_order(records: &[BlobRecord]) -> Result<()> {
    for pair in records.windows(2) {
        if pair[0].internal_key > pair[1].internal_key {
            return Err(Error::invalid_options(
                "blob records must be sorted by internal key",
            ));
        }
    }
    Ok(())
}

fn decode_records(file_id: u64, bytes: &[u8]) -> Result<Vec<BlobFileRecord>> {
    let mut cursor = Cursor::new(bytes);
    let mut records = Vec::new();
    while cursor.remaining_len() != 0 {
        if cursor.remaining_len() < MIN_BLOB_RECORD_FRAME_BYTES {
            return Err(invalid_blob("short blob record frame"));
        }
        let offset = usize_to_u64(cursor.offset, "blob record offset")?
            .checked_add(BLOB_HEADER_LEN as u64)
            .ok_or_else(|| invalid_blob("blob record offset overflow"))?;
        let body_len = u64_to_usize(cursor.read_u64()?, "blob record length")?;
        let record_checksum = cursor.read_u32()?;
        let body = cursor.read_exact(body_len)?;
        if checksum(body) != record_checksum {
            return Err(Error::Corruption {
                message: "blob record checksum mismatch".to_owned(),
            });
        }
        records.push(decode_record_body(file_id, offset, record_checksum, body)?);
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

fn decode_record_body(
    file_id: u64,
    offset: u64,
    record_checksum: u32,
    body: &[u8],
) -> Result<BlobFileRecord> {
    let mut cursor = Cursor::new(body);
    let internal_key = cursor.read_internal_key()?;
    if internal_key.kind() != ValueKind::Put {
        return Err(invalid_blob("blob record internal key is not a put"));
    }
    let value_len = cursor.read_u64()?;
    let encoded_len = cursor.read_u64()?;
    let compression = cursor.read_codec()?;
    let value_checksum = cursor.read_u32()?;
    let encoded_value = cursor.read_exact(u64_to_usize(encoded_len, "encoded blob length")?)?;
    if cursor.remaining_len() != 0 {
        return Err(invalid_blob("blob record has trailing bytes"));
    }

    let value = codec::decode_block(
        compression,
        encoded_value,
        u64_to_usize(value_len, "blob value length")?,
    )
    .map_err(|error| Error::Corruption {
        message: format!("blob value cannot be decoded: {error}"),
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

fn properties_from_records(records: &[BlobFileRecord]) -> Result<BlobFileProperties> {
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

fn put_header(bytes: &mut Vec<u8>, header: BlobFileHeader) {
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

fn decode_header(bytes: &[u8]) -> Result<BlobFileHeader> {
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

fn encode_properties(properties: &BlobFileProperties) -> Result<Vec<u8>> {
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

fn decode_properties(bytes: &[u8]) -> Result<BlobFileProperties> {
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
    Ok(BlobFileProperties {
        record_count: cursor.read_u64()?,
        value_bytes: cursor.read_u64()?,
        encoded_bytes: cursor.read_u64()?,
        compression_saved_bytes: cursor.read_u64()?,
        smallest_internal_key: cursor.read_internal_key()?,
        largest_internal_key: cursor.read_internal_key()?,
        smallest_sequence: Sequence::new(cursor.read_u64()?),
        largest_sequence: Sequence::new(cursor.read_u64()?),
    })
}

fn put_footer(bytes: &mut Vec<u8>, properties_offset: u64, properties_len: u64) {
    let mut footer = Vec::with_capacity(BLOB_FOOTER_LEN);
    put_u64(&mut footer, properties_offset);
    put_u64(&mut footer, properties_len);
    let footer_checksum = checksum(&footer);
    put_u32(&mut footer, footer_checksum);
    put_u32(&mut footer, BLOB_FOOTER_MAGIC);
    bytes.extend_from_slice(&footer);
}

fn decode_footer(footer: &[u8]) -> Result<(u64, u64)> {
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

fn put_internal_key(bytes: &mut Vec<u8>, internal_key: &InternalKey) -> Result<()> {
    put_bytes(bytes, internal_key.user_key())?;
    put_u64(bytes, internal_key.sequence().get());
    put_value_kind(bytes, internal_key.kind());
    put_u32(bytes, internal_key.batch_index());
    Ok(())
}

fn put_value_kind(bytes: &mut Vec<u8>, kind: ValueKind) {
    put_u8(
        bytes,
        match kind {
            ValueKind::Put => 1,
            ValueKind::PointDelete => 2,
            ValueKind::RangeDelete => 3,
        },
    );
}

fn value_kind_from_tag(tag: u8) -> Result<ValueKind> {
    match tag {
        1 => Ok(ValueKind::Put),
        2 => Ok(ValueKind::PointDelete),
        3 => Ok(ValueKind::RangeDelete),
        tag => Err(Error::InvalidFormat {
            message: format!("unknown blob value kind {tag}"),
        }),
    }
}

fn put_codec(bytes: &mut Vec<u8>, codec: CodecId) {
    put_u8(
        bytes,
        match codec {
            CodecId::None => 0,
            CodecId::FastLz4Block => 1,
        },
    );
}

fn codec_from_tag(tag: u8) -> Result<CodecId> {
    match tag {
        0 => Ok(CodecId::None),
        1 => Ok(CodecId::FastLz4Block),
        tag => Err(Error::UnsupportedFormat {
            message: format!("unknown blob codec {tag}"),
        }),
    }
}

fn put_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn put_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("blob byte field exceeds u32::MAX"))?;
    put_u32(bytes, len);
    bytes.extend_from_slice(value);
    Ok(())
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_blob("short u32"))?;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid_blob("short u64"))?;
    Ok(u64::from_le_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

fn checked_blob_offset_add(left: u64, right: u64, field: &'static str) -> Result<u64> {
    left.checked_add(right).ok_or_else(|| Error::Corruption {
        message: format!("{field} overflow"),
    })
}

fn usize_to_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u64::MAX")))
}

fn u64_to_usize(value: u64, field: &'static str) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::Corruption {
        message: format!("{field} exceeds usize"),
    })
}

fn invalid_blob(message: &'static str) -> Error {
    Error::InvalidFormat {
        message: format!("invalid blob file: {message}"),
    }
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

struct Cursor<'payload> {
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
        let value = self
            .payload
            .get(self.offset..self.offset + len)
            .ok_or_else(|| invalid_blob("short byte field"))?;
        self.offset += len;
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

#[cfg(test)]
mod tests {
    use crate::{
        blob::{
            BlobFileHeader, BlobRecord, decode_blob_file, encode_blob_file, read_indexed_value,
        },
        codec::CodecId,
        internal_key::{InternalKey, ValueKind},
        types::Sequence,
    };

    #[test]
    fn blob_file_round_trips_ordered_records() {
        let header = BlobFileHeader::new(7, Sequence::new(42), 64 * 1024, CodecId::None);
        let records = vec![
            blob_record("user:1", 3, 0, b"Ada".to_vec(), CodecId::None),
            blob_record(
                "user:2",
                2,
                1,
                b"Lin Lin Lin Lin".to_vec(),
                CodecId::FastLz4Block,
            ),
        ];

        let (bytes, indexes) = encode_blob_file(header, &records).expect("blob encodes");
        let decoded = decode_blob_file(&bytes).expect("blob decodes");

        assert_eq!(decoded.header, header);
        assert_eq!(indexes.len(), 2);
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].index, indexes[0]);
        assert_eq!(decoded.records[0].record, records[0]);
        assert_eq!(decoded.records[1].index, indexes[1]);
        assert_eq!(decoded.records[1].record, records[1]);
        assert_eq!(decoded.properties.record_count, 2);
        assert_eq!(
            decoded.properties.value_bytes,
            (b"Ada".len() + b"Lin Lin Lin Lin".len()) as u64
        );
    }

    #[test]
    fn blob_file_rejects_corrupt_footer() {
        let (mut bytes, _) = encode_blob_file(
            BlobFileHeader::new(9, Sequence::new(1), 8, CodecId::None),
            &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
        )
        .expect("blob encodes");

        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;

        let error = decode_blob_file(&bytes).expect_err("corrupt footer fails");
        assert!(error.to_string().contains("footer magic mismatch"));
    }

    #[test]
    fn blob_file_rejects_header_checksum_mismatch() {
        let (mut bytes, _) = encode_blob_file(
            BlobFileHeader::new(9, Sequence::new(1), 8, CodecId::None),
            &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
        )
        .expect("blob encodes");

        bytes[4] ^= 0xff;

        let error = decode_blob_file(&bytes).expect_err("corrupt header fails");
        assert!(error.to_string().contains("blob header checksum mismatch"));
    }

    #[test]
    fn blob_file_rejects_properties_checksum_mismatch() {
        let (mut bytes, _) = encode_blob_file(
            BlobFileHeader::new(9, Sequence::new(1), 8, CodecId::None),
            &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
        )
        .expect("blob encodes");
        let footer_start = bytes.len() - super::BLOB_FOOTER_LEN;
        let properties_offset = usize::try_from(
            super::read_u64_at(&bytes[footer_start..], 0).expect("footer offset reads"),
        )
        .expect("footer offset fits usize");

        bytes[properties_offset] ^= 0xff;

        let error = decode_blob_file(&bytes).expect_err("corrupt properties fail");
        assert!(
            error
                .to_string()
                .contains("blob properties checksum mismatch")
        );
    }

    #[test]
    fn blob_file_rejects_record_checksum_mismatch() {
        let (mut bytes, _) = encode_blob_file(
            BlobFileHeader::new(10, Sequence::new(1), 8, CodecId::None),
            &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
        )
        .expect("blob encodes");

        bytes[super::BLOB_HEADER_LEN + super::MIN_BLOB_RECORD_FRAME_BYTES] ^= 0xff;

        let error = decode_blob_file(&bytes).expect_err("corrupt record fails");
        assert!(error.to_string().contains("blob record checksum mismatch"));
    }

    #[test]
    fn blob_file_rejects_value_checksum_mismatch() {
        let (mut bytes, _) = encode_blob_file(
            BlobFileHeader::new(10, Sequence::new(1), 8, CodecId::None),
            &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
        )
        .expect("blob encodes");

        let body_start = super::BLOB_HEADER_LEN + super::MIN_BLOB_RECORD_FRAME_BYTES;
        let value_checksum_offset = body_start + internal_key_len("key") + 8 + 8 + 1;
        bytes[value_checksum_offset] ^= 0xff;
        rewrite_record_checksum(&mut bytes);

        let error = decode_blob_file(&bytes).expect_err("corrupt value checksum fails");
        assert!(error.to_string().contains("blob value checksum mismatch"));
    }

    #[test]
    fn blob_file_rejects_unknown_record_compression() {
        let (mut bytes, _) = encode_blob_file(
            BlobFileHeader::new(10, Sequence::new(1), 8, CodecId::None),
            &[blob_record("key", 1, 0, b"value".to_vec(), CodecId::None)],
        )
        .expect("blob encodes");

        let body_start = super::BLOB_HEADER_LEN + super::MIN_BLOB_RECORD_FRAME_BYTES;
        let compression_offset = body_start + internal_key_len("key") + 8 + 8;
        bytes[compression_offset] = 9;
        rewrite_record_checksum(&mut bytes);

        let error = decode_blob_file(&bytes).expect_err("unknown codec fails");
        assert!(error.to_string().contains("unknown blob codec 9"));
    }

    #[test]
    fn blob_file_rejects_unordered_records() {
        let header = BlobFileHeader::new(11, Sequence::new(1), 8, CodecId::None);
        let records = vec![
            blob_record("z", 1, 0, b"value".to_vec(), CodecId::None),
            blob_record("a", 1, 0, b"value".to_vec(), CodecId::None),
        ];

        let error = encode_blob_file(header, &records).expect_err("unordered records fail");
        assert!(error.to_string().contains("sorted by internal key"));
    }

    #[test]
    fn indexed_read_validates_exact_blob_index() {
        let temp = temp_blob_dir("indexed-read-validates");

        let header = BlobFileHeader::new(12, Sequence::new(1), 8, CodecId::None);
        let record = blob_record("key", 1, 0, b"value".to_vec(), CodecId::None);
        let (bytes, indexes) = encode_blob_file(header, &[record]).expect("blob encodes");
        std::fs::write(super::blob_path(&temp, 12), bytes).expect("blob writes");

        let value = read_indexed_value(&temp, &indexes[0], None).expect("indexed read works");
        assert_eq!(value, b"value");

        let mut bad_index = indexes[0];
        bad_index.value_len += 1;
        let error = read_indexed_value(&temp, &bad_index, None).expect_err("bad index fails");
        assert!(error.to_string().contains("metadata mismatch"));

        std::fs::remove_dir_all(temp).expect("cleanup temp dir");
    }

    #[test]
    fn indexed_read_uses_only_target_record() {
        let temp = temp_blob_dir("indexed-read-target-record");

        let header = BlobFileHeader::new(13, Sequence::new(1), 8, CodecId::None);
        let records = vec![
            blob_record("key-a", 1, 0, b"value-a".to_vec(), CodecId::None),
            blob_record("key-b", 1, 0, b"value-b".to_vec(), CodecId::None),
        ];
        let (mut bytes, indexes) = encode_blob_file(header, &records).expect("blob encodes");
        let corrupt_second_body = usize::try_from(indexes[1].offset)
            .expect("offset fits usize")
            .saturating_add(super::MIN_BLOB_RECORD_FRAME_BYTES);
        bytes[corrupt_second_body] ^= 0xff;
        assert!(
            decode_blob_file(&bytes).is_err(),
            "full blob decode should notice the unrelated corrupt record"
        );
        std::fs::write(super::blob_path(&temp, 13), bytes).expect("blob writes");

        let value = read_indexed_value(&temp, &indexes[0], None)
            .expect("targeted indexed read skips unrelated corrupt record");
        assert_eq!(value, b"value-a");

        std::fs::remove_dir_all(temp).expect("cleanup temp dir");
    }

    #[test]
    fn properties_read_skips_record_payload_decode() {
        let temp = temp_blob_dir("properties-read");

        let header = BlobFileHeader::new(14, Sequence::new(1), 8, CodecId::None);
        let records = vec![
            blob_record("key-a", 1, 0, b"value-a".to_vec(), CodecId::None),
            blob_record("key-b", 1, 0, b"value-b".to_vec(), CodecId::None),
        ];
        let (mut bytes, indexes) = encode_blob_file(header, &records).expect("blob encodes");
        let expected = decode_blob_file(&bytes)
            .expect("blob decodes before corruption")
            .properties;
        let corrupt_second_body = usize::try_from(indexes[1].offset)
            .expect("offset fits usize")
            .saturating_add(super::MIN_BLOB_RECORD_FRAME_BYTES);
        bytes[corrupt_second_body] ^= 0xff;
        std::fs::write(super::blob_path(&temp, 14), bytes).expect("blob writes");

        assert_eq!(
            super::read_blob_file_properties(&temp, 14).expect("properties read succeeds"),
            expected
        );
        assert!(
            super::read_blob_file(&temp, 14).is_err(),
            "full validation should still decode and verify blob records"
        );

        std::fs::remove_dir_all(temp).expect("cleanup temp dir");
    }

    #[test]
    fn write_blob_file_creates_final_object_without_leftover_tmp() {
        let temp = temp_blob_dir("write-object");
        let header = BlobFileHeader::new(15, Sequence::new(1), 8, CodecId::None);
        let record = blob_record("key", 1, 0, b"value".to_vec(), CodecId::None);

        let indexes =
            super::write_blob_file(&temp, 15, header, &[record]).expect("blob file writes");

        let path = super::blob_path(&temp, 15);
        assert_eq!(indexes.len(), 1);
        assert!(path.exists(), "final blob object should exist");
        assert!(
            !path.with_extension("tmp").exists(),
            "successful blob write should leave no temporary file"
        );
        assert_eq!(
            super::read_blob_file(&temp, 15)
                .expect("written blob file reads")
                .header,
            header
        );

        std::fs::remove_dir_all(temp).expect("cleanup temp dir");
    }

    fn blob_record(
        key: &str,
        sequence: u64,
        batch_index: u32,
        value: Vec<u8>,
        compression: CodecId,
    ) -> BlobRecord {
        BlobRecord {
            internal_key: InternalKey::new(
                key,
                Sequence::new(sequence),
                ValueKind::Put,
                batch_index,
            ),
            value,
            compression,
        }
    }

    fn internal_key_len(key: &str) -> usize {
        4 + key.len() + 8 + 1 + 4
    }

    fn rewrite_record_checksum(bytes: &mut [u8]) {
        let checksum_offset = super::BLOB_HEADER_LEN + 8;
        let body_start = super::BLOB_HEADER_LEN + super::MIN_BLOB_RECORD_FRAME_BYTES;
        let body_len = usize::try_from(
            super::read_u64_at(bytes, super::BLOB_HEADER_LEN).expect("record length reads"),
        )
        .expect("record length fits usize");
        let checksum = super::checksum(&bytes[body_start..body_start + body_len]);
        bytes[checksum_offset..checksum_offset + 4].copy_from_slice(&checksum.to_le_bytes());
    }

    fn temp_blob_dir(name: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "trine-kv-blob-{name}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir creates");
        dir
    }
}
