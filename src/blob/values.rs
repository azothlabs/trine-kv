use super::{
    BTreeMap, BlobFileHeader, BlobRecord, CodecId, DurabilityMode, Entry, Error, InternalKey,
    NativeFileBackend, NativeFileObject, Path, Result, Sequence, StorageObjectWriteBackend,
    StorageReadBackend, ValueRef, blob_object_len, blob_storage_backend, checked_blob_read_len,
    checksum, open_blob_read_object_with_backend, open_blob_read_object_with_backend_async,
    read_blob_exact_at, read_blob_exact_at_async, read_indexed_blob_record,
    read_indexed_value_with_backend, read_indexed_value_with_backend_async, usize_to_u64,
    validate_indexed_blob_header, write_blob_file_with_backend_async,
    write_blob_file_with_backend_with_durability,
};

#[allow(dead_code)]
pub(crate) fn write_large_values(
    db_path: &Path,
    file_id: u64,
    threshold: usize,
    compression: CodecId,
    records: &[(InternalKey, Option<ValueRef>)],
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let backend = blob_storage_backend();
    write_large_values_with_backend(&backend, db_path, file_id, threshold, compression, records)
}

pub(crate) fn write_large_values_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    file_id: u64,
    threshold: usize,
    compression: CodecId,
    records: &[(InternalKey, Option<ValueRef>)],
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    write_large_values_with_backend_with_durability(
        backend,
        db_path,
        file_id,
        threshold,
        compression,
        records,
        DurabilityMode::SyncAll,
    )
}

pub(crate) fn write_large_values_with_backend_with_durability(
    backend: &NativeFileBackend,
    db_path: &Path,
    file_id: u64,
    threshold: usize,
    compression: CodecId,
    records: &[(InternalKey, Option<ValueRef>)],
    durability: DurabilityMode,
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
    let indexes = write_blob_file_with_backend_with_durability(
        backend,
        db_path,
        file_id,
        header,
        &blob_records,
        durability,
    )?;
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
pub(crate) async fn write_large_values_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    file_id: u64,
    threshold: usize,
    compression: CodecId,
    records: &[(InternalKey, Option<ValueRef>)],
    durability: DurabilityMode,
) -> Result<Vec<(InternalKey, Option<ValueRef>)>>
where
    B: StorageObjectWriteBackend,
{
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
    let indexes = write_blob_file_with_backend_async(
        backend,
        db_path,
        file_id,
        header,
        &blob_records,
        durability,
    )
    .await?;
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

#[allow(dead_code)]
pub(crate) fn inline_blob_values(
    db_path: &Path,
    records: &[(InternalKey, Option<ValueRef>)],
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let backend = blob_storage_backend();
    inline_blob_values_with_backend(&backend, db_path, records)
}

pub(crate) fn inline_blob_values_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    records: &[(InternalKey, Option<ValueRef>)],
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let mut rewritten = Vec::with_capacity(records.len());
    let mut blob_files = BTreeMap::new();
    for (internal_key, value) in records {
        let value = match value {
            Some(ValueRef::Inline(bytes)) => Some(ValueRef::Inline(bytes.clone())),
            Some(value @ (ValueRef::BlobIndex(_) | ValueRef::Blob { .. })) => {
                Some(ValueRef::Inline(read_value_for_internal_key_cached(
                    backend,
                    db_path,
                    value,
                    Some(internal_key),
                    &mut blob_files,
                )?))
            }
            None => None,
        };
        rewritten.push((internal_key.clone(), value));
    }
    Ok(rewritten)
}

pub(super) struct CachedBlobFile {
    object: NativeFileObject,
    len: u64,
}

pub(super) fn read_value_for_internal_key_cached(
    backend: &NativeFileBackend,
    db_path: &Path,
    value: &ValueRef,
    expected_internal_key: Option<&InternalKey>,
    blob_files: &mut BTreeMap<u64, CachedBlobFile>,
) -> Result<Vec<u8>> {
    match value {
        ValueRef::Inline(bytes) => Ok(bytes.clone()),
        ValueRef::BlobIndex(index) => {
            let blob_file = cached_blob_file(backend, db_path, index.file_id, blob_files)?;
            let record = read_indexed_blob_record(&blob_file.object, blob_file.len, index)?;
            if record.index != *index {
                return Err(Error::Corruption {
                    message: "blob index metadata mismatch".to_owned(),
                });
            }
            if expected_internal_key.is_some_and(|expected| record.record.internal_key != *expected)
            {
                return Err(Error::Corruption {
                    message: "blob record internal key mismatch".to_owned(),
                });
            }
            Ok(record.record.value)
        }
        ValueRef::Blob {
            file_id,
            offset,
            len,
            checksum: expected_checksum,
        } => {
            let len = checked_blob_read_len(*len)?;
            let blob_file = cached_blob_file(backend, db_path, *file_id, blob_files)?;
            let mut bytes = vec![0_u8; len];
            read_blob_exact_at(
                &blob_file.object,
                *offset,
                &mut bytes,
                "referenced blob bytes cannot be read",
            )?;
            if checksum(&bytes) != *expected_checksum {
                return Err(Error::Corruption {
                    message: "blob checksum mismatch".to_owned(),
                });
            }
            Ok(bytes)
        }
    }
}

pub(super) fn cached_blob_file<'files>(
    backend: &NativeFileBackend,
    db_path: &Path,
    file_id: u64,
    blob_files: &'files mut BTreeMap<u64, CachedBlobFile>,
) -> Result<&'files CachedBlobFile> {
    match blob_files.entry(file_id) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => {
            let object = open_blob_read_object_with_backend(backend, db_path, file_id)?;
            let len = blob_object_len(&object, "referenced blob file metadata cannot be read")?;
            validate_indexed_blob_header(&object, file_id)?;
            Ok(entry.insert(CachedBlobFile { object, len }))
        }
    }
}
pub(crate) async fn inline_blob_values_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    records: &[(InternalKey, Option<ValueRef>)],
) -> Result<Vec<(InternalKey, Option<ValueRef>)>>
where
    B: StorageReadBackend,
{
    let mut rewritten = Vec::with_capacity(records.len());
    for (internal_key, value) in records {
        let value = match value {
            Some(ValueRef::Inline(bytes)) => Some(ValueRef::Inline(bytes.clone())),
            Some(value @ (ValueRef::BlobIndex(_) | ValueRef::Blob { .. })) => {
                Some(ValueRef::Inline(
                    read_value_for_internal_key_with_backend_async(
                        backend,
                        db_path,
                        value,
                        Some(internal_key),
                    )
                    .await?,
                ))
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
    let backend = blob_storage_backend();
    read_value_for_internal_key_with_backend(&backend, db_path, value, expected_internal_key)
}

pub(crate) fn read_value_for_internal_key_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    value: &ValueRef,
    expected_internal_key: Option<&InternalKey>,
) -> Result<Vec<u8>> {
    match value {
        ValueRef::Inline(bytes) => Ok(bytes.clone()),
        ValueRef::BlobIndex(index) => {
            read_indexed_value_with_backend(backend, db_path, index, expected_internal_key)
        }
        ValueRef::Blob {
            file_id,
            offset,
            len,
            checksum: expected_checksum,
        } => {
            let len = checked_blob_read_len(*len)?;
            let mut bytes = vec![0_u8; len];
            let object = open_blob_read_object_with_backend(backend, db_path, *file_id)?;
            read_blob_exact_at(
                &object,
                *offset,
                &mut bytes,
                "referenced blob bytes cannot be read",
            )?;
            if checksum(&bytes) != *expected_checksum {
                return Err(Error::Corruption {
                    message: "blob checksum mismatch".to_owned(),
                });
            }
            Ok(bytes)
        }
    }
}
pub(crate) async fn read_value_for_internal_key_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    value: &ValueRef,
    expected_internal_key: Option<&InternalKey>,
) -> Result<Vec<u8>>
where
    B: StorageReadBackend,
{
    match value {
        ValueRef::Inline(bytes) => Ok(bytes.clone()),
        ValueRef::BlobIndex(index) => {
            read_indexed_value_with_backend_async(backend, db_path, index, expected_internal_key)
                .await
        }
        ValueRef::Blob {
            file_id,
            offset,
            len,
            checksum: expected_checksum,
        } => {
            let object =
                open_blob_read_object_with_backend_async(backend, db_path, *file_id).await?;
            let len = checked_blob_read_len(*len)?;
            let mut bytes = vec![0_u8; len];
            read_blob_exact_at_async(
                &object,
                *offset,
                &mut bytes,
                "referenced blob bytes cannot be read",
            )
            .await?;
            if checksum(&bytes) != *expected_checksum {
                return Err(Error::Corruption {
                    message: "blob checksum mismatch".to_owned(),
                });
            }
            Ok(bytes)
        }
    }
}
