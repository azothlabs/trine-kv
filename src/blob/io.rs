use super::{
    Arc, BLOB_FOOTER_LEN, BLOB_HEADER_LEN, BlobFile, BlobFileHeader, BlobFileProperties, BlobIndex,
    BlobRecord, BlockingStorageObjectWriteBackend, BlockingStorageReadBackend,
    BlockingStorageReadObject, Bytes, DurabilityMode, Error, NativeFileBackend, Path, Result,
    StorageCapability, StorageObjectId, StorageObjectKind, StorageObjectWriteBackend,
    StorageReadBackend, StorageReadObject, blob_path, checked_blob_offset_add,
    checked_blob_properties_len, checked_whole_blob_file_len, decode_blob_file, decode_footer,
    decode_properties, encode_blob_file, invalid_blob, u64_to_usize, validate_indexed_blob_header,
};

#[allow(dead_code)]
pub(crate) fn validate_blob_file(db_path: &Path, file_id: u64) -> Result<BlobFileProperties> {
    let blob_file = read_blob_file(db_path, file_id)?;
    Ok(blob_file.properties)
}

pub(crate) fn validate_blob_file_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    file_id: u64,
) -> Result<BlobFileProperties> {
    let blob_file = read_blob_file_with_backend(backend, db_path, file_id)?;
    Ok(blob_file.properties)
}

#[allow(dead_code)]
pub(crate) fn read_blob_file_properties(
    db_path: &Path,
    file_id: u64,
) -> Result<BlobFileProperties> {
    let backend = blob_storage_backend();
    read_blob_file_properties_with_backend(&backend, db_path, file_id)
}

pub(crate) fn read_blob_file_properties_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    file_id: u64,
) -> Result<BlobFileProperties> {
    // GC candidate selection needs byte estimates, not value payloads. Recovery
    // still uses `read_blob_file` so every referenced record is fully checked.
    let object = open_blob_read_object_with_backend(backend, db_path, file_id)?;
    let file_len = blob_object_len(&object, "referenced blob file metadata cannot be read")?;
    if file_len < (BLOB_HEADER_LEN + BLOB_FOOTER_LEN) as u64 {
        return Err(invalid_blob("file is too short"));
    }

    validate_indexed_blob_header(&object, file_id)?;
    let footer_start = file_len - BLOB_FOOTER_LEN as u64;
    let mut footer = [0_u8; BLOB_FOOTER_LEN];
    read_blob_exact_at(
        &object,
        footer_start,
        &mut footer,
        "referenced blob footer cannot be read",
    )?;
    let (properties_offset, properties_len) = decode_footer(&footer)?;
    let properties_len = checked_blob_properties_len(properties_len)?;
    let properties_end =
        checked_blob_offset_add(properties_offset, properties_len, "blob properties bounds")?;
    if properties_offset < BLOB_HEADER_LEN as u64 || properties_end > footer_start {
        return Err(invalid_blob("properties bounds are outside the blob file"));
    }

    let mut properties_bytes = vec![0_u8; u64_to_usize(properties_len, "blob properties length")?];
    read_blob_exact_at(
        &object,
        properties_offset,
        &mut properties_bytes,
        "referenced blob properties cannot be read",
    )?;
    decode_properties(&properties_bytes)
}

#[allow(dead_code)]
pub(crate) async fn read_blob_file_properties_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    file_id: u64,
) -> Result<BlobFileProperties>
where
    B: StorageReadBackend,
{
    let blob_file = read_blob_file_with_backend_async(backend, db_path, file_id).await?;
    Ok(blob_file.properties)
}

pub(crate) fn read_blob_file(db_path: &Path, file_id: u64) -> Result<BlobFile> {
    let backend = blob_storage_backend();
    read_blob_file_with_backend(&backend, db_path, file_id)
}

pub(crate) fn read_blob_file_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    file_id: u64,
) -> Result<BlobFile> {
    let object = open_blob_read_object_with_backend(backend, db_path, file_id)?;
    let file_len = blob_object_len(&object, "referenced blob file metadata cannot be read")?;
    let file_len = checked_whole_blob_file_len(file_len)?;
    let mut bytes = vec![0_u8; u64_to_usize(file_len, "blob file length")?];
    read_blob_exact_at(
        &object,
        0,
        &mut bytes,
        "referenced blob file cannot be read",
    )?;
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

#[allow(dead_code)]
pub(crate) async fn read_blob_file_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    file_id: u64,
) -> Result<BlobFile>
where
    B: StorageReadBackend,
{
    let bytes = read_blob_file_bytes_with_backend_async(backend, db_path, file_id).await?;
    let blob_file = decode_blob_file(bytes.as_ref())?;
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

#[allow(dead_code)]
pub(crate) fn write_blob_file(
    db_path: &Path,
    file_id: u64,
    header: BlobFileHeader,
    records: &[BlobRecord],
) -> Result<Vec<BlobIndex>> {
    let backend = blob_storage_backend();
    write_blob_file_with_backend(&backend, db_path, file_id, header, records)
}

pub(crate) fn write_blob_file_with_backend(
    backend: &NativeFileBackend,
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

#[allow(dead_code)]
pub(crate) async fn write_blob_file_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    file_id: u64,
    header: BlobFileHeader,
    records: &[BlobRecord],
    durability: DurabilityMode,
) -> Result<Vec<BlobIndex>>
where
    B: StorageObjectWriteBackend,
{
    if header.file_id != file_id {
        return Err(Error::invalid_options(
            "blob header file id must match the output file id",
        ));
    }
    let (blob_bytes, indexes) = encode_blob_file(header, records)?;
    let path = blob_path(db_path, file_id);
    backend
        .capabilities()
        .require(StorageCapability::ObjectWrite)?;
    backend
        .write_object(
            blob_storage_object(&path),
            Arc::from(blob_bytes.into_boxed_slice()),
            durability,
        )
        .await?;
    Ok(indexes)
}

pub(super) fn blob_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::Blob, path)
}

pub(super) fn open_blob_read_object_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    file_id: u64,
) -> Result<<NativeFileBackend as StorageReadBackend>::ReadObject> {
    let path = blob_path(db_path, file_id);
    backend
        .capabilities()
        .require(StorageCapability::RandomRead)?;
    backend.open_read_blocking(blob_storage_object(&path))
}

pub(super) async fn open_blob_read_object_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    file_id: u64,
) -> Result<B::ReadObject>
where
    B: StorageReadBackend,
{
    let path = blob_path(db_path, file_id);
    backend
        .capabilities()
        .require(StorageCapability::RandomRead)?;
    backend.open_read(blob_storage_object(&path)).await
}

pub(super) async fn read_blob_file_bytes_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    file_id: u64,
) -> Result<Bytes>
where
    B: StorageReadBackend,
{
    let object = open_blob_read_object_with_backend_async(backend, db_path, file_id).await?;
    let file_len =
        blob_object_len_async(&object, "referenced blob file metadata cannot be read").await?;
    let file_len = checked_whole_blob_file_len(file_len)?;
    let file_len = u64_to_usize(file_len, "blob file length")?;
    let bytes = object
        .read_exact_at_owned(0, file_len)
        .await
        .map_err(|error| blob_read_error("referenced blob file cannot be read", &error))?;
    Ok(bytes.into_bytes())
}

pub(super) fn blob_object_len(
    object: &impl BlockingStorageReadObject,
    context: &'static str,
) -> Result<u64> {
    object
        .len_blocking()
        .map_err(|error| blob_read_error(context, &error))
}

pub(super) async fn blob_object_len_async(
    object: &impl StorageReadObject,
    context: &'static str,
) -> Result<u64> {
    object
        .len()
        .await
        .map_err(|error| blob_read_error(context, &error))
}

pub(super) fn read_blob_exact_at(
    object: &impl BlockingStorageReadObject,
    offset: u64,
    bytes: &mut [u8],
    context: &'static str,
) -> Result<()> {
    let offset = u64_to_usize(offset, "blob read offset")?;
    object
        .read_exact_at_blocking(offset, bytes)
        .map_err(|error| blob_read_error(context, &error))
}

pub(super) async fn read_blob_exact_at_async(
    object: &impl StorageReadObject,
    offset: u64,
    bytes: &mut [u8],
    context: &'static str,
) -> Result<()> {
    let offset = u64_to_usize(offset, "blob read offset")?;
    object
        .read_exact_at(offset, bytes)
        .await
        .map_err(|error| blob_read_error(context, &error))
}

pub(super) fn blob_read_error(context: &'static str, error: &Error) -> Error {
    Error::Corruption {
        message: format!("{context}: {error}"),
    }
}

pub(super) fn blob_storage_backend() -> NativeFileBackend {
    NativeFileBackend::new()
}
