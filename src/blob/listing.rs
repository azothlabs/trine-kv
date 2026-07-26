use super::{
    BLOB_FILE_EXTENSION, BTreeSet, BlockingStorageObjectListBackend, Error, NativeFileBackend,
    Path, Result, StorageCapability, StorageObjectId, StorageObjectKind, StorageObjectListBackend,
    StorageObjectListRequest, StorageReadBackend, blob_storage_backend,
};

pub(crate) fn list_blob_file_ids(db_path: &Path) -> Result<BTreeSet<u64>> {
    let backend = blob_storage_backend();
    list_blob_file_ids_with_backend(&backend, db_path)
}

pub(crate) fn list_blob_file_ids_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<BTreeSet<u64>> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectListing)?;
    let request = StorageObjectListRequest::native_file(StorageObjectKind::Blob, db_path)
        .with_file_extension(BLOB_FILE_EXTENSION);
    blob_file_ids_from_objects(backend.list_objects_blocking(request)?)
}

#[allow(dead_code)]
pub(crate) async fn list_blob_file_ids_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
) -> Result<BTreeSet<u64>>
where
    B: StorageObjectListBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::ObjectListing)?;
    let request = StorageObjectListRequest::native_file(StorageObjectKind::Blob, db_path)
        .with_file_extension(BLOB_FILE_EXTENSION);
    blob_file_ids_from_objects(backend.list_objects(request).await?)
}

pub(crate) fn blob_file_ids_from_objects(
    objects: impl IntoIterator<Item = StorageObjectId>,
) -> Result<BTreeSet<u64>> {
    let mut file_ids = BTreeSet::new();
    for object in objects {
        if let Some(file_id) = blob_file_id_from_path(object.path())? {
            file_ids.insert(file_id);
        }
    }

    Ok(file_ids)
}

pub(crate) fn blob_file_id_from_path(path: &Path) -> Result<Option<u64>> {
    let has_blob_extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case(BLOB_FILE_EXTENSION));
    if !has_blob_extension {
        return Ok(None);
    }

    let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
        return Ok(None);
    };
    let Some(file_id) = stem.strip_prefix("blob-") else {
        return Ok(None);
    };
    file_id
        .parse::<u64>()
        .map(Some)
        .map_err(|_| Error::Corruption {
            message: format!("invalid blob file name: {}", path.display()),
        })
}
