use super::{
    Arc, BTreeMap, BlockingStorageDirectoryListBackend, DEFAULT_BUCKET_NAME, DbOptions, Error,
    FailOnCorruptionPolicy, LsmTree, ManifestState, ManifestStore, NativeFileBackend, Path, Result,
    StorageCapability, StorageDirectoryFile, StorageDirectoryId, StorageDirectoryListBackend,
    StorageManifestReadBackend, StorageObjectListBackend, StorageObjectReadBackend,
    StorageReadBackend, allowed_blob_file_ids_from_manifest, io, manifest, recovery,
    referenced_blob_file_ids_from_manifest, referenced_table_file_ids, table,
    validate_bucket_options,
};

pub(in crate::db) fn acquire_persistent_process_lock(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
) -> Result<Option<recovery::ProcessLock>> {
    if options.read_only {
        return Ok(None);
    }
    recovery::ProcessLock::acquire_with_backend(backend, db_path).map(Some)
}

pub(in crate::db) async fn acquire_persistent_process_lock_async(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
) -> Result<Option<recovery::ProcessLock>> {
    if options.read_only {
        return Ok(None);
    }
    recovery::ProcessLock::acquire_with_backend_async(backend, db_path)
        .await
        .map(Some)
}

pub(in crate::db) fn list_persistent_directory_files(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<Vec<StorageDirectoryFile>> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    backend.list_directory_files_blocking(StorageDirectoryId::native_file(db_path))
}

pub(in crate::db) async fn list_persistent_directory_files_async(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<Vec<StorageDirectoryFile>> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    backend
        .list_directory_files(StorageDirectoryId::native_file(db_path))
        .await
}

pub(in crate::db) fn repair_safe_temporary_files_for_open(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
    directory_files: &[StorageDirectoryFile],
) -> Result<()> {
    let policy = if options.read_only {
        FailOnCorruptionPolicy::FailClosed
    } else {
        options.fail_on_corruption
    };
    recovery::repair_safe_temporary_files_from_directory_files_with_backend(
        backend,
        db_path,
        policy,
        directory_files,
    )?;
    Ok(())
}

pub(in crate::db) async fn repair_safe_temporary_files_for_open_from_directory_files_async(
    backend: &NativeFileBackend,
    db_path: &Path,
    options: &DbOptions,
    directory_files: &[StorageDirectoryFile],
) -> Result<()> {
    let policy = if options.read_only {
        FailOnCorruptionPolicy::FailClosed
    } else {
        options.fail_on_corruption
    };
    recovery::repair_safe_temporary_files_from_directory_files_with_backend_async(
        backend,
        db_path,
        policy,
        directory_files,
    )
    .await?;
    Ok(())
}

pub(in crate::db) fn run_persistent_recovery_checks(
    backend: &NativeFileBackend,
    db_path: &Path,
    manifest: &ManifestState,
    directory_files: &[StorageDirectoryFile],
) -> Result<()> {
    let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest);
    let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest);
    recovery::fail_on_missing_referenced_blob_files_with_backend(
        backend,
        db_path,
        &referenced_blob_ids,
    )?;
    recovery::fail_on_invalid_referenced_blob_files_with_backend(backend, db_path, manifest)?;
    recovery::fail_on_unreferenced_storage_files_from_directory_files(
        db_path,
        directory_files,
        &referenced_table_file_ids(manifest),
        &allowed_blob_ids,
    )
}

pub(in crate::db) async fn run_persistent_recovery_checks_from_directory_files_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
    directory_files: &[StorageDirectoryFile],
) -> Result<()>
where
    B: StorageReadBackend + StorageObjectReadBackend,
{
    let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest);
    let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest);
    recovery::fail_on_missing_referenced_blob_files_with_backend_async(
        backend,
        db_path,
        &referenced_blob_ids,
    )
    .await?;
    recovery::fail_on_invalid_referenced_blob_files_with_backend_async(backend, db_path, manifest)
        .await?;
    recovery::fail_on_unreferenced_storage_files_from_directory_files(
        db_path,
        directory_files,
        &referenced_table_file_ids(manifest),
        &allowed_blob_ids,
    )
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    allow(dead_code)
)]
pub(in crate::db) async fn run_persistent_recovery_checks_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<()>
where
    B: StorageReadBackend
        + StorageObjectReadBackend
        + StorageObjectListBackend
        + StorageDirectoryListBackend,
{
    let referenced_blob_ids = referenced_blob_file_ids_from_manifest(manifest);
    let allowed_blob_ids = allowed_blob_file_ids_from_manifest(manifest);
    recovery::fail_on_missing_referenced_blob_files_with_backend_async(
        backend,
        db_path,
        &referenced_blob_ids,
    )
    .await?;
    recovery::fail_on_invalid_referenced_blob_files_with_backend_async(backend, db_path, manifest)
        .await?;
    recovery::fail_on_unreferenced_storage_files_with_backend_async(
        backend,
        db_path,
        &referenced_table_file_ids(manifest),
        &allowed_blob_ids,
    )
    .await
}

pub(in crate::db) fn buckets_from_manifest(
    backend: &NativeFileBackend,
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<BTreeMap<String, Arc<LsmTree>>> {
    let mut buckets = BTreeMap::new();

    for (name, options) in manifest.buckets() {
        validate_bucket_options(options)?;
        let mut tables = Vec::new();
        for properties in manifest.tables().get(name).into_iter().flatten() {
            let table_path = table::table_path(db_path, properties.id);
            let table = table::read_table_with_backend(backend, &table_path)?
                .with_manifest_properties(properties)?;
            tables.push(Arc::new(table));
        }

        buckets.insert(
            name.clone(),
            Arc::new(LsmTree::new_with_generation(
                options.clone(),
                tables,
                manifest
                    .bucket_generation(name)
                    .ok_or_else(|| Error::Corruption {
                        message: format!("manifest bucket {name:?} has no generation"),
                    })?,
            )?),
        );
    }

    Ok(buckets)
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    allow(dead_code)
)]
pub(in crate::db) async fn buckets_from_manifest_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
    inline_blob_values: bool,
) -> Result<BTreeMap<String, Arc<LsmTree>>>
where
    B: StorageReadBackend + StorageObjectReadBackend,
{
    let mut buckets = BTreeMap::new();

    for (name, options) in manifest.buckets() {
        validate_bucket_options(options)?;
        let mut tables = Vec::new();
        for properties in manifest.tables().get(name).into_iter().flatten() {
            let table_path = table::table_path(db_path, properties.id);
            let mut table = table::read_table_with_backend_async(backend, &table_path)
                .await?
                .with_manifest_properties(properties)?;
            if inline_blob_values {
                table =
                    table::inline_blob_values_with_backend_async(backend, db_path, table).await?;
            }
            tables.push(Arc::new(table));
        }

        buckets.insert(
            name.clone(),
            Arc::new(LsmTree::new_with_generation(
                options.clone(),
                tables,
                manifest
                    .bucket_generation(name)
                    .ok_or_else(|| Error::Corruption {
                        message: format!("manifest bucket {name:?} has no generation"),
                    })?,
            )?),
        );
    }

    Ok(buckets)
}

#[allow(dead_code)]
pub(in crate::db) async fn read_manifest_or_empty_with_backend_async<B>(
    backend: &B,
    path: &Path,
) -> Result<ManifestState>
where
    B: StorageManifestReadBackend,
{
    match manifest::read_manifest_with_backend_async(backend, path).await {
        Ok(manifest) => Ok(manifest),
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            Ok(ManifestState::empty())
        }
        Err(error) => Err(error),
    }
}

pub(in crate::db) fn ensure_default_bucket_in_manifest(
    manifest: &mut ManifestStore,
    options: &DbOptions,
) -> Result<()> {
    if manifest.state().buckets().contains_key(DEFAULT_BUCKET_NAME) || options.read_only {
        return Ok(());
    }

    manifest.create_bucket(DEFAULT_BUCKET_NAME, options.default_bucket_options.clone())
}

#[cfg_attr(
    not(all(target_arch = "wasm32", target_os = "unknown")),
    allow(dead_code)
)]
pub(in crate::db) async fn ensure_default_bucket_in_manifest_async(
    manifest: &mut ManifestStore,
    options: &DbOptions,
) -> Result<()> {
    if manifest.state().buckets().contains_key(DEFAULT_BUCKET_NAME) || options.read_only {
        return Ok(());
    }

    manifest
        .create_bucket_async(
            DEFAULT_BUCKET_NAME.to_owned(),
            options.default_bucket_options.clone(),
        )
        .await
}

pub(in crate::db) fn ensure_default_bucket_loaded(
    buckets: &mut BTreeMap<String, Arc<LsmTree>>,
    options: &DbOptions,
) -> Result<()> {
    if buckets.contains_key(DEFAULT_BUCKET_NAME) {
        return Ok(());
    }

    // Read-only opens cannot publish a missing manifest entry, but the public
    // API still treats the default bucket as always present.
    buckets.insert(
        DEFAULT_BUCKET_NAME.to_owned(),
        Arc::new(LsmTree::new(
            options.default_bucket_options.clone(),
            Vec::new(),
        )?),
    );
    Ok(())
}
