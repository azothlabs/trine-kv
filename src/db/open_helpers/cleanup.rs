use super::{
    Arc, BlockingStorageDirectoryCreateBackend, BlockingStorageDirectorySyncBackend,
    BlockingStorageObjectDeleteBackend, Error, ManifestStore, Mutex, NativeFileBackend, Path,
    Result, SnapshotTracker, StorageCapability, StorageDirectoryCreateBackend, StorageDirectoryId,
    StorageDirectorySyncBackend, StorageObjectDeleteBackend, StorageObjectId, StorageObjectKind,
    StorageReadBackend, Table, blob, deletable_pending_blob_file_ids, lock_poisoned, table,
};

pub(in crate::db) fn cleanup_pending_obsolete_table_files(
    backend: &NativeFileBackend,
    db_path: Option<&Path>,
    pending_tables: &Mutex<Vec<Arc<Table>>>,
) -> Result<()> {
    let Some(db_path) = db_path else {
        return Ok(());
    };

    let deletable = take_deletable_obsolete_tables(pending_tables)?;
    if deletable.is_empty() {
        return Ok(());
    }

    let table_ids = deletable
        .iter()
        .map(|table| table.properties().id)
        .collect::<Vec<_>>();
    if let Err(error) = remove_table_files(backend, db_path, &table_ids) {
        pending_tables
            .lock()
            .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?
            .extend(deletable);
        return Err(error);
    }

    Ok(())
}

/// Takes the obsolete tables whose files are safe to delete now: those the
/// cleanup queue is the sole owner of (`Arc::strong_count == 1`). Once a table
/// leaves the current version no new reader can acquire it, so its strong count
/// only falls; a count of 1 proves no in-flight reader still pins the file.
/// Tables still pinned stay queued and are retried on the next pass.
pub(in crate::db) fn take_deletable_obsolete_tables(
    pending_tables: &Mutex<Vec<Arc<Table>>>,
) -> Result<Vec<Arc<Table>>> {
    let mut pending = pending_tables
        .lock()
        .map_err(|_| lock_poisoned("obsolete table cleanup queue"))?;
    if pending.is_empty() {
        return Ok(Vec::new());
    }

    let mut deletable = Vec::new();
    let mut retained = Vec::with_capacity(pending.len());
    for table in pending.drain(..) {
        if Arc::strong_count(&table) == 1 {
            deletable.push(table);
        } else {
            retained.push(table);
        }
    }
    *pending = retained;
    Ok(deletable)
}

pub(in crate::db) fn cleanup_pending_obsolete_blob_files(
    backend: &NativeFileBackend,
    db_path: Option<&Path>,
    snapshots: &SnapshotTracker,
    manifest: Option<&Mutex<ManifestStore>>,
) -> Result<()> {
    if db_path.is_none() {
        return Ok(());
    }
    let (manifest, pending_file_ids) =
        delete_pending_obsolete_blob_files(backend, db_path, snapshots, manifest)?;
    if pending_file_ids.is_empty() {
        return Ok(());
    }

    manifest
        .lock()
        .map_err(|_| lock_poisoned("manifest store"))?
        .clear_pending_blob_deletions(&pending_file_ids)
}

pub(in crate::db) fn delete_pending_obsolete_blob_files<'manifest>(
    backend: &NativeFileBackend,
    db_path: Option<&Path>,
    snapshots: &SnapshotTracker,
    manifest: Option<&'manifest Mutex<ManifestStore>>,
) -> Result<(&'manifest Mutex<ManifestStore>, Vec<u64>)> {
    let Some(db_path) = db_path else {
        return Err(Error::Corruption {
            message: "persistent blob cleanup is missing database path".to_owned(),
        });
    };
    let manifest = manifest.ok_or_else(|| Error::Corruption {
        message: "persistent database is missing manifest store".to_owned(),
    })?;
    if snapshots.active_count() != 0 {
        return Ok((manifest, Vec::new()));
    }

    let pending_file_ids = {
        let manifest = manifest
            .lock()
            .map_err(|_| lock_poisoned("manifest store"))?;
        // Manifest metadata is the deletion authority. A pending entry that is
        // still referenced is inconsistent, so leave it on disk instead of
        // risking a read-visible blob file.
        deletable_pending_blob_file_ids(manifest.state())
    };
    if pending_file_ids.is_empty() {
        return Ok((manifest, Vec::new()));
    }

    for file_id in &pending_file_ids {
        delete_storage_object(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, *file_id),
        )?;
    }

    Ok((manifest, pending_file_ids))
}

pub(in crate::db) fn remove_table_files(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    for table_id in table_ids {
        delete_storage_object(
            backend,
            StorageObjectKind::Table,
            &table::table_path(db_path, *table_id),
        )?;
    }

    Ok(())
}

pub(in crate::db) fn remove_blob_files(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    for table_id in table_ids {
        delete_storage_object(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, table_id.get()),
        )?;
    }

    Ok(())
}

pub(in crate::db) fn delete_storage_object(
    backend: &NativeFileBackend,
    kind: StorageObjectKind,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectDelete)?;
    backend.delete_object_blocking(StorageObjectId::native_file(kind, path))
}

pub(in crate::db) async fn delete_storage_object_async(
    backend: &impl StorageObjectDeleteBackend,
    kind: StorageObjectKind,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectDelete)?;
    backend
        .delete_object(StorageObjectId::native_file(kind, path))
        .await
}

pub(in crate::db) fn sync_storage_directory_after_renames(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectorySync)?;
    backend.sync_directory_after_renames_blocking(StorageDirectoryId::native_file(path))
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(in crate::db) async fn sync_storage_directory_after_renames_async(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectorySync)?;
    backend
        .sync_directory_after_renames(StorageDirectoryId::native_file(path))
        .await
}

pub(in crate::db) fn create_storage_directory_all(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryCreate)?;
    backend.create_directory_all_blocking(StorageDirectoryId::native_file(path))
}

pub(in crate::db) async fn create_storage_directory_all_async(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryCreate)?;
    backend
        .create_directory_all(StorageDirectoryId::native_file(path))
        .await
}

pub(in crate::db) fn remove_storage_files(
    backend: &NativeFileBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    // A table write uses the table id as the blob file id for large values.
    // Before manifest publish succeeds, both files are unpublished output and
    // can be removed together after a failed flush or compaction attempt.
    remove_table_files(backend, db_path, table_ids)?;
    remove_blob_files(backend, db_path, table_ids)
}

pub(in crate::db) async fn remove_storage_files_async(
    backend: &impl StorageObjectDeleteBackend,
    db_path: &Path,
    table_ids: &[table::TableId],
) -> Result<()> {
    for table_id in table_ids {
        delete_storage_object_async(
            backend,
            StorageObjectKind::Table,
            &table::table_path(db_path, *table_id),
        )
        .await?;
        delete_storage_object_async(
            backend,
            StorageObjectKind::Blob,
            &blob::blob_path(db_path, table_id.get()),
        )
        .await?;
    }

    Ok(())
}
