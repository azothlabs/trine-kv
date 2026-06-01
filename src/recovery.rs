use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    blob,
    error::{Error, Result},
    manifest::ManifestState,
    options::{DurabilityMode, FailOnCorruptionPolicy},
    storage::{
        BlockingStorageDirectoryListBackend, BlockingStorageDirectorySyncBackend,
        BlockingStorageObjectDeleteBackend, BlockingStorageObjectReadBackend,
        BlockingStorageObjectWriteBackend, BlockingStorageReadBackend,
        BlockingStorageWriterLeaseBackend, NativeFileBackend, NativeFileWriterLease,
        StorageCapability, StorageDirectoryId, StorageDirectoryListBackend,
        StorageObjectDeleteBackend, StorageObjectId, StorageObjectKind, StorageObjectListBackend,
        StorageObjectReadBackend, StorageObjectWriteBackend, StorageReadBackend,
        StorageWriterLeaseBackend,
    },
    table::{self, TableId},
    wal,
};

pub const RECOVERY_REPORT_FILE_NAME: &str = "RECOVERY_REPORT";
pub(crate) const PROCESS_LOCK_FILE_NAME: &str = "LOCK";

#[derive(Debug)]
pub(crate) struct ProcessLock {
    _lease: NativeFileWriterLease,
}

impl ProcessLock {
    #[allow(dead_code)]
    pub(crate) fn acquire(db_path: &Path) -> Result<Self> {
        let backend = NativeFileBackend::new();
        Self::acquire_with_backend(&backend, db_path)
    }

    pub(crate) fn acquire_with_backend(
        backend: &NativeFileBackend,
        db_path: &Path,
    ) -> Result<Self> {
        backend
            .capabilities()
            .require(StorageCapability::WriterLease)?;
        let lease = backend.acquire_writer_lease_blocking(StorageObjectId::native_file(
            StorageObjectKind::WriterLease,
            db_path.join(PROCESS_LOCK_FILE_NAME),
        ))?;
        Ok(Self { _lease: lease })
    }

    pub(crate) async fn acquire_with_backend_async(
        backend: &NativeFileBackend,
        db_path: &Path,
    ) -> Result<Self> {
        backend
            .capabilities()
            .require(StorageCapability::WriterLease)?;
        let lease = backend
            .acquire_writer_lease(StorageObjectId::native_file(
                StorageObjectKind::WriterLease,
                db_path.join(PROCESS_LOCK_FILE_NAME),
            ))
            .await?;
        Ok(Self { _lease: lease })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    repaired_temporary_files: Vec<String>,
}

impl RecoveryReport {
    #[must_use]
    pub fn repaired_temporary_files(&self) -> &[String] {
        &self.repaired_temporary_files
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.repaired_temporary_files.is_empty()
    }
}

#[must_use]
pub fn recovery_report_path(db_path: &Path) -> PathBuf {
    db_path.join(RECOVERY_REPORT_FILE_NAME)
}

pub fn read_recovery_report(db_path: &Path) -> Result<RecoveryReport> {
    let path = recovery_report_path(db_path);
    let bytes = read_recovery_report_bytes(&path)?;
    let text = String::from_utf8(bytes.to_vec()).map_err(|error| {
        Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("recovery report is not valid UTF-8: {error}"),
        ))
    })?;
    decode_report(&text)
}

#[allow(dead_code)]
pub(crate) fn repair_safe_temporary_files(
    db_path: &Path,
    policy: FailOnCorruptionPolicy,
) -> Result<Option<RecoveryReport>> {
    let backend = NativeFileBackend::new();
    repair_safe_temporary_files_with_backend(&backend, db_path, policy)
}

pub(crate) fn repair_safe_temporary_files_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    policy: FailOnCorruptionPolicy,
) -> Result<Option<RecoveryReport>> {
    let temporary_files = safe_temporary_files_with_backend(backend, db_path)?;
    if temporary_files.is_empty() {
        return Ok(None);
    }

    if matches!(policy, FailOnCorruptionPolicy::FailClosed) {
        let names = temporary_files
            .iter()
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(Error::Corruption {
            message: format!("safe temporary files require explicit repair: {names}"),
        });
    }

    for temporary_file in &temporary_files {
        delete_safe_temporary_file_with_backend(backend, &temporary_file.path)?;
    }

    let report = RecoveryReport {
        repaired_temporary_files: temporary_files.into_iter().map(|file| file.name).collect(),
    };
    write_recovery_report_with_backend(backend, db_path, &report)?;

    Ok(Some(report))
}

#[allow(dead_code)]
pub(crate) async fn repair_safe_temporary_files_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    policy: FailOnCorruptionPolicy,
) -> Result<Option<RecoveryReport>>
where
    B: StorageDirectoryListBackend + StorageObjectDeleteBackend + StorageObjectWriteBackend,
{
    let temporary_files = safe_temporary_files_with_backend_async(backend, db_path).await?;
    if temporary_files.is_empty() {
        return Ok(None);
    }

    if matches!(policy, FailOnCorruptionPolicy::FailClosed) {
        let names = temporary_files
            .iter()
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(Error::Corruption {
            message: format!("safe temporary files require explicit repair: {names}"),
        });
    }

    for temporary_file in &temporary_files {
        delete_safe_temporary_file_with_backend_async(backend, &temporary_file.path).await?;
    }

    let report = RecoveryReport {
        repaired_temporary_files: temporary_files.into_iter().map(|file| file.name).collect(),
    };
    write_recovery_report_with_backend_async(backend, db_path, &report).await?;

    Ok(Some(report))
}

#[allow(dead_code)]
pub(crate) fn fail_on_unreferenced_storage_files(
    db_path: &Path,
    referenced_table_ids: &BTreeSet<TableId>,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<()> {
    let backend = NativeFileBackend::new();
    fail_on_unreferenced_storage_files_with_backend(
        &backend,
        db_path,
        referenced_table_ids,
        referenced_blob_ids,
    )
}

pub(crate) fn fail_on_unreferenced_storage_files_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    referenced_table_ids: &BTreeSet<TableId>,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<()> {
    // Formal table/blob files are stronger evidence than safe tmp files. Do
    // not delete them during startup; report them so the operator can decide.
    let unreferenced_files = unreferenced_storage_files_with_backend(
        backend,
        db_path,
        referenced_table_ids,
        referenced_blob_ids,
    )?;
    if unreferenced_files.is_empty() {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "unreferenced table/blob files require operator review: {}",
            unreferenced_files.join(", ")
        ),
    })
}

#[allow(dead_code)]
pub(crate) async fn fail_on_unreferenced_storage_files_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    referenced_table_ids: &BTreeSet<TableId>,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<()>
where
    B: StorageObjectListBackend,
{
    let mut unreferenced_files = Vec::new();

    for table_id in table::list_table_file_ids_with_backend_async(backend, db_path).await? {
        if !referenced_table_ids.contains(&table_id) {
            unreferenced_files.push(storage_file_name(&table::table_path(db_path, table_id))?);
        }
    }

    for blob_id in blob::list_blob_file_ids_with_backend_async(backend, db_path).await? {
        if !referenced_blob_ids.contains(&blob_id) {
            unreferenced_files.push(storage_file_name(&blob::blob_path(db_path, blob_id))?);
        }
    }

    unreferenced_files.sort();
    if unreferenced_files.is_empty() {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "unreferenced table/blob files require operator review: {}",
            unreferenced_files.join(", ")
        ),
    })
}

#[allow(dead_code)]
pub(crate) fn fail_on_missing_referenced_blob_files(
    db_path: &Path,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<()> {
    let backend = NativeFileBackend::new();
    fail_on_missing_referenced_blob_files_with_backend(&backend, db_path, referenced_blob_ids)
}

pub(crate) fn fail_on_missing_referenced_blob_files_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<()> {
    let missing_files = referenced_blob_ids
        .iter()
        .copied()
        .filter_map(|blob_id| {
            let path = blob::blob_path(db_path, blob_id);
            (!storage_object_exists_with_backend(backend, StorageObjectKind::Blob, &path))
                .then(|| storage_file_name(&path))
        })
        .collect::<Result<Vec<_>>>()?;

    if missing_files.is_empty() {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "referenced blob files are missing: {}",
            missing_files.join(", ")
        ),
    })
}

#[allow(dead_code)]
pub(crate) async fn fail_on_missing_referenced_blob_files_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<()>
where
    B: StorageObjectReadBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::ObjectRead)?;
    let mut missing_files = Vec::new();
    for blob_id in referenced_blob_ids {
        let path = blob::blob_path(db_path, *blob_id);
        let object = StorageObjectId::native_file(StorageObjectKind::Blob, &path);
        if backend.read_object_bytes(object).await?.is_none() {
            missing_files.push(storage_file_name(&path)?);
        }
    }

    if missing_files.is_empty() {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "referenced blob files are missing: {}",
            missing_files.join(", ")
        ),
    })
}

#[allow(dead_code)]
pub(crate) fn fail_on_invalid_referenced_blob_files(
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<()> {
    let backend = NativeFileBackend::new();
    fail_on_invalid_referenced_blob_files_with_backend(&backend, db_path, manifest)
}

pub(crate) fn fail_on_invalid_referenced_blob_files_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<()> {
    let referenced_blob_ids = manifest
        .tables()
        .values()
        .flat_map(|tables| {
            tables
                .iter()
                .flat_map(|properties| properties.blob_file_ids().iter().copied())
        })
        .collect::<BTreeSet<_>>();
    let mut blob_properties = BTreeMap::new();
    for blob_id in referenced_blob_ids {
        blob_properties.insert(
            blob_id,
            blob::validate_blob_file_with_backend(backend, db_path, blob_id)?,
        );
    }

    for (bucket, tables) in manifest.tables() {
        for table in tables {
            let reference_ids = table
                .blob_references()
                .iter()
                .map(|reference| reference.file_id)
                .collect::<BTreeSet<_>>();
            let file_ids = table
                .blob_file_ids()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            if file_ids != reference_ids {
                return Err(Error::Corruption {
                    message: format!(
                        "table {} in bucket {bucket} has inconsistent blob reference ids",
                        table.id.get()
                    ),
                });
            }

            for reference in table.blob_references() {
                let properties =
                    blob_properties
                        .get(&reference.file_id)
                        .ok_or_else(|| Error::Corruption {
                            message: format!(
                                "table {} in bucket {bucket} references missing blob metadata {}",
                                table.id.get(),
                                reference.file_id
                            ),
                        })?;
                if reference.referenced_bytes > properties.encoded_bytes {
                    return Err(Error::Corruption {
                        message: format!(
                            "table {} in bucket {bucket} references too many blob bytes in file {}",
                            table.id.get(),
                            reference.file_id
                        ),
                    });
                }
                if reference.smallest_internal_key < properties.smallest_internal_key
                    || reference.largest_internal_key > properties.largest_internal_key
                {
                    return Err(Error::Corruption {
                        message: format!(
                            "table {} in bucket {bucket} has blob key span outside file {}",
                            table.id.get(),
                            reference.file_id
                        ),
                    });
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn fail_on_invalid_referenced_blob_files_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    manifest: &ManifestState,
) -> Result<()>
where
    B: StorageReadBackend,
{
    let referenced_blob_ids = manifest
        .tables()
        .values()
        .flat_map(|tables| {
            tables
                .iter()
                .flat_map(|properties| properties.blob_file_ids().iter().copied())
        })
        .collect::<BTreeSet<_>>();
    let mut blob_properties = BTreeMap::new();
    for blob_id in referenced_blob_ids {
        blob_properties.insert(
            blob_id,
            blob::read_blob_file_properties_with_backend_async(backend, db_path, blob_id).await?,
        );
    }

    for (bucket, tables) in manifest.tables() {
        for table in tables {
            let reference_ids = table
                .blob_references()
                .iter()
                .map(|reference| reference.file_id)
                .collect::<BTreeSet<_>>();
            let file_ids = table
                .blob_file_ids()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            if file_ids != reference_ids {
                return Err(Error::Corruption {
                    message: format!(
                        "table {} in bucket {bucket} has inconsistent blob reference ids",
                        table.id.get()
                    ),
                });
            }

            for reference in table.blob_references() {
                let properties =
                    blob_properties
                        .get(&reference.file_id)
                        .ok_or_else(|| Error::Corruption {
                            message: format!(
                                "table {} in bucket {bucket} references missing blob metadata {}",
                                table.id.get(),
                                reference.file_id
                            ),
                        })?;
                if reference.referenced_bytes > properties.encoded_bytes {
                    return Err(Error::Corruption {
                        message: format!(
                            "table {} in bucket {bucket} references too many blob bytes in file {}",
                            table.id.get(),
                            reference.file_id
                        ),
                    });
                }
                if reference.smallest_internal_key < properties.smallest_internal_key
                    || reference.largest_internal_key > properties.largest_internal_key
                {
                    return Err(Error::Corruption {
                        message: format!(
                            "table {} in bucket {bucket} has blob key span outside file {}",
                            table.id.get(),
                            reference.file_id
                        ),
                    });
                }
            }
        }
    }

    Ok(())
}

#[allow(dead_code)]
pub(crate) async fn fail_on_safe_temporary_files_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
) -> Result<()>
where
    B: StorageDirectoryListBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    let directory_files = match backend
        .list_directory_files(StorageDirectoryId::native_file(db_path))
        .await
    {
        Ok(files) => files,
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    let mut temporary_files = Vec::new();
    for directory_file in directory_files {
        let name = storage_file_name(directory_file.path())?;
        if is_safe_temporary_file(&name) {
            temporary_files.push(name);
        }
    }
    temporary_files.sort();

    if temporary_files.is_empty() {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "safe temporary files require explicit repair: {}",
            temporary_files.join(", ")
        ),
    })
}

struct TemporaryFile {
    name: String,
    path: PathBuf,
}

fn safe_temporary_files_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
) -> Result<Vec<TemporaryFile>> {
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    let directory_files = match backend
        .list_directory_files_blocking(StorageDirectoryId::native_file(db_path))
    {
        Ok(files) => files,
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };

    let mut files = Vec::new();
    for directory_file in directory_files {
        let path = directory_file.path().to_path_buf();
        let name = storage_file_name(&path)?;
        if is_safe_temporary_file(&name) {
            files.push(TemporaryFile { name, path });
        }
    }
    files.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(files)
}

async fn safe_temporary_files_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
) -> Result<Vec<TemporaryFile>>
where
    B: StorageDirectoryListBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::DirectoryListing)?;
    let directory_files = match backend
        .list_directory_files(StorageDirectoryId::native_file(db_path))
        .await
    {
        Ok(files) => files,
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };

    let mut files = Vec::new();
    for directory_file in directory_files {
        let path = directory_file.path().to_path_buf();
        let name = storage_file_name(&path)?;
        if is_safe_temporary_file(&name) {
            files.push(TemporaryFile { name, path });
        }
    }
    files.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(files)
}

fn delete_safe_temporary_file_with_backend(backend: &NativeFileBackend, path: &Path) -> Result<()> {
    backend
        .capabilities()
        .require(StorageCapability::ObjectDelete)?;
    backend.delete_object_blocking(StorageObjectId::native_file(
        StorageObjectKind::Temporary,
        path,
    ))
}

async fn delete_safe_temporary_file_with_backend_async<B>(backend: &B, path: &Path) -> Result<()>
where
    B: StorageObjectDeleteBackend,
{
    backend
        .capabilities()
        .require(StorageCapability::ObjectDelete)?;
    backend
        .delete_object(StorageObjectId::native_file(
            StorageObjectKind::Temporary,
            path,
        ))
        .await
}

fn storage_object_exists_with_backend(
    backend: &NativeFileBackend,
    kind: StorageObjectKind,
    path: &Path,
) -> bool {
    if backend
        .capabilities()
        .require(StorageCapability::RandomRead)
        .is_err()
    {
        return false;
    }

    backend
        .open_read_blocking(StorageObjectId::native_file(kind, path))
        .is_ok()
}

fn is_safe_temporary_file(name: &str) -> bool {
    // These names come from atomic write paths before their final rename.
    // The manifest never references them, so recovery may delete them only
    // when the caller explicitly chooses the repair policy.
    name == "MANIFEST.tmp"
        || name == "RECOVERY_REPORT.tmp"
        || wal::is_wal_rewrite_temporary_file_name(name)
        || (name.starts_with("table-") && has_tmp_extension(name))
        || (name.starts_with("blob-") && has_tmp_extension(name))
}

fn has_tmp_extension(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"))
}

fn unreferenced_storage_files_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    referenced_table_ids: &BTreeSet<TableId>,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<Vec<String>> {
    let mut files = Vec::new();

    for table_id in table::list_table_file_ids_with_backend(backend, db_path)? {
        if !referenced_table_ids.contains(&table_id) {
            files.push(storage_file_name(&table::table_path(db_path, table_id))?);
        }
    }

    for blob_id in blob::list_blob_file_ids_with_backend(backend, db_path)? {
        if !referenced_blob_ids.contains(&blob_id) {
            files.push(storage_file_name(&blob::blob_path(db_path, blob_id))?);
        }
    }

    files.sort();
    Ok(files)
}

fn storage_file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or_else(|| Error::Corruption {
            message: format!("storage file name is not valid UTF-8: {}", path.display()),
        })
}

fn write_recovery_report_with_backend(
    backend: &NativeFileBackend,
    db_path: &Path,
    report: &RecoveryReport,
) -> Result<()> {
    let path = recovery_report_path(db_path);
    let bytes: Arc<[u8]> = Arc::from(encode_report(report).into_bytes());
    backend
        .capabilities()
        .require(StorageCapability::ObjectWrite)?;
    backend.write_object_blocking(
        recovery_report_storage_object(&path),
        bytes,
        DurabilityMode::SyncAll,
    )?;
    sync_recovery_report_parent_directory_after_rename_with_backend(backend, &path)?;

    Ok(())
}

async fn write_recovery_report_with_backend_async<B>(
    backend: &B,
    db_path: &Path,
    report: &RecoveryReport,
) -> Result<()>
where
    B: StorageObjectWriteBackend,
{
    let path = recovery_report_path(db_path);
    let bytes: Arc<[u8]> = Arc::from(encode_report(report).into_bytes());
    backend
        .capabilities()
        .require(StorageCapability::ObjectWrite)?;
    backend
        .write_object(
            recovery_report_storage_object(&path),
            bytes,
            DurabilityMode::Flush,
        )
        .await
}

fn read_recovery_report_bytes(path: &Path) -> Result<Arc<[u8]>> {
    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::ObjectRead)?;
    backend
        .read_object_bytes_blocking(recovery_report_storage_object(path))?
        .ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("recovery report {} not found", path.display()),
            ))
        })
}

fn recovery_report_storage_object(path: &Path) -> StorageObjectId {
    StorageObjectId::native_file(StorageObjectKind::RecoveryReport, path)
}

fn sync_recovery_report_parent_directory_after_rename_with_backend(
    backend: &NativeFileBackend,
    path: &Path,
) -> Result<()> {
    let Some(parent) = StorageDirectoryId::native_file_parent_of(path) else {
        return Ok(());
    };
    backend
        .capabilities()
        .require(StorageCapability::DirectorySync)?;
    backend.sync_directory_after_renames_blocking(parent)
}

fn encode_report(report: &RecoveryReport) -> String {
    let mut text = String::from("trine-kv recovery report v1\n");
    text.push_str("repaired_temporary_files:\n");
    for file in &report.repaired_temporary_files {
        text.push_str("- ");
        text.push_str(file);
        text.push('\n');
    }
    text
}

fn decode_report(text: &str) -> Result<RecoveryReport> {
    let mut lines = text.lines();
    if lines.next() != Some("trine-kv recovery report v1") {
        return Err(Error::InvalidFormat {
            message: "unknown recovery report header".to_owned(),
        });
    }
    if lines.next() != Some("repaired_temporary_files:") {
        return Err(Error::InvalidFormat {
            message: "missing recovery report file list".to_owned(),
        });
    }

    let mut repaired_temporary_files = Vec::new();
    for line in lines {
        let Some(file) = line.strip_prefix("- ") else {
            return Err(Error::InvalidFormat {
                message: "invalid recovery report file entry".to_owned(),
            });
        };
        repaired_temporary_files.push(file.to_owned());
    }

    Ok(RecoveryReport {
        repaired_temporary_files,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        io,
        task::{Context, Poll, Waker},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{
        RecoveryReport, decode_report, encode_report, read_recovery_report,
        repair_safe_temporary_files_with_backend, repair_safe_temporary_files_with_backend_async,
    };
    use crate::{options::FailOnCorruptionPolicy, storage::NativeFileBackend};

    #[test]
    fn recovery_report_round_trips_repaired_files() {
        let report = RecoveryReport {
            repaired_temporary_files: vec![
                "MANIFEST.tmp".to_owned(),
                "table-00000000000000000001.tmp".to_owned(),
            ],
        };

        assert_eq!(
            decode_report(&encode_report(&report)).expect("report decodes"),
            report
        );
    }

    #[test]
    fn read_recovery_report_missing_file_returns_not_found() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-recovery-report-missing-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));

        let error = read_recovery_report(&root).expect_err("missing report is not found");
        assert!(matches!(
            error,
            crate::Error::Io(ref io_error) if io_error.kind() == io::ErrorKind::NotFound
        ));
    }

    #[test]
    fn backend_repair_safe_temporary_files_writes_report() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-recovery-backend-repair-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");
        let temporary_path = root.join("MANIFEST.tmp");
        std::fs::write(&temporary_path, b"temporary manifest").expect("temporary file writes");

        let backend = NativeFileBackend::new();
        let report = repair_safe_temporary_files_with_backend(
            &backend,
            &root,
            FailOnCorruptionPolicy::RepairSafeTemporaryFiles,
        )
        .expect("backend repair succeeds")
        .expect("repair report exists");

        assert_eq!(
            report.repaired_temporary_files(),
            &["MANIFEST.tmp".to_owned()]
        );
        assert!(!temporary_path.exists());
        assert_eq!(
            read_recovery_report(&root).expect("recovery report reads"),
            report
        );

        std::fs::remove_dir_all(root).expect("cleanup test dir");
    }

    #[test]
    fn async_backend_repair_safe_temporary_files_writes_report() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-recovery-async-backend-repair-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("test dir creates");
        let temporary_path = root.join("RECOVERY_REPORT.tmp");
        std::fs::write(&temporary_path, b"temporary report").expect("temporary file writes");

        let backend = NativeFileBackend::new();
        let report = poll_ready(repair_safe_temporary_files_with_backend_async(
            &backend,
            &root,
            FailOnCorruptionPolicy::RepairSafeTemporaryFiles,
        ))
        .expect("async backend repair succeeds")
        .expect("repair report exists");

        assert_eq!(
            report.repaired_temporary_files(),
            &["RECOVERY_REPORT.tmp".to_owned()]
        );
        assert!(!temporary_path.exists());
        assert_eq!(
            read_recovery_report(&root).expect("recovery report reads"),
            report
        );

        std::fs::remove_dir_all(root).expect("cleanup test dir");
    }

    fn poll_ready<T>(future: impl Future<Output = crate::Result<T>>) -> crate::Result<T> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("recovery storage future unexpectedly pending"),
        }
    }
}
