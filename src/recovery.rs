use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    blob,
    error::{Error, Result},
    manifest::ManifestState,
    options::{DurabilityMode, FailOnCorruptionPolicy},
    storage::{
        BlockingStorageDirectorySyncBackend, BlockingStorageObjectWriteBackend,
        BlockingStorageWriterLeaseBackend, NativeFileBackend, NativeFileWriterLease,
        StorageCapability, StorageDirectoryId, StorageObjectId, StorageObjectKind,
        StorageReadBackend,
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
    pub(crate) fn acquire(db_path: &Path) -> Result<Self> {
        let backend = NativeFileBackend::new();
        backend
            .capabilities()
            .require(StorageCapability::WriterLease)?;
        let lease = backend.acquire_writer_lease_blocking(StorageObjectId::native_file(
            StorageObjectKind::WriterLease,
            db_path.join(PROCESS_LOCK_FILE_NAME),
        ))?;
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
    let mut text = String::new();
    File::open(recovery_report_path(db_path))?.read_to_string(&mut text)?;
    decode_report(&text)
}

pub(crate) fn repair_safe_temporary_files(
    db_path: &Path,
    policy: FailOnCorruptionPolicy,
) -> Result<Option<RecoveryReport>> {
    let temporary_files = safe_temporary_files(db_path)?;
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
        fs::remove_file(&temporary_file.path)?;
    }

    let report = RecoveryReport {
        repaired_temporary_files: temporary_files.into_iter().map(|file| file.name).collect(),
    };
    write_recovery_report(db_path, &report)?;

    Ok(Some(report))
}

pub(crate) fn fail_on_unreferenced_storage_files(
    db_path: &Path,
    referenced_table_ids: &BTreeSet<TableId>,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<()> {
    // Formal table/blob files are stronger evidence than safe tmp files. Do
    // not delete them during startup; report them so the operator can decide.
    let unreferenced_files =
        unreferenced_storage_files(db_path, referenced_table_ids, referenced_blob_ids)?;
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

pub(crate) fn fail_on_missing_referenced_blob_files(
    db_path: &Path,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<()> {
    let missing_files = referenced_blob_ids
        .iter()
        .copied()
        .filter_map(|blob_id| {
            let path = blob::blob_path(db_path, blob_id);
            (!path.is_file()).then(|| storage_file_name(&path))
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

pub(crate) fn fail_on_invalid_referenced_blob_files(
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
        blob_properties.insert(blob_id, blob::validate_blob_file(db_path, blob_id)?);
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

struct TemporaryFile {
    name: String,
    path: PathBuf,
}

fn safe_temporary_files(db_path: &Path) -> Result<Vec<TemporaryFile>> {
    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(db_path)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if is_safe_temporary_file(name) {
            files.push(TemporaryFile {
                name: name.to_owned(),
                path,
            });
        }
    }
    files.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(files)
}

fn is_safe_temporary_file(name: &str) -> bool {
    // These names come from atomic write paths before their final rename.
    // The manifest never references them, so recovery may delete them only
    // when the caller explicitly chooses the repair policy.
    name == "MANIFEST.tmp"
        || name == "RECOVERY_REPORT.tmp"
        || name == wal::WAL_REWRITE_TMP_FILE_NAME
        || (name.starts_with("table-") && has_tmp_extension(name))
        || (name.starts_with("blob-") && has_tmp_extension(name))
}

fn has_tmp_extension(name: &str) -> bool {
    Path::new(name)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"))
}

fn unreferenced_storage_files(
    db_path: &Path,
    referenced_table_ids: &BTreeSet<TableId>,
    referenced_blob_ids: &BTreeSet<u64>,
) -> Result<Vec<String>> {
    let mut files = Vec::new();

    for table_id in table::list_table_file_ids(db_path)? {
        if !referenced_table_ids.contains(&table_id) {
            files.push(storage_file_name(&table::table_path(db_path, table_id))?);
        }
    }

    for blob_id in blob::list_blob_file_ids(db_path)? {
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

fn write_recovery_report(db_path: &Path, report: &RecoveryReport) -> Result<()> {
    let path = recovery_report_path(db_path);
    let bytes: Arc<[u8]> = Arc::from(encode_report(report).into_bytes());
    let backend = NativeFileBackend::new();
    backend
        .capabilities()
        .require(StorageCapability::ObjectWrite)?;
    backend.write_object_blocking(
        StorageObjectId::native_file(StorageObjectKind::RecoveryReport, &path),
        bytes,
        DurabilityMode::SyncAll,
    )?;
    sync_recovery_report_parent_directory_after_rename(&path)?;

    Ok(())
}

fn sync_recovery_report_parent_directory_after_rename(path: &Path) -> Result<()> {
    let Some(parent) = StorageDirectoryId::native_file_parent_of(path) else {
        return Ok(());
    };
    let backend = NativeFileBackend::new();
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
    use super::{RecoveryReport, decode_report, encode_report};

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
}
