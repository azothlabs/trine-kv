use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    blob,
    durability::sync_parent_dir_after_rename,
    error::{Error, Result},
    options::FailOnCorruptionPolicy,
    table::{self, TableId},
    wal,
};

pub const RECOVERY_REPORT_FILE_NAME: &str = "RECOVERY_REPORT";
pub(crate) const PROCESS_LOCK_FILE_NAME: &str = "LOCK";

#[derive(Debug)]
pub(crate) struct ProcessLock {
    path: PathBuf,
    owner: String,
    file: Option<File>,
}

impl ProcessLock {
    pub(crate) fn acquire(db_path: &Path) -> Result<Self> {
        let path = db_path.join(PROCESS_LOCK_FILE_NAME);
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                // Existing lock files are not safe recovery leftovers. They may
                // mark a live writer or a stale crash marker, so startup fails
                // closed until an operator removes the file deliberately.
                return Err(Error::Corruption {
                    message: format!("database lock is already held: {}", path.display()),
                });
            }
            Err(error) => return Err(Error::Io(error)),
        };

        let owner = lock_owner_text();
        if let Err(error) = write_lock_owner(&mut file, &owner) {
            let _ = fs::remove_file(&path);
            return Err(error);
        }

        Ok(Self {
            path,
            owner,
            file: Some(file),
        })
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        // Do not blindly remove a path named LOCK. If an operator deleted this
        // file and another process created a new one, this handle no longer
        // owns the on-disk marker.
        let should_remove = fs::read_to_string(&self.path)
            .is_ok_and(|contents| contents.as_str() == self.owner.as_str());
        drop(self.file.take());
        if should_remove {
            let _ = fs::remove_file(&self.path);
        }
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

fn lock_owner_text() -> String {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("pid={}\nnonce={nonce}\n", std::process::id())
}

fn write_lock_owner(file: &mut File, owner: &str) -> Result<()> {
    file.write_all(owner.as_bytes())?;
    file.sync_all()?;
    Ok(())
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
    let tmp_path = path.with_extension("tmp");
    let mut file = File::create(&tmp_path)?;
    file.write_all(encode_report(report).as_bytes())?;
    file.sync_all()?;
    drop(file);
    fs::rename(tmp_path, &path)?;
    sync_parent_dir_after_rename(&path)?;

    Ok(())
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
