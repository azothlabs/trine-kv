use std::path::Path;

use crate::error::Result;

pub(crate) fn sync_parent_dir_after_rename(path: &Path) -> Result<()> {
    let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(());
    };

    sync_directory(parent)
}

pub(crate) fn sync_dir_after_renames(path: &Path) -> Result<()> {
    sync_directory(path)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<()> {
    use std::fs::File;

    // A file sync protects the new file bytes, while rename changes the parent
    // directory entry. Syncing the directory after rename makes the published
    // file name durable on Unix filesystems that require the extra step.
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_directory(path: &Path) -> Result<()> {
    use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;

    // Windows requires backup semantics to open a directory handle. Once the
    // handle is open, `sync_all` asks the platform to flush that directory
    // metadata, which is the directory-entry half of atomic file publish.
    OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?
        .sync_all()?;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
#[allow(clippy::unnecessary_wraps)]
fn sync_directory(_path: &Path) -> Result<()> {
    // Rust's standard library does not expose a portable directory sync for all
    // platforms. Targets without a concrete implementation keep the previous
    // best-effort behavior.
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, File},
        io::Write,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{sync_dir_after_renames, sync_parent_dir_after_rename};

    #[test]
    fn sync_parent_dir_after_rename_accepts_published_file() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-durability-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create test directory");

        let tmp_path = root.join("value.tmp");
        let published_path = root.join("value.trinet");
        {
            let mut file = File::create(&tmp_path).expect("create test file");
            file.write_all(b"durable").expect("write test file");
            file.sync_all().expect("sync test file");
        }
        fs::rename(&tmp_path, &published_path).expect("rename test file");

        sync_parent_dir_after_rename(&published_path).expect("sync parent directory");
        sync_dir_after_renames(&root).expect("sync directory directly");
        assert_eq!(
            fs::read(&published_path).expect("read published file"),
            b"durable"
        );

        fs::remove_dir_all(root).expect("cleanup test directory");
    }
}
