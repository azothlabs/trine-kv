use std::fs::File;
use std::path::Path;

#[cfg(any(windows, target_os = "macos"))]
use std::io;

#[cfg(any(windows, target_os = "macos"))]
use crate::error::Error;
use crate::error::Result;
use crate::options::DurabilityMode;

/// Whether `durability` asks for an actual device-level file sync (as opposed to
/// buffering or a userspace flush).
#[must_use]
pub(crate) fn requires_file_sync(durability: DurabilityMode) -> bool {
    matches!(
        durability,
        DurabilityMode::SyncData | DurabilityMode::SyncAll | DurabilityMode::SyncAllStrict
    )
}

/// Whether `durability` is the strict tier that must survive sudden power loss.
// Used by the native sync paths and tests; the browser backend performs no
// device sync, so it is dead code there.
#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
#[must_use]
pub(crate) const fn durability_is_strict(durability: DurabilityMode) -> bool {
    matches!(durability, DurabilityMode::SyncAllStrict)
}

/// The single place that decides *how hard* to flush a file for a durability
/// mode, so call sites never special-case the platform.
///
/// macOS is the reason this abstraction exists. Apple's `fsync(2)` only pushes
/// bytes to the drive; the drive may keep them in a volatile cache and lose them
/// on sudden power loss or a kernel panic. Only `fcntl(fd, F_FULLFSYNC)` flushes
/// that cache to the platters, and it is much slower (it is the per-commit floor
/// behind single-key sync throughput). Trine therefore makes it opt-in:
///
/// - `SyncAllStrict` (strict) -> `F_FULLFSYNC` on macOS: true power-loss durability.
/// - `SyncData` / `SyncAll` (non-strict, the default) -> plain `fsync` on macOS:
///   faster, still durable across a process crash or kernel panic, but NOT
///   guaranteed across sudden power loss.
///
/// On Linux and Windows the ordinary `fsync` / `FlushFileBuffers` already flushes
/// durably, so strict and non-strict resolve to the same real sync there.
pub(crate) fn sync_file_for_durability(file: &File, durability: DurabilityMode) -> Result<()> {
    if !requires_file_sync(durability) {
        return Ok(());
    }
    sync_file_platform(file, durability)
}

#[cfg(target_os = "macos")]
fn sync_file_platform(file: &File, durability: DurabilityMode) -> Result<()> {
    use std::os::fd::AsRawFd;
    sync_macos_fd(file.as_raw_fd(), durability)
}

#[cfg(not(target_os = "macos"))]
fn sync_file_platform(file: &File, durability: DurabilityMode) -> Result<()> {
    // fsync/fdatasync/FlushFileBuffers already flush durably here, so the strict
    // tier maps to the same full sync as `SyncAll`.
    match durability {
        DurabilityMode::SyncData => file.sync_data().map_err(crate::error::Error::Io),
        _ => file.sync_all().map_err(crate::error::Error::Io),
    }
}

/// macOS device sync for a sync-requiring durability mode, working directly on a
/// raw descriptor so the native (`DispatchIO`) backend shares the same decision
/// as the std path.
#[cfg(all(target_os = "macos", feature = "platform-io-native"))]
pub(crate) fn sync_fd_for_durability(
    fd: std::os::fd::RawFd,
    durability: DurabilityMode,
) -> Result<()> {
    if !requires_file_sync(durability) {
        return Ok(());
    }
    sync_macos_fd(fd, durability)
}

#[cfg(target_os = "macos")]
fn sync_macos_fd(fd: std::os::fd::RawFd, durability: DurabilityMode) -> Result<()> {
    if durability_is_strict(durability) {
        full_fsync(fd)
    } else {
        plain_fsync(fd)
    }
}

/// Forces `fd` to permanent storage with `F_FULLFSYNC`, the only macOS call that
/// flushes the drive's volatile cache to the platters. Falls back to `fsync`
/// only when the filesystem reports `F_FULLFSYNC` unsupported (e.g. some network
/// mounts), where it is the strongest guarantee available.
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn full_fsync(fd: std::os::fd::RawFd) -> Result<()> {
    // SAFETY: `fcntl` with `F_FULLFSYNC` only reads `fd`; it is a valid open
    // descriptor for the file being synced and is not retained past the call.
    if unsafe { libc::fcntl(fd, libc::F_FULLFSYNC) } != -1 {
        return Ok(());
    }
    let full_error = io::Error::last_os_error();
    if matches!(
        full_error.raw_os_error(),
        Some(libc::ENOTSUP | libc::ENOTTY | libc::EINVAL)
    ) {
        return plain_fsync(fd);
    }
    Err(Error::Io(full_error))
}

/// Plain `fsync`: flushes to the drive but not necessarily through its volatile
/// cache (see [`sync_file_for_durability`]).
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn plain_fsync(fd: std::os::fd::RawFd) -> Result<()> {
    // SAFETY: `fsync` only reads `fd`, a valid open descriptor for the file being
    // synced; it is not retained past the call.
    if unsafe { libc::fsync(fd) } == 0 {
        Ok(())
    } else {
        Err(Error::Io(io::Error::last_os_error()))
    }
}

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

    // Windows requires backup semantics to open a directory handle. Some
    // filesystems and runners reject directory flush with ERROR_ACCESS_DENIED;
    // Trine still syncs the file itself and treats this directory step as the
    // strongest available best effort on Windows.
    let file = match OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if is_windows_directory_sync_permission_denied(&error) => return Ok(()),
        Err(error) => return Err(Error::Io(error)),
    };
    finish_windows_directory_sync(file.sync_all())
}

#[cfg(windows)]
pub(crate) fn finish_windows_directory_sync(result: io::Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if is_windows_directory_sync_permission_denied(&error) => Ok(()),
        Err(error) => Err(Error::Io(error)),
    }
}

#[cfg(windows)]
pub(crate) fn is_windows_directory_sync_permission_denied(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::PermissionDenied || error.raw_os_error() == Some(5)
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

    use super::{
        durability_is_strict, requires_file_sync, sync_dir_after_renames, sync_file_for_durability,
        sync_parent_dir_after_rename,
    };
    use crate::options::DurabilityMode;

    #[test]
    fn requires_file_sync_and_strictness_classify_modes() {
        assert!(!requires_file_sync(DurabilityMode::Buffered));
        assert!(!requires_file_sync(DurabilityMode::Flush));
        assert!(requires_file_sync(DurabilityMode::SyncData));
        assert!(requires_file_sync(DurabilityMode::SyncAll));
        assert!(requires_file_sync(DurabilityMode::SyncAllStrict));

        assert!(!durability_is_strict(DurabilityMode::SyncAll));
        assert!(durability_is_strict(DurabilityMode::SyncAllStrict));
    }

    #[test]
    fn sync_file_for_durability_succeeds_for_every_mode() {
        let root = std::env::temp_dir().join(format!(
            "trine-kv-fsync-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create test directory");
        let path = root.join("data");

        for mode in [
            DurabilityMode::Buffered,
            DurabilityMode::Flush,
            DurabilityMode::SyncData,
            DurabilityMode::SyncAll,
            DurabilityMode::SyncAllStrict,
        ] {
            let mut file = File::create(&path).expect("create test file");
            file.write_all(b"durable").expect("write test file");
            // Both the strict (F_FULLFSYNC on macOS) and non-strict (fsync) paths
            // must complete without error on a regular filesystem.
            sync_file_for_durability(&file, mode).unwrap_or_else(|error| {
                panic!("sync for {mode:?} failed: {error}");
            });
        }

        fs::remove_dir_all(root).expect("cleanup test directory");
    }

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
