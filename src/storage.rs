use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

use crate::{
    block::BlockReadSource,
    error::{Error, Result},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StorageObjectKind {
    Table,
}

impl StorageObjectKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StorageObjectId {
    kind: StorageObjectKind,
    path: PathBuf,
}

impl StorageObjectId {
    pub(crate) fn native_file(kind: StorageObjectKind, path: impl Into<PathBuf>) -> Self {
        Self {
            kind,
            path: path.into(),
        }
    }

    pub(crate) const fn kind(&self) -> StorageObjectKind {
        self.kind
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug)]
pub(crate) struct NativeFileObject {
    object: StorageObjectId,
    file: Mutex<File>,
}

impl NativeFileObject {
    pub(crate) fn open(object: StorageObjectId) -> Result<Self> {
        let file = open_native_file(&object)?;
        Ok(Self {
            object,
            file: Mutex::new(file),
        })
    }

    pub(crate) const fn object(&self) -> &StorageObjectId {
        &self.object
    }

    pub(crate) fn len(&self) -> Result<u64> {
        let file = self.lock_file()?;
        Ok(file.metadata()?.len())
    }

    fn read_exact_at_offset(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        let mut file = self.lock_file()?;
        read_exact_at_native_file(&mut file, offset, bytes)
    }

    fn lock_file(&self) -> Result<MutexGuard<'_, File>> {
        self.file.lock().map_err(|_| Error::Corruption {
            message: format!(
                "referenced {} {} handle lock poisoned",
                self.object.kind().as_str(),
                self.object.path().display()
            ),
        })
    }
}

pub(crate) trait NativeFileReadHandle {
    fn read_exact_at(&self, offset: usize, bytes: &mut [u8]) -> Result<()>;
}

impl NativeFileReadHandle for NativeFileObject {
    fn read_exact_at(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        self.read_exact_at_offset(offset, bytes)
    }
}

pub(crate) struct NativeFileReadSource<'src, H> {
    object: StorageObjectId,
    cached: Option<&'src H>,
}

impl<'src, H> NativeFileReadSource<'src, H> {
    pub(crate) fn new(object: StorageObjectId, cached: Option<&'src H>) -> Self {
        Self { object, cached }
    }
}

impl<H> BlockReadSource for NativeFileReadSource<'_, H>
where
    H: NativeFileReadHandle,
{
    fn read_exact_at(&self, offset: usize, bytes: &mut [u8]) -> Result<()> {
        if let Some(cached) = self.cached {
            return cached.read_exact_at(offset, bytes);
        }

        read_exact_from_native_file(&self.object, offset, bytes)
    }
}

fn read_exact_from_native_file(
    object: &StorageObjectId,
    offset: usize,
    bytes: &mut [u8],
) -> Result<()> {
    let mut file = open_native_file(object)?;
    read_exact_at_native_file(&mut file, offset, bytes)
}

fn open_native_file(object: &StorageObjectId) -> Result<File> {
    File::open(object.path()).map_err(|error| Error::Corruption {
        message: format!(
            "referenced {} {} cannot be opened: {error}",
            object.kind().as_str(),
            object.path().display()
        ),
    })
}

fn read_exact_at_native_file(file: &mut File, offset: usize, bytes: &mut [u8]) -> Result<()> {
    file.seek(SeekFrom::Start(usize_to_u64(
        offset,
        "storage object read offset",
    )?))?;
    file.read_exact(bytes)?;
    Ok(())
}

fn usize_to_u64(value: usize, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::invalid_options(format!("{field} exceeds u64::MAX")))
}
