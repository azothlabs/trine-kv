//! Typed storage object and directory coordinates.

use std::path::{Path, PathBuf};

use crate::{
    error::{Error, Result},
    limits,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum StorageObjectKind {
    Blob,
    ContentAccessBarrier,
    ContentChunk,
    ContentDescriptor,
    ContentUpload,
    Manifest,
    RecoveryReport,
    Table,
    Temporary,
    Wal,
    WriterLease,
}

impl StorageObjectKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::ContentAccessBarrier => "content access barrier",
            Self::ContentChunk => "content chunk",
            Self::ContentDescriptor => "content descriptor",
            Self::ContentUpload => "content upload state",
            Self::Manifest => "manifest",
            Self::RecoveryReport => "recovery report",
            Self::Table => "table",
            Self::Temporary => "temporary",
            Self::Wal => "WAL",
            Self::WriterLease => "writer lease",
        }
    }
}

pub(crate) fn max_whole_object_read_bytes(kind: StorageObjectKind) -> usize {
    match kind {
        StorageObjectKind::Blob => limits::MAX_WHOLE_BLOB_DECODE_BYTES,
        StorageObjectKind::ContentChunk => 16 * 1024 * 1024 + 128,
        StorageObjectKind::ContentAccessBarrier
        | StorageObjectKind::ContentDescriptor
        | StorageObjectKind::ContentUpload => 4 * 1024,
        StorageObjectKind::Manifest => limits::MAX_MANIFEST_PAYLOAD_BYTES + 14,
        StorageObjectKind::RecoveryReport => limits::MAX_MANIFEST_PAYLOAD_BYTES,
        StorageObjectKind::Table => 14 + limits::MAX_WHOLE_TABLE_DECODE_BYTES,
        StorageObjectKind::Temporary => limits::MAX_WHOLE_TABLE_DECODE_BYTES,
        StorageObjectKind::Wal => limits::MAX_WAL_FRAME_PAYLOAD_BYTES * 16,
        StorageObjectKind::WriterLease => 64 * 1024,
    }
}

pub(crate) fn ensure_whole_object_read_len(object: &StorageObjectId, len: usize) -> Result<()> {
    let max = max_whole_object_read_bytes(object.kind());
    if len <= max {
        return Ok(());
    }

    Err(Error::Corruption {
        message: format!(
            "{} object {} length {len} exceeds maximum {max}",
            object.kind().as_str(),
            object.path().display()
        ),
    })
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

    #[allow(dead_code)]
    pub(crate) fn memory(kind: StorageObjectKind, name: impl Into<PathBuf>) -> Self {
        Self {
            kind,
            path: name.into(),
        }
    }

    pub(crate) const fn kind(&self) -> StorageObjectKind {
        self.kind
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StorageDirectoryId {
    path: PathBuf,
}

impl StorageDirectoryId {
    pub(crate) fn native_file(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub(crate) fn native_file_parent_of(path: &Path) -> Option<Self> {
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Self::native_file)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StorageDirectoryFile {
    path: PathBuf,
    byte_len: Option<u64>,
}

impl StorageDirectoryFile {
    #[allow(dead_code)]
    pub(crate) fn native_file(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            byte_len: None,
        }
    }

    pub(crate) fn native_file_with_len(path: impl Into<PathBuf>, byte_len: u64) -> Self {
        Self {
            path: path.into(),
            byte_len: Some(byte_len),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) const fn byte_len(&self) -> Option<u64> {
        self.byte_len
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageObjectListRequest {
    kind: StorageObjectKind,
    root: PathBuf,
    file_extension: Option<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageObjectListPage {
    pub(crate) objects: Vec<StorageObjectId>,
    pub(crate) next_after: Option<String>,
}

pub(crate) const OBJECT_LIST_PAGE_SIZE: usize = 1_024;

impl StorageObjectListRequest {
    pub(crate) fn native_file(kind: StorageObjectKind, root: impl Into<PathBuf>) -> Self {
        Self {
            kind,
            root: root.into(),
            file_extension: None,
        }
    }

    pub(crate) fn with_file_extension(mut self, file_extension: &'static str) -> Self {
        self.file_extension = Some(file_extension);
        self
    }

    pub(crate) const fn kind(&self) -> StorageObjectKind {
        self.kind
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) const fn file_extension(&self) -> Option<&'static str> {
        self.file_extension
    }
}
