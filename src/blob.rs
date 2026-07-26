use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    codec::{self, CodecId},
    error::{Error, Result},
    internal_key::{InternalKey, ValueKind},
    limits,
    options::DurabilityMode,
    storage::{
        BlockingStorageObjectListBackend, BlockingStorageObjectWriteBackend,
        BlockingStorageReadBackend, BlockingStorageReadObject, NativeFileBackend, NativeFileObject,
        StorageCapability, StorageObjectId, StorageObjectKind, StorageObjectListBackend,
        StorageObjectListRequest, StorageObjectWriteBackend, StorageReadBackend, StorageReadObject,
    },
    types::Sequence,
};
use bytes::Bytes;

pub const BLOB_FILE_EXTENSION: &str = "trineb";
pub const BLOB_FILE_FORMAT_VERSION: u16 = 3;

const BLOB_MAGIC: u32 = 0x5452_424c;
const BLOB_FOOTER_MAGIC: u32 = 0x5452_4246;
const BLOB_HEADER_WITHOUT_CHECKSUM_LEN: usize = 39;
const BLOB_HEADER_LEN: usize = BLOB_HEADER_WITHOUT_CHECKSUM_LEN + 4;
const BLOB_FOOTER_LEN: usize = 24;
const MIN_BLOB_RECORD_FRAME_BYTES: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobIndex {
    pub file_id: u64,
    pub offset: u64,
    pub encoded_len: u64,
    pub value_len: u64,
    pub value_checksum: u32,
    pub record_checksum: u32,
    pub compression: CodecId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobFileHeader {
    pub file_id: u64,
    pub creation_sequence: Sequence,
    pub bucket_options_digest: u64,
    pub blob_threshold_bytes: u64,
    pub default_compression: CodecId,
}

impl BlobFileHeader {
    #[must_use]
    pub const fn new(
        file_id: u64,
        creation_sequence: Sequence,
        blob_threshold_bytes: u64,
        default_compression: CodecId,
    ) -> Self {
        Self {
            file_id,
            creation_sequence,
            bucket_options_digest: 0,
            blob_threshold_bytes,
            default_compression,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRecord {
    pub internal_key: InternalKey,
    pub value: Vec<u8>,
    pub compression: CodecId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobFileRecord {
    pub index: BlobIndex,
    pub record: BlobRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobFileProperties {
    pub record_count: u64,
    pub value_bytes: u64,
    pub encoded_bytes: u64,
    pub compression_saved_bytes: u64,
    pub smallest_internal_key: InternalKey,
    pub largest_internal_key: InternalKey,
    pub smallest_sequence: Sequence,
    pub largest_sequence: Sequence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobFile {
    pub header: BlobFileHeader,
    pub properties: BlobFileProperties,
    pub records: Vec<BlobFileRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueRef {
    Inline(Vec<u8>),
    BlobIndex(BlobIndex),
}

impl ValueRef {
    #[must_use]
    pub fn len(&self) -> u64 {
        match self {
            Self::Inline(bytes) => bytes.len() as u64,
            Self::BlobIndex(index) => index.value_len,
        }
    }
}

#[must_use]
pub fn blob_path(db_path: &Path, file_id: u64) -> PathBuf {
    db_path.join(format!("blob-{file_id:020}.{BLOB_FILE_EXTENSION}"))
}

#[path = "blob/codec.rs"]
mod file_format;
mod io;
#[allow(dead_code)]
mod listing;
mod values;

pub(crate) use file_format::*;
pub(crate) use io::*;
pub(crate) use listing::*;
pub(crate) use values::*;
#[cfg(test)]
mod tests;
