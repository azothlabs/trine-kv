use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    error::{Error, Result},
    internal_key::InternalKey,
};

pub const BLOB_FILE_EXTENSION: &str = "trineb";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueRef {
    Inline(Vec<u8>),
    Blob {
        file_id: u64,
        offset: u64,
        len: u64,
        checksum: u32,
    },
}

impl ValueRef {
    #[must_use]
    pub fn len(&self) -> u64 {
        match self {
            Self::Inline(bytes) => bytes.len() as u64,
            Self::Blob { len, .. } => *len,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn inline_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Inline(bytes) => Some(bytes),
            Self::Blob { .. } => None,
        }
    }
}

#[must_use]
pub fn blob_path(db_path: &Path, file_id: u64) -> PathBuf {
    db_path.join(format!("blob-{file_id:020}.{BLOB_FILE_EXTENSION}",))
}

pub(crate) fn list_blob_file_ids(db_path: &Path) -> Result<BTreeSet<u64>> {
    let mut file_ids = BTreeSet::new();

    for entry in fs::read_dir(db_path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }

        let path = entry.path();
        let has_blob_extension = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(BLOB_FILE_EXTENSION));
        if !has_blob_extension {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Some(file_id) = stem.strip_prefix("blob-") else {
            continue;
        };
        let file_id = file_id.parse::<u64>().map_err(|_| Error::Corruption {
            message: format!("invalid blob file name: {}", path.display()),
        })?;
        file_ids.insert(file_id);
    }

    Ok(file_ids)
}

pub(crate) fn write_large_values(
    db_path: &Path,
    file_id: u64,
    threshold: usize,
    records: &[(InternalKey, Option<ValueRef>)],
) -> Result<Vec<(InternalKey, Option<ValueRef>)>> {
    let needs_blob_file = records.iter().any(
        |(_, value)| matches!(value, Some(ValueRef::Inline(bytes)) if bytes.len() >= threshold),
    );
    if !needs_blob_file {
        return Ok(records.to_vec());
    }

    let path = blob_path(db_path, file_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp_path = path.with_extension("tmp");
    let mut file = File::create(&tmp_path)?;
    let mut offset = 0_u64;
    let mut rewritten = Vec::with_capacity(records.len());

    for (internal_key, value) in records {
        let value = match value {
            Some(ValueRef::Inline(bytes)) if bytes.len() >= threshold => {
                file.write_all(bytes)?;
                let len = u64::try_from(bytes.len())
                    .map_err(|_| Error::invalid_options("blob value exceeds u64::MAX"))?;
                let value = ValueRef::Blob {
                    file_id,
                    offset,
                    len,
                    checksum: checksum(bytes),
                };
                offset = offset.checked_add(len).ok_or_else(|| Error::Corruption {
                    message: "blob file offset overflow".to_owned(),
                })?;
                Some(value)
            }
            value => value.clone(),
        };
        rewritten.push((internal_key.clone(), value));
    }

    file.sync_all()?;
    drop(file);
    fs::rename(tmp_path, &path)?;

    Ok(rewritten)
}

pub(crate) fn read_value(db_path: &Path, value: &ValueRef) -> Result<Vec<u8>> {
    match value {
        ValueRef::Inline(bytes) => Ok(bytes.clone()),
        ValueRef::Blob {
            file_id,
            offset,
            len,
            checksum: expected_checksum,
        } => {
            let len = usize::try_from(*len).map_err(|_| Error::Corruption {
                message: "blob length exceeds usize".to_owned(),
            })?;
            let mut file =
                File::open(blob_path(db_path, *file_id)).map_err(|error| Error::Corruption {
                    message: format!("referenced blob file cannot be opened: {error}"),
                })?;
            file.seek(SeekFrom::Start(*offset))?;
            let mut bytes = vec![0_u8; len];
            file.read_exact(&mut bytes)
                .map_err(|error| Error::Corruption {
                    message: format!("referenced blob bytes cannot be read: {error}"),
                })?;
            if checksum(&bytes) != *expected_checksum {
                return Err(Error::Corruption {
                    message: "blob checksum mismatch".to_owned(),
                });
            }
            Ok(bytes)
        }
    }
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c_9dc5_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}
