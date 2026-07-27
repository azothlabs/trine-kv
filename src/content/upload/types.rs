use super::{
    Arc, ContentUpload, Error, Result, SealedContent, UPLOAD_ID_TOMBSTONE_LEN,
    UPLOAD_ID_TOMBSTONE_MAGIC, UploadId, array_at,
};

/// Result of resuming durable state for an [`UploadId`].
///
/// A successful seal is remembered by upload identity. Callers therefore get
/// either a writable open session or the exact prior seal result; they never
/// reopen a sealed upload as writable state.
#[derive(Debug)]
pub enum ContentUploadResume {
    /// The upload remains open and may accept bytes at [`ContentUpload::len`].
    Open(ContentUpload),
    /// The upload was already sealed; this is the idempotent prior result.
    Sealed(SealedContent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UploadIdRetirement {
    Aborted,
    Sealed,
}

pub(crate) fn encode_upload_id_tombstone(
    upload_id: UploadId,
    retirement: UploadIdRetirement,
) -> Arc<[u8]> {
    let mut bytes = Vec::with_capacity(UPLOAD_ID_TOMBSTONE_LEN);
    bytes.extend_from_slice(UPLOAD_ID_TOMBSTONE_MAGIC);
    bytes.push(match retirement {
        UploadIdRetirement::Aborted => 0,
        UploadIdRetirement::Sealed => 1,
    });
    bytes.extend_from_slice(&upload_id.to_bytes());
    debug_assert_eq!(bytes.len(), UPLOAD_ID_TOMBSTONE_LEN);
    Arc::from(bytes)
}

pub(crate) fn decode_upload_id_tombstone(
    bytes: &[u8],
    expected: UploadId,
) -> Result<Option<UploadIdRetirement>> {
    if bytes.get(..8) != Some(UPLOAD_ID_TOMBSTONE_MAGIC) {
        return Ok(None);
    }
    if bytes.len() != UPLOAD_ID_TOMBSTONE_LEN {
        return Err(Error::InvalidFormat {
            message: "invalid retired upload-id marker length".to_owned(),
        });
    }
    let retirement = match bytes[8] {
        0 => UploadIdRetirement::Aborted,
        1 => UploadIdRetirement::Sealed,
        _ => {
            return Err(Error::InvalidFormat {
                message: "invalid retired upload-id marker state".to_owned(),
            });
        }
    };
    let encoded = UploadId(array_at::<16>(bytes, 9, "retired upload identity")?);
    if encoded != expected {
        return Err(Error::InvalidFormat {
            message: "retired upload-id marker identity mismatch".to_owned(),
        });
    }
    Ok(Some(retirement))
}

/// Durable lifecycle visible to content-upload maintenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentUploadState {
    /// The upload can still accept bytes.
    Open,
    /// Descriptor publication started and must be resumed rather than discarded.
    Sealing,
    /// The upload completed; its state remains only for idempotent retries.
    Sealed,
    /// Abort cleanup started and can be resumed.
    Aborting,
}

/// One durable upload discovered by the maintenance index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentUploadInfo {
    pub(in crate::content::upload) upload_id: UploadId,
    pub(in crate::content::upload) state: ContentUploadState,
    pub(in crate::content::upload) updated_at_unix_ms: u64,
    pub(in crate::content::upload) length: u64,
}

impl ContentUploadInfo {
    /// Returns the stable upload identity used by resume, abort, and seal APIs.
    #[must_use]
    pub const fn upload_id(self) -> UploadId {
        self.upload_id
    }

    /// Returns the durable lifecycle observed while listing.
    #[must_use]
    pub const fn state(self) -> ContentUploadState {
        self.state
    }

    /// Returns the last durable update time in Unix milliseconds.
    #[must_use]
    pub const fn updated_at_unix_ms(self) -> u64 {
        self.updated_at_unix_ms
    }

    /// Returns the durable original-byte length.
    #[must_use]
    pub const fn len(self) -> u64 {
        self.length
    }

    /// Returns whether the durable original-byte length is zero.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.length == 0
    }
}

/// Counts returned after one idempotent upload-maintenance pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContentUploadMaintenanceReport {
    pub(crate) scanned: u64,
    pub(crate) aborted: u64,
    pub(crate) pruned_sealed: u64,
}

impl ContentUploadMaintenanceReport {
    /// Returns the number of durable upload states inspected.
    #[must_use]
    pub const fn scanned(self) -> u64 {
        self.scanned
    }

    /// Returns the number of inactive open or aborting uploads fully removed.
    #[must_use]
    pub const fn aborted(self) -> u64 {
        self.aborted
    }

    /// Returns the number of sealed idempotency states removed.
    #[must_use]
    pub const fn pruned_sealed(self) -> u64 {
        self.pruned_sealed
    }
}

impl ContentUploadResume {
    /// Returns the open upload, or `None` when it was already sealed.
    #[must_use]
    pub fn into_open(self) -> Option<ContentUpload> {
        match self {
            Self::Open(upload) => Some(upload),
            Self::Sealed(_) => None,
        }
    }

    /// Returns the prior seal result, or `None` while the upload remains open.
    #[must_use]
    pub const fn sealed(&self) -> Option<SealedContent> {
        match self {
            Self::Open(_) => None,
            Self::Sealed(sealed) => Some(*sealed),
        }
    }
}
