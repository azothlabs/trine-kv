use std::{error, fmt, io};

use crate::{options::DurabilityMode, types::ReadVersion};

/// Convenient result alias used by Trine KV APIs.
pub type Result<T> = std::result::Result<T, Error>;

/// Error returned by database, storage, recovery, and transaction operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Underlying I/O error from the selected storage backend.
    Io(io::Error),
    /// Durable data failed an integrity or consistency check.
    Corruption {
        /// Human-readable corruption detail.
        message: String,
    },
    /// Bytes could not be decoded as a valid Trine storage record.
    InvalidFormat {
        /// Human-readable decode failure detail.
        message: String,
    },
    /// Bytes use a storage format that this crate version does not support.
    UnsupportedFormat {
        /// Human-readable unsupported-format detail.
        message: String,
    },
    /// The requested codec is not available in this build.
    CodecUnavailable {
        /// Codec name or identifier that was requested.
        codec: String,
    },
    /// An optimistic transaction conflicted with a committed write.
    Conflict {
        /// Human-readable conflict detail.
        message: String,
    },
    /// This writer was fenced: another writer holds a newer fencing epoch for the
    /// same object-store database, so this writer's manifest publish was rejected
    /// to preserve the single-writer invariant. The holder must stop writing.
    Fenced {
        /// The fencing epoch this writer holds.
        held_epoch: u64,
        /// The newer epoch already recorded in the published manifest.
        current_epoch: u64,
    },
    /// The requested read version is newer than the latest visible database
    /// state.
    ReadVersionTooNew {
        /// Read version requested by the caller.
        requested: ReadVersion,
        /// Newest read version visible to readers when the request was
        /// checked.
        latest: ReadVersion,
    },
    /// The requested read version is older than Trine's retained history.
    ReadVersionExpired {
        /// Read version requested by the caller.
        requested: ReadVersion,
        /// Oldest read version Trine promises to answer when the request was
        /// checked.
        oldest_retained: ReadVersion,
    },
    /// A checkpoint with the requested name already exists.
    CheckpointAlreadyExists {
        /// Existing checkpoint name.
        name: String,
    },
    /// The requested checkpoint name was not found.
    CheckpointNotFound {
        /// Missing checkpoint name.
        name: String,
    },
    /// No sealed `ContentObject` descriptor exists for the requested identity.
    ContentNotFound {
        /// `StorageDomainId` rendered for diagnostics.
        storage_domain_id: String,
        /// Algorithm-tagged `ContentId` rendered for diagnostics.
        content_id: String,
    },
    /// No durable upload session exists for the requested identity.
    ContentUploadNotFound {
        /// `UploadId` rendered for diagnostics.
        upload_id: String,
    },
    /// A write or abort targeted an upload that was already sealed.
    ContentUploadSealed {
        /// `UploadId` rendered for diagnostics.
        upload_id: String,
    },
    /// A stale upload handle attempted to advance a newer durable revision.
    ContentUploadConflict {
        /// `UploadId` rendered for diagnostics.
        upload_id: String,
        /// Revision held by the caller.
        expected_revision: u64,
        /// Current durable revision.
        actual_revision: u64,
    },
    /// No issued attachment authority matches the supplied bearer token.
    UploadTokenInvalid,
    /// The authenticated domain or owner does not match the token claims.
    UploadTokenScopeMismatch,
    /// Available attachment authority reached its wall-clock deadline.
    UploadTokenExpired {
        /// Token deadline as Unix epoch milliseconds.
        expired_at_unix_ms: u64,
    },
    /// Another `ChangeId` has already consumed this attachment authority.
    UploadTokenAlreadyConsumed,
    /// A content range begins after the original byte sequence.
    ContentRangeOutOfBounds {
        /// Requested zero-based start offset.
        start: u64,
        /// Original content length.
        length: u64,
    },
    /// Content bytes did not match their expected cryptographic identity.
    ContentDigestMismatch {
        /// Expected algorithm-tagged digest.
        expected: String,
        /// Digest computed from the observed bytes.
        actual: String,
    },
    /// A sealed upload's original byte length differed from its expectation.
    ContentLengthMismatch {
        /// Length declared when the upload was opened.
        expected: u64,
        /// Length accepted before seal.
        actual: u64,
    },
    /// The database was opened read-only and a write was requested.
    ReadOnly,
    /// The database handle is closed.
    Closed,
    /// The configured runtime cannot accept the requested work now.
    RuntimeBusy {
        /// Human-readable runtime capacity detail.
        message: String,
    },
    /// A named bucket required by durable metadata was not found.
    BucketMissing {
        /// Missing bucket name.
        name: String,
    },
    /// Options were invalid or inconsistent.
    InvalidOptions {
        /// Human-readable options failure detail.
        message: String,
    },
    /// A Trine feature is unavailable in the current runtime or build.
    Unsupported {
        /// Feature name that is unavailable.
        feature: &'static str,
    },
    /// The selected storage backend does not provide a required capability.
    UnsupportedBackend {
        /// Backend capability that is unavailable.
        feature: &'static str,
    },
    /// The selected storage backend cannot provide the requested durability.
    UnsupportedDurability {
        /// Durability mode requested by the caller.
        requested: DurabilityMode,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum ErrorSnapshot {
    Io {
        message: String,
    },
    Corruption {
        message: String,
    },
    InvalidFormat {
        message: String,
    },
    UnsupportedFormat {
        message: String,
    },
    CodecUnavailable {
        codec: String,
    },
    Conflict {
        message: String,
    },
    Fenced {
        held_epoch: u64,
        current_epoch: u64,
    },
    ReadVersionTooNew {
        requested: ReadVersion,
        latest: ReadVersion,
    },
    ReadVersionExpired {
        requested: ReadVersion,
        oldest_retained: ReadVersion,
    },
    CheckpointAlreadyExists {
        name: String,
    },
    CheckpointNotFound {
        name: String,
    },
    ContentNotFound {
        storage_domain_id: String,
        content_id: String,
    },
    ContentUploadNotFound {
        upload_id: String,
    },
    ContentUploadSealed {
        upload_id: String,
    },
    ContentUploadConflict {
        upload_id: String,
        expected_revision: u64,
        actual_revision: u64,
    },
    UploadTokenInvalid,
    UploadTokenScopeMismatch,
    UploadTokenExpired {
        expired_at_unix_ms: u64,
    },
    UploadTokenAlreadyConsumed,
    ContentRangeOutOfBounds {
        start: u64,
        length: u64,
    },
    ContentDigestMismatch {
        expected: String,
        actual: String,
    },
    ContentLengthMismatch {
        expected: u64,
        actual: u64,
    },
    ReadOnly,
    Closed,
    RuntimeBusy {
        message: String,
    },
    BucketMissing {
        name: String,
    },
    InvalidOptions {
        message: String,
    },
    Unsupported {
        feature: &'static str,
    },
    UnsupportedBackend {
        feature: &'static str,
    },
    UnsupportedDurability {
        requested: DurabilityMode,
    },
}

impl ErrorSnapshot {
    #[must_use]
    pub(crate) fn capture(error: &Error) -> Self {
        match error {
            Error::Io(error) => Self::Io {
                message: error.to_string(),
            },
            Error::Corruption { message } => Self::Corruption {
                message: message.clone(),
            },
            Error::InvalidFormat { message } => Self::InvalidFormat {
                message: message.clone(),
            },
            Error::UnsupportedFormat { message } => Self::UnsupportedFormat {
                message: message.clone(),
            },
            Error::CodecUnavailable { codec } => Self::CodecUnavailable {
                codec: codec.clone(),
            },
            Error::Conflict { message } => Self::Conflict {
                message: message.clone(),
            },
            Error::Fenced {
                held_epoch,
                current_epoch,
            } => Self::Fenced {
                held_epoch: *held_epoch,
                current_epoch: *current_epoch,
            },
            Error::ReadVersionTooNew { requested, latest } => Self::ReadVersionTooNew {
                requested: *requested,
                latest: *latest,
            },
            Error::ReadVersionExpired {
                requested,
                oldest_retained,
            } => Self::ReadVersionExpired {
                requested: *requested,
                oldest_retained: *oldest_retained,
            },
            Error::CheckpointAlreadyExists { name } => {
                Self::CheckpointAlreadyExists { name: name.clone() }
            }
            Error::CheckpointNotFound { name } => Self::CheckpointNotFound { name: name.clone() },
            Error::ContentNotFound {
                storage_domain_id,
                content_id,
            } => Self::ContentNotFound {
                storage_domain_id: storage_domain_id.clone(),
                content_id: content_id.clone(),
            },
            Error::ContentUploadNotFound { upload_id } => Self::ContentUploadNotFound {
                upload_id: upload_id.clone(),
            },
            Error::ContentUploadSealed { upload_id } => Self::ContentUploadSealed {
                upload_id: upload_id.clone(),
            },
            Error::ContentUploadConflict {
                upload_id,
                expected_revision,
                actual_revision,
            } => Self::ContentUploadConflict {
                upload_id: upload_id.clone(),
                expected_revision: *expected_revision,
                actual_revision: *actual_revision,
            },
            Error::UploadTokenInvalid => Self::UploadTokenInvalid,
            Error::UploadTokenScopeMismatch => Self::UploadTokenScopeMismatch,
            Error::UploadTokenExpired { expired_at_unix_ms } => Self::UploadTokenExpired {
                expired_at_unix_ms: *expired_at_unix_ms,
            },
            Error::UploadTokenAlreadyConsumed => Self::UploadTokenAlreadyConsumed,
            Error::ContentRangeOutOfBounds { start, length } => Self::ContentRangeOutOfBounds {
                start: *start,
                length: *length,
            },
            Error::ContentDigestMismatch { expected, actual } => Self::ContentDigestMismatch {
                expected: expected.clone(),
                actual: actual.clone(),
            },
            Error::ContentLengthMismatch { expected, actual } => Self::ContentLengthMismatch {
                expected: *expected,
                actual: *actual,
            },
            Error::ReadOnly => Self::ReadOnly,
            Error::Closed => Self::Closed,
            Error::RuntimeBusy { message } => Self::RuntimeBusy {
                message: message.clone(),
            },
            Error::BucketMissing { name } => Self::BucketMissing { name: name.clone() },
            Error::InvalidOptions { message } => Self::InvalidOptions {
                message: message.clone(),
            },
            Error::Unsupported { feature } => Self::Unsupported { feature },
            Error::UnsupportedBackend { feature } => Self::UnsupportedBackend { feature },
            Error::UnsupportedDurability { requested } => Self::UnsupportedDurability {
                requested: *requested,
            },
        }
    }

    pub(crate) fn into_error(self) -> Error {
        match self {
            Self::Io { message } => Error::Io(io::Error::other(message)),
            Self::Corruption { message } => Error::Corruption { message },
            Self::InvalidFormat { message } => Error::InvalidFormat { message },
            Self::UnsupportedFormat { message } => Error::UnsupportedFormat { message },
            Self::CodecUnavailable { codec } => Error::CodecUnavailable { codec },
            Self::Conflict { message } => Error::Conflict { message },
            Self::Fenced {
                held_epoch,
                current_epoch,
            } => Error::Fenced {
                held_epoch,
                current_epoch,
            },
            Self::ReadVersionTooNew { requested, latest } => {
                Error::ReadVersionTooNew { requested, latest }
            }
            Self::ReadVersionExpired {
                requested,
                oldest_retained,
            } => Error::ReadVersionExpired {
                requested,
                oldest_retained,
            },
            Self::CheckpointAlreadyExists { name } => Error::CheckpointAlreadyExists { name },
            Self::CheckpointNotFound { name } => Error::CheckpointNotFound { name },
            Self::ContentNotFound {
                storage_domain_id,
                content_id,
            } => Error::ContentNotFound {
                storage_domain_id,
                content_id,
            },
            Self::ContentUploadNotFound { upload_id } => Error::ContentUploadNotFound { upload_id },
            Self::ContentUploadSealed { upload_id } => Error::ContentUploadSealed { upload_id },
            Self::ContentUploadConflict {
                upload_id,
                expected_revision,
                actual_revision,
            } => Error::ContentUploadConflict {
                upload_id,
                expected_revision,
                actual_revision,
            },
            Self::UploadTokenInvalid => Error::UploadTokenInvalid,
            Self::UploadTokenScopeMismatch => Error::UploadTokenScopeMismatch,
            Self::UploadTokenExpired { expired_at_unix_ms } => {
                Error::UploadTokenExpired { expired_at_unix_ms }
            }
            Self::UploadTokenAlreadyConsumed => Error::UploadTokenAlreadyConsumed,
            Self::ContentRangeOutOfBounds { start, length } => {
                Error::ContentRangeOutOfBounds { start, length }
            }
            Self::ContentDigestMismatch { expected, actual } => {
                Error::ContentDigestMismatch { expected, actual }
            }
            Self::ContentLengthMismatch { expected, actual } => {
                Error::ContentLengthMismatch { expected, actual }
            }
            Self::ReadOnly => Error::ReadOnly,
            Self::Closed => Error::Closed,
            Self::RuntimeBusy { message } => Error::RuntimeBusy { message },
            Self::BucketMissing { name } => Error::BucketMissing { name },
            Self::InvalidOptions { message } => Error::InvalidOptions { message },
            Self::Unsupported { feature } => Error::Unsupported { feature },
            Self::UnsupportedBackend { feature } => Error::UnsupportedBackend { feature },
            Self::UnsupportedDurability { requested } => Error::UnsupportedDurability { requested },
        }
    }
}

impl Error {
    /// Creates an unsupported-feature error.
    #[must_use]
    pub const fn unsupported(feature: &'static str) -> Self {
        Self::Unsupported { feature }
    }

    /// Creates an unsupported-backend error.
    #[must_use]
    pub const fn unsupported_backend(feature: &'static str) -> Self {
        Self::UnsupportedBackend { feature }
    }

    /// Creates an unsupported-durability error.
    #[must_use]
    pub const fn unsupported_durability(requested: DurabilityMode) -> Self {
        Self::UnsupportedDurability { requested }
    }

    /// Creates an invalid-options error.
    #[must_use]
    pub fn invalid_options(message: impl Into<String>) -> Self {
        Self::InvalidOptions {
            message: message.into(),
        }
    }

    /// Creates a runtime-busy error.
    #[must_use]
    pub fn runtime_busy(message: impl Into<String>) -> Self {
        Self::RuntimeBusy {
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "io error: {error}"),
            Self::Corruption { message } => write!(formatter, "corruption: {message}"),
            Self::InvalidFormat { message } => write!(formatter, "invalid format: {message}"),
            Self::UnsupportedFormat { message } => {
                write!(formatter, "unsupported format: {message}")
            }
            Self::CodecUnavailable { codec } => write!(formatter, "codec unavailable: {codec}"),
            Self::Conflict { message } => write!(formatter, "transaction conflict: {message}"),
            Self::Fenced {
                held_epoch,
                current_epoch,
            } => write!(
                formatter,
                "writer fenced: held epoch {held_epoch} but manifest is at epoch {current_epoch}"
            ),
            Self::ReadVersionTooNew { requested, latest } => write!(
                formatter,
                "read version {} is newer than latest read version {}",
                requested.as_u64(),
                latest.as_u64()
            ),
            Self::ReadVersionExpired {
                requested,
                oldest_retained,
            } => write!(
                formatter,
                "read version {} is older than oldest retained read version {}",
                requested.as_u64(),
                oldest_retained.as_u64()
            ),
            Self::CheckpointAlreadyExists { name } => {
                write!(formatter, "checkpoint already exists: {name}")
            }
            Self::CheckpointNotFound { name } => write!(formatter, "checkpoint not found: {name}"),
            Self::ContentNotFound {
                storage_domain_id,
                content_id,
            } => write!(
                formatter,
                "content not found in storage domain {storage_domain_id}: {content_id}"
            ),
            Self::ContentUploadNotFound { upload_id } => {
                write!(formatter, "content upload not found: {upload_id}")
            }
            Self::ContentUploadSealed { upload_id } => {
                write!(formatter, "content upload is already sealed: {upload_id}")
            }
            Self::ContentUploadConflict {
                upload_id,
                expected_revision,
                actual_revision,
            } => write!(
                formatter,
                "content upload {upload_id} revision conflict: caller has {expected_revision}, durable state is {actual_revision}"
            ),
            Self::UploadTokenInvalid => formatter.write_str("upload token is invalid"),
            Self::UploadTokenScopeMismatch => {
                formatter.write_str("upload token scope does not match the authenticated scope")
            }
            Self::UploadTokenExpired { expired_at_unix_ms } => write!(
                formatter,
                "upload token expired at Unix millisecond {expired_at_unix_ms}"
            ),
            Self::UploadTokenAlreadyConsumed => {
                formatter.write_str("upload token was consumed by another change")
            }
            Self::ContentRangeOutOfBounds { start, length } => write!(
                formatter,
                "content range starts at {start}, after content length {length}"
            ),
            Self::ContentDigestMismatch { expected, actual } => write!(
                formatter,
                "content digest mismatch: expected {expected}, got {actual}"
            ),
            Self::ContentLengthMismatch { expected, actual } => write!(
                formatter,
                "content length mismatch: expected {expected}, got {actual}"
            ),
            Self::ReadOnly => formatter.write_str("database is read-only"),
            Self::Closed => formatter.write_str("database is closed"),
            Self::RuntimeBusy { message } => write!(formatter, "runtime busy: {message}"),
            Self::BucketMissing { name } => write!(formatter, "bucket is missing: {name}"),
            Self::InvalidOptions { message } => write!(formatter, "invalid options: {message}"),
            Self::Unsupported { feature } => write!(formatter, "unsupported feature: {feature}"),
            Self::UnsupportedBackend { feature } => {
                write!(formatter, "unsupported storage backend feature: {feature}")
            }
            Self::UnsupportedDurability { requested } => {
                write!(
                    formatter,
                    "unsupported durability mode: {}",
                    requested.as_str()
                )
            }
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
