use std::{error, fmt, io};

use crate::{
    content::{ContentAccessBarrierId, ContentPhysicalHoldId, ContentPhysicalHoldKind},
    options::DurabilityMode,
    types::ReadVersion,
};

/// Convenient result alias used by Trine KV APIs.
pub type Result<T> = std::result::Result<T, Error>;

/// Physical reason that reclaim intent cannot currently be recorded.
///
/// Each variant carries the coordinate a higher-layer worker needs to decide
/// whether to wait or obtain a new exact proof. These blockers do not mutate
/// content and never authorize deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentReclaimBlocker {
    /// The storage domain still permits new unleased content handles.
    UnleasedAccessAllowed,
    /// The backend barrier is active but its protected commit coordinate is
    /// not yet visible, usually because publication was interrupted.
    LeasedOnlyBarrierUncoordinated {
        /// Durable barrier identity that a writer can resume.
        barrier_id: ContentAccessBarrierId,
    },
    /// The leased-only barrier has no trusted pre-barrier reader-drain
    /// attestation, so quarantine cannot begin.
    ReaderDrainNotAttested {
        /// Barrier identity that still needs a matching attestation.
        barrier_id: ContentAccessBarrierId,
    },
    /// No exact accepted reclaim intent matches the quarantine request.
    ReclaimIntentRequired,
    /// The higher-layer proof reached its exclusive wall-clock deadline.
    ProofExpired {
        /// Proof deadline as Unix epoch milliseconds.
        expired_at_unix_ms: u64,
    },
    /// Durable physical activity occurred after the proof's stable read point.
    Superseded {
        /// Latest protected content-activity commit sequence.
        activity_at_commit_seq: u64,
        /// Stable commit sequence verified by the reclaim proof.
        verified_at_commit_seq: u64,
    },
    /// An unexpired upload token can still attach this content.
    UploadToken {
        /// Upload-token deadline as Unix epoch milliseconds.
        expires_at_unix_ms: u64,
    },
    /// An unexpired read lease still protects this content.
    ReadLease {
        /// Read-lease deadline as Unix epoch milliseconds.
        expires_at_unix_ms: u64,
    },
    /// A migration, backup, repair, provider, or administrative hold remains.
    PhysicalHold {
        /// Durable hold identity that blocked intent.
        hold_id: ContentPhysicalHoldId,
        /// Operational reason for retaining the physical bytes.
        kind: ContentPhysicalHoldKind,
        /// Exclusive deadline, or `None` when explicit release is required.
        expires_at_unix_ms: Option<u64>,
    },
}

impl fmt::Display for ContentReclaimBlocker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnleasedAccessAllowed => {
                formatter.write_str("the storage domain still permits unleased content opens")
            }
            Self::LeasedOnlyBarrierUncoordinated { barrier_id } => write!(
                formatter,
                "leased-only barrier {barrier_id} has no protected commit coordinate"
            ),
            Self::ReaderDrainNotAttested { barrier_id } => write!(
                formatter,
                "leased-only barrier {barrier_id} has no reader-drain attestation"
            ),
            Self::ReclaimIntentRequired => {
                formatter.write_str("content quarantine requires the exact accepted reclaim intent")
            }
            Self::ProofExpired { expired_at_unix_ms } => write!(
                formatter,
                "proof expired at Unix millisecond {expired_at_unix_ms}"
            ),
            Self::Superseded {
                activity_at_commit_seq,
                verified_at_commit_seq,
            } => write!(
                formatter,
                "proof at commit {verified_at_commit_seq} was superseded by physical activity at commit {activity_at_commit_seq}"
            ),
            Self::UploadToken { expires_at_unix_ms } => write!(
                formatter,
                "upload authority remains until Unix millisecond {expires_at_unix_ms}"
            ),
            Self::ReadLease { expires_at_unix_ms } => write!(
                formatter,
                "a read lease remains until Unix millisecond {expires_at_unix_ms}"
            ),
            Self::PhysicalHold {
                hold_id,
                kind,
                expires_at_unix_ms,
            } => match expires_at_unix_ms {
                Some(expires_at_unix_ms) => write!(
                    formatter,
                    "{kind} physical hold {hold_id} remains until Unix millisecond {expires_at_unix_ms}"
                ),
                None => write!(
                    formatter,
                    "{kind} physical hold {hold_id} remains until explicit release"
                ),
            },
        }
    }
}

impl ContentReclaimBlocker {
    fn fmt_as_error(self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "content reclaim is blocked: {self}")
    }
}

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
    /// No durable content read lease exists for the supplied identity.
    ContentLeaseNotFound {
        /// Missing `ContentLeaseId` rendered for diagnostics.
        lease_id: String,
    },
    /// A content read lease reached its wall-clock deadline.
    ContentLeaseExpired {
        /// Lease deadline as Unix epoch milliseconds.
        expired_at_unix_ms: u64,
    },
    /// The storage domain requires a durable read lease for new content opens.
    ContentLeaseRequired {
        /// Barrier identity that established leased-only access.
        barrier_id: ContentAccessBarrierId,
    },
    /// New leased reads are fenced by durable content quarantine.
    ContentQuarantined {
        /// Commit sequence that established quarantine.
        quarantined_at: ReadVersion,
    },
    /// No durable physical content hold exists for the supplied identity.
    ContentPhysicalHoldNotFound {
        /// Missing hold identity rendered for diagnostics.
        hold_id: String,
    },
    /// A physical content hold reached its exclusive wall-clock deadline.
    ContentPhysicalHoldExpired {
        /// Hold deadline as Unix epoch milliseconds.
        expired_at_unix_ms: u64,
    },
    /// The supplied owner does not control the durable physical content hold.
    ContentPhysicalHoldOwnerMismatch,
    /// Physical state currently prevents durable reclaim intent.
    ContentReclaimBlocked {
        /// Typed blocker and its recovery coordinate.
        blocker: ContentReclaimBlocker,
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
    ContentLeaseNotFound {
        lease_id: String,
    },
    ContentLeaseExpired {
        expired_at_unix_ms: u64,
    },
    ContentLeaseRequired {
        barrier_id: ContentAccessBarrierId,
    },
    ContentQuarantined {
        quarantined_at: ReadVersion,
    },
    ContentPhysicalHoldNotFound {
        hold_id: String,
    },
    ContentPhysicalHoldExpired {
        expired_at_unix_ms: u64,
    },
    ContentPhysicalHoldOwnerMismatch,
    ContentReclaimBlocked {
        blocker: ContentReclaimBlocker,
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
    fn io(message: String) -> Self {
        Self::Io { message }
    }

    fn corruption(message: &str) -> Self {
        Self::Corruption {
            message: message.to_owned(),
        }
    }

    fn invalid_format(message: &str) -> Self {
        Self::InvalidFormat {
            message: message.to_owned(),
        }
    }

    fn content_missing(storage_domain_id: &str, content_id: &str) -> Self {
        Self::ContentNotFound {
            storage_domain_id: storage_domain_id.to_owned(),
            content_id: content_id.to_owned(),
        }
    }

    fn lease_missing(lease_id: &str) -> Self {
        Self::ContentLeaseNotFound {
            lease_id: lease_id.to_owned(),
        }
    }

    const fn lease_dead(expired_at_unix_ms: u64) -> Self {
        Self::ContentLeaseExpired { expired_at_unix_ms }
    }

    const fn lease_required(barrier_id: ContentAccessBarrierId) -> Self {
        Self::ContentLeaseRequired { barrier_id }
    }

    const fn content_quarantined(quarantined_at: ReadVersion) -> Self {
        Self::ContentQuarantined { quarantined_at }
    }

    fn physical_hold_missing(hold_id: &str) -> Self {
        Self::ContentPhysicalHoldNotFound {
            hold_id: hold_id.to_owned(),
        }
    }

    const fn physical_hold_dead(expired_at_unix_ms: u64) -> Self {
        Self::ContentPhysicalHoldExpired { expired_at_unix_ms }
    }

    const fn content_reclaim_blocked(blocker: ContentReclaimBlocker) -> Self {
        Self::ContentReclaimBlocked { blocker }
    }

    const fn token_dead(expired_at_unix_ms: u64) -> Self {
        Self::UploadTokenExpired { expired_at_unix_ms }
    }

    fn upload_missing(upload_id: &str) -> Self {
        Self::ContentUploadNotFound {
            upload_id: upload_id.to_owned(),
        }
    }

    fn upload_sealed(upload_id: &str) -> Self {
        Self::ContentUploadSealed {
            upload_id: upload_id.to_owned(),
        }
    }

    #[must_use]
    pub(crate) fn capture(error: &Error) -> Self {
        match error {
            Error::Io(error) => Self::io(error.to_string()),
            Error::Corruption { message } => Self::corruption(message),
            Error::InvalidFormat { message } => Self::invalid_format(message),
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
            } => Self::content_missing(storage_domain_id, content_id),
            Error::ContentLeaseNotFound { lease_id } => Self::lease_missing(lease_id),
            Error::ContentLeaseExpired {
                expired_at_unix_ms: expiry,
            } => Self::lease_dead(*expiry),
            Error::ContentLeaseRequired { barrier_id } => Self::lease_required(*barrier_id),
            Error::ContentQuarantined { quarantined_at } => {
                Self::content_quarantined(*quarantined_at)
            }
            Error::ContentPhysicalHoldNotFound { hold_id } => Self::physical_hold_missing(hold_id),
            Error::ContentPhysicalHoldExpired { expired_at_unix_ms } => {
                Self::physical_hold_dead(*expired_at_unix_ms)
            }
            Error::ContentPhysicalHoldOwnerMismatch => Self::ContentPhysicalHoldOwnerMismatch,
            Error::ContentReclaimBlocked { blocker } => Self::content_reclaim_blocked(*blocker),
            Error::ContentUploadNotFound { upload_id } => Self::upload_missing(upload_id),
            Error::ContentUploadSealed { upload_id } => Self::upload_sealed(upload_id),
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
            Error::UploadTokenExpired {
                expired_at_unix_ms: expiry,
            } => Self::token_dead(*expiry),
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
            Self::ContentLeaseNotFound { lease_id } => Error::ContentLeaseNotFound { lease_id },
            Self::ContentLeaseExpired { expired_at_unix_ms } => {
                Error::ContentLeaseExpired { expired_at_unix_ms }
            }
            Self::ContentLeaseRequired { barrier_id } => Error::ContentLeaseRequired { barrier_id },
            Self::ContentQuarantined { quarantined_at } => {
                Error::ContentQuarantined { quarantined_at }
            }
            Self::ContentPhysicalHoldNotFound { hold_id } => {
                Error::ContentPhysicalHoldNotFound { hold_id }
            }
            Self::ContentPhysicalHoldExpired { expired_at_unix_ms } => {
                Error::ContentPhysicalHoldExpired { expired_at_unix_ms }
            }
            Self::ContentPhysicalHoldOwnerMismatch => Error::ContentPhysicalHoldOwnerMismatch,
            Self::ContentReclaimBlocked { blocker } => Error::ContentReclaimBlocked { blocker },
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
            Self::UnsupportedFormat { message } => fmt_unsupported_format(formatter, message),
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
            } => fmt_content_missing(formatter, storage_domain_id, content_id),
            Self::ContentLeaseNotFound { lease_id } => fmt_lease_missing(formatter, lease_id),
            Self::ContentLeaseExpired { expired_at_unix_ms } => {
                fmt_lease_expired(formatter, *expired_at_unix_ms)
            }
            Self::ContentLeaseRequired { barrier_id } => fmt_lease_required(formatter, *barrier_id),
            Self::ContentQuarantined { quarantined_at } => {
                fmt_content_quarantined(formatter, *quarantined_at)
            }
            Self::ContentPhysicalHoldNotFound { hold_id } => fmt_hold_missing(formatter, hold_id),
            Self::ContentPhysicalHoldExpired { expired_at_unix_ms } => {
                fmt_hold_expired(formatter, *expired_at_unix_ms)
            }
            Self::ContentPhysicalHoldOwnerMismatch => fmt_hold_owner_mismatch(formatter),
            Self::ContentReclaimBlocked { blocker } => blocker.fmt_as_error(formatter),
            Self::ContentUploadNotFound { upload_id } => fmt_upload_missing(formatter, upload_id),
            Self::ContentUploadSealed { upload_id } => fmt_upload_sealed(formatter, upload_id),
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
            Self::UnsupportedBackend { feature } => fmt_unsupported_backend(formatter, feature),
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

fn fmt_upload_missing(formatter: &mut fmt::Formatter<'_>, upload_id: &str) -> fmt::Result {
    write!(formatter, "content upload {upload_id} was not found")
}

fn fmt_unsupported_format(formatter: &mut fmt::Formatter<'_>, message: &str) -> fmt::Result {
    write!(formatter, "unsupported format: {message}")
}

fn fmt_unsupported_backend(formatter: &mut fmt::Formatter<'_>, feature: &str) -> fmt::Result {
    write!(formatter, "unsupported storage backend feature: {feature}")
}

fn fmt_content_quarantined(
    formatter: &mut fmt::Formatter<'_>,
    quarantined_at: ReadVersion,
) -> fmt::Result {
    write!(
        formatter,
        "content was quarantined at commit {}",
        quarantined_at.as_u64()
    )
}

fn fmt_upload_sealed(formatter: &mut fmt::Formatter<'_>, upload_id: &str) -> fmt::Result {
    write!(formatter, "content upload {upload_id} is already sealed")
}

fn fmt_content_missing(
    formatter: &mut fmt::Formatter<'_>,
    storage_domain_id: &str,
    content_id: &str,
) -> fmt::Result {
    write!(
        formatter,
        "content not found in storage domain {storage_domain_id}: {content_id}"
    )
}

fn fmt_lease_missing(formatter: &mut fmt::Formatter<'_>, lease_id: &str) -> fmt::Result {
    write!(formatter, "content lease not found: {lease_id}")
}

fn fmt_lease_expired(formatter: &mut fmt::Formatter<'_>, expired_at_unix_ms: u64) -> fmt::Result {
    write!(
        formatter,
        "content lease expired at Unix millisecond {expired_at_unix_ms}"
    )
}

fn fmt_lease_required(
    formatter: &mut fmt::Formatter<'_>,
    barrier_id: ContentAccessBarrierId,
) -> fmt::Result {
    write!(
        formatter,
        "content access requires a durable read lease after barrier {barrier_id}"
    )
}

fn fmt_hold_missing(formatter: &mut fmt::Formatter<'_>, hold_id: &str) -> fmt::Result {
    write!(formatter, "content physical hold not found: {hold_id}")
}

fn fmt_hold_expired(formatter: &mut fmt::Formatter<'_>, expired_at_unix_ms: u64) -> fmt::Result {
    write!(
        formatter,
        "content physical hold expired at Unix millisecond {expired_at_unix_ms}"
    )
}

fn fmt_hold_owner_mismatch(formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter.write_str("content physical hold owner does not match")
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
