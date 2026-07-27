//! Content identities, authorization scopes, and lifecycle option values.

use super::{
    CONTENT_ID_SHA256_TAG, CONTENT_LEASE_ID_VERSION, CONTENT_PHYSICAL_ACCOUNT_KEY,
    CONTENT_PHYSICAL_ACCOUNT_MAGIC, CONTENT_PHYSICAL_HOLD_ID_VERSION, CONTENT_PHYSICAL_QUOTA_KEY,
    CONTENT_PHYSICAL_QUOTA_MAGIC, CONTENT_PHYSICAL_RESERVATION_KEY,
    CONTENT_PHYSICAL_RESERVATION_MAGIC, Digest, DurabilityMode, Duration, Error, MAX_CHUNK_BYTES,
    MIN_CHUNK_BYTES, Result, Sha256, UPLOAD_TOKEN_VERSION, array_at, duration_millis, fmt,
    write_hex,
};

mod attachment;
mod attachment_scope;
mod content_id;
mod lease_id;
mod lease_options;
mod physical_hold;
mod quota;
mod token;
mod upload_id;
mod upload_options;

pub use attachment::{ContentAttachment, SealedContent};
pub use attachment_scope::ContentAttachmentScope;
pub use content_id::{ContentHashAlgorithm, ContentId};
pub use lease_id::{ContentLeaseId, ContentLeaseOwnerId, OwnerScopeId};
pub use lease_options::ContentLeaseOptions;
pub use physical_hold::{
    ContentPhysicalHoldId, ContentPhysicalHoldKind, ContentPhysicalHoldOptions,
    ContentPhysicalHoldOwnerId,
};
pub use quota::{ContentPhysicalQuota, StorageDomainId};
pub use token::{ContentChangeId, UploadToken};
pub use upload_id::UploadId;
pub use upload_options::ContentUploadOptions;

pub(crate) use quota::{
    ContentPhysicalAccountRecord, ContentPhysicalReservationRecord, content_physical_account_key,
    content_physical_quota_key, content_physical_reservation_key,
};
