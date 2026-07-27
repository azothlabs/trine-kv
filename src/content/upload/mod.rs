//! Upload sessions and their durable content format.
//!
//! Public session values, the live upload handle, session-state encoding, and
//! immutable descriptor/chunk encoding have separate owners. This façade keeps
//! their public and crate-internal API unchanged.

use super::{
    Arc, CHUNK_HEADER_LEN, CHUNK_MAGIC, ContentAttachmentScope, ContentHashAlgorithm, ContentId,
    ContentUploadOptions, DESCRIPTOR_LEN, DESCRIPTOR_MAGIC, Db, Digest, DurabilityMode, Duration,
    Error, MAX_CHUNK_BYTES, MIN_CHUNK_BYTES, OwnerScopeId, Result, SealedContent, Sha256,
    StorageDomainId, UPLOAD_ID_TOMBSTONE_LEN, UPLOAD_ID_TOMBSTONE_MAGIC, UPLOAD_STATE_ABORTING,
    UPLOAD_STATE_LEN, UPLOAD_STATE_MAGIC, UPLOAD_STATE_OPEN, UPLOAD_STATE_SEALED,
    UPLOAD_STATE_SEALING, UPLOAD_STATE_UPDATED_AT_OFFSET, UploadId, UploadToken, array_at,
    current_epoch_millis, decode_content_id, decode_durability, decode_optional_content_id,
    decode_optional_u64, digest_string, encode_durability, encode_optional_content_id,
    encode_optional_u64, fmt, mem,
};

mod descriptor;
mod handle;
mod session;
mod types;

pub use handle::ContentUpload;
pub use types::{
    ContentUploadInfo, ContentUploadMaintenanceReport, ContentUploadResume, ContentUploadState,
};

pub(crate) use descriptor::*;
pub(crate) use session::*;
pub(crate) use types::{
    UploadIdRetirement, decode_upload_id_tombstone, encode_upload_id_tombstone,
};
