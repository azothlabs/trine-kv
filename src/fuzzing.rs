//! Stable entry points that connect external fuzz runners to production
//! decoders. This module exists only with the `fuzzing` feature.

use crate::{UploadId, content::UploadSessionState, types::Sequence};

/// Exercises the complete manifest envelope and payload decoder.
pub fn decode_manifest(bytes: &[u8]) {
    let _ = crate::manifest::decode_manifest(bytes);
}

/// Exercises WAL frame validation, checksums, operation budgets, and payloads.
pub fn decode_wal(bytes: &[u8]) {
    let _ = crate::wal::decode_frames_after(bytes, Sequence::ZERO);
}

/// Exercises the complete table envelope, footer, blocks, filters, and indexes.
pub fn decode_table(bytes: &[u8]) {
    crate::table::fuzz_decode_table(bytes);
}

/// Exercises the complete blob envelope, records, codecs, and checksums.
pub fn decode_blob(bytes: &[u8]) {
    let _ = crate::blob::decode_blob_file(bytes);
}

/// Exercises the fixed-format upload state and all lifecycle validation.
pub fn decode_upload(bytes: &[u8]) {
    let mut identity = [0_u8; 16];
    if let Some(encoded) = bytes.get(9..25) {
        identity.copy_from_slice(encoded);
    }
    let _ = UploadSessionState::decode(bytes, UploadId::from_bytes(identity));
}

/// Exercises object-store lease and chained WAL-segment formats.
pub fn decode_object_control(bytes: &[u8]) {
    crate::substrate::fuzz_decode_object_control(bytes);
}
