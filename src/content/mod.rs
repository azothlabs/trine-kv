//! Immutable, cryptographically identified content objects.
//!
//! This module is the storage-layer primitive used by higher layers that need
//! files or other large byte sequences. Content is accepted incrementally,
//! sealed by publishing a fixed-size descriptor after all chunks are durable,
//! and read through verified ranges or a sequential stream. Ordinary key/value
//! values do not enter this path automatically.

use std::{
    fmt, mem,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use sha2::{Digest, Sha256};

use crate::{Db, DurabilityMode, Error, ObjectStoreReclamationEvidenceDigest, Result};

const CONTENT_ID_SHA256_TAG: u8 = 1;
const UPLOAD_TOKEN_VERSION: u8 = 1;
const CONTENT_LEASE_ID_VERSION: u8 = 1;
const CONTENT_ACCESS_BARRIER_ID_VERSION: u8 = 1;
const CONTENT_READER_DRAIN_ATTESTATION_ID_VERSION: u8 = 1;
const CONTENT_READER_DRAIN_EVIDENCE_SHA256_TAG: u8 = 1;
const CONTENT_RECLAIM_CLOCK_ATTESTATION_ID_VERSION: u8 = 1;
const CONTENT_RECLAIM_CLOCK_EVIDENCE_SHA256_TAG: u8 = 1;
const CONTENT_PHYSICAL_HOLD_ID_VERSION: u8 = 1;
const DESCRIPTOR_MAGIC: &[u8; 8] = b"TRNCNTD2";
const CHUNK_MAGIC: &[u8; 8] = b"TRNCNTC1";
const UPLOAD_STATE_MAGIC: &[u8; 8] = b"TRNUPLD3";
const DESCRIPTOR_LEN: usize = 8 + 16 + 1 + 32 + 16 + 8 + 4 + 8;
const CHUNK_HEADER_LEN: usize = 8 + 16 + 8 + 4 + 32;
const UPLOAD_STATE_LEN: usize =
    8 + 1 + 16 + 8 + 4 + 8 + 8 + 4 + 1 + 8 + 1 + 33 + 16 + 16 + 32 + 8 + 8 + 1 + 33;
const UPLOAD_STATE_OPEN: u8 = 0;
const UPLOAD_STATE_SEALING: u8 = 1;
const UPLOAD_STATE_SEALED: u8 = 2;
const UPLOAD_STATE_ABORTING: u8 = 3;
const MIN_CHUNK_BYTES: usize = 64 * 1024;
const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const CONTENT_LEASE_BUCKET: &str = "\u{1}trine-content-lease\u{1}";
pub(crate) const CONTENT_LEASE_MAGIC: &[u8; 8] = b"TRNCNLS1";
pub(crate) const CONTENT_PHYSICAL_HOLD_BUCKET: &str = "\u{1}trine-content-physical-hold\u{1}";
const CONTENT_PHYSICAL_HOLD_MAGIC: &[u8; 8] = b"TRNCPHL1";
pub(crate) const CONTENT_CONTROL_BUCKET: &str = "\u{1}trine-content-control\u{1}";
pub(crate) const CONTENT_TOKEN_INDEX_BUCKET: &str = "\u{1}trine-content-token-index\u{1}";
const CONTENT_CONTROL_MAGIC: &[u8; 8] = b"TRNCRCL1";
const CONTENT_TOKEN_INDEX_MAGIC: &[u8; 8] = b"TRNCTIX1";
const CONTENT_CONTROL_ACTIVE: u8 = 0;
const CONTENT_CONTROL_RECLAIM_INTENT: u8 = 1;
const CONTENT_RECLAIM_PROOF_TOKEN_BYTES: usize = 49;
const CONTENT_ACCESS_BARRIER_MAGIC: &[u8; 8] = b"TRNCABR1";
const CONTENT_ACCESS_COORDINATE_MAGIC: &[u8; 8] = b"TRNCACO1";
const CONTENT_READER_DRAIN_ATTESTATION_MAGIC: &[u8; 8] = b"TRNCRDA1";
const CONTENT_READER_DRAIN_EVIDENCE_DOMAIN: &[u8] = b"trine-content-reader-drain-evidence-v1";
const CONTENT_QUARANTINE_MAGIC: &[u8; 8] = b"TRNCQRT1";
const CONTENT_RECLAIM_GRACE_MAGIC: &[u8; 8] = b"TRNCRGR1";
const CONTENT_RECLAIM_SWEEP_MAGIC: &[u8; 8] = b"TRNCRSW2";
const CONTENT_RECLAIM_CLOCK_EVIDENCE_DOMAIN: &[u8] = b"trine-content-reclaim-clock-evidence-v1";
const CONTENT_RECLAIM_SWEEP_PREPARED: u8 = 0;
const CONTENT_RECLAIM_SWEEP_RECLAIMED: u8 = 1;
pub(crate) const CONTENT_PHYSICAL_QUOTA_MAGIC: &[u8; 8] = b"TRNCPQO1";
pub(crate) const CONTENT_PHYSICAL_RESERVATION_MAGIC: &[u8; 8] = b"TRNCPQR1";
pub(crate) const CONTENT_PHYSICAL_ACCOUNT_MAGIC: &[u8; 8] = b"TRNCPQA1";
const CONTENT_PHYSICAL_QUOTA_KEY: u8 = b'Q';
const CONTENT_PHYSICAL_RESERVATION_KEY: u8 = b'U';
const CONTENT_PHYSICAL_ACCOUNT_KEY: u8 = b'A';

mod codec;
mod identity;
mod lease_hold;
mod reclaim;
mod upload;

pub(crate) use codec::*;
pub use identity::*;
pub use lease_hold::*;
pub use reclaim::*;
pub use upload::*;

#[cfg(test)]
mod tests;
