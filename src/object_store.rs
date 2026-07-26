//! Provider-agnostic object-store client and an in-memory fake.
//!
//! This is the seam the (planned) object-storage backend is written against —
//! see `docs/object-storage-backend.md`. Real providers (S3 and compatible)
//! implement [`ObjectClient`]; the [`InMemoryObjectStore`] here reproduces the
//! semantics that matter — whole-object `put`/`get`, range reads, listing by
//! prefix, idempotent delete, and **conditional writes with `ETags`** — so the
//! backend's harder pieces (segmented WAL, manifest CAS, writer-lease fencing)
//! can be built and tested deterministically with no cloud dependency.
//!
//! ETag/conditional-write semantics mirror object stores: every store assigns a
//! fresh `ETag`, `IfNoneMatch` creates only when absent, and `IfMatch` stores only
//! when the current `ETag` matches. A failed precondition reports the current
//! `ETag` so a compare-and-swap caller (the manifest commit) can retry.
//!
//! This trait is the public "bring your own object store" seam: implement
//! [`ObjectClient`] for your provider (S3 and compatible) and open a database
//! with [`crate::Db::open_object_store`]. The crate's manifest commit and remote
//! WAL head rely on `put_if` providing a real conditional write
//! (compare-and-swap); a backend that cannot honor `If-None-Match` /
//! `If-Match` is unsafe for concurrent writers. After a successful conditional
//! write, later `get`/`head` calls for that same key must observe that version
//! or a newer one. Recovery and read-only refresh follow the lease/head and
//! manifest keys directly; object listing is used for cleanup, so eventually
//! consistent listings may delay garbage collection but must not define
//! committed state.

use std::{future::Future, path::Path, pin::Pin, sync::Arc};

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

mod backend;
mod contract;
mod memory;

pub use contract::{ObjectClient, qualify_object_store_reclamation, verify_object_client_contract};
pub use memory::InMemoryObjectStore;

pub(crate) use backend::{ObjectStoreBackend, ObjectStoreReadObject};
pub(crate) use contract::verify_object_client_contract_for_open;

#[cfg(test)]
mod tests;

pub(crate) fn canonical_object_prefix(value: &str) -> Result<String> {
    if value.as_bytes().contains(&0) {
        return Err(Error::invalid_options(
            "object-store prefix cannot contain a NUL byte",
        ));
    }
    let mut components = Vec::new();
    for component in value.split(['/', '\\']) {
        match component {
            "" | "." => {}
            ".." => {
                return Err(Error::invalid_options(
                    "object-store prefix cannot contain a parent component",
                ));
            }
            component => components.push(component),
        }
    }
    // Object-provider paths are always relative to the configured bucket.
    // Keeping a leading separator here is unstable: object_store::Path strips
    // it on requests and listings then return the stripped key, which makes our
    // canonical-key equality checks disagree with the key we wrote.
    Ok(components.join("/"))
}

pub(crate) fn canonical_object_key(path: &Path) -> Result<String> {
    let value = path.to_str().ok_or_else(|| {
        Error::invalid_options("object-store keys must contain valid UTF-8 components")
    })?;
    canonical_object_prefix(value)
}

/// Boxed future returned by [`ObjectClient`] methods. Mirrors the storage
/// layer's `StorageFuture`: object stores are used through `dyn`, so the async
/// methods return a boxed future rather than `async fn`. The `Send` bound is
/// dropped only on the single-threaded wasm target, matching `StorageFuture`.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub type ObjectFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + 'op>>;

/// See the wasm variant above.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub type ObjectFuture<'op, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'op>>;

/// An opaque entity tag identifying a specific stored version of an object. A
/// new value is minted on every store, so an unchanged `ETag` means the object
/// has not been overwritten since it was observed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ETag(Arc<str>);

impl ETag {
    /// Wraps a provider's entity-tag string (e.g. an S3 `ETag` response header).
    #[must_use]
    pub fn new(tag: impl Into<Arc<str>>) -> Self {
        Self(tag.into())
    }

    /// The underlying entity-tag string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Provider identity for one stored object version.
///
/// This is distinct from an [`ETag`]. Version-enabled S3-compatible stores can
/// retain old bytes after a key-only delete and return a version identifier for
/// the current object. Trine exposes that fact so irreversible reclamation can
/// reject versioned namespaces instead of mistaking a delete marker for byte
/// removal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectVersion(Arc<str>);

impl ObjectVersion {
    /// Wraps the provider's opaque object-version identifier.
    #[must_use]
    pub fn new(version: impl Into<Arc<str>>) -> Self {
        Self(version.into())
    }

    /// Returns the provider's opaque object-version identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

const OBJECT_RECLAMATION_EVIDENCE_SHA256_TAG: u8 = 1;

/// Digest of host-retained evidence for one object-store reclamation contract.
///
/// The source evidence should identify the provider, bucket, exact Trine key
/// prefix, configuration revision, and the control-plane observations proving
/// that the prefix is exclusively owned by Trine, unversioned, and not covered
/// by bucket locks, retention rules, legal holds, or restore-on-delete
/// automation. Do not hash credentials or other secrets into this value.
///
/// Trine persists this digest in every Prepared cloud sweep. Reopening with a
/// different qualification therefore retains bytes rather than continuing an
/// irreversible operation under changed evidence.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObjectStoreReclamationEvidenceDigest([u8; 33]);

impl ObjectStoreReclamationEvidenceDigest {
    /// Hashes a canonical, non-secret evidence document with SHA-256.
    #[must_use]
    pub fn for_bytes(evidence: &[u8]) -> Self {
        let digest = Sha256::digest(evidence);
        let mut bytes = [0_u8; 33];
        bytes[0] = OBJECT_RECLAMATION_EVIDENCE_SHA256_TAG;
        bytes[1..].copy_from_slice(&digest);
        Self(bytes)
    }

    /// Reconstructs a digest from its portable algorithm-tagged bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidFormat`] when the algorithm tag is unknown.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        if bytes[0] != OBJECT_RECLAMATION_EVIDENCE_SHA256_TAG {
            return Err(Error::InvalidFormat {
                message: "unknown object-store reclamation evidence digest".to_owned(),
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the portable algorithm-tagged bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 33] {
        self.0
    }
}

impl std::fmt::Debug for ObjectStoreReclamationEvidenceDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ObjectStoreReclamationEvidenceDigest(..)")
    }
}

/// Host assertion that a specific object namespace is eligible for a live
/// reclamation qualification probe.
///
/// Constructing this value does not enable deletion. It records the digest of
/// independently retained control-plane evidence. Call
/// [`qualify_object_store_reclamation`] with the exact client and database
/// prefix to perform the mandatory data-plane probe and obtain the capability
/// accepted by [`ContentReclamationMode`](crate::ContentReclamationMode).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStoreReclamationAttestation {
    evidence_digest: ObjectStoreReclamationEvidenceDigest,
}

impl ObjectStoreReclamationAttestation {
    /// Binds independently retained provider evidence to a later live probe.
    #[must_use]
    pub const fn new(evidence_digest: ObjectStoreReclamationEvidenceDigest) -> Self {
        Self { evidence_digest }
    }

    /// Returns the digest of the external provider evidence.
    #[must_use]
    pub const fn evidence_digest(self) -> ObjectStoreReclamationEvidenceDigest {
        self.evidence_digest
    }
}

/// Verified capability for unversioned object-store reclamation.
///
/// Values can be obtained only from [`qualify_object_store_reclamation`]. The
/// capability retains and is valid only for the exact [`ObjectClient`] instance
/// that ran the probe, the database prefix, and the external evidence digest.
/// It cannot authorize another wrapper or client even when that client names the
/// same provider namespace. The same capability must be supplied again when
/// reopening a database with a Prepared cloud sweep.
#[derive(Clone)]
pub struct QualifiedObjectStoreReclamation {
    evidence_digest: ObjectStoreReclamationEvidenceDigest,
    namespace_digest: [u8; 32],
    client: Arc<dyn ObjectClient>,
}

impl QualifiedObjectStoreReclamation {
    /// Returns the provider-evidence digest bound to this qualification.
    #[must_use]
    pub const fn evidence_digest(&self) -> ObjectStoreReclamationEvidenceDigest {
        self.evidence_digest
    }

    pub(crate) fn matches_prefix(&self, prefix: &Path) -> bool {
        contract::object_store_reclamation_namespace_digest(prefix)
            .is_ok_and(|digest| self.namespace_digest == digest)
    }

    pub(crate) fn matches_client(&self, client: &Arc<dyn ObjectClient>) -> bool {
        Arc::ptr_eq(&self.client, client)
    }
}

impl std::fmt::Debug for QualifiedObjectStoreReclamation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QualifiedObjectStoreReclamation")
            .field("evidence_digest", &self.evidence_digest)
            .field("namespace_digest", &self.namespace_digest)
            .finish_non_exhaustive()
    }
}

impl PartialEq for QualifiedObjectStoreReclamation {
    fn eq(&self, other: &Self) -> bool {
        self.evidence_digest == other.evidence_digest
            && self.namespace_digest == other.namespace_digest
            && Arc::ptr_eq(&self.client, &other.client)
    }
}

impl Eq for QualifiedObjectStoreReclamation {}

/// Precondition for a conditional write ([`ObjectClient::put_if`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// Store only if the object does not exist (create). `If-None-Match: *`.
    IfNoneMatch,
    /// Store only if the object exists and its current `ETag` equals this one
    /// (compare-and-swap). `If-Match: <etag>`.
    IfMatch(ETag),
}

/// Outcome of a conditional write. A failed precondition is **not** an error:
/// it is the expected, retryable result of losing a compare-and-swap race, and
/// carries the current `ETag` (or `None` when the object is absent) so the caller
/// can re-read and retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PutIf {
    /// The write was applied; the object now has this `ETag`.
    Stored {
        /// The new entity tag of the stored object.
        etag: ETag,
    },
    /// The precondition did not hold; the object was left unchanged.
    PreconditionFailed {
        /// The object's current entity tag, or `None` if it does not exist.
        current: Option<ETag>,
    },
}

/// Metadata for one object returned by [`ObjectClient::list`] / [`ObjectClient::head`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Object key.
    pub key: String,
    /// Object size in bytes.
    pub size: u64,
    /// Current entity tag.
    pub etag: ETag,
    /// Opaque provider version, or `None` for an unversioned object.
    ///
    /// A cloud reclamation probe rejects any observed version, including a
    /// provider's `null` version marker, because key-only deletion cannot prove
    /// that historical bytes are gone.
    pub version: Option<ObjectVersion>,
}

/// One bounded page of a prefix listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectListPage {
    /// Objects in key order.
    pub objects: Vec<ObjectMeta>,
    /// Exclusive key offset for the next page, or `None` at end of listing.
    pub next_after: Option<String>,
}
