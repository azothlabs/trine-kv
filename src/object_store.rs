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

use std::{
    collections::BTreeMap,
    future::Future,
    ops::Bound,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::options::DurabilityMode;
use crate::storage::{
    StorageCapabilities, StorageFuture, StorageObjectDeleteBackend, StorageObjectId,
    StorageObjectKind, StorageObjectListBackend, StorageObjectListPage as StorageListPage,
    StorageObjectListRequest, StorageObjectReadBackend, StorageObjectWriteBackend,
    StorageReadBackend, StorageReadFuture, StorageReadObject, ensure_whole_object_read_len,
};

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
        object_store_reclamation_namespace_digest(prefix)
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

fn validate_object_list_page(
    prefix: &str,
    after: Option<&str>,
    limit: usize,
    page: &ObjectListPage,
) -> Result<()> {
    if limit == 0 {
        return Err(Error::invalid_options(
            "object listing page limit must be non-zero",
        ));
    }
    if page.objects.len() > limit {
        return Err(Error::Corruption {
            message: format!(
                "object listing returned {} entries for page limit {limit}",
                page.objects.len()
            ),
        });
    }
    let mut previous = after;
    for meta in &page.objects {
        if !meta.key.starts_with(prefix) {
            return Err(Error::Corruption {
                message: format!(
                    "object listing key {:?} does not start with prefix {prefix:?}",
                    meta.key
                ),
            });
        }
        if previous.is_some_and(|previous| meta.key.as_str() <= previous) {
            return Err(Error::Corruption {
                message: format!(
                    "object listing key {:?} does not advance exclusive cursor {:?}",
                    meta.key, previous
                ),
            });
        }
        previous = Some(&meta.key);
    }
    if let Some(next_after) = &page.next_after {
        let Some(last) = page.objects.last() else {
            return Err(Error::Corruption {
                message: "object listing returned an empty page with a continuation cursor"
                    .to_owned(),
            });
        };
        if next_after != &last.key {
            return Err(Error::Corruption {
                message: format!(
                    "object listing continuation {:?} does not equal last key {:?}",
                    next_after, last.key
                ),
            });
        }
    }
    Ok(())
}

/// A flat key/value object store: keys are strings, values are immutable byte
/// blobs with an `ETag`. All methods are async (real providers are network I/O);
/// the in-memory fake completes synchronously.
///
/// Contract the backend relies on:
/// - `put` always stores and returns a fresh `ETag` (overwrites bump the `ETag`).
/// - `get` returns `None` for an absent key; `get_range` errors for an absent
///   key, an out-of-bounds range, or an `ETag` mismatch.
/// - After `put`, or after a `put_if` returns [`PutIf::Stored`], later `get` and
///   `head` calls for the same key observe that object version or a newer one.
/// - `delete` is idempotent (deleting an absent key succeeds).
/// - `list` returns objects whose key starts with the prefix, in key order.
///   Listing drives orphan cleanup only; recovery and read-only refresh do not
///   infer committed state from a listing result.
/// - `head` returns an object's metadata (size + `ETag`) without its bytes, or
///   `None` when the key is absent (like S3 `HEAD`).
/// - `put_if` applies the write only when the precondition holds, otherwise
///   reports [`PutIf::PreconditionFailed`] with the current `ETag`.
///
/// Object-store opens trust this contract by default. That keeps open cheap and
/// predictable, but it also means a faulty adapter can pass open and fail only
/// later when WAL, lease, manifest, or recovery checks observe the bad behavior.
/// Run [`verify_object_client_contract`] in CI, process startup, or a deployment
/// health check before trusting a custom adapter in production. If you need open
/// itself to fail closed while developing or diagnosing an adapter, configure
/// [`ObjectClientTrustMode::VerifyOnOpen`](crate::ObjectClientTrustMode).
///
/// # WAL durability sink and split tiers
///
/// A database opened with [`Db::open_object_store_at`](crate::Db::open_object_store_at)
/// writes everything through one client — `SSTable` segments, blobs, the
/// manifest CAS, the writer lease, and the **write-ahead log** — and a commit is
/// acknowledged only after its WAL bytes and WAL head are durable.
///
/// A database opened with
/// [`Db::open_object_store_with_wal_at`](crate::Db::open_object_store_with_wal_at)
/// splits that responsibility: the storage client stores bulk objects and the
/// manifest, while the WAL client stores the writer lease, remote WAL head, and
/// WAL segments. The WAL client is then the commit-latency and commit-durability
/// sink; the storage client remains the long-term table/blob tier.
///
/// Because `Arc<C>` is itself an `ObjectClient`, one client can be **shared
/// across many open databases**. A higher layer (e.g. a multi-tenant service)
/// can either provide an explicit WAL client through the split-tier open API or
/// supply a custom shared client that recognizes WAL writes with
/// [`is_wal_object_key`](crate::is_wal_object_key) and coalesces them across
/// databases. In both forms, the client handling WAL keys must provide the
/// conditional-write and same-key visibility guarantees described above.
pub trait ObjectClient: Send + Sync {
    /// Reads the whole object, or `None` when the key is absent.
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>>;

    /// Reads `len` bytes starting at `offset` only when the stored object still
    /// has `expected_etag`. This prevents separate range requests from silently
    /// splicing bytes from different object versions.
    fn get_range<'op>(
        &'op self,
        key: &str,
        offset: u64,
        len: u64,
        expected_etag: &ETag,
    ) -> ObjectFuture<'op, Arc<[u8]>>;

    /// Stores the object unconditionally, returning its new `ETag`.
    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag>;

    /// Deletes the object (idempotent: deleting an absent key succeeds).
    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()>;

    /// Lists objects whose key starts with `prefix`, in key order.
    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>>;

    /// Lists at most `limit` objects after the exclusive `after` key.
    ///
    /// Implementations must retain only bounded page state even when an
    /// underlying provider exposes listing as a stream rather than a page API.
    /// Pages contain no more than `limit` objects, keys are strictly increasing
    /// and start with `prefix`, and `next_after` is either `None` or the last
    /// returned key.
    fn list_page<'op>(
        &'op self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> ObjectFuture<'op, ObjectListPage>;

    /// Returns the object's metadata (size + `ETag`) without its bytes, or `None`
    /// when the key is absent (like S3 `HEAD`).
    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>>;

    /// Conditional write (compare-and-swap): stores `bytes` only if `precondition`
    /// holds, otherwise reports [`PutIf::PreconditionFailed`] with the current
    /// `ETag`. This is the manifest commit point; it must be a real CAS.
    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf>;
}

static OBJECT_CLIENT_CONTRACT_PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Verifies the same-key semantics Trine requires from an [`ObjectClient`].
///
/// This is a health-check helper for object-store adapters. It writes, reads,
/// conditionally overwrites, and deletes one temporary probe object under
/// `prefix`, then returns [`Ok(())`](std::result::Result::Ok) only if the client
/// behaves like a real compare-and-swap object store for that key.
///
/// # What This Checks
///
/// The probe verifies:
///
/// - `put` stores bytes and returns an `ETag` visible through `head` and `get`;
/// - `put_if(..., IfNoneMatch)` refuses to overwrite an existing object;
/// - `put_if(..., IfMatch(wrong_etag))` refuses to overwrite;
/// - `put_if(..., IfMatch(current_etag))` stores and returns a new `ETag`;
/// - same-key `head` and `get` observe the bytes from the successful write.
///
/// # Limits
///
/// This is not a proof that every future request will be correct. It validates
/// one temporary key at one moment. Use it before deployment, during process
/// startup, or as a service health check; Trine's open path defaults to trusting
/// the client so normal opens do not pay these extra object-store requests.
///
/// # Parameters
///
/// - `client`: object-store adapter to validate.
/// - `prefix`: object-key prefix where the temporary probe object may be
///   created. The function appends a unique hidden key and deletes it before
///   returning. The caller must grant read, write, conditional-write, metadata,
///   and delete permissions for this prefix.
///
/// # Errors
///
/// Returns any error from the client. Returns [`Error::Corruption`] when the
/// observed behavior violates Trine's required object-store semantics. If the
/// final cleanup delete fails after a successful probe, that cleanup error is
/// returned so the caller can treat the health check as failed.
pub async fn verify_object_client_contract(
    client: Arc<dyn ObjectClient>,
    prefix: impl Into<String>,
) -> Result<()> {
    let key = object_client_contract_probe_key(Path::new(&prefix.into()))?;
    let result = verify_object_client_contract_at_key(&client, &key).await;
    let cleanup = client.delete(&key).await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
    }
}

/// Qualifies one exact object-store namespace for irreversible content-byte
/// reclamation.
///
/// The caller must first retain control-plane evidence identified by
/// `attestation`: the exact `prefix` is exclusively owned by this Trine
/// database, provider versioning and delete markers are disabled, no bucket
/// lock, retention rule, legal hold, backup restore, replication target, or
/// lifecycle process can preserve or recreate deleted keys, and configuration
/// changes invalidate the evidence before another sweep is resumed.
///
/// The live probe creates temporary objects in both content-deletion path
/// families (`content-v1/chunks` and `content-v1/domains`), overwrites each with
/// a compare-and-swap, confirms that no observation reports a provider version,
/// deletes each key, and requires immediate absence through `HEAD`, `GET`, and
/// `LIST`. It repeats each delete to verify idempotency. This costs four writes,
/// four deletes, and several metadata/read requests. Every successful path is
/// absent before the function returns; a failing path gets a best-effort cleanup.
///
/// # Safety boundary
///
/// Data-plane requests cannot enumerate every provider control-plane policy.
/// The live probe supplements but does not replace the external evidence. In
/// particular, a false assertion about hidden historical versions can leave the
/// tiny probe object as an unreachable provider version. Never call this
/// function before checking the provider configuration.
///
/// # Parameters
///
/// - `client`: the same object client later passed to the database open call.
/// - `prefix`: the exact database key prefix, not merely the bucket root.
/// - `attestation`: digest of stable, independently retained provider evidence.
///
/// # Errors
///
/// Returns the provider error when the probe cannot read, write, list, or
/// delete. Returns [`Error::Corruption`] when versions are visible, deletion is
/// not immediately observable, listing disagrees with same-key reads, or the
/// adapter violates compare-and-swap behavior. No qualification is returned on
/// uncertainty.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use trine_kv::{
///     ContentReclamationMode, DbOptions, InMemoryObjectStore, ObjectClient,
///     ObjectStoreReclamationAttestation, ObjectStoreReclamationEvidenceDigest,
///     qualify_object_store_reclamation,
/// };
///
/// # fn main() -> trine_kv::Result<()> {
/// let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
/// let evidence = ObjectStoreReclamationEvidenceDigest::for_bytes(
///     b"provider=test;bucket=example;prefix=database-a;versioning=disabled",
/// );
/// let qualification = futures::executor::block_on(qualify_object_store_reclamation(
///     client,
///     "database-a",
///     ObjectStoreReclamationAttestation::new(evidence),
/// ))?;
/// let options = DbOptions::object_store().with_content_reclamation(
///     ContentReclamationMode::QualifiedObjectStore(qualification),
/// );
/// // Pass `options` and the same client/prefix contract to the database open call.
/// # let _ = options;
/// # Ok(())
/// # }
/// ```
pub async fn qualify_object_store_reclamation(
    client: Arc<dyn ObjectClient>,
    prefix: impl Into<String>,
    attestation: ObjectStoreReclamationAttestation,
) -> Result<QualifiedObjectStoreReclamation> {
    let prefix = canonical_object_prefix(&prefix.into())?;
    for (path, role) in [
        ("content-v1/chunks", "reclamation-chunk"),
        ("content-v1/domains", "reclamation-descriptor"),
    ] {
        let root = Path::new(&prefix).join(path);
        let key = object_client_contract_probe_key_for_role(&root, role)?;
        if let Err(error) = verify_object_store_reclamation_at_key(&client, &key).await {
            let _ = client.delete(&key).await;
            return Err(error);
        }
    }
    Ok(QualifiedObjectStoreReclamation {
        evidence_digest: attestation.evidence_digest,
        namespace_digest: object_store_reclamation_namespace_digest(Path::new(&prefix))?,
        client,
    })
}

fn object_store_reclamation_namespace_digest(prefix: &Path) -> Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    hasher.update(b"trine-object-store-reclamation-namespace-v1");
    hasher.update([0]);
    hasher.update(canonical_object_key(prefix)?.as_bytes());
    Ok(hasher.finalize().into())
}

async fn verify_object_store_reclamation_at_key(
    client: &Arc<dyn ObjectClient>,
    key: &str,
) -> Result<()> {
    let first = Arc::<[u8]>::from(b"trine-object-reclamation:first".as_slice());
    let second = Arc::<[u8]>::from(b"trine-object-reclamation:second".as_slice());
    let first_etag = match client
        .put_if(key, Arc::clone(&first), Precondition::IfNoneMatch)
        .await?
    {
        PutIf::Stored { etag } => etag,
        PutIf::PreconditionFailed { .. } => {
            return Err(Error::Corruption {
                message: format!("object reclamation probe key {key} unexpectedly exists"),
            });
        }
    };
    verify_unversioned_object(client, key, &first, &first_etag, "create").await?;

    let second_etag = match client
        .put_if(
            key,
            Arc::clone(&second),
            Precondition::IfMatch(first_etag.clone()),
        )
        .await?
    {
        PutIf::Stored { etag } => etag,
        PutIf::PreconditionFailed { current } => {
            return Err(Error::Corruption {
                message: format!(
                    "object reclamation probe for {key} lost its overwrite fence: {current:?}"
                ),
            });
        }
    };
    if second_etag == first_etag {
        return Err(Error::Corruption {
            message: format!("object reclamation probe for {key} reused an ETag after overwrite"),
        });
    }
    verify_unversioned_object(client, key, &second, &second_etag, "overwrite").await?;

    client.delete(key).await?;
    verify_object_store_reclamation_absent(client, key).await?;
    client.delete(key).await?;
    verify_object_store_reclamation_absent(client, key).await
}

async fn verify_unversioned_object(
    client: &Arc<dyn ObjectClient>,
    key: &str,
    expected: &Arc<[u8]>,
    expected_etag: &ETag,
    operation: &str,
) -> Result<()> {
    let head = client.head(key).await?.ok_or_else(|| Error::Corruption {
        message: format!("object reclamation probe for {key} lost head after {operation}"),
    })?;
    if let Some(version) = &head.version {
        return Err(Error::Corruption {
            message: format!(
                "object reclamation probe for {key} observed provider version {} after {operation}",
                version.as_str()
            ),
        });
    }
    if &head.etag != expected_etag || head.size != expected.len() as u64 {
        return Err(Error::Corruption {
            message: format!(
                "object reclamation probe for {key} observed stale metadata after {operation}"
            ),
        });
    }
    let bytes = client.get(key).await?.ok_or_else(|| Error::Corruption {
        message: format!("object reclamation probe for {key} lost bytes after {operation}"),
    })?;
    if bytes.as_ref() != expected.as_ref() {
        return Err(Error::Corruption {
            message: format!(
                "object reclamation probe for {key} observed stale bytes after {operation}"
            ),
        });
    }
    let ranged = client
        .get_range(key, 0, expected.len() as u64, expected_etag)
        .await?;
    if ranged.as_ref() != expected.as_ref() {
        return Err(Error::Corruption {
            message: format!(
                "object client contract probe for {key} observed stale range bytes after {operation}"
            ),
        });
    }
    let list_prefix = key.rsplit_once('/').map_or("", |(parent, _)| parent);
    let listed = client.list(list_prefix).await?;
    let mut exact = listed.iter().filter(|meta| meta.key == key);
    if exact.next().is_none_or(|meta| meta.version.is_some()) || exact.next().is_some() {
        return Err(Error::Corruption {
            message: format!(
                "object reclamation probe for {key} observed inconsistent listing after {operation}: {listed:?}"
            ),
        });
    }
    let page = client.list_page(key, None, 1).await?;
    validate_object_list_page(key, None, 1, &page)?;
    if page.objects.len() != 1 || page.objects[0].key != key {
        return Err(Error::Corruption {
            message: format!(
                "object client contract probe for {key} observed inconsistent bounded listing after {operation}: {page:?}"
            ),
        });
    }
    Ok(())
}

async fn verify_object_store_reclamation_absent(
    client: &Arc<dyn ObjectClient>,
    key: &str,
) -> Result<()> {
    let list_prefix = key.rsplit_once('/').map_or("", |(parent, _)| parent);
    if client.head(key).await?.is_some()
        || client.get(key).await?.is_some()
        || client
            .list(list_prefix)
            .await?
            .iter()
            .any(|meta| meta.key == key)
    {
        return Err(Error::Corruption {
            message: format!("object reclamation probe for {key} remained observable after delete"),
        });
    }
    Ok(())
}

pub(crate) async fn verify_object_client_contract_for_open(
    client: &Arc<dyn ObjectClient>,
    db_path: &Path,
    role: &str,
) -> Result<()> {
    let key = object_client_contract_probe_key_for_role(db_path, role)?;
    let result = verify_object_client_contract_at_key(client, &key).await;
    let cleanup = client.delete(&key).await;
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
    }
}

async fn verify_object_client_contract_at_key(
    client: &Arc<dyn ObjectClient>,
    key: &str,
) -> Result<()> {
    client.delete(key).await?;

    let first = Arc::<[u8]>::from(b"trine-object-client-contract:first".as_slice());
    let second = Arc::<[u8]>::from(b"trine-object-client-contract:second".as_slice());
    let first_etag = client.put(key, Arc::clone(&first)).await?;
    verify_object_client_observed_bytes(client, key, &first, &first_etag, "put").await?;

    match client
        .put_if(key, Arc::clone(&second), Precondition::IfNoneMatch)
        .await?
    {
        PutIf::PreconditionFailed { current } if current.as_ref() == Some(&first_etag) => {}
        PutIf::PreconditionFailed { current } => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} returned wrong IfNoneMatch ETag: {current:?}"
                ),
            });
        }
        PutIf::Stored { .. } => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} stored despite IfNoneMatch on an existing object"
                ),
            });
        }
    }

    let mismatched = ETag::new("trine-object-client-contract-mismatch");
    match client
        .put_if(key, Arc::clone(&second), Precondition::IfMatch(mismatched))
        .await?
    {
        PutIf::PreconditionFailed { .. } => {}
        PutIf::Stored { .. } => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} stored despite a mismatched IfMatch ETag"
                ),
            });
        }
    }

    let second_etag = match client
        .put_if(
            key,
            Arc::clone(&second),
            Precondition::IfMatch(first_etag.clone()),
        )
        .await?
    {
        PutIf::Stored { etag } => etag,
        PutIf::PreconditionFailed { current } => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} rejected a matching IfMatch ETag: {current:?}"
                ),
            });
        }
    };
    if second_etag == first_etag {
        return Err(Error::Corruption {
            message: format!(
                "object client contract probe for {key} reused an ETag after overwriting bytes"
            ),
        });
    }
    if client
        .get_range(key, 0, second.len() as u64, &first_etag)
        .await
        .is_ok()
    {
        return Err(Error::Corruption {
            message: format!(
                "object client contract probe for {key} accepted a stale ETag for a range read"
            ),
        });
    }
    verify_object_client_observed_bytes(client, key, &second, &second_etag, "put_if").await
}

async fn verify_object_client_observed_bytes(
    client: &Arc<dyn ObjectClient>,
    key: &str,
    expected: &Arc<[u8]>,
    expected_etag: &ETag,
    operation: &str,
) -> Result<()> {
    let head = client.head(key).await?.ok_or_else(|| Error::Corruption {
        message: format!("object client contract probe for {key} lost head after {operation}"),
    })?;
    if &head.etag != expected_etag || head.size != expected.len() as u64 {
        return Err(Error::Corruption {
            message: format!(
                "object client contract probe for {key} observed stale head after {operation}"
            ),
        });
    }
    let bytes = client.get(key).await?.ok_or_else(|| Error::Corruption {
        message: format!("object client contract probe for {key} lost bytes after {operation}"),
    })?;
    if bytes.as_ref() != expected.as_ref() {
        return Err(Error::Corruption {
            message: format!(
                "object client contract probe for {key} observed stale bytes after {operation}"
            ),
        });
    }
    Ok(())
}

fn object_client_contract_probe_key(prefix: &Path) -> Result<String> {
    object_client_contract_probe_key_for_role(prefix, "health")
}

fn object_client_contract_probe_key_for_role(db_path: &Path, role: &str) -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::Corruption {
            message: format!("system clock is before UNIX_EPOCH: {error}"),
        })?;
    let counter = OBJECT_CLIENT_CONTRACT_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed);
    canonical_object_key(&db_path.join(format!(
        ".trine-object-client-contract-{role}-{}-{counter}",
        now.as_nanos()
    )))
}

/// One stored object: its bytes and current `ETag`.
#[derive(Debug, Clone)]
struct StoredObject {
    bytes: Arc<[u8]>,
    etag: ETag,
}

/// An in-memory [`ObjectClient`] with real `ETag` and conditional-write
/// semantics, for building and testing the object-storage backend without a
/// cloud dependency.
#[derive(Debug, Default)]
pub struct InMemoryObjectStore {
    objects: Mutex<BTreeMap<String, StoredObject>>,
    next_etag: AtomicU64,
}

impl InMemoryObjectStore {
    /// Creates an empty in-memory object store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn mint_etag(&self) -> ETag {
        let value = self.next_etag.fetch_add(1, Ordering::Relaxed);
        ETag(Arc::from(format!("etag-{value}")))
    }

    fn lock(&self) -> Result<MutexGuard<'_, BTreeMap<String, StoredObject>>> {
        self.objects.lock().map_err(|_| Error::Corruption {
            message: "in-memory object store lock poisoned".to_owned(),
        })
    }

    fn get_inner(&self, key: &str) -> Result<Option<Arc<[u8]>>> {
        Ok(self
            .lock()?
            .get(key)
            .map(|object| Arc::clone(&object.bytes)))
    }

    fn get_range_inner(
        &self,
        key: &str,
        offset: u64,
        len: u64,
        expected_etag: &ETag,
    ) -> Result<Arc<[u8]>> {
        let objects = self.lock()?;
        let object = objects.get(key).ok_or_else(|| Error::Corruption {
            message: format!("object {key} not found for range read"),
        })?;
        if &object.etag != expected_etag {
            return Err(Error::Corruption {
                message: format!("object {key} changed during immutable range reads"),
            });
        }
        let offset = usize::try_from(offset)
            .map_err(|_| Error::invalid_options("object range offset overflow"))?;
        let len = usize::try_from(len)
            .map_err(|_| Error::invalid_options("object range length overflow"))?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| Error::invalid_options("object range end overflow"))?;
        let slice = object
            .bytes
            .get(offset..end)
            .ok_or_else(|| Error::Corruption {
                message: format!("object {key} short read for range {offset}..{end}"),
            })?;
        Ok(Arc::from(slice))
    }

    fn put_inner(&self, key: &str, bytes: Arc<[u8]>) -> Result<ETag> {
        let etag = self.mint_etag();
        self.lock()?.insert(
            key.to_owned(),
            StoredObject {
                bytes,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    fn delete_inner(&self, key: &str) -> Result<()> {
        self.lock()?.remove(key);
        Ok(())
    }

    fn list_inner(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let objects = self.lock()?;
        Ok(objects
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, object)| ObjectMeta {
                key: key.clone(),
                size: object.bytes.len() as u64,
                etag: object.etag.clone(),
                version: None,
            })
            .collect())
    }

    fn head_inner(&self, key: &str) -> Result<Option<ObjectMeta>> {
        Ok(self.lock()?.get(key).map(|object| ObjectMeta {
            key: key.to_owned(),
            size: object.bytes.len() as u64,
            etag: object.etag.clone(),
            version: None,
        }))
    }

    fn put_if_inner(
        &self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: &Precondition,
    ) -> Result<PutIf> {
        let mut objects = self.lock()?;
        let current = objects.get(key).map(|object| object.etag.clone());
        let allowed = match (precondition, &current) {
            (Precondition::IfNoneMatch, None) => true,
            (Precondition::IfMatch(expected), Some(actual)) => expected == actual,
            (Precondition::IfNoneMatch, Some(_)) | (Precondition::IfMatch(_), None) => false,
        };
        if !allowed {
            return Ok(PutIf::PreconditionFailed { current });
        }
        let etag = self.mint_etag();
        objects.insert(
            key.to_owned(),
            StoredObject {
                bytes,
                etag: etag.clone(),
            },
        );
        Ok(PutIf::Stored { etag })
    }
}

impl ObjectClient for InMemoryObjectStore {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        let key = key.to_owned();
        Box::pin(async move { self.get_inner(&key) })
    }

    fn get_range<'op>(
        &'op self,
        key: &str,
        offset: u64,
        len: u64,
        expected_etag: &ETag,
    ) -> ObjectFuture<'op, Arc<[u8]>> {
        let key = key.to_owned();
        let expected_etag = expected_etag.clone();
        Box::pin(async move { self.get_range_inner(&key, offset, len, &expected_etag) })
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        let key = key.to_owned();
        Box::pin(async move { self.put_inner(&key, bytes) })
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        let key = key.to_owned();
        Box::pin(async move { self.delete_inner(&key) })
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        let prefix = prefix.to_owned();
        Box::pin(async move { self.list_inner(&prefix) })
    }

    fn list_page<'op>(
        &'op self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> ObjectFuture<'op, ObjectListPage> {
        let prefix = prefix.to_owned();
        let after = after.map(str::to_owned);
        Box::pin(async move {
            if limit == 0 {
                return Err(Error::invalid_options(
                    "object listing page limit must be non-zero",
                ));
            }
            let take = limit
                .checked_add(1)
                .ok_or_else(|| Error::invalid_options("object listing page limit overflow"))?;
            let objects = self.lock()?;
            let start = after.map_or_else(|| Bound::Included(prefix.clone()), Bound::Excluded);
            let mut page = objects
                .range((start, Bound::Unbounded))
                .take_while(|(key, _)| key.starts_with(&prefix))
                .take(take)
                .map(|(key, object)| ObjectMeta {
                    key: key.clone(),
                    size: object.bytes.len() as u64,
                    etag: object.etag.clone(),
                    version: None,
                })
                .collect::<Vec<_>>();
            let has_more = page.len() > limit;
            if has_more {
                page.pop();
            }
            let next_after =
                has_more.then(|| page.last().expect("non-empty bounded page").key.clone());
            Ok(ObjectListPage {
                objects: page,
                next_after,
            })
        })
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        let key = key.to_owned();
        Box::pin(async move { self.head_inner(&key) })
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        let key = key.to_owned();
        Box::pin(async move { self.put_if_inner(&key, bytes, &precondition) })
    }
}

/// A shared [`ObjectClient`] is itself an `ObjectClient`, so several components
/// (e.g. the manifest store and the byte backend) can share one client by
/// holding `Arc<C>` clones.
impl<C: ObjectClient + ?Sized> ObjectClient for Arc<C> {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        (**self).get(key)
    }

    fn get_range<'op>(
        &'op self,
        key: &str,
        offset: u64,
        len: u64,
        expected_etag: &ETag,
    ) -> ObjectFuture<'op, Arc<[u8]>> {
        (**self).get_range(key, offset, len, expected_etag)
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        (**self).put(key, bytes)
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        (**self).delete(key)
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        (**self).list(prefix)
    }

    fn list_page<'op>(
        &'op self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> ObjectFuture<'op, ObjectListPage> {
        (**self).list_page(prefix, after, limit)
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        (**self).head(key)
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        (**self).put_if(key, bytes, precondition)
    }
}

/// An object-storage **byte** backend: `SSTable` and blob object IO over an
/// [`ObjectClient`].
///
/// It implements the async `Storage*Backend` byte traits the already-generic
/// table/blob async helpers are written against, so flush, compaction, and reads
/// work over object storage. The WAL, the manifest CAS, and the writer lease are
/// deliberately **not** here — those are the object-storage durability
/// substrate's job (manifest CAS lives in [`crate::manifest::ObjectManifestStore`]).
///
/// A [`StorageObjectId`]'s path is used directly as the object key, so keys are
/// consistent across read / write / list / delete (the open path joins file
/// names under the database's key prefix, mirroring the filesystem layout).
#[derive(Clone)]
pub(crate) struct ObjectStoreBackend {
    client: Arc<dyn ObjectClient>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreBackend")
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    pub(crate) fn new(client: Arc<dyn ObjectClient>) -> Self {
        Self { client }
    }

    pub(crate) fn client(&self) -> Arc<dyn ObjectClient> {
        Arc::clone(&self.client)
    }

    fn object_key(object: &StorageObjectId) -> Result<String> {
        canonical_object_key(object.path())
    }

    pub(crate) async fn delete_unversioned_object_verified(
        &self,
        object: StorageObjectId,
    ) -> Result<()> {
        let key = Self::object_key(&object)?;
        match self.client.head(&key).await? {
            Some(meta) if meta.version.is_some() => {
                return Err(Error::unsupported_backend(
                    "object-store content key has a provider version",
                ));
            }
            Some(_) => {
                self.client.delete(&key).await?;
            }
            None => {}
        }
        verify_object_store_reclamation_absent(&self.client, &key).await
    }

    pub(crate) async fn read_object_versioned(
        &self,
        object: &StorageObjectId,
    ) -> Result<Option<(Arc<[u8]>, ETag)>> {
        let key = Self::object_key(object)?;
        let Some(meta) = self.client.head(&key).await? else {
            return Ok(None);
        };
        let bytes = read_object_bytes_by_meta(self.client.as_ref(), &key, object, &meta).await?;
        Ok(Some((bytes, meta.etag)))
    }

    pub(crate) async fn put_object_if(
        &self,
        object: &StorageObjectId,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> Result<PutIf> {
        let key = Self::object_key(object)?;
        self.client.put_if(&key, bytes, precondition).await
    }
}

/// A bounded random-access object handle.
///
/// Opening performs only `HEAD`; table/blob sections are fetched with range
/// requests on demand. This keeps database open memory proportional to metadata
/// rather than to the sum of every referenced immutable object.
#[derive(Clone)]
pub(crate) struct ObjectStoreReadObject {
    object: StorageObjectId,
    client: Arc<dyn ObjectClient>,
    key: String,
    len: u64,
    etag: ETag,
}

impl std::fmt::Debug for ObjectStoreReadObject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreReadObject")
            .field("object", &self.object)
            .field("key", &self.key)
            .field("len", &self.len)
            .field("etag", &self.etag)
            .finish_non_exhaustive()
    }
}

impl StorageReadObject for ObjectStoreReadObject {
    fn object(&self) -> &StorageObjectId {
        &self.object
    }

    fn len(&self) -> StorageReadFuture<'_, u64> {
        let len = self.len;
        Box::pin(async move { Ok(len) })
    }

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()> {
        Box::pin(async move {
            let offset = u64::try_from(offset)
                .map_err(|_| Error::invalid_options("object read offset overflow"))?;
            let len = u64::try_from(bytes.len())
                .map_err(|_| Error::invalid_options("object read length overflow"))?;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| Error::invalid_options("object read range overflow"))?;
            if end > self.len {
                return Err(Error::Corruption {
                    message: format!("object {} short read", self.object.path().display()),
                });
            }
            if bytes.is_empty() {
                return Ok(());
            }
            let read = self
                .client
                .get_range(&self.key, offset, len, &self.etag)
                .await?;
            if read.len() != bytes.len() {
                return Err(Error::Corruption {
                    message: format!(
                        "object {} range read returned {} bytes for requested length {}",
                        self.object.path().display(),
                        read.len(),
                        bytes.len()
                    ),
                });
            }
            bytes.copy_from_slice(&read);
            Ok(())
        })
    }
}

impl StorageReadBackend for ObjectStoreBackend {
    type ReadObject = ObjectStoreReadObject;

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::object_store()
    }

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject> {
        Box::pin(async move {
            let key = Self::object_key(&object)?;
            let meta = self
                .client
                .head(&key)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!("referenced object {key} cannot be opened"),
                })?;
            ensure_object_meta_read_len(&object, &meta)?;
            Ok(ObjectStoreReadObject {
                object,
                client: Arc::clone(&self.client),
                key,
                len: meta.size,
                etag: meta.etag,
            })
        })
    }
}

impl StorageObjectReadBackend for ObjectStoreBackend {
    fn read_object_bytes(&self, object: StorageObjectId) -> StorageFuture<'_, Option<Arc<[u8]>>> {
        Box::pin(async move {
            let key = Self::object_key(&object)?;
            let Some(meta) = self.client.head(&key).await? else {
                return Ok(None);
            };
            read_object_bytes_by_meta(self.client.as_ref(), &key, &object, &meta)
                .await
                .map(Some)
        })
    }
}

impl StorageObjectWriteBackend for ObjectStoreBackend {
    fn write_object(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        _durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        // A PUT is durable once the store acknowledges it, so durability hints do
        // not apply (there is no separate flush/fsync step).
        Box::pin(async move {
            let key = Self::object_key(&object)?;
            if matches!(
                object.kind(),
                StorageObjectKind::Table
                    | StorageObjectKind::Blob
                    | StorageObjectKind::ContentAccessBarrier
                    | StorageObjectKind::ContentChunk
                    | StorageObjectKind::ContentDescriptor
            ) {
                let intended = Arc::clone(&bytes);
                return match self
                    .client
                    .put_if(&key, bytes, Precondition::IfNoneMatch)
                    .await
                {
                    Ok(PutIf::Stored { .. }) => Ok(()),
                    Ok(PutIf::PreconditionFailed { .. }) => match self.client.get(&key).await? {
                        Some(current) if current == intended => Ok(()),
                        _ => Err(Error::Corruption {
                            message: format!(
                                "immutable object {key} already exists with different bytes"
                            ),
                        }),
                    },
                    Err(error) => match self.client.get(&key).await {
                        Ok(Some(current)) if current == intended => Ok(()),
                        Ok(Some(_)) => Err(Error::Corruption {
                            message: format!(
                                "immutable object {key} appeared with different bytes after an uncertain create"
                            ),
                        }),
                        Ok(None) | Err(_) => Err(error),
                    },
                };
            }
            self.client.put(&key, bytes).await.map(|_| ())
        })
    }
}

impl StorageObjectDeleteBackend for ObjectStoreBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()> {
        Box::pin(async move {
            if matches!(
                object.kind(),
                StorageObjectKind::Table | StorageObjectKind::Blob
            ) {
                return Err(Error::unsupported_backend(
                    "immutable object-store table/blob deletion requires a durable reader-retirement protocol",
                ));
            }
            let key = Self::object_key(&object)?;
            self.client.delete(&key).await
        })
    }
}

impl StorageObjectListBackend for ObjectStoreBackend {
    fn list_objects(
        &self,
        request: StorageObjectListRequest,
    ) -> StorageFuture<'_, Vec<StorageObjectId>> {
        Box::pin(async move {
            let mut objects = Vec::new();
            let mut after = None;
            loop {
                let page = self
                    .list_objects_page(request.clone(), after.as_deref(), 1_024)
                    .await?;
                objects.extend(page.objects);
                let Some(next_after) = page.next_after else {
                    break;
                };
                after = Some(next_after);
            }
            objects.sort_unstable();
            Ok(objects)
        })
    }

    fn list_objects_page(
        &self,
        request: StorageObjectListRequest,
        after: Option<&str>,
        limit: usize,
    ) -> StorageFuture<'_, StorageListPage> {
        let after = after.map(str::to_owned);
        Box::pin(async move {
            let kind = request.kind();
            let extension = request.file_extension();
            let prefix = canonical_object_key(request.root())?;
            let root = PathBuf::from(&prefix);
            let listing_prefix = if prefix.is_empty() {
                prefix.clone()
            } else {
                format!("{prefix}/")
            };
            let page = self
                .client
                .list_page(&listing_prefix, after.as_deref(), limit)
                .await?;
            validate_object_list_page(&listing_prefix, after.as_deref(), limit, &page)?;
            let mut objects = Vec::new();
            for meta in page.objects {
                let canonical = canonical_object_prefix(&meta.key)?;
                if canonical != meta.key {
                    return Err(Error::Corruption {
                        message: format!("object store returned non-canonical key {:?}", meta.key),
                    });
                }
                let path = PathBuf::from(canonical);
                if path.parent() == Some(root.as_path()) && path_matches_extension(&path, extension)
                {
                    objects.push(StorageObjectId::native_file(kind, path));
                }
            }
            objects.sort_unstable();
            Ok(StorageListPage {
                objects,
                next_after: page.next_after,
            })
        })
    }
}

fn path_matches_extension(path: &Path, expected: Option<&str>) -> bool {
    expected.is_none_or(|expected| {
        path.extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
    })
}

fn ensure_object_meta_read_len(object: &StorageObjectId, meta: &ObjectMeta) -> Result<()> {
    let len = usize::try_from(meta.size).map_err(|_| Error::Corruption {
        message: format!("object {} length exceeds usize", object.path().display()),
    })?;
    ensure_whole_object_read_len(object, len)
}

async fn read_object_bytes_by_meta(
    client: &dyn ObjectClient,
    key: &str,
    object: &StorageObjectId,
    meta: &ObjectMeta,
) -> Result<Arc<[u8]>> {
    ensure_object_meta_read_len(object, meta)?;
    let expected_len = usize::try_from(meta.size).map_err(|_| Error::Corruption {
        message: format!("object {} length exceeds usize", object.path().display()),
    })?;
    if expected_len == 0 {
        let current = client.head(key).await?.ok_or_else(|| Error::Corruption {
            message: format!("object {} disappeared after HEAD", object.path().display()),
        })?;
        if current.size != 0 || current.etag != meta.etag {
            return Err(Error::Corruption {
                message: format!(
                    "object {} changed after metadata was read",
                    object.path().display()
                ),
            });
        }
        return Ok(Arc::from([]));
    }
    let bytes = client.get_range(key, 0, meta.size, &meta.etag).await?;
    if bytes.len() != expected_len {
        return Err(Error::Corruption {
            message: format!(
                "object {} range read returned {} bytes for declared length {expected_len}",
                object.path().display(),
                bytes.len()
            ),
        });
    }
    ensure_whole_object_read_len(object, bytes.len())?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use super::{
        ETag, InMemoryObjectStore, ObjectClient, ObjectFuture, ObjectListPage, ObjectMeta,
        ObjectStoreBackend, ObjectStoreReclamationAttestation,
        ObjectStoreReclamationEvidenceDigest, ObjectVersion, Precondition, PutIf,
        canonical_object_key, canonical_object_prefix, qualify_object_store_reclamation,
    };
    use crate::error::{Error, Result};
    use crate::options::DurabilityMode;
    use crate::storage::{
        StorageObjectDeleteBackend, StorageObjectId, StorageObjectListBackend,
        StorageObjectReadBackend, StorageObjectWriteBackend, StorageReadBackend, StorageReadObject,
    };

    fn bytes(data: &[u8]) -> Arc<[u8]> {
        Arc::from(data)
    }

    #[test]
    fn object_keys_have_one_cross_platform_canonical_form() {
        assert_eq!(
            canonical_object_prefix(r"tenant\db//./tables").expect("Windows separators normalize"),
            "tenant/db/tables"
        );
        assert_eq!(
            canonical_object_prefix("/tenant/db/tables").expect("Unix separators normalize"),
            "tenant/db/tables"
        );
        assert_eq!(
            canonical_object_key(Path::new(r"\tenant\db\MANIFEST"))
                .expect("absolute Windows-style key normalizes"),
            "tenant/db/MANIFEST"
        );
        assert!(matches!(
            canonical_object_prefix("tenant/../other"),
            Err(Error::InvalidOptions { .. })
        ));
        assert!(matches!(
            canonical_object_prefix("tenant/\0/other"),
            Err(Error::InvalidOptions { .. })
        ));
    }

    /// Drives an [`ObjectFuture`] to completion. The in-memory store never
    /// yields, so a single poll with a no-op waker suffices.
    fn block_on<T>(future: ObjectFuture<'_, T>) -> Result<T> {
        use std::task::{Context, Poll, Wake, Waker};

        struct NoopWaker;
        impl Wake for NoopWaker {
            fn wake(self: Arc<Self>) {}
        }

        let waker = Waker::from(Arc::new(NoopWaker));
        let mut context = Context::from_waker(&waker);
        let mut future = future;
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("in-memory object store future should be ready immediately"),
        }
    }

    #[derive(Debug, Default)]
    struct OversizedHeadClient {
        get_calls: AtomicU64,
    }

    impl ObjectClient for OversizedHeadClient {
        fn get<'op>(&'op self, _key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
            Box::pin(async move {
                self.get_calls.fetch_add(1, Ordering::Relaxed);
                Ok(Some(bytes(b"unreachable")))
            })
        }

        fn get_range<'op>(
            &'op self,
            _key: &str,
            _offset: u64,
            _len: u64,
            _expected_etag: &ETag,
        ) -> ObjectFuture<'op, Arc<[u8]>> {
            Box::pin(async move { Err(Error::invalid_options("unexpected range read")) })
        }

        fn put<'op>(&'op self, _key: &str, _bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
            Box::pin(async move { Err(Error::invalid_options("unexpected put")) })
        }

        fn delete<'op>(&'op self, _key: &str) -> ObjectFuture<'op, ()> {
            Box::pin(async move { Err(Error::invalid_options("unexpected delete")) })
        }

        fn list<'op>(&'op self, _prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
            Box::pin(async move { Err(Error::invalid_options("unexpected list")) })
        }

        fn list_page<'op>(
            &'op self,
            _prefix: &str,
            _after: Option<&str>,
            _limit: usize,
        ) -> ObjectFuture<'op, ObjectListPage> {
            Box::pin(async move { Err(Error::invalid_options("unexpected list page")) })
        }

        fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
            let key = key.to_owned();
            Box::pin(async move {
                Ok(Some(ObjectMeta {
                    key,
                    size: u64::MAX,
                    etag: ETag::new("oversized"),
                    version: None,
                }))
            })
        }

        fn put_if<'op>(
            &'op self,
            _key: &str,
            _bytes: Arc<[u8]>,
            _precondition: Precondition,
        ) -> ObjectFuture<'op, PutIf> {
            Box::pin(async move { Err(Error::invalid_options("unexpected put_if")) })
        }
    }

    #[derive(Debug, Default)]
    struct ShortRangeClient {
        get_calls: AtomicU64,
        range_calls: AtomicU64,
    }

    impl ObjectClient for ShortRangeClient {
        fn get<'op>(&'op self, _key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
            Box::pin(async move {
                self.get_calls.fetch_add(1, Ordering::Relaxed);
                Ok(Some(bytes(b"unreachable")))
            })
        }

        fn get_range<'op>(
            &'op self,
            _key: &str,
            _offset: u64,
            _len: u64,
            _expected_etag: &ETag,
        ) -> ObjectFuture<'op, Arc<[u8]>> {
            Box::pin(async move {
                self.range_calls.fetch_add(1, Ordering::Relaxed);
                Ok(bytes(b"abc"))
            })
        }

        fn put<'op>(&'op self, _key: &str, _bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
            Box::pin(async move { Err(Error::invalid_options("unexpected put")) })
        }

        fn delete<'op>(&'op self, _key: &str) -> ObjectFuture<'op, ()> {
            Box::pin(async move { Err(Error::invalid_options("unexpected delete")) })
        }

        fn list<'op>(&'op self, _prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
            Box::pin(async move { Err(Error::invalid_options("unexpected list")) })
        }

        fn list_page<'op>(
            &'op self,
            _prefix: &str,
            _after: Option<&str>,
            _limit: usize,
        ) -> ObjectFuture<'op, ObjectListPage> {
            Box::pin(async move { Err(Error::invalid_options("unexpected list page")) })
        }

        fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
            let key = key.to_owned();
            Box::pin(async move {
                Ok(Some(ObjectMeta {
                    key,
                    size: 5,
                    etag: ETag::new("short-range"),
                    version: None,
                }))
            })
        }

        fn put_if<'op>(
            &'op self,
            _key: &str,
            _bytes: Arc<[u8]>,
            _precondition: Precondition,
        ) -> ObjectFuture<'op, PutIf> {
            Box::pin(async move { Err(Error::invalid_options("unexpected put_if")) })
        }
    }

    #[derive(Debug)]
    struct ReclamationProbeClient {
        inner: InMemoryObjectStore,
        report_version: bool,
        retain_on_delete: bool,
        hide_head: bool,
    }

    impl ReclamationProbeClient {
        fn new(report_version: bool, retain_on_delete: bool, hide_head: bool) -> Self {
            Self {
                inner: InMemoryObjectStore::new(),
                report_version,
                retain_on_delete,
                hide_head,
            }
        }

        fn decorate_meta(&self, mut meta: ObjectMeta) -> ObjectMeta {
            if self.report_version {
                meta.version = Some(ObjectVersion::new("provider-version-1"));
            }
            meta
        }
    }

    impl ObjectClient for ReclamationProbeClient {
        fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
            self.inner.get(key)
        }

        fn get_range<'op>(
            &'op self,
            key: &str,
            offset: u64,
            len: u64,
            expected_etag: &ETag,
        ) -> ObjectFuture<'op, Arc<[u8]>> {
            self.inner.get_range(key, offset, len, expected_etag)
        }

        fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
            self.inner.put(key, bytes)
        }

        fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
            if self.retain_on_delete {
                Box::pin(async { Ok(()) })
            } else {
                self.inner.delete(key)
            }
        }

        fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
            let prefix = prefix.to_owned();
            Box::pin(async move {
                self.inner.list(&prefix).await.map(|metas| {
                    metas
                        .into_iter()
                        .map(|meta| self.decorate_meta(meta))
                        .collect()
                })
            })
        }

        fn list_page<'op>(
            &'op self,
            prefix: &str,
            after: Option<&str>,
            limit: usize,
        ) -> ObjectFuture<'op, ObjectListPage> {
            let prefix = prefix.to_owned();
            let after = after.map(str::to_owned);
            Box::pin(async move {
                self.inner
                    .list_page(&prefix, after.as_deref(), limit)
                    .await
                    .map(|mut page| {
                        page.objects = page
                            .objects
                            .into_iter()
                            .map(|meta| self.decorate_meta(meta))
                            .collect();
                        page
                    })
            })
        }

        fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
            let key = key.to_owned();
            Box::pin(async move {
                if self.hide_head {
                    return Ok(None);
                }
                self.inner
                    .head(&key)
                    .await
                    .map(|meta| meta.map(|meta| self.decorate_meta(meta)))
            })
        }

        fn put_if<'op>(
            &'op self,
            key: &str,
            bytes: Arc<[u8]>,
            precondition: Precondition,
        ) -> ObjectFuture<'op, PutIf> {
            self.inner.put_if(key, bytes, precondition)
        }
    }

    #[test]
    fn put_then_get_roundtrips_and_overwrite_changes_etag() {
        let store = InMemoryObjectStore::new();
        let first = block_on(store.put("k", bytes(b"hello"))).unwrap();
        assert_eq!(
            block_on(store.get("k")).unwrap().as_deref(),
            Some(b"hello".as_slice())
        );
        let second = block_on(store.put("k", bytes(b"world"))).unwrap();
        assert_ne!(first, second, "overwrite mints a new ETag");
        assert_eq!(
            block_on(store.get("k")).unwrap().as_deref(),
            Some(b"world".as_slice())
        );
    }

    #[test]
    fn reclamation_qualification_requires_unversioned_observable_delete() {
        let evidence = ObjectStoreReclamationAttestation::new(
            ObjectStoreReclamationEvidenceDigest::for_bytes(b"test provider evidence"),
        );
        let qualified_client: Arc<dyn ObjectClient> =
            Arc::new(ReclamationProbeClient::new(false, false, false));
        let qualification = block_on(Box::pin(qualify_object_store_reclamation(
            Arc::clone(&qualified_client),
            "qualified-prefix",
            evidence,
        )))
        .expect("unversioned strong delete qualifies");
        assert_eq!(qualification.evidence_digest(), evidence.evidence_digest());
        assert!(qualification.matches_prefix(Path::new("qualified-prefix")));
        assert!(!qualification.matches_prefix(Path::new("different-prefix")));
        assert!(qualification.matches_client(&qualified_client));
        let different_client: Arc<dyn ObjectClient> =
            Arc::new(ReclamationProbeClient::new(false, false, false));
        assert!(!qualification.matches_client(&different_client));

        let versioned_client: Arc<dyn ObjectClient> =
            Arc::new(ReclamationProbeClient::new(true, false, false));
        assert!(matches!(
            block_on(Box::pin(qualify_object_store_reclamation(
                versioned_client,
                "versioned-prefix",
                evidence,
            ))),
            Err(Error::Corruption { .. })
        ));

        let sticky_client: Arc<dyn ObjectClient> =
            Arc::new(ReclamationProbeClient::new(false, true, false));
        assert!(matches!(
            block_on(Box::pin(qualify_object_store_reclamation(
                sticky_client,
                "sticky-prefix",
                evidence,
            ))),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn verified_delete_does_not_trust_head_absence_alone() {
        let concrete = Arc::new(ReclamationProbeClient::new(false, false, true));
        block_on(concrete.put("db/content-object", bytes(b"still present")))
            .expect("seed hidden object");
        let client: Arc<dyn ObjectClient> = concrete.clone();
        let backend = ObjectStoreBackend::new(client);
        let object = StorageObjectId::native_file(
            crate::storage::StorageObjectKind::ContentChunk,
            "db/content-object",
        );

        assert!(matches!(
            block_on(Box::pin(backend.delete_unversioned_object_verified(object))),
            Err(Error::Corruption { .. })
        ));
        assert_eq!(
            block_on(concrete.get("db/content-object"))
                .expect("hidden object reads")
                .as_deref(),
            Some(b"still present".as_slice())
        );
    }

    #[test]
    fn get_absent_is_none_and_range_reads_a_window() {
        let store = InMemoryObjectStore::new();
        assert!(block_on(store.get("missing")).unwrap().is_none());
        let etag = block_on(store.put("k", bytes(b"abcdef"))).unwrap();
        assert_eq!(
            block_on(store.get_range("k", 2, 3, &etag))
                .unwrap()
                .as_ref(),
            b"cde"
        );
        // Absent key and out-of-bounds range are both errors.
        assert!(block_on(store.get_range("missing", 0, 1, &etag)).is_err());
        assert!(block_on(store.get_range("k", 4, 10, &etag)).is_err());

        block_on(store.put("k", bytes(b"replacement"))).expect("overwrite changes ETag");
        assert!(
            block_on(store.get_range("k", 0, 3, &etag)).is_err(),
            "a range read may not splice bytes from a newer object version"
        );
    }

    #[test]
    fn delete_is_idempotent() {
        let store = InMemoryObjectStore::new();
        block_on(store.put("k", bytes(b"x"))).unwrap();
        block_on(store.delete("k")).unwrap();
        assert!(block_on(store.get("k")).unwrap().is_none());
        // Deleting an absent key still succeeds.
        block_on(store.delete("k")).unwrap();
    }

    #[test]
    fn list_returns_prefix_matches_in_key_order() {
        let store = InMemoryObjectStore::new();
        block_on(store.put("wal/2", bytes(b"b"))).unwrap();
        block_on(store.put("wal/1", bytes(b"aa"))).unwrap();
        block_on(store.put("table/9", bytes(b"c"))).unwrap();
        let listed = block_on(store.list("wal/")).unwrap();
        let keys: Vec<&str> = listed.iter().map(|meta| meta.key.as_str()).collect();
        assert_eq!(keys, ["wal/1", "wal/2"], "prefix-filtered, key-ordered");
        assert_eq!(listed[0].size, 2);
        assert_eq!(listed[1].size, 1);
    }

    #[test]
    fn list_page_uses_exclusive_continuation_without_duplicates() {
        let store = InMemoryObjectStore::new();
        for key in ["wal/1", "wal/2", "wal/3"] {
            block_on(store.put(key, bytes(key.as_bytes()))).unwrap();
        }
        let first = block_on(store.list_page("wal/", None, 2)).unwrap();
        assert_eq!(
            first
                .objects
                .iter()
                .map(|meta| meta.key.as_str())
                .collect::<Vec<_>>(),
            ["wal/1", "wal/2"]
        );
        let second = block_on(store.list_page("wal/", first.next_after.as_deref(), 2)).unwrap();
        assert_eq!(
            second
                .objects
                .iter()
                .map(|meta| meta.key.as_str())
                .collect::<Vec<_>>(),
            ["wal/3"]
        );
        assert!(second.next_after.is_none());
    }

    #[test]
    fn head_returns_metadata_without_bytes_and_none_when_absent() {
        let store = InMemoryObjectStore::new();
        assert!(block_on(store.head("k")).unwrap().is_none());
        let etag = block_on(store.put("k", bytes(b"hello"))).unwrap();
        let meta = block_on(store.head("k")).unwrap().expect("present");
        assert_eq!(meta.key, "k");
        assert_eq!(meta.size, 5);
        assert_eq!(meta.etag, etag);
    }

    #[test]
    fn put_if_none_match_creates_only_when_absent() {
        let store = InMemoryObjectStore::new();
        let created = block_on(store.put_if("k", bytes(b"v1"), Precondition::IfNoneMatch)).unwrap();
        let etag = match created {
            PutIf::Stored { etag } => etag,
            PutIf::PreconditionFailed { .. } => panic!("create should succeed when absent"),
        };
        // A second create is refused and reports the current ETag.
        match block_on(store.put_if("k", bytes(b"v2"), Precondition::IfNoneMatch)).unwrap() {
            PutIf::PreconditionFailed { current } => assert_eq!(current, Some(etag)),
            PutIf::Stored { .. } => panic!("create should fail when present"),
        }
        assert_eq!(
            block_on(store.get("k")).unwrap().as_deref(),
            Some(b"v1".as_slice()),
            "refused create left the object unchanged"
        );
    }

    #[test]
    fn put_if_match_is_a_compare_and_swap() {
        let store = InMemoryObjectStore::new();
        let v1 = block_on(store.put("k", bytes(b"v1"))).unwrap();

        // CAS with the current ETag wins and advances the ETag.
        let v2 = match block_on(store.put_if("k", bytes(b"v2"), Precondition::IfMatch(v1.clone())))
            .unwrap()
        {
            PutIf::Stored { etag } => etag,
            PutIf::PreconditionFailed { .. } => panic!("CAS with current ETag should win"),
        };
        assert_ne!(v1, v2);

        // A second CAS with the now-stale ETag loses and reports the current one
        // — this is the manifest-commit retry signal.
        match block_on(store.put_if("k", bytes(b"v3"), Precondition::IfMatch(v1))).unwrap() {
            PutIf::PreconditionFailed { current } => assert_eq!(current, Some(v2)),
            PutIf::Stored { .. } => panic!("CAS with stale ETag should lose"),
        }
        assert_eq!(
            block_on(store.get("k")).unwrap().as_deref(),
            Some(b"v2".as_slice()),
            "the losing CAS left v2 in place"
        );
    }

    #[test]
    fn put_if_match_on_absent_object_fails() {
        let store = InMemoryObjectStore::new();
        let phantom = ETag(Arc::from("etag-phantom"));
        match block_on(store.put_if("k", bytes(b"v"), Precondition::IfMatch(phantom))).unwrap() {
            PutIf::PreconditionFailed { current } => assert_eq!(current, None),
            PutIf::Stored { .. } => panic!("IfMatch cannot match a missing object"),
        }
    }

    #[test]
    fn object_store_backend_round_trips_an_object() {
        use crate::storage::StorageObjectKind;

        let backend = ObjectStoreBackend::new(Arc::new(InMemoryObjectStore::new()));
        let id = StorageObjectId::native_file(StorageObjectKind::ContentChunk, "/db/0001.trinec");

        block_on(backend.write_object(id.clone(), bytes(b"hello world"), DurabilityMode::Flush))
            .unwrap();

        // Whole-object read.
        assert_eq!(
            block_on(backend.read_object_bytes(id.clone()))
                .unwrap()
                .as_deref(),
            Some(b"hello world".as_slice())
        );

        // Random-access read via the ranged read handle.
        let object = block_on(backend.open_read(id.clone())).unwrap();
        assert_eq!(block_on(StorageReadObject::len(&object)).unwrap(), 11);
        let mut window = [0_u8; 5];
        block_on(StorageReadObject::read_exact_at(&object, 6, &mut window)).unwrap();
        assert_eq!(&window, b"world");

        // Delete, then it is gone.
        block_on(backend.delete_object(id.clone())).unwrap();
        assert!(block_on(backend.read_object_bytes(id)).unwrap().is_none());
    }

    #[test]
    fn immutable_table_object_cannot_be_overwritten_or_deleted() {
        use crate::storage::StorageObjectKind;

        let client = Arc::new(InMemoryObjectStore::new());
        let backend = ObjectStoreBackend::new(client.clone());
        let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/0001.trinet");

        block_on(backend.write_object(id.clone(), bytes(b"first"), DurabilityMode::Flush))
            .expect("initial immutable write");
        block_on(backend.write_object(id.clone(), bytes(b"first"), DurabilityMode::Flush))
            .expect("identical retry is idempotent");
        let error =
            block_on(backend.write_object(id.clone(), bytes(b"different"), DurabilityMode::Flush))
                .expect_err("different bytes cannot replace immutable object");
        assert!(matches!(error, Error::Corruption { .. }));
        assert!(
            block_on(backend.delete_object(id.clone())).is_err(),
            "table deletion requires a reader-retirement protocol"
        );
        assert_eq!(
            block_on(client.get("db/0001.trinet")).unwrap().as_deref(),
            Some(b"first".as_slice())
        );
    }

    #[test]
    fn immutable_content_chunk_rejects_conflicting_retry() {
        let backend = ObjectStoreBackend::new(Arc::new(InMemoryObjectStore::new()));
        let id = StorageObjectId::native_file(
            crate::storage::StorageObjectKind::ContentChunk,
            "/db/content/upload/partial-0001-0007.trinec",
        );

        block_on(backend.write_object(id.clone(), bytes(b"first"), DurabilityMode::Flush))
            .expect("initial immutable chunk write");
        block_on(backend.write_object(id.clone(), bytes(b"first"), DurabilityMode::Flush))
            .expect("identical chunk retry is idempotent");
        assert!(matches!(
            block_on(backend.write_object(id, bytes(b"stale"), DurabilityMode::Flush)),
            Err(Error::Corruption { .. })
        ));
    }

    #[test]
    fn object_store_backend_rejects_oversized_head_before_get() {
        use crate::storage::StorageObjectKind;

        let client = Arc::new(OversizedHeadClient::default());
        let backend = ObjectStoreBackend::new(client.clone());
        let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/huge.trinet");

        let error =
            block_on(backend.read_object_bytes(id.clone())).expect_err("oversized head fails");
        assert!(error.to_string().contains("exceeds maximum"));
        assert_eq!(
            client.get_calls.load(Ordering::Relaxed),
            0,
            "oversized object should fail on HEAD before GET"
        );

        let error = block_on(backend.open_read(id)).expect_err("oversized head fails");
        assert!(error.to_string().contains("exceeds maximum"));
        assert_eq!(
            client.get_calls.load(Ordering::Relaxed),
            0,
            "open_read should also fail on HEAD before GET"
        );
    }

    #[test]
    fn object_store_backend_rejects_short_range_without_whole_get() {
        use crate::storage::StorageObjectKind;

        let client = Arc::new(ShortRangeClient::default());
        let backend = ObjectStoreBackend::new(client.clone());
        let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/short.trinet");

        let error = block_on(backend.read_object_bytes(id)).expect_err("short range fails");
        assert!(error.to_string().contains("declared length"));
        assert_eq!(
            client.get_calls.load(Ordering::Relaxed),
            0,
            "bounded range reads should not fall back to whole-object GET"
        );

        let id = StorageObjectId::native_file(StorageObjectKind::Table, "/db/short.trinet");
        let object = block_on(backend.open_read(id)).expect("HEAD-only open succeeds");
        assert_eq!(
            client.range_calls.load(Ordering::Relaxed),
            1,
            "open itself performs no additional range request"
        );
        let mut out = [0_u8; 5];
        let error = block_on(StorageReadObject::read_exact_at(&object, 0, &mut out))
            .expect_err("short ranged handle read fails");
        assert!(error.to_string().contains("requested length"));
        assert_eq!(client.range_calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn object_store_backend_lists_direct_children_by_extension() {
        use crate::storage::{StorageObjectKind, StorageObjectListRequest};

        let backend = ObjectStoreBackend::new(Arc::new(InMemoryObjectStore::new()));
        let write = |key: &'static str| {
            block_on(backend.write_object(
                StorageObjectId::native_file(StorageObjectKind::Table, key),
                bytes(b"x"),
                DurabilityMode::Flush,
            ))
            .unwrap();
        };
        write("/db/0002.trinet");
        write("/db/0001.trinet");
        write("/db/MANIFEST"); // wrong extension
        write("/db/sub/9999.trinet"); // not a direct child of /db

        let listed = block_on(
            backend.list_objects(
                StorageObjectListRequest::native_file(StorageObjectKind::Table, "/db")
                    .with_file_extension("trinet"),
            ),
        )
        .unwrap();
        let paths: Vec<String> = listed
            .iter()
            .map(|id| id.path().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            paths,
            ["db/0001.trinet", "db/0002.trinet"],
            "only direct .trinet children, in key order"
        );
    }
}
