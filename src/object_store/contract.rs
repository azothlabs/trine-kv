use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};

use super::{
    ETag, ObjectFuture, ObjectListPage, ObjectMeta, ObjectStoreReclamationAttestation,
    Precondition, PutIf, QualifiedObjectStoreReclamation, canonical_object_key,
    canonical_object_prefix,
};
use crate::error::{Error, Result};

pub(super) fn validate_object_list_page(
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
/// - `get` returns `None` for an absent key; `get_range` returns
///   [`Error::ObjectVersionChanged`] for an absent key or `ETag` mismatch and
///   errors for an out-of-bounds range.
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
    /// splicing bytes from different object versions. Implementations return
    /// [`Error::ObjectVersionChanged`] when the key is absent or its current
    /// `ETag` differs.
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

pub(super) fn object_store_reclamation_namespace_digest(prefix: &Path) -> Result<[u8; 32]> {
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

pub(super) async fn verify_object_store_reclamation_absent(
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
    match client
        .get_range(key, 0, second.len() as u64, &first_etag)
        .await
    {
        Err(Error::ObjectVersionChanged { .. }) => {}
        Ok(_) => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} accepted a stale ETag for a range read"
                ),
            });
        }
        Err(error) => {
            return Err(Error::Corruption {
                message: format!(
                    "object client contract probe for {key} returned {error:?} instead of ObjectVersionChanged for a stale ETag"
                ),
            });
        }
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
/// A shared [`ObjectClient`] is itself an `ObjectClient`, allowing components
/// to share one client through cheap [`Arc`] clones.
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
