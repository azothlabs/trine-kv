use super::{
    Arc, AtomicBool, AtomicU64, CONTENT_LEASE_MAGIC, CONTENT_PHYSICAL_HOLD_MAGIC,
    ContentDescriptor, ContentId, ContentLeaseId, ContentLeaseOwnerId, ContentPhysicalHoldId,
    ContentPhysicalHoldKind, ContentPhysicalHoldOwnerId, Db, Digest, Duration, Error, Ordering,
    Result, Sha256, StorageDomainId, array_at, decode_chunk, decode_content_id, fmt,
};

/// Result of one content-authority cleanup transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ContentLifecycleMaintenanceReport {
    pub(crate) scanned: u64,
    pub(crate) expired_tokens_removed: u64,
    pub(crate) expired_leases_removed: u64,
    pub(crate) inactive_holds_removed: u64,
}

impl ContentLifecycleMaintenanceReport {
    /// Returns the number of token, lease, and hold records inspected.
    #[must_use]
    pub const fn scanned(self) -> u64 {
        self.scanned
    }

    /// Returns expired upload-token authority records removed.
    #[must_use]
    pub const fn expired_tokens_removed(self) -> u64 {
        self.expired_tokens_removed
    }

    /// Returns expired read-lease records removed.
    #[must_use]
    pub const fn expired_leases_removed(self) -> u64 {
        self.expired_leases_removed
    }

    /// Returns released or expired physical-hold records removed.
    #[must_use]
    pub const fn inactive_holds_removed(self) -> u64 {
        self.inactive_holds_removed
    }
}

#[derive(Debug)]
pub(crate) struct ContentLease {
    id: ContentLeaseId,
    owner_id: ContentLeaseOwnerId,
    expires_at_unix_ms: AtomicU64,
}

impl ContentLease {
    pub(crate) const fn new(
        id: ContentLeaseId,
        owner_id: ContentLeaseOwnerId,
        expires_at_unix_ms: u64,
    ) -> Self {
        Self {
            id,
            owner_id,
            expires_at_unix_ms: AtomicU64::new(expires_at_unix_ms),
        }
    }

    pub(crate) const fn id(&self) -> ContentLeaseId {
        self.id
    }

    pub(crate) const fn owner_id(&self) -> ContentLeaseOwnerId {
        self.owner_id
    }

    pub(crate) fn expires_at_unix_ms(&self) -> u64 {
        self.expires_at_unix_ms.load(Ordering::Acquire)
    }

    pub(crate) fn publish_expiry(&self, expires_at_unix_ms: u64) {
        self.expires_at_unix_ms
            .store(expires_at_unix_ms, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentLeaseRecord {
    pub(crate) lease_id: ContentLeaseId,
    pub(crate) owner_id: ContentLeaseOwnerId,
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) expires_at_unix_ms: u64,
}

impl ContentLeaseRecord {
    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16 + 16 + 33 + 8);
        bytes.extend_from_slice(CONTENT_LEASE_MAGIC);
        bytes.extend_from_slice(&self.lease_id.to_bytes());
        bytes.extend_from_slice(&self.owner_id.to_bytes());
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.expires_at_unix_ms.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        lease_id: ContentLeaseId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16 + 16 + 33 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_LEASE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content lease record header or length".to_owned(),
            });
        }
        let stored_lease_id =
            ContentLeaseId::from_bytes(array_at::<16>(bytes, 8, "content lease identity")?)?;
        let owner_id =
            ContentLeaseOwnerId::from_bytes(array_at::<16>(bytes, 24, "content lease owner")?);
        let stored_domain =
            StorageDomainId::from_bytes(array_at::<16>(bytes, 40, "content lease storage domain")?);
        let stored_content = decode_content_id(bytes, 56, "content lease content identity")?;
        let expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 89, "content lease expiry")?);
        if stored_lease_id != lease_id
            || stored_domain != storage_domain_id
            || stored_content != content_id
        {
            return Err(Error::Corruption {
                message: "content lease record differs from its protected key".to_owned(),
            });
        }
        Ok(Self {
            lease_id,
            owner_id,
            storage_domain_id,
            content_id,
            expires_at_unix_ms,
        })
    }
}

pub(crate) fn content_lease_prefix(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + 33);
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

pub(crate) fn content_lease_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    lease_id: ContentLeaseId,
) -> Vec<u8> {
    let mut key = content_lease_prefix(storage_domain_id, content_id);
    key.extend_from_slice(&lease_id.to_bytes());
    key
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentPhysicalHoldRecordState {
    Active,
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentPhysicalHoldRecord {
    pub(crate) hold_id: ContentPhysicalHoldId,
    pub(crate) owner_id: ContentPhysicalHoldOwnerId,
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) kind: ContentPhysicalHoldKind,
    pub(crate) expires_at_unix_ms: u64,
    pub(crate) state: ContentPhysicalHoldRecordState,
}

impl ContentPhysicalHoldRecord {
    pub(crate) const fn is_active_at(self, now_unix_ms: u64) -> bool {
        matches!(self.state, ContentPhysicalHoldRecordState::Active)
            && (self.expires_at_unix_ms == 0 || now_unix_ms < self.expires_at_unix_ms)
    }

    pub(crate) const fn is_released(self) -> bool {
        matches!(self.state, ContentPhysicalHoldRecordState::Released)
    }

    pub(crate) const fn released(mut self) -> Self {
        self.state = ContentPhysicalHoldRecordState::Released;
        self
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16 + 16 + 33 + 1 + 1 + 8);
        bytes.extend_from_slice(CONTENT_PHYSICAL_HOLD_MAGIC);
        bytes.extend_from_slice(&self.hold_id.to_bytes());
        bytes.extend_from_slice(&self.owner_id.to_bytes());
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.push(self.kind.tag());
        bytes.push(match self.state {
            ContentPhysicalHoldRecordState::Active => 0,
            ContentPhysicalHoldRecordState::Released => 1,
        });
        bytes.extend_from_slice(&self.expires_at_unix_ms.to_le_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        hold_id: ContentPhysicalHoldId,
    ) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16 + 16 + 33 + 1 + 1 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_PHYSICAL_HOLD_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content physical-hold record header or length".to_owned(),
            });
        }
        let stored_hold_id = ContentPhysicalHoldId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content physical-hold identity",
        )?)?;
        let owner_id = ContentPhysicalHoldOwnerId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content physical-hold owner",
        )?);
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            40,
            "content physical-hold storage domain",
        )?);
        let stored_content =
            decode_content_id(bytes, 56, "content physical-hold content identity")?;
        let kind = ContentPhysicalHoldKind::from_tag(bytes[89])?;
        let state = match bytes[90] {
            0 => ContentPhysicalHoldRecordState::Active,
            1 => ContentPhysicalHoldRecordState::Released,
            state => {
                return Err(Error::UnsupportedFormat {
                    message: format!("unsupported content physical-hold state {state}"),
                });
            }
        };
        let expires_at_unix_ms =
            u64::from_le_bytes(array_at::<8>(bytes, 91, "content physical-hold expiry")?);
        if stored_hold_id != hold_id
            || stored_domain != storage_domain_id
            || stored_content != content_id
        {
            return Err(Error::Corruption {
                message: "content physical-hold record differs from its protected key".to_owned(),
            });
        }
        Ok(Self {
            hold_id,
            owner_id,
            storage_domain_id,
            content_id,
            kind,
            expires_at_unix_ms,
            state,
        })
    }
}

pub(crate) fn content_physical_hold_prefix(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    content_lease_prefix(storage_domain_id, content_id)
}

pub(crate) fn content_physical_hold_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    hold_id: ContentPhysicalHoldId,
) -> Vec<u8> {
    let mut key = content_physical_hold_prefix(storage_domain_id, content_id);
    key.extend_from_slice(&hold_id.to_bytes());
    key
}

pub(crate) fn current_epoch_millis() -> Result<u64> {
    crate::platform::now_unix_millis()
}

pub(crate) fn duration_millis(duration: Duration, name: &str) -> Result<u64> {
    let millis = u64::try_from(duration.as_millis())
        .map_err(|_| Error::invalid_options(format!("{name} milliseconds exceed u64::MAX")))?;
    if millis == 0 {
        return Err(Error::invalid_options(format!(
            "{name} must be at least one millisecond"
        )));
    }
    Ok(millis)
}

#[derive(Debug)]
struct ContentPhysicalHoldState {
    expires_at_unix_ms: AtomicU64,
    released: AtomicBool,
}

/// Durable reason that one exact content identity must remain physically readable.
///
/// Clones share local release and expiry state, while the protected database
/// record is the cross-process source of truth. Dropping the value performs no
/// asynchronous I/O. Expiring holds become inert at their deadline; an
/// until-released hold must be resumed and explicitly released after a crash.
#[derive(Clone)]
pub struct ContentPhysicalHold {
    db: Db,
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
    hold_id: ContentPhysicalHoldId,
    owner_id: ContentPhysicalHoldOwnerId,
    kind: ContentPhysicalHoldKind,
    state: Arc<ContentPhysicalHoldState>,
}

impl ContentPhysicalHold {
    pub(crate) fn from_record(db: Db, record: ContentPhysicalHoldRecord) -> Self {
        Self {
            db,
            storage_domain_id: record.storage_domain_id,
            content_id: record.content_id,
            hold_id: record.hold_id,
            owner_id: record.owner_id,
            kind: record.kind,
            state: Arc::new(ContentPhysicalHoldState {
                expires_at_unix_ms: AtomicU64::new(record.expires_at_unix_ms),
                released: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the exact physical lifecycle domain.
    #[must_use]
    pub const fn storage_domain_id(&self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns the immutable byte identity protected by this hold.
    #[must_use]
    pub const fn content_id(&self) -> ContentId {
        self.content_id
    }

    /// Returns the durable hold identity.
    #[must_use]
    pub const fn id(&self) -> ContentPhysicalHoldId {
        self.hold_id
    }

    /// Returns the operational hold class.
    #[must_use]
    pub const fn kind(&self) -> ContentPhysicalHoldKind {
        self.kind
    }

    /// Returns the current exclusive deadline, or `None` for explicit release.
    ///
    /// All clones observe a successful renewal only after its durable commit.
    #[must_use]
    pub fn expires_at_unix_ms(&self) -> Option<u64> {
        match self.state.expires_at_unix_ms.load(Ordering::Acquire) {
            0 => None,
            expires_at_unix_ms => Some(expires_at_unix_ms),
        }
    }

    /// Returns whether this process has observed a successful explicit release.
    ///
    /// This local convenience flag is not the cross-process source of truth.
    #[must_use]
    pub fn is_released(&self) -> bool {
        self.state.released.load(Ordering::Acquire)
    }

    /// Renews an unexpired expiring hold for `ttl` from the current time.
    ///
    /// The caller must repeat higher-layer authorization first. Renewal cannot
    /// revive expiry and does not convert an until-released hold.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentPhysicalHoldNotFound`] after release or missing
    /// durable state, [`Error::ContentPhysicalHoldExpired`] after the deadline,
    /// [`Error::InvalidOptions`] for an until-released hold or invalid `ttl`,
    /// [`Error::ContentPhysicalHoldOwnerMismatch`] for wrong authority,
    /// [`Error::ReadOnly`] when protected state cannot be written, or a
    /// storage/conflict error when renewal cannot publish.
    pub async fn renew(&self, ttl: Duration) -> Result<u64> {
        self.db.renew_content_physical_hold(self, ttl).await
    }

    /// Durably releases this exact hold idempotently.
    ///
    /// All clones observe release after the durable Released tombstone commits.
    /// A missing record is accepted as an already-completed retry for recovery
    /// compatibility. Drop does not call this method.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentPhysicalHoldOwnerMismatch`] if the durable owner
    /// differs, [`Error::ReadOnly`] when protected state cannot be updated, or
    /// a storage/conflict error if release cannot converge.
    pub async fn release(&self) -> Result<()> {
        self.db.release_content_physical_hold(self).await
    }

    pub(crate) const fn owner_id(&self) -> ContentPhysicalHoldOwnerId {
        self.owner_id
    }

    pub(crate) fn publish_expiry(&self, expires_at_unix_ms: u64) {
        self.state
            .expires_at_unix_ms
            .store(expires_at_unix_ms, Ordering::Release);
    }

    pub(crate) fn publish_released(&self) {
        self.state.released.store(true, Ordering::Release);
    }
}

impl fmt::Debug for ContentPhysicalHold {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContentPhysicalHold")
            .field("storage_domain_id", &self.storage_domain_id)
            .field("content_id", &self.content_id)
            .field("hold_id", &self.hold_id)
            .field("kind", &self.kind)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms())
            .field("released", &self.is_released())
            .finish_non_exhaustive()
    }
}

/// Stable read handle for one sealed immutable `ContentObject`.
///
/// The handle fixes one descriptor when opened. This prototype has no physical
/// relocation, so the descriptor's upload identity and chunk boundaries remain
/// stable for the handle lifetime. A handle opened with
/// [`Db::open_content_leased`](crate::Db::open_content_leased) additionally
/// carries one durable short-lived read lease shared by all of its clones.
#[derive(Debug, Clone)]
pub struct ContentHandle {
    db: Db,
    descriptor: ContentDescriptor,
    lease: Option<Arc<ContentLease>>,
}

impl ContentHandle {
    pub(crate) fn new(db: Db, descriptor: ContentDescriptor) -> Self {
        Self {
            db,
            descriptor,
            lease: None,
        }
    }

    pub(crate) fn with_lease(mut self, lease: ContentLease) -> Self {
        self.lease = Some(Arc::new(lease));
        self
    }

    pub(crate) fn lease(&self) -> Option<&Arc<ContentLease>> {
        self.lease.as_ref()
    }

    /// Returns the immutable content identity fixed by this handle.
    #[must_use]
    pub const fn content_id(&self) -> ContentId {
        self.descriptor.content_id
    }

    /// Returns the storage domain fixed by this handle.
    #[must_use]
    pub const fn storage_domain_id(&self) -> StorageDomainId {
        self.descriptor.storage_domain_id
    }

    /// Returns the original byte length.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.descriptor.length
    }

    /// Returns whether the original byte sequence is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.descriptor.length == 0
    }

    /// Returns the durable read lease identity, when this handle was opened
    /// through [`Db::open_content_leased`](crate::Db::open_content_leased).
    ///
    /// Ordinary [`Db::open_content`](crate::Db::open_content) handles return
    /// `None` and are not protected from future physical reclamation.
    #[must_use]
    pub fn lease_id(&self) -> Option<ContentLeaseId> {
        self.lease.as_deref().map(ContentLease::id)
    }

    /// Returns the current lease deadline as Unix epoch milliseconds.
    ///
    /// All clones share this value. `None` identifies an unleased handle.
    /// Renewal publishes durable state before this local deadline advances.
    #[must_use]
    pub fn lease_expires_at_unix_ms(&self) -> Option<u64> {
        self.lease.as_deref().map(ContentLease::expires_at_unix_ms)
    }

    /// Renews this handle's durable read lease for `ttl` from the current time.
    ///
    /// The caller must repeat its higher-layer authorization before calling.
    /// Trine KV validates only the opaque owner and exact content identity. All
    /// handle clones observe the new deadline after durable publication.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentLeaseNotFound`] for an unleased handle or missing
    /// durable record, [`Error::ContentLeaseExpired`] once the old deadline is
    /// reached, [`Error::InvalidOptions`] for a sub-millisecond or overflowing
    /// lifetime, [`Error::ContentQuarantined`] if quarantine won the race after
    /// the lease expired, or a storage/conflict error if renewal cannot publish.
    pub async fn renew_lease(&self, ttl: Duration) -> Result<u64> {
        self.db.renew_content_lease(self, ttl).await
    }

    fn ensure_lease_active(&self) -> Result<()> {
        let Some(lease) = self.lease.as_deref() else {
            return Ok(());
        };
        let now = current_epoch_millis()?;
        let expires_at_unix_ms = lease.expires_at_unix_ms();
        if now >= expires_at_unix_ms {
            return Err(Error::ContentLeaseExpired {
                expired_at_unix_ms: expires_at_unix_ms,
            });
        }
        Ok(())
    }

    /// Reads a verified byte range using common half-open range semantics.
    ///
    /// `start > len()` returns [`Error::ContentRangeOutOfBounds`]. A zero-byte
    /// request or `start == len()` returns empty bytes. A request extending past
    /// EOF returns the verified suffix. Only chunks touched by the result are
    /// read, and each is verified before any of its payload is copied.
    ///
    /// # Errors
    ///
    /// Returns a typed range error, a storage error, or
    /// [`Error::ContentDigestMismatch`] / [`Error::Corruption`] if a chunk is
    /// missing, malformed, or tampered with.
    pub async fn read_range(&self, start: u64, length: u64) -> Result<Arc<[u8]>> {
        self.ensure_lease_active()?;
        if start > self.descriptor.length() {
            return Err(Error::ContentRangeOutOfBounds {
                start,
                length: self.descriptor.length(),
            });
        }
        if length == 0 || start == self.descriptor.length() {
            return Ok(Arc::from([]));
        }
        let end = start.saturating_add(length).min(self.descriptor.length());
        let result_len = usize::try_from(end - start)
            .map_err(|_| Error::invalid_options("requested content range exceeds usize"))?;
        let mut result = Vec::with_capacity(result_len);
        let chunk_bytes = u64::from(self.descriptor.chunk_bytes());
        let mut position = start;
        while position < end {
            self.ensure_lease_active()?;
            let chunk_index = position / chunk_bytes;
            let frame = self
                .db
                .read_content_chunk(self.descriptor.upload_id(), chunk_index)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!(
                        "content {} is missing chunk {chunk_index}",
                        self.descriptor.content_id()
                    ),
                })?;
            let payload = decode_chunk(&frame, self.descriptor.upload_id(), chunk_index)?;
            let offset = usize::try_from(position % chunk_bytes)
                .map_err(|_| Error::invalid_options("content chunk offset exceeds usize"))?;
            let available = payload
                .len()
                .checked_sub(offset)
                .ok_or_else(|| Error::Corruption {
                    message: format!("content chunk {chunk_index} is shorter than its descriptor"),
                })?;
            let remaining = usize::try_from(end - position)
                .map_err(|_| Error::invalid_options("content range remainder exceeds usize"))?;
            let take = available.min(remaining);
            if take == 0 {
                return Err(Error::Corruption {
                    message: format!("content chunk {chunk_index} made no read progress"),
                });
            }
            result.extend_from_slice(&payload[offset..offset + take]);
            position =
                position
                    .checked_add(u64::try_from(take).map_err(|_| {
                        Error::invalid_options("content range progress exceeds u64")
                    })?)
                    .ok_or_else(|| Error::invalid_options("content range position overflow"))?;
        }
        Ok(Arc::from(result))
    }

    /// Creates a sequential stream starting at byte zero.
    #[must_use]
    pub fn stream(&self) -> ContentStream {
        ContentStream {
            handle: self.clone(),
            position: 0,
        }
    }

    /// Recomputes the complete `ContentId` through a bounded-memory stream.
    ///
    /// This additionally verifies each chunk frame. Success means both the
    /// per-chunk digests and the complete original-byte digest match.
    ///
    /// # Errors
    ///
    /// Returns a storage, format, or digest error at the first invalid chunk.
    pub async fn verify(&self) -> Result<()> {
        let mut stream = self.stream();
        let mut hasher = Sha256::new();
        while let Some(bytes) = stream.next().await? {
            hasher.update(&bytes);
        }
        let actual = ContentId::from_sha256(hasher.finalize().into());
        if actual != self.descriptor.content_id() {
            return Err(Error::ContentDigestMismatch {
                expected: self.descriptor.content_id().to_string(),
                actual: actual.to_string(),
            });
        }
        Ok(())
    }
}

/// Sequential verified reader over one [`ContentHandle`].
#[derive(Debug, Clone)]
pub struct ContentStream {
    handle: ContentHandle,
    position: u64,
}

impl ContentStream {
    /// Returns the next verified chunk, or `None` at EOF.
    ///
    /// Each returned value is at most the upload's configured chunk size. The
    /// stream therefore does not allocate in proportion to total content size.
    ///
    /// # Errors
    ///
    /// Propagates the same storage, format, and integrity errors as
    /// [`ContentHandle::read_range`].
    pub async fn next(&mut self) -> Result<Option<Arc<[u8]>>> {
        if self.position == self.handle.len() {
            return Ok(None);
        }
        let remaining = self.handle.len() - self.position;
        let length = remaining.min(u64::from(self.handle.descriptor.chunk_bytes()));
        let bytes = self.handle.read_range(self.position, length).await?;
        self.position =
            self.position
                .checked_add(u64::try_from(bytes.len()).map_err(|_| {
                    Error::invalid_options("content stream chunk length exceeds u64")
                })?)
                .ok_or_else(|| Error::invalid_options("content stream position overflow"))?;
        Ok(Some(bytes))
    }

    /// Returns the next unread original-byte offset.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }
}
