use super::{
    CONTENT_ID_SHA256_TAG, CONTENT_LEASE_ID_VERSION, CONTENT_PHYSICAL_ACCOUNT_KEY,
    CONTENT_PHYSICAL_ACCOUNT_MAGIC, CONTENT_PHYSICAL_HOLD_ID_VERSION, CONTENT_PHYSICAL_QUOTA_KEY,
    CONTENT_PHYSICAL_QUOTA_MAGIC, CONTENT_PHYSICAL_RESERVATION_KEY,
    CONTENT_PHYSICAL_RESERVATION_MAGIC, Digest, DurabilityMode, Duration, Error, MAX_CHUNK_BYTES,
    MIN_CHUNK_BYTES, Result, Sha256, UPLOAD_TOKEN_VERSION, array_at, duration_millis, fmt,
    write_hex,
};

/// Opaque control-plane identity for one physical content boundary.
///
/// Deduplication, encryption, physical quota, and reclamation are scoped to
/// this identity. Trine KV compares and persists the bytes but does not parse
/// tenant or project semantics from them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageDomainId(pub(super) [u8; 16]);

impl StorageDomainId {
    /// Reconstructs an identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for StorageDomainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StorageDomainId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for StorageDomainId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Durable physical content-byte accounting for one storage domain.
///
/// `unique_content_bytes` counts original bytes for sealed unique
/// `ContentObject`s. `upload_reserved_bytes` counts conservative reservations
/// for open or sealing uploads. Framing, encryption, provider-version and
/// replica overhead are not included in this v1 counter and must be budgeted by
/// the deployment separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentPhysicalQuota {
    pub(super) storage_domain_id: StorageDomainId,
    pub(super) unique_content_bytes: u64,
    pub(super) upload_reserved_bytes: u64,
    pub(super) limit: Option<u64>,
}

impl ContentPhysicalQuota {
    pub(crate) const fn new(
        storage_domain_id: StorageDomainId,
        unique_content_bytes: u64,
        upload_reserved_bytes: u64,
        limit: Option<u64>,
    ) -> Self {
        Self {
            storage_domain_id,
            unique_content_bytes,
            upload_reserved_bytes,
            limit,
        }
    }

    /// Returns the physical deduplication and accounting boundary.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns original bytes held by unique sealed content descriptors.
    #[must_use]
    pub const fn unique_content_bytes(self) -> u64 {
        self.unique_content_bytes
    }

    /// Returns bytes conservatively reserved by unfinished uploads.
    #[must_use]
    pub const fn upload_reserved_bytes(self) -> u64 {
        self.upload_reserved_bytes
    }

    /// Returns the sum checked against the configured limit.
    #[must_use]
    pub const fn accounted_bytes(self) -> u64 {
        self.unique_content_bytes
            .saturating_add(self.upload_reserved_bytes)
    }

    /// Returns the inclusive original-byte limit, or `None` when disabled.
    #[must_use]
    pub const fn limit(self) -> Option<u64> {
        self.limit
    }

    /// Returns remaining capacity when a limit is configured.
    #[must_use]
    pub const fn remaining(self) -> Option<u64> {
        match self.limit {
            Some(limit) => Some(limit.saturating_sub(self.accounted_bytes())),
            None => None,
        }
    }

    pub(crate) fn with_limit(self, limit: Option<u64>) -> Self {
        Self { limit, ..self }
    }

    pub(crate) fn with_counts(self, unique: u64, reserved: u64) -> Self {
        Self {
            unique_content_bytes: unique,
            upload_reserved_bytes: reserved,
            ..self
        }
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(49);
        bytes.extend_from_slice(CONTENT_PHYSICAL_QUOTA_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.unique_content_bytes.to_be_bytes());
        bytes.extend_from_slice(&self.upload_reserved_bytes.to_be_bytes());
        if let Some(limit) = self.limit {
            bytes.push(1);
            bytes.extend_from_slice(&limit.to_be_bytes());
        } else {
            bytes.push(0);
            bytes.extend_from_slice(&0_u64.to_be_bytes());
        }
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], storage_domain_id: StorageDomainId) -> Result<Self> {
        if bytes.len() != 49 || bytes.get(..8) != Some(CONTENT_PHYSICAL_QUOTA_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content physical quota record".to_owned(),
            });
        }
        let stored =
            StorageDomainId::from_bytes(array_at::<16>(bytes, 8, "physical quota storage domain")?);
        if stored != storage_domain_id {
            return Err(Error::InvalidFormat {
                message: "content physical quota identity differs from its key".to_owned(),
            });
        }
        let unique_content_bytes =
            u64::from_be_bytes(array_at::<8>(bytes, 24, "physical quota unique bytes")?);
        let upload_reserved_bytes =
            u64::from_be_bytes(array_at::<8>(bytes, 32, "physical quota reserved bytes")?);
        let limit_value = u64::from_be_bytes(array_at::<8>(bytes, 41, "physical quota limit")?);
        let limit = match bytes[40] {
            0 if limit_value == 0 => None,
            1 => Some(limit_value),
            _ => {
                return Err(Error::InvalidFormat {
                    message: "invalid content physical quota limit option".to_owned(),
                });
            }
        };
        unique_content_bytes
            .checked_add(upload_reserved_bytes)
            .ok_or_else(|| Error::InvalidFormat {
                message: "content physical quota counters overflow".to_owned(),
            })?;
        Ok(Self::new(
            storage_domain_id,
            unique_content_bytes,
            upload_reserved_bytes,
            limit,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentPhysicalReservationRecord {
    pub(crate) upload_id: UploadId,
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) reserved_bytes: u64,
}

impl ContentPhysicalReservationRecord {
    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(48);
        bytes.extend_from_slice(CONTENT_PHYSICAL_RESERVATION_MAGIC);
        bytes.extend_from_slice(&self.upload_id.to_bytes());
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.reserved_bytes.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], upload_id: UploadId) -> Result<Self> {
        if bytes.len() != 48 || bytes.get(..8) != Some(CONTENT_PHYSICAL_RESERVATION_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content physical reservation record".to_owned(),
            });
        }
        let stored_upload =
            UploadId::from_bytes(array_at::<16>(bytes, 8, "physical reservation upload id")?);
        if stored_upload != upload_id {
            return Err(Error::InvalidFormat {
                message: "content physical reservation identity differs from its key".to_owned(),
            });
        }
        Ok(Self {
            upload_id,
            storage_domain_id: StorageDomainId::from_bytes(array_at::<16>(
                bytes,
                24,
                "physical reservation storage domain",
            )?),
            reserved_bytes: u64::from_be_bytes(array_at::<8>(
                bytes,
                40,
                "physical reservation bytes",
            )?),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentPhysicalAccountRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) content_id: ContentId,
    pub(crate) original_bytes: u64,
}

impl ContentPhysicalAccountRecord {
    pub(crate) fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(65);
        bytes.extend_from_slice(CONTENT_PHYSICAL_ACCOUNT_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.content_id.to_bytes());
        bytes.extend_from_slice(&self.original_bytes.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(
        bytes: &[u8],
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Self> {
        if bytes.len() != 65 || bytes.get(..8) != Some(CONTENT_PHYSICAL_ACCOUNT_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content physical account record".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "physical account storage domain",
        )?);
        let stored_content =
            ContentId::from_bytes(array_at::<33>(bytes, 24, "physical account content id")?)?;
        if stored_domain != storage_domain_id || stored_content != content_id {
            return Err(Error::InvalidFormat {
                message: "content physical account identity differs from its key".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            original_bytes: u64::from_be_bytes(array_at::<8>(
                bytes,
                57,
                "physical account original bytes",
            )?),
        })
    }
}

pub(crate) fn content_physical_quota_key(storage_domain_id: StorageDomainId) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(CONTENT_PHYSICAL_QUOTA_KEY);
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key
}

pub(crate) fn content_physical_reservation_key(upload_id: UploadId) -> Vec<u8> {
    let mut key = Vec::with_capacity(17);
    key.push(CONTENT_PHYSICAL_RESERVATION_KEY);
    key.extend_from_slice(&upload_id.to_bytes());
    key
}

pub(crate) fn content_physical_account_key(
    storage_domain_id: StorageDomainId,
    content_id: ContentId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(50);
    key.push(CONTENT_PHYSICAL_ACCOUNT_KEY);
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key.extend_from_slice(&content_id.to_bytes());
    key
}

/// Opaque authenticated owner scope supplied by the database layer.
///
/// This identity can represent a project, tenant, or another authorization
/// boundary. Trine KV only requires exact equality when a token is consumed.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OwnerScopeId(pub(super) [u8; 16]);

impl OwnerScopeId {
    /// Reconstructs an identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for OwnerScopeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OwnerScopeId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Opaque higher-layer owner of one short-lived content read lease.
///
/// Trine KV persists and compares this identity but assigns it no Principal,
/// tenant, or Policy meaning. The caller must authorize before opening or
/// renewing a leased handle.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentLeaseOwnerId(pub(super) [u8; 16]);

impl ContentLeaseOwnerId {
    /// Reconstructs an opaque lease owner from portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the opaque owner bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentLeaseOwnerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentLeaseOwnerId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Generated identity of one durable short-lived content read lease.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentLeaseId(pub(super) [u8; 16]);

impl ContentLeaseId {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("content lease entropy: {error}")))?;
        bytes[0] = CONTENT_LEASE_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned lease identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the first byte carries an
    /// unknown identity format.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_LEASE_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!("unsupported content lease identity version {}", bytes[0]),
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the versioned portable identity bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentLeaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentLeaseId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ContentLeaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Options for opening a sealed `ContentObject` under a short-lived read lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentLeaseOptions {
    pub(super) owner_id: ContentLeaseOwnerId,
    pub(super) ttl: Duration,
}

impl ContentLeaseOptions {
    /// Creates lease options for an already-authorized opaque owner.
    ///
    /// `ttl` is rounded down to whole milliseconds and must be at least one
    /// millisecond. The deadline is computed during leased-open acquisition,
    /// immediately before Trine KV stages the durable record.
    #[must_use]
    pub const fn new(owner_id: ContentLeaseOwnerId, ttl: Duration) -> Self {
        Self { owner_id, ttl }
    }

    /// Returns the opaque higher-layer owner identity.
    #[must_use]
    pub const fn owner_id(self) -> ContentLeaseOwnerId {
        self.owner_id
    }

    /// Returns the requested lease lifetime.
    #[must_use]
    pub const fn ttl(self) -> Duration {
        self.ttl
    }

    pub(crate) fn ttl_ms(self) -> Result<u64> {
        let millis = u64::try_from(self.ttl.as_millis()).map_err(|_| {
            Error::invalid_options("content lease lifetime milliseconds exceed u64::MAX")
        })?;
        if millis == 0 {
            return Err(Error::invalid_options(
                "content lease lifetime must be at least one millisecond",
            ));
        }
        Ok(millis)
    }
}

/// Classifies why physical content bytes must remain available.
///
/// The class is operational metadata, not authorization. Every class enters
/// the same reclaim fence; callers must not infer weaker protection from one
/// variant than another.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ContentPhysicalHoldKind {
    /// A storage migration still reads or copies a representation.
    Migration,
    /// A backup workflow has not durably completed its copy.
    Backup,
    /// A repair workflow needs the current bytes or replicas.
    Repair,
    /// A storage provider operation still references provider-side objects.
    Provider,
    /// An explicit operator or compliance hold remains in force.
    Administrative,
    /// Durable asynchronous processing still depends on the source bytes.
    Processing,
    /// A verified device-local offline cache entry must remain readable.
    Offline,
}

impl ContentPhysicalHoldKind {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::Migration => 1,
            Self::Backup => 2,
            Self::Repair => 3,
            Self::Provider => 4,
            Self::Administrative => 5,
            Self::Processing => 6,
            Self::Offline => 7,
        }
    }

    pub(super) fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            1 => Ok(Self::Migration),
            2 => Ok(Self::Backup),
            3 => Ok(Self::Repair),
            4 => Ok(Self::Provider),
            5 => Ok(Self::Administrative),
            6 => Ok(Self::Processing),
            7 => Ok(Self::Offline),
            _ => Err(Error::UnsupportedFormat {
                message: format!("unsupported content physical-hold kind {tag}"),
            }),
        }
    }
}

impl fmt::Display for ContentPhysicalHoldKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Migration => "migration",
            Self::Backup => "backup",
            Self::Repair => "repair",
            Self::Provider => "provider",
            Self::Administrative => "administrative",
            Self::Processing => "processing",
            Self::Offline => "offline",
        })
    }
}

/// Generated identity of one durable physical content hold.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentPhysicalHoldId(pub(super) [u8; 16]);

impl ContentPhysicalHoldId {
    /// Generates a new versioned hold identity from operating-system entropy.
    ///
    /// Callers should durably retain this identity before acquiring an
    /// until-released hold. Passing the same identity to acquisition after a
    /// lost response makes the operation idempotent and recoverable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when secure entropy is unavailable.
    pub fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|error| {
            Error::runtime_busy(format!("content physical-hold entropy: {error}"))
        })?;
        bytes[0] = CONTENT_PHYSICAL_HOLD_ID_VERSION;
        Ok(Self(bytes))
    }

    /// Decodes the versioned portable hold identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when byte zero names an unknown
    /// identity format.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self> {
        if bytes[0] != CONTENT_PHYSICAL_HOLD_ID_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!(
                    "unsupported content physical-hold identity version {}",
                    bytes[0]
                ),
            });
        }
        Ok(Self(bytes))
    }

    /// Returns the versioned portable identity bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentPhysicalHoldId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentPhysicalHoldId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

impl fmt::Display for ContentPhysicalHoldId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Opaque higher-layer owner of a physical content hold.
///
/// Trine KV compares this identity during resume, renewal, and release but does
/// not interpret it as a Principal, tenant, workflow, or Policy identity.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentPhysicalHoldOwnerId(pub(super) [u8; 16]);

impl ContentPhysicalHoldOwnerId {
    /// Reconstructs an opaque owner from portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the opaque owner bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentPhysicalHoldOwnerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentPhysicalHoldOwnerId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContentPhysicalHoldLifetime {
    Expiring(Duration),
    UntilReleased,
}

/// Options for acquiring one durable physical content hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentPhysicalHoldOptions {
    pub(super) kind: ContentPhysicalHoldKind,
    pub(super) owner_id: ContentPhysicalHoldOwnerId,
    pub(super) lifetime: ContentPhysicalHoldLifetime,
}

impl ContentPhysicalHoldOptions {
    /// Creates an expiring hold for an already-authorized workflow.
    ///
    /// `ttl` is rounded down to whole milliseconds and must retain at least one
    /// millisecond. An expired hold is inert and cannot be renewed.
    #[must_use]
    pub const fn expiring(
        kind: ContentPhysicalHoldKind,
        owner_id: ContentPhysicalHoldOwnerId,
        ttl: Duration,
    ) -> Self {
        Self {
            kind,
            owner_id,
            lifetime: ContentPhysicalHoldLifetime::Expiring(ttl),
        }
    }

    /// Creates a hold that remains active until an explicit durable release.
    ///
    /// This form is appropriate only when the owner has a recovery path that
    /// can resume and release the hold after a process crash.
    #[must_use]
    pub const fn until_released(
        kind: ContentPhysicalHoldKind,
        owner_id: ContentPhysicalHoldOwnerId,
    ) -> Self {
        Self {
            kind,
            owner_id,
            lifetime: ContentPhysicalHoldLifetime::UntilReleased,
        }
    }

    /// Returns the operational hold class.
    #[must_use]
    pub const fn kind(self) -> ContentPhysicalHoldKind {
        self.kind
    }

    /// Returns the opaque higher-layer owner.
    #[must_use]
    pub const fn owner_id(self) -> ContentPhysicalHoldOwnerId {
        self.owner_id
    }

    /// Returns the requested expiring lifetime, or `None` for explicit release.
    #[must_use]
    pub const fn ttl(self) -> Option<Duration> {
        match self.lifetime {
            ContentPhysicalHoldLifetime::Expiring(ttl) => Some(ttl),
            ContentPhysicalHoldLifetime::UntilReleased => None,
        }
    }

    pub(crate) fn expires_at_unix_ms(self, now_unix_ms: u64) -> Result<u64> {
        let ContentPhysicalHoldLifetime::Expiring(ttl) = self.lifetime else {
            return Ok(0);
        };
        let ttl_ms = duration_millis(ttl, "content physical-hold lifetime")?;
        now_unix_ms
            .checked_add(ttl_ms)
            .ok_or_else(|| Error::invalid_options("content physical-hold expiry overflow"))
    }
}

/// Scope that an attachment token is issued to and later verified against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ContentAttachmentScope {
    pub(super) storage_domain_id: StorageDomainId,
    pub(super) owner_scope_id: OwnerScopeId,
}

impl ContentAttachmentScope {
    /// Creates an exact storage-domain and owner binding.
    #[must_use]
    pub const fn new(storage_domain_id: StorageDomainId, owner_scope_id: OwnerScopeId) -> Self {
        Self {
            storage_domain_id,
            owner_scope_id,
        }
    }

    /// Returns the physical content boundary.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.storage_domain_id
    }

    /// Returns the authenticated owner boundary.
    #[must_use]
    pub const fn owner_scope_id(self) -> OwnerScopeId {
        self.owner_scope_id
    }
}

/// Opaque bearer authority returned only after an upload is sealed.
///
/// The 32 random bytes are secret. Logging implementations deliberately redact
/// them; callers that persist or transmit a token should protect it like any
/// other short-lived capability.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UploadToken(pub(super) [u8; 32]);

impl UploadToken {
    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("upload token entropy: {error}")))?;
        Ok(Self(bytes))
    }

    pub(crate) const fn secret(self) -> [u8; 32] {
        self.0
    }

    /// Decodes the versioned 33-byte bearer representation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the first byte names an
    /// unknown token format version.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        if bytes[0] != UPLOAD_TOKEN_VERSION {
            return Err(Error::UnsupportedFormat {
                message: format!("unsupported upload token version {}", bytes[0]),
            });
        }
        let mut secret = [0_u8; 32];
        secret.copy_from_slice(&bytes[1..]);
        Ok(Self(secret))
    }

    /// Returns the versioned 33-byte bearer representation.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 33] {
        let mut bytes = [0_u8; 33];
        bytes[0] = UPLOAD_TOKEN_VERSION;
        bytes[1..].copy_from_slice(&self.0);
        bytes
    }
}

impl fmt::Debug for UploadToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UploadToken([REDACTED])")
    }
}

/// Opaque database-layer change identity used for idempotent consumption.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentChangeId(pub(super) [u8; 16]);

impl ContentChangeId {
    /// Reconstructs a change identity from its portable bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for ContentChangeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContentChangeId(")?;
        write_hex(formatter, &self.0)?;
        formatter.write_str(")")
    }
}

/// Cryptographic algorithm carried by a [`ContentId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum ContentHashAlgorithm {
    /// SHA-256 over the complete original byte sequence.
    Sha256,
}

impl ContentHashAlgorithm {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::Sha256 => CONTENT_ID_SHA256_TAG,
        }
    }

    pub(super) fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            CONTENT_ID_SHA256_TAG => Ok(Self::Sha256),
            _ => Err(Error::UnsupportedFormat {
                message: format!("unsupported content hash algorithm tag {tag}"),
            }),
        }
    }
}

/// Algorithm-tagged identity of one immutable original byte sequence.
///
/// Equality means byte identity under the carried cryptographic algorithm. It
/// does not identify a file name, owner, directory entry, or physical object.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentId {
    pub(super) algorithm: ContentHashAlgorithm,
    pub(super) digest: [u8; 32],
}

impl ContentId {
    /// Decodes the portable 33-byte content identity.
    ///
    /// Byte zero selects the digest algorithm and the remaining 32 bytes carry
    /// its digest. Unknown algorithm tags fail closed so stored catalog values
    /// are never reinterpreted under a different hash scheme.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnsupportedFormat`] when the algorithm tag is not
    /// supported by this build.
    pub fn from_bytes(bytes: [u8; 33]) -> Result<Self> {
        let algorithm = ContentHashAlgorithm::from_tag(bytes[0])?;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&bytes[1..]);
        Ok(Self { algorithm, digest })
    }

    /// Returns the portable algorithm-tagged 33-byte content identity.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 33] {
        let mut bytes = [0_u8; 33];
        bytes[0] = self.algorithm.tag();
        bytes[1..].copy_from_slice(&self.digest);
        bytes
    }

    /// Computes the identity of an in-memory byte slice.
    ///
    /// Incremental uploads compute the same value without retaining all bytes;
    /// this convenience constructor is intended for expected-digest checks and
    /// small inputs.
    #[must_use]
    pub fn for_bytes(bytes: &[u8]) -> Self {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        Self {
            algorithm: ContentHashAlgorithm::Sha256,
            digest,
        }
    }

    /// Returns the hash algorithm carried by this identity.
    #[must_use]
    pub const fn algorithm(self) -> ContentHashAlgorithm {
        self.algorithm
    }

    /// Returns the 32-byte digest without its algorithm tag.
    #[must_use]
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }

    pub(crate) fn from_sha256(digest: [u8; 32]) -> Self {
        Self {
            algorithm: ContentHashAlgorithm::Sha256,
            digest,
        }
    }

    pub(super) fn encode_into(self, bytes: &mut Vec<u8>) {
        bytes.push(self.algorithm.tag());
        bytes.extend_from_slice(&self.digest);
    }
}

impl fmt::Debug for ContentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ContentId({self})")
    }
}

impl fmt::Display for ContentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("sha256:")?;
        write_hex(formatter, &self.digest)
    }
}

/// Globally random identity of one upload attempt.
///
/// Upload identity is temporary transfer state. It is deliberately distinct
/// from [`ContentId`], which is known only after the complete byte sequence has
/// been hashed or supplied and verified.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UploadId(pub(super) [u8; 16]);

impl UploadId {
    /// Creates a new random upload identity for caller-controlled idempotency.
    ///
    /// Generate and persist this identity before starting a transfer when an
    /// uncertain response must be retried through
    /// [`Db::begin_content_upload_with_id`](crate::Db::begin_content_upload_with_id).
    /// The identity is not secret and does not itself authorize content access.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RuntimeBusy`] when the operating system cannot provide
    /// cryptographic randomness. No durable state is created by this method.
    pub fn new() -> Result<Self> {
        Self::generate()
    }

    pub(crate) fn generate() -> Result<Self> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes)
            .map_err(|error| Error::runtime_busy(format!("upload identity entropy: {error}")))?;
        Ok(Self(bytes))
    }

    pub(crate) const fn bytes(self) -> [u8; 16] {
        self.0
    }

    /// Reconstructs an upload identity from its portable 16-byte form.
    ///
    /// Upload identities are not secrets. Persist this value when an upload
    /// must be resumed after the current process or database handle is gone.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the portable 16-byte representation.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for UploadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "UploadId({self})")
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_hex(formatter, &self.0)
    }
}

/// Configuration for a sequential content upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentUploadOptions {
    pub(super) attachment_scope: ContentAttachmentScope,
    pub(super) token_ttl: Duration,
    pub(super) chunk_bytes: usize,
    pub(super) expected_length: Option<u64>,
    pub(super) expected_content_id: Option<ContentId>,
}

impl ContentUploadOptions {
    /// Default chunk size used by uploads and sequential reads.
    pub const DEFAULT_CHUNK_BYTES: usize = 4 * 1024 * 1024;

    /// Creates options with an explicit attachment scope and token lifetime.
    ///
    /// The token lifetime starts when seal first reaches its durable sealing
    /// state, not when the upload begins. A zero or sub-millisecond lifetime is
    /// rejected by [`Db::begin_content_upload`](crate::Db::begin_content_upload).
    /// The default chunk bound is 4 MiB and no final identity is expected.
    #[must_use]
    pub const fn new(attachment_scope: ContentAttachmentScope, token_ttl: Duration) -> Self {
        Self {
            attachment_scope,
            token_ttl,
            chunk_bytes: Self::DEFAULT_CHUNK_BYTES,
            expected_length: None,
            expected_content_id: None,
        }
    }

    /// Sets the maximum unsealed payload bytes retained by the upload.
    ///
    /// Valid values are 64 KiB through 16 MiB, inclusive. The value also fixes
    /// chunk boundaries in the sealed descriptor. Invalid values are reported
    /// by [`Db::begin_content_upload`](crate::Db::begin_content_upload).
    #[must_use]
    pub const fn with_chunk_bytes(mut self, chunk_bytes: usize) -> Self {
        self.chunk_bytes = chunk_bytes;
        self
    }

    /// Requires the final original byte length to equal `expected_length`.
    #[must_use]
    pub const fn with_expected_length(mut self, expected_length: u64) -> Self {
        self.expected_length = Some(expected_length);
        self
    }

    /// Requires the final complete digest to equal `expected_content_id`.
    #[must_use]
    pub const fn with_expected_content_id(mut self, expected_content_id: ContentId) -> Self {
        self.expected_content_id = Some(expected_content_id);
        self
    }

    /// Returns the configured chunk bound in bytes.
    #[must_use]
    pub const fn chunk_bytes(self) -> usize {
        self.chunk_bytes
    }

    /// Returns the scope that the sealed token will be bound to.
    #[must_use]
    pub const fn attachment_scope(self) -> ContentAttachmentScope {
        self.attachment_scope
    }

    /// Returns the requested retry lifetime starting at seal.
    #[must_use]
    pub const fn token_ttl(self) -> Duration {
        self.token_ttl
    }

    pub(crate) fn validate(self) -> Result<Self> {
        if !(MIN_CHUNK_BYTES..=MAX_CHUNK_BYTES).contains(&self.chunk_bytes) {
            return Err(Error::invalid_options(format!(
                "content chunk size {} is outside {MIN_CHUNK_BYTES}..={MAX_CHUNK_BYTES}",
                self.chunk_bytes
            )));
        }
        let ttl_ms = self.token_ttl.as_millis();
        if ttl_ms == 0 || ttl_ms > u128::from(u64::MAX) {
            return Err(Error::invalid_options(
                "upload token lifetime must be 1..=u64::MAX milliseconds",
            ));
        }
        Ok(self)
    }

    pub(crate) const fn expected_length(self) -> Option<u64> {
        self.expected_length
    }

    pub(crate) const fn expected_content_id(self) -> Option<ContentId> {
        self.expected_content_id
    }

    pub(crate) fn token_ttl_ms(self) -> Result<u64> {
        u64::try_from(self.token_ttl.as_millis()).map_err(|_| {
            Error::invalid_options("upload token lifetime milliseconds exceed u64::MAX")
        })
    }
}

/// Result of sealing one immutable content upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedContent {
    pub(super) attachment_scope: ContentAttachmentScope,
    pub(super) content_id: ContentId,
    pub(super) length: u64,
    pub(super) upload_token: UploadToken,
    pub(super) token_expires_at_unix_ms: u64,
    pub(super) durability: DurabilityMode,
}

impl SealedContent {
    /// Returns the verified identity of the complete original bytes.
    #[must_use]
    pub const fn content_id(self) -> ContentId {
        self.content_id
    }

    /// Returns the storage domain in which this content was sealed.
    #[must_use]
    pub const fn storage_domain_id(self) -> StorageDomainId {
        self.attachment_scope.storage_domain_id
    }

    /// Returns the exact domain and owner binding carried by the token.
    #[must_use]
    pub const fn attachment_scope(self) -> ContentAttachmentScope {
        self.attachment_scope
    }

    /// Returns the original byte length.
    #[must_use]
    pub const fn len(self) -> u64 {
        self.length
    }

    /// Returns whether the sealed content is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.length == 0
    }

    /// Returns the short-lived bearer authority for atomic attachment.
    #[must_use]
    pub const fn upload_token(self) -> UploadToken {
        self.upload_token
    }

    /// Returns the token deadline as Unix epoch milliseconds.
    ///
    /// An available token is invalid once the current time is greater than or
    /// equal to this value. A token already consumed by its `ChangeId` remains an
    /// idempotency record rather than reusable authority.
    #[must_use]
    pub const fn token_expires_at_unix_ms(self) -> u64 {
        self.token_expires_at_unix_ms
    }

    /// Returns the durability result satisfied before token issue.
    #[must_use]
    pub const fn durability(self) -> DurabilityMode {
        self.durability
    }
}

/// Claims verified while staging one upload-token consumption.
///
/// Returning this value does not itself attach content. The token transition
/// and the caller's catalog writes become durable together only if the owning
/// [`crate::Transaction`] commits successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentAttachment {
    pub(super) upload_id: UploadId,
    pub(super) scope: ContentAttachmentScope,
    pub(super) content_id: ContentId,
    pub(super) length: u64,
    pub(super) token_expires_at_unix_ms: u64,
    pub(super) durability: DurabilityMode,
}

impl ContentAttachment {
    pub(crate) const fn new(
        upload_id: UploadId,
        scope: ContentAttachmentScope,
        content_id: ContentId,
        length: u64,
        token_expires_at_unix_ms: u64,
        durability: DurabilityMode,
    ) -> Self {
        Self {
            upload_id,
            scope,
            content_id,
            length,
            token_expires_at_unix_ms,
            durability,
        }
    }

    /// Returns the upload attempt that issued the token.
    #[must_use]
    pub const fn upload_id(self) -> UploadId {
        self.upload_id
    }

    /// Returns the exact storage-domain and owner binding.
    #[must_use]
    pub const fn scope(self) -> ContentAttachmentScope {
        self.scope
    }

    /// Returns the immutable byte identity authorized for attachment.
    #[must_use]
    pub const fn content_id(self) -> ContentId {
        self.content_id
    }

    /// Returns the original byte length.
    #[must_use]
    pub const fn len(self) -> u64 {
        self.length
    }

    /// Returns whether the authorized original byte sequence is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.length == 0
    }

    /// Returns the original token deadline as Unix epoch milliseconds.
    #[must_use]
    pub const fn token_expires_at_unix_ms(self) -> u64 {
        self.token_expires_at_unix_ms
    }

    /// Returns the durability result that was satisfied before token issue.
    #[must_use]
    pub const fn durability(self) -> DurabilityMode {
        self.durability
    }
}
