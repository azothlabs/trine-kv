use super::{
    CONTENT_PHYSICAL_ACCOUNT_KEY, CONTENT_PHYSICAL_ACCOUNT_MAGIC, CONTENT_PHYSICAL_QUOTA_KEY,
    CONTENT_PHYSICAL_QUOTA_MAGIC, CONTENT_PHYSICAL_RESERVATION_KEY,
    CONTENT_PHYSICAL_RESERVATION_MAGIC, ContentId, Error, Result, UploadId, array_at, fmt,
    write_hex,
};

/// Opaque control-plane identity for one physical content boundary.
///
/// Deduplication, encryption, physical quota, and reclamation are scoped to
/// this identity. Trine KV compares and persists the bytes but does not parse
/// tenant or project semantics from them.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageDomainId(pub(in crate::content) [u8; 16]);

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
    pub(in crate::content) storage_domain_id: StorageDomainId,
    pub(in crate::content) unique_content_bytes: u64,
    pub(in crate::content) upload_reserved_bytes: u64,
    pub(in crate::content) limit: Option<u64>,
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
