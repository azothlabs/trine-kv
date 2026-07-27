use super::{
    Arc, CONTENT_ACCESS_BARRIER_MAGIC, CONTENT_ACCESS_COORDINATE_MAGIC, ContentAccessBarrierId,
    Error, Result, StorageDomainId, array_at,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentAccessBarrierRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) barrier_id: ContentAccessBarrierId,
}

impl ContentAccessBarrierRecord {
    pub(crate) fn encode(self) -> Arc<[u8]> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16);
        bytes.extend_from_slice(CONTENT_ACCESS_BARRIER_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        bytes.extend_from_slice(&self.barrier_id.to_bytes());
        bytes.into()
    }

    pub(crate) fn decode(bytes: &[u8], storage_domain_id: StorageDomainId) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_ACCESS_BARRIER_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content access-barrier header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content access-barrier storage domain",
        )?);
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content access-barrier identity",
        )?)?;
        if stored_domain != storage_domain_id {
            return Err(Error::Corruption {
                message: "content access-barrier record differs from its storage path".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            barrier_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentAccessCoordinateRecord {
    pub(crate) storage_domain_id: StorageDomainId,
    pub(crate) barrier_id: ContentAccessBarrierId,
    pub(crate) enforced_at: crate::ReadVersion,
}

impl ContentAccessCoordinateRecord {
    pub(crate) fn commit_prefix(
        storage_domain_id: StorageDomainId,
        barrier_id: ContentAccessBarrierId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + 16 + 16);
        bytes.extend_from_slice(CONTENT_ACCESS_COORDINATE_MAGIC);
        bytes.extend_from_slice(&storage_domain_id.to_bytes());
        bytes.extend_from_slice(&barrier_id.to_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], storage_domain_id: StorageDomainId) -> Result<Self> {
        const RECORD_LEN: usize = 8 + 16 + 16 + 8;
        if bytes.len() != RECORD_LEN || bytes.get(..8) != Some(CONTENT_ACCESS_COORDINATE_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content access-coordinate header or length".to_owned(),
            });
        }
        let stored_domain = StorageDomainId::from_bytes(array_at::<16>(
            bytes,
            8,
            "content access-coordinate storage domain",
        )?);
        let barrier_id = ContentAccessBarrierId::from_bytes(array_at::<16>(
            bytes,
            24,
            "content access-coordinate barrier identity",
        )?)?;
        let enforced_at = crate::ReadVersion::from_u64(u64::from_be_bytes(array_at::<8>(
            bytes,
            40,
            "content access-coordinate commit sequence",
        )?));
        if stored_domain != storage_domain_id || enforced_at.as_u64() == 0 {
            return Err(Error::Corruption {
                message: "content access-coordinate has invalid protected coordinates".to_owned(),
            });
        }
        Ok(Self {
            storage_domain_id,
            barrier_id,
            enforced_at,
        })
    }
}

pub(crate) fn content_access_coordinate_key(storage_domain_id: StorageDomainId) -> Vec<u8> {
    let mut key = Vec::with_capacity(7 + 16);
    key.extend_from_slice(b"access:");
    key.extend_from_slice(&storage_domain_id.to_bytes());
    key
}
