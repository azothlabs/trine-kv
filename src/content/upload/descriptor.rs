use super::{
    Arc, CHUNK_HEADER_LEN, CHUNK_MAGIC, ContentHashAlgorithm, ContentId, DESCRIPTOR_LEN,
    DESCRIPTOR_MAGIC, Digest, Error, MAX_CHUNK_BYTES, MIN_CHUNK_BYTES, Result, Sha256,
    StorageDomainId, UploadId, array_at, digest_string,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContentDescriptor {
    pub(in crate::content) storage_domain_id: StorageDomainId,
    pub(in crate::content) content_id: ContentId,
    pub(in crate::content) upload_id: UploadId,
    pub(in crate::content) length: u64,
    pub(in crate::content) chunk_bytes: u32,
    pub(in crate::content) chunk_count: u64,
}

impl ContentDescriptor {
    pub(crate) fn new(
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        upload_id: UploadId,
        length: u64,
        chunk_bytes: usize,
        chunk_count: u64,
    ) -> Result<Self> {
        let chunk_bytes = u32::try_from(chunk_bytes)
            .map_err(|_| Error::invalid_options("content chunk size exceeds u32"))?;
        Ok(Self {
            storage_domain_id,
            content_id,
            upload_id,
            length,
            chunk_bytes,
            chunk_count,
        })
    }

    pub(crate) const fn upload_id(self) -> UploadId {
        self.upload_id
    }

    pub(crate) const fn content_id(self) -> ContentId {
        self.content_id
    }

    pub(crate) const fn length(self) -> u64 {
        self.length
    }

    pub(crate) const fn chunk_bytes(self) -> u32 {
        self.chunk_bytes
    }

    pub(crate) const fn chunk_count(self) -> u64 {
        self.chunk_count
    }

    pub(crate) fn encode(self) -> Arc<[u8]> {
        let mut bytes = Vec::with_capacity(DESCRIPTOR_LEN);
        bytes.extend_from_slice(DESCRIPTOR_MAGIC);
        bytes.extend_from_slice(&self.storage_domain_id.to_bytes());
        self.content_id.encode_into(&mut bytes);
        bytes.extend_from_slice(&self.upload_id.bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_count.to_le_bytes());
        Arc::from(bytes)
    }

    pub(crate) fn decode(
        bytes: &[u8],
        expected_domain: StorageDomainId,
        expected_content: ContentId,
    ) -> Result<Self> {
        if bytes.len() != DESCRIPTOR_LEN || bytes.get(..8) != Some(DESCRIPTOR_MAGIC) {
            return Err(Error::InvalidFormat {
                message: "invalid content descriptor header or length".to_owned(),
            });
        }
        let storage_domain_id = StorageDomainId(array_at::<16>(
            bytes,
            8,
            "content descriptor storage domain",
        )?);
        if storage_domain_id != expected_domain {
            return Err(Error::InvalidFormat {
                message: "content descriptor storage domain mismatch".to_owned(),
            });
        }
        let algorithm = ContentHashAlgorithm::from_tag(bytes[24])?;
        let digest = array_at::<32>(bytes, 25, "content descriptor digest")?;
        let content_id = ContentId { algorithm, digest };
        if content_id != expected_content {
            return Err(Error::ContentDigestMismatch {
                expected: expected_content.to_string(),
                actual: content_id.to_string(),
            });
        }
        let upload_id = UploadId(array_at::<16>(bytes, 57, "content descriptor upload id")?);
        let length = u64::from_le_bytes(array_at::<8>(bytes, 73, "content descriptor length")?);
        let chunk_bytes =
            u32::from_le_bytes(array_at::<4>(bytes, 81, "content descriptor chunk size")?);
        let chunk_count =
            u64::from_le_bytes(array_at::<8>(bytes, 85, "content descriptor chunk count")?);
        if !(MIN_CHUNK_BYTES..=MAX_CHUNK_BYTES).contains(&usize::try_from(chunk_bytes).map_err(
            |_| Error::InvalidFormat {
                message: "content descriptor chunk size exceeds usize".to_owned(),
            },
        )?) {
            return Err(Error::InvalidFormat {
                message: format!("invalid content descriptor chunk size {chunk_bytes}"),
            });
        }
        let expected_chunks = if length == 0 {
            0
        } else {
            length.div_ceil(u64::from(chunk_bytes))
        };
        if chunk_count != expected_chunks {
            return Err(Error::InvalidFormat {
                message: format!(
                    "content descriptor chunk count {chunk_count} does not match {expected_chunks}"
                ),
            });
        }
        Ok(Self {
            storage_domain_id,
            content_id,
            upload_id,
            length,
            chunk_bytes,
            chunk_count,
        })
    }
}

pub(super) fn encode_chunk(upload_id: UploadId, index: u64, payload: &[u8]) -> Result<Arc<[u8]>> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| Error::invalid_options("content chunk payload exceeds u32"))?;
    let digest: [u8; 32] = Sha256::digest(payload).into();
    let mut bytes = Vec::with_capacity(CHUNK_HEADER_LEN + payload.len());
    bytes.extend_from_slice(CHUNK_MAGIC);
    bytes.extend_from_slice(&upload_id.bytes());
    bytes.extend_from_slice(&index.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&digest);
    bytes.extend_from_slice(payload);
    Ok(Arc::from(bytes))
}

pub(crate) fn decode_chunk(
    bytes: &[u8],
    expected_upload: UploadId,
    expected_index: u64,
) -> Result<&[u8]> {
    if bytes.len() < CHUNK_HEADER_LEN || bytes.get(..8) != Some(CHUNK_MAGIC) {
        return Err(Error::InvalidFormat {
            message: format!("invalid content chunk {expected_index} header"),
        });
    }
    let upload_id = UploadId(array_at::<16>(bytes, 8, "content chunk upload id")?);
    let index = u64::from_le_bytes(array_at::<8>(bytes, 24, "content chunk index")?);
    let payload_len = usize::try_from(u32::from_le_bytes(array_at::<4>(
        bytes,
        32,
        "content chunk payload length",
    )?))
    .map_err(|_| Error::InvalidFormat {
        message: "content chunk payload length exceeds usize".to_owned(),
    })?;
    let expected_digest = array_at::<32>(bytes, 36, "content chunk digest")?;
    if upload_id != expected_upload || index != expected_index {
        return Err(Error::InvalidFormat {
            message: format!("content chunk identity mismatch at index {expected_index}"),
        });
    }
    let payload = bytes
        .get(CHUNK_HEADER_LEN..)
        .ok_or_else(|| Error::InvalidFormat {
            message: format!("content chunk {expected_index} payload is missing"),
        })?;
    if payload.len() != payload_len {
        return Err(Error::InvalidFormat {
            message: format!(
                "content chunk {expected_index} length {} does not match {payload_len}",
                payload.len()
            ),
        });
    }
    let actual_digest: [u8; 32] = Sha256::digest(payload).into();
    if actual_digest != expected_digest {
        return Err(Error::ContentDigestMismatch {
            expected: digest_string(expected_digest),
            actual: digest_string(actual_digest),
        });
    }
    Ok(payload)
}
