use sha2::Digest;

use super::super::{
    ContentId, Db, Error, Result, Sha256, UploadId, UploadSessionState, UploadSessionStatus,
};

impl Db {
    /// Aborts durable upload state and schedules no content visibility.
    ///
    /// A durable `aborting` state is published before chunk cleanup, then
    /// replaced by a permanent retired-ID marker. A crash during cleanup can
    /// therefore be resumed without ever reopening the upload for writes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentUploadNotFound`] for an unknown identity,
    /// [`Error::ContentUploadSealed`] after seal, or a backend deletion error.
    pub async fn abort_content_upload(&self, upload_id: UploadId) -> Result<()> {
        self.abort_content_upload_at(upload_id, None).await
    }

    pub(crate) async fn abort_content_upload_at(
        &self,
        upload_id: UploadId,
        expected_revision: Option<u64>,
    ) -> Result<()> {
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let _upload = self.lock_content_upload(upload_id).await;
        let state = self.require_upload_state(upload_id).await?;
        if let Some(expected_revision) = expected_revision {
            state.require_open_revision(expected_revision)?;
        }
        if matches!(
            state.status(),
            UploadSessionStatus::Sealing(_) | UploadSessionStatus::Sealed(_)
        ) {
            return Err(Error::ContentUploadSealed {
                upload_id: upload_id.to_string(),
            });
        }
        self.discard_open_upload(&state).await
    }

    pub(super) async fn hash_upload_state(&self, state: &UploadSessionState) -> Result<ContentId> {
        let mut hasher = Sha256::new();
        let expected_full_len = state.options().chunk_bytes();
        for index in 0..state.complete_chunks() {
            let frame = self
                .read_content_chunk(state.upload_id(), index)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!(
                        "content upload {} is missing complete chunk {index}",
                        state.upload_id()
                    ),
                })?;
            let payload = crate::content::decode_chunk(&frame, state.upload_id(), index)?;
            if payload.len() != expected_full_len {
                return Err(Error::Corruption {
                    message: format!(
                        "content upload {} complete chunk {index} has length {}, expected {expected_full_len}",
                        state.upload_id(),
                        payload.len()
                    ),
                });
            }
            hasher.update(payload);
        }
        if state.partial_len() != 0 {
            let index = state.complete_chunks();
            let frame = self
                .read_content_partial_chunk(state.upload_id(), index, state.revision())
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!(
                        "content upload {} is missing partial chunk",
                        state.upload_id()
                    ),
                })?;
            let payload = crate::content::decode_chunk(&frame, state.upload_id(), index)?;
            let durable_len =
                usize::try_from(state.partial_len()).map_err(|_| Error::InvalidFormat {
                    message: "content partial length exceeds usize".to_owned(),
                })?;
            let durable = payload
                .get(..durable_len)
                .ok_or_else(|| Error::Corruption {
                    message: format!(
                        "content upload {} partial chunk is shorter than durable state",
                        state.upload_id()
                    ),
                })?;
            hasher.update(durable);
        }
        Ok(ContentId::from_sha256(hasher.finalize().into()))
    }

    pub(super) async fn discard_open_upload(&self, state: &UploadSessionState) -> Result<()> {
        let aborting = match state.status() {
            UploadSessionStatus::Open => {
                let aborting = (*state).into_aborting()?;
                self.write_upload_state(&aborting).await?;
                aborting
            }
            UploadSessionStatus::Aborting => *state,
            UploadSessionStatus::Sealing(_) | UploadSessionStatus::Sealed(_) => {
                return Err(Error::ContentUploadSealed {
                    upload_id: state.upload_id().to_string(),
                });
            }
        };
        self.cleanup_upload_chunks(&aborting).await?;
        self.release_content_upload_reservation(
            aborting.options().attachment_scope().storage_domain_id(),
            aborting.upload_id(),
        )
        .await?;
        self.delete_upload_state(aborting.upload_id()).await?;
        Ok(())
    }
    pub(super) async fn cleanup_upload_chunks(&self, state: &UploadSessionState) -> Result<()> {
        for object in self
            .list_content_upload_chunk_objects(state.upload_id())
            .await?
        {
            self.delete_content_chunk_object(object).await?;
        }
        Ok(())
    }
}
