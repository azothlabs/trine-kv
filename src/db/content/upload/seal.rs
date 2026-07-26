use super::super::{
    CONTENT_CONTROL_BUCKET, CONTENT_LEASE_BUCKET, CONTENT_TOKEN_BUCKET, CONTENT_TOKEN_INDEX_BUCKET,
    ContentDescriptor, ContentId, ContentTokenIndexRecord, Db, Error, Result, SealedContent,
    StorageDomainId, TransactionOptions, UploadId, UploadSessionState, UploadSessionStatus,
    UploadTokenRecord, WriteOptions, content_token_index_key, current_epoch_millis,
    upload_token_key,
};

impl Db {
    /// Seals an upload idempotently by durable [`UploadId`].
    ///
    /// The complete SHA-256 is recomputed from verified durable chunks, so this
    /// works after process restart without serializing hash-library internals.
    /// A durable `sealing` checkpoint is published before the descriptor,
    /// followed by one transactional token record and the final `sealed`
    /// checkpoint. A retry after any crash completes any missing descriptor and
    /// returns the same bearer token, expiry, identity, length, and durability
    /// result.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentUploadNotFound`], a typed expected length/digest
    /// mismatch, or a storage/format/integrity error. An expectation mismatch
    /// aborts the session. Failures after the durable `sealing` checkpoint leave
    /// the upload in an idempotently completable state.
    pub async fn seal_content_upload(&self, upload_id: UploadId) -> Result<SealedContent> {
        self.seal_content_upload_at(upload_id, None).await
    }

    pub(crate) async fn seal_content_upload_at(
        &self,
        upload_id: UploadId,
        expected_revision: Option<u64>,
    ) -> Result<SealedContent> {
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
        if state.status() == UploadSessionStatus::Aborting {
            return Err(Error::ContentUploadNotFound {
                upload_id: upload_id.to_string(),
            });
        }
        if let UploadSessionStatus::Sealed(sealed) = state.status() {
            let descriptor = self
                .read_content_descriptor(sealed.storage_domain_id(), sealed.content_id())
                .await?
                .ok_or_else(|| Error::ContentNotFound {
                    storage_domain_id: sealed.storage_domain_id().to_string(),
                    content_id: sealed.content_id().to_string(),
                })?;
            ContentDescriptor::decode(
                &descriptor,
                sealed.storage_domain_id(),
                sealed.content_id(),
            )?;
            return Ok(sealed);
        }
        let (sealing_state, sealed, reused) = self.prepare_upload_seal(&state).await?;

        if reused {
            self.cleanup_upload_chunks(&sealing_state).await?;
        }

        self.finalize_content_upload_quota(&sealing_state, sealed)
            .await?;

        self.ensure_upload_token_record(upload_id, sealed).await?;
        let sealed_state = sealing_state.into_sealed()?;
        self.write_upload_state(&sealed_state).await?;
        Ok(sealed)
    }

    pub(super) async fn prepare_upload_seal(
        &self,
        state: &UploadSessionState,
    ) -> Result<(UploadSessionState, SealedContent, bool)> {
        let upload_id = state.upload_id();
        match state.status() {
            UploadSessionStatus::Sealed(_) => Err(Error::InvalidFormat {
                message: "sealed upload entered seal preparation".to_owned(),
            }),
            UploadSessionStatus::Sealing(sealed) => {
                let _seal = self.lock_content_seal().await;
                self.require_content_descriptor_publication_allowed(
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                )
                .await?;
                let reused = self.ensure_sealing_descriptor(state, sealed).await?;
                Ok((*state, sealed, reused))
            }
            UploadSessionStatus::Open => self.prepare_open_upload_seal(state).await,
            UploadSessionStatus::Aborting => Err(Error::ContentUploadNotFound {
                upload_id: upload_id.to_string(),
            }),
        }
    }

    pub(super) async fn prepare_open_upload_seal(
        &self,
        state: &UploadSessionState,
    ) -> Result<(UploadSessionState, SealedContent, bool)> {
        let upload_id = state.upload_id();
        let content_id = self.hash_upload_state(state).await?;
        if let Some(expected) = state.options().expected_length()
            && expected != state.length()
        {
            self.discard_open_upload(state).await?;
            return Err(Error::ContentLengthMismatch {
                expected,
                actual: state.length(),
            });
        }
        if let Some(expected) = state.options().expected_content_id()
            && expected != content_id
        {
            self.discard_open_upload(state).await?;
            return Err(Error::ContentDigestMismatch {
                expected: expected.to_string(),
                actual: content_id.to_string(),
            });
        }

        let expires_at = current_epoch_millis()?
            .checked_add(state.options().token_ttl_ms()?)
            .ok_or_else(|| Error::invalid_options("upload token expiry overflow"))?;
        let storage_domain_id = state.options().attachment_scope().storage_domain_id();
        let seal_guard = self.lock_content_seal().await;
        self.require_content_descriptor_publication_allowed(storage_domain_id, content_id)
            .await?;
        let existing = self
            .read_content_descriptor(storage_domain_id, content_id)
            .await?;
        if let Some(existing) = &existing {
            let existing = ContentDescriptor::decode(existing, storage_domain_id, content_id)?;
            if existing.length() != state.length() {
                return Err(Error::Corruption {
                    message: format!(
                        "content descriptor {content_id} length {} differs from upload length {}",
                        existing.length(),
                        state.length()
                    ),
                });
            }
        }
        let sealing_state =
            (*state).into_sealing(content_id, expires_at, self.content_durability())?;
        // The recovery marker must become durable before the descriptor becomes
        // visible. Once the session is Sealing, abort/reaper paths refuse to
        // delete its chunks and a retry can safely publish a missing descriptor.
        self.write_upload_state(&sealing_state).await?;
        let UploadSessionStatus::Sealing(sealed) = sealing_state.status() else {
            return Err(Error::InvalidFormat {
                message: "content upload did not enter sealing state".to_owned(),
            });
        };
        let reused = if let Some(existing) = existing {
            ContentDescriptor::decode(&existing, storage_domain_id, content_id)?.upload_id()
                != upload_id
        } else {
            self.promote_sealing_partial_chunk(&sealing_state).await?;
            self.write_content_descriptor(
                storage_domain_id,
                content_id,
                Self::descriptor_for_sealing_state(&sealing_state, sealed)?.encode(),
            )
            .await?;
            self.remove_sealing_partial_chunk(&sealing_state).await?;
            false
        };
        drop(seal_guard);
        Ok((sealing_state, sealed, reused))
    }

    pub(super) async fn ensure_sealing_descriptor(
        &self,
        state: &UploadSessionState,
        sealed: SealedContent,
    ) -> Result<bool> {
        let storage_domain_id = sealed.storage_domain_id();
        let content_id = sealed.content_id();
        if let Some(existing) = self
            .read_content_descriptor(storage_domain_id, content_id)
            .await?
        {
            let existing = ContentDescriptor::decode(&existing, storage_domain_id, content_id)?;
            if existing.length() != state.length() {
                return Err(Error::Corruption {
                    message: format!(
                        "content descriptor {content_id} length {} differs from upload length {}",
                        existing.length(),
                        state.length()
                    ),
                });
            }
            let reused = existing.upload_id() != state.upload_id();
            if !reused {
                self.remove_sealing_partial_chunk(state).await?;
            }
            return Ok(reused);
        }

        self.promote_sealing_partial_chunk(state).await?;
        let descriptor = Self::descriptor_for_sealing_state(state, sealed)?;
        self.write_content_descriptor(storage_domain_id, content_id, descriptor.encode())
            .await?;
        self.remove_sealing_partial_chunk(state).await?;
        Ok(false)
    }

    pub(super) async fn promote_sealing_partial_chunk(
        &self,
        state: &UploadSessionState,
    ) -> Result<()> {
        if state.partial_len() == 0 {
            return Ok(());
        }
        let source_revision =
            state
                .revision()
                .checked_sub(1)
                .ok_or_else(|| Error::InvalidFormat {
                    message: "sealing upload has no preceding open revision".to_owned(),
                })?;
        let index = state.complete_chunks();
        let frame = self
            .read_content_partial_chunk(state.upload_id(), index, source_revision)
            .await?
            .ok_or_else(|| Error::Corruption {
                message: format!(
                    "content upload {} is missing immutable partial chunk revision {source_revision}",
                    state.upload_id()
                ),
            })?;
        let payload = crate::content::decode_chunk(&frame, state.upload_id(), index)?;
        if payload.len()
            != usize::try_from(state.partial_len()).map_err(|_| Error::InvalidFormat {
                message: "content partial length exceeds usize".to_owned(),
            })?
        {
            return Err(Error::Corruption {
                message: format!(
                    "content upload {} partial chunk length differs from sealing state",
                    state.upload_id()
                ),
            });
        }
        self.write_content_chunk(state.upload_id(), index, frame)
            .await
    }

    pub(super) async fn remove_sealing_partial_chunk(
        &self,
        state: &UploadSessionState,
    ) -> Result<()> {
        if state.partial_len() == 0 {
            return Ok(());
        }
        let source_revision =
            state
                .revision()
                .checked_sub(1)
                .ok_or_else(|| Error::InvalidFormat {
                    message: "sealing upload has no preceding open revision".to_owned(),
                })?;
        self.delete_content_partial_chunk(
            state.upload_id(),
            state.complete_chunks(),
            source_revision,
        )
        .await
    }

    pub(super) fn descriptor_for_sealing_state(
        state: &UploadSessionState,
        sealed: SealedContent,
    ) -> Result<ContentDescriptor> {
        ContentDescriptor::new(
            sealed.storage_domain_id(),
            sealed.content_id(),
            state.upload_id(),
            state.length(),
            state.options().chunk_bytes(),
            state.chunk_count(),
        )
    }

    pub(super) async fn ensure_upload_token_record(
        &self,
        upload_id: UploadId,
        sealed: SealedContent,
    ) -> Result<()> {
        self.internal_bucket(CONTENT_TOKEN_BUCKET).await?;
        self.internal_bucket(CONTENT_TOKEN_INDEX_BUCKET).await?;
        self.internal_bucket(CONTENT_CONTROL_BUCKET).await?;
        self.internal_bucket(CONTENT_LEASE_BUCKET).await?;
        let expected = UploadTokenRecord::available(upload_id, sealed);
        let key = upload_token_key(sealed.upload_token());
        let index = ContentTokenIndexRecord::for_token(sealed);
        let index_key = content_token_index_key(
            sealed.storage_domain_id(),
            sealed.content_id(),
            sealed.upload_token(),
        );
        let mut transaction = self.transaction(TransactionOptions {
            write_options: WriteOptions::new(sealed.durability()),
        });
        if let Some(bytes) = transaction
            .get_internal_bucket(CONTENT_TOKEN_BUCKET, &key)
            .await?
        {
            let existing = UploadTokenRecord::decode(&bytes, sealed.upload_token())?;
            if existing.attachment() != expected.attachment() {
                return Err(Error::Corruption {
                    message: format!("upload {upload_id} token claims changed during seal retry"),
                });
            }
            let indexed = transaction
                .get_internal_bucket(CONTENT_TOKEN_INDEX_BUCKET, &index_key)
                .await?;
            if existing.is_available() {
                let indexed = indexed.ok_or_else(|| Error::Corruption {
                    message: format!("upload {upload_id} is missing its token-authority index"),
                })?;
                ContentTokenIndexRecord::decode(
                    &indexed,
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                    crate::content::upload_token_hash(sealed.upload_token()),
                )?;
            } else if indexed.is_some() {
                return Err(Error::Corruption {
                    message: format!("consumed upload {upload_id} retained token authority"),
                });
            }
            let control_key = crate::content::content_control_key(
                sealed.storage_domain_id(),
                sealed.content_id(),
            );
            let control = transaction
                .get_internal_bucket(CONTENT_CONTROL_BUCKET, &control_key)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!("sealed upload {upload_id} is missing content control state"),
                })?;
            crate::content::ContentControlRecord::decode(
                &control,
                sealed.storage_domain_id(),
                sealed.content_id(),
            )?;
            return Ok(());
        }
        transaction.put_internal_bucket(CONTENT_TOKEN_BUCKET, key, expected.encode())?;
        transaction.put_internal_bucket(CONTENT_TOKEN_INDEX_BUCKET, index_key, index.encode())?;
        transaction
            .stage_content_activity(sealed.storage_domain_id(), sealed.content_id())
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(super) async fn require_content_descriptor_publication_allowed(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<()> {
        if let Some(sweep) = self
            .content_reclaim_sweep(storage_domain_id, content_id)
            .await?
            && sweep.reclaimed_at().is_none()
        {
            return Err(Error::ContentReclaimBlocked {
                blocker: crate::ContentReclaimBlocker::SweepPrepared {
                    prepared_at_commit_seq: sweep.prepared_at().as_u64(),
                },
            });
        }
        Ok(())
    }
}
