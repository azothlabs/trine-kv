use super::{
    CONTENT_CONTROL_BUCKET, CONTENT_TOKEN_BUCKET, CONTENT_TOKEN_INDEX_BUCKET, ContentAttachment,
    ContentAttachmentScope, ContentChangeId, ContentControlRecord, ContentQuarantineRecord,
    ContentReclaimBlocker, ContentReclaimGraceRecord, ContentReclaimSweepRecord,
    ContentReclaimSweepRecordState, Error, Result, Transaction, UploadToken, UploadTokenRecord,
    content_control_key, content_quarantine_key, content_reclaim_grace_key,
    content_reclaim_sweep_key, content_token_index_key, current_epoch_millis, upload_token_key,
};

impl Transaction {
    /// Verifies and stages one upload-token consumption synchronously.
    ///
    /// The supplied scope must come from the authenticated database layer. A
    /// successful call only stages the transition; the token and all other
    /// writes in this transaction become durable together at commit. Dropping
    /// the transaction leaves an available token unchanged.
    ///
    /// Repeating a committed consumption with the same `change_id` returns the
    /// same claims. A different `ChangeId` cannot reuse the token.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UploadTokenInvalid`] for an unknown bearer token,
    /// [`Error::UploadTokenScopeMismatch`] for a wrong domain or owner,
    /// [`Error::UploadTokenExpired`] for expired available authority, or
    /// [`Error::UploadTokenAlreadyConsumed`] when another `ChangeId` owns it.
    /// Storage and format errors are propagated without staging consumption.
    pub fn consume_upload_token_sync(
        &mut self,
        token: UploadToken,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
    ) -> Result<ContentAttachment> {
        let now_unix_ms = current_epoch_millis()?;
        self.consume_upload_token_at_sync(token, expected_scope, change_id, now_unix_ms)
    }

    pub(crate) fn consume_upload_token_at_sync(
        &mut self,
        token: UploadToken,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
        now_unix_ms: u64,
    ) -> Result<ContentAttachment> {
        let key = upload_token_key(token);
        self.require_consistent_staged_token(&key, change_id)?;
        let bytes = match self.get_internal_bucket_sync(CONTENT_TOKEN_BUCKET, &key) {
            Ok(Some(bytes)) => bytes,
            Ok(None) | Err(Error::BucketMissing { .. }) => {
                return Err(Error::UploadTokenInvalid);
            }
            Err(error) => return Err(error),
        };
        let consumed = UploadTokenRecord::decode(&bytes, token)?.consume(
            expected_scope,
            change_id,
            now_unix_ms,
        )?;
        let attachment = consumed.attachment();
        self.stage_content_activity_sync(
            attachment.scope().storage_domain_id(),
            attachment.content_id(),
        )?;
        self.delete_internal_bucket(
            CONTENT_TOKEN_INDEX_BUCKET,
            content_token_index_key(
                attachment.scope().storage_domain_id(),
                attachment.content_id(),
                token,
            ),
        )?;
        self.put_internal_bucket(CONTENT_TOKEN_BUCKET, key.clone(), consumed.encode())?;
        self.record_extension_claim(key, change_id.to_bytes());
        Ok(attachment)
    }

    pub(crate) fn stage_content_activity_sync(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
    ) -> Result<()> {
        let sweep_key = content_reclaim_sweep_key(storage_domain_id, content_id);
        if let Some(bytes) = self.get_internal_bucket_sync(CONTENT_CONTROL_BUCKET, &sweep_key)? {
            let sweep = ContentReclaimSweepRecord::decode(&bytes, storage_domain_id, content_id)?;
            match sweep.state {
                ContentReclaimSweepRecordState::Prepared => {
                    return Err(Error::ContentReclaimBlocked {
                        blocker: ContentReclaimBlocker::SweepPrepared {
                            prepared_at_commit_seq: sweep.prepared_at.as_u64(),
                        },
                    });
                }
                ContentReclaimSweepRecordState::Reclaimed => {
                    self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, sweep_key)?;
                }
            }
        }
        let key = content_control_key(storage_domain_id, content_id);
        if let Some(bytes) = self.get_internal_bucket_sync(CONTENT_CONTROL_BUCKET, &key)? {
            ContentControlRecord::decode(&bytes, storage_domain_id, content_id)?;
        }
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        let quarantine = self
            .get_internal_bucket_sync(CONTENT_CONTROL_BUCKET, &quarantine_key)?
            .map(|bytes| ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id))
            .transpose()?;
        let grace_key = content_reclaim_grace_key(storage_domain_id, content_id);
        let grace = self
            .get_internal_bucket_sync(CONTENT_CONTROL_BUCKET, &grace_key)?
            .map(|bytes| ContentReclaimGraceRecord::decode(&bytes, storage_domain_id, content_id))
            .transpose()?;
        if let Some(grace) = grace
            && !quarantine.is_some_and(|record| grace.matches_quarantine(record))
        {
            return Err(Error::Corruption {
                message: "content reclaim grace differs from its quarantine fence".to_owned(),
            });
        }
        if quarantine.is_some() {
            self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, quarantine_key)?;
        }
        if grace.is_some() {
            self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, grace_key)?;
        }
        self.stage_active_content_control(storage_domain_id, content_id, key)
    }

    fn require_consistent_staged_token(
        &self,
        key: &[u8],
        change_id: ContentChangeId,
    ) -> Result<()> {
        if self
            .extension_claim(key)
            .is_some_and(|existing| existing != change_id.to_bytes())
        {
            return Err(Error::UploadTokenAlreadyConsumed);
        }
        Ok(())
    }

    /// Verifies and stages one upload-token consumption asynchronously.
    ///
    /// This is the async counterpart of
    /// [`consume_upload_token_sync`](Self::consume_upload_token_sync). Token
    /// state and the caller's other staged writes share this transaction's one
    /// optimistic commit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentChangeId, ContentUploadOptions, Db, DbOptions,
    ///     OwnerScopeId, StorageDomainId, TransactionOptions,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let scope = ContentAttachmentScope::new(
    ///         StorageDomainId::from_bytes([1; 16]),
    ///         OwnerScopeId::from_bytes([2; 16]),
    ///     );
    ///     let mut upload = db
    ///         .begin_content_upload(ContentUploadOptions::new(
    ///             scope,
    ///             Duration::from_secs(3600),
    ///         ))
    ///         .await?;
    ///     upload.write(b"content bytes").await?;
    ///     let sealed = upload.seal().await?;
    ///
    ///     let mut transaction = db.transaction(TransactionOptions::default());
    ///     let claims = transaction
    ///         .consume_upload_token(
    ///             sealed.upload_token(),
    ///             scope,
    ///             ContentChangeId::from_bytes([3; 16]),
    ///         )
    ///         .await?;
    ///     assert_eq!(claims.content_id(), sealed.content_id());
    ///     transaction.put(b"catalog:file", b"attached");
    ///     transaction.commit().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn consume_upload_token(
        &mut self,
        token: UploadToken,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
    ) -> Result<ContentAttachment> {
        let now_unix_ms = current_epoch_millis()?;
        self.consume_upload_token_at(token, expected_scope, change_id, now_unix_ms)
            .await
    }

    pub(crate) async fn consume_upload_token_at(
        &mut self,
        token: UploadToken,
        expected_scope: ContentAttachmentScope,
        change_id: ContentChangeId,
        now_unix_ms: u64,
    ) -> Result<ContentAttachment> {
        let key = upload_token_key(token);
        self.require_consistent_staged_token(&key, change_id)?;
        let bytes = match self.get_internal_bucket(CONTENT_TOKEN_BUCKET, &key).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) | Err(Error::BucketMissing { .. }) => {
                return Err(Error::UploadTokenInvalid);
            }
            Err(error) => return Err(error),
        };
        let consumed = UploadTokenRecord::decode(&bytes, token)?.consume(
            expected_scope,
            change_id,
            now_unix_ms,
        )?;
        let attachment = consumed.attachment();
        self.stage_content_activity(
            attachment.scope().storage_domain_id(),
            attachment.content_id(),
        )
        .await?;
        self.delete_internal_bucket(
            CONTENT_TOKEN_INDEX_BUCKET,
            content_token_index_key(
                attachment.scope().storage_domain_id(),
                attachment.content_id(),
                token,
            ),
        )?;
        self.put_internal_bucket(CONTENT_TOKEN_BUCKET, key.clone(), consumed.encode())?;
        self.record_extension_claim(key, change_id.to_bytes());
        Ok(attachment)
    }

    pub(crate) async fn stage_content_activity(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
    ) -> Result<()> {
        let sweep_key = content_reclaim_sweep_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &sweep_key)
            .await?
        {
            let sweep = ContentReclaimSweepRecord::decode(&bytes, storage_domain_id, content_id)?;
            match sweep.state {
                ContentReclaimSweepRecordState::Prepared => {
                    return Err(Error::ContentReclaimBlocked {
                        blocker: ContentReclaimBlocker::SweepPrepared {
                            prepared_at_commit_seq: sweep.prepared_at.as_u64(),
                        },
                    });
                }
                ContentReclaimSweepRecordState::Reclaimed => {
                    self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, sweep_key)?;
                }
            }
        }
        let key = content_control_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
        {
            ContentControlRecord::decode(&bytes, storage_domain_id, content_id)?;
        }
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        let quarantine = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
            .map(|bytes| ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id))
            .transpose()?;
        let grace_key = content_reclaim_grace_key(storage_domain_id, content_id);
        let grace = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &grace_key)
            .await?
            .map(|bytes| ContentReclaimGraceRecord::decode(&bytes, storage_domain_id, content_id))
            .transpose()?;
        if let Some(grace) = grace
            && !quarantine.is_some_and(|record| grace.matches_quarantine(record))
        {
            return Err(Error::Corruption {
                message: "content reclaim grace differs from its quarantine fence".to_owned(),
            });
        }
        if quarantine.is_some() {
            self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, quarantine_key)?;
        }
        if grace.is_some() {
            self.delete_internal_bucket(CONTENT_CONTROL_BUCKET, grace_key)?;
        }
        self.stage_active_content_control(storage_domain_id, content_id, key)
    }

    pub(crate) async fn stage_content_read_activity(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
    ) -> Result<()> {
        let sweep_key = content_reclaim_sweep_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &sweep_key)
            .await?
        {
            let sweep = ContentReclaimSweepRecord::decode(&bytes, storage_domain_id, content_id)?;
            return match sweep.state {
                ContentReclaimSweepRecordState::Prepared => Err(Error::ContentReclaimBlocked {
                    blocker: ContentReclaimBlocker::SweepPrepared {
                        prepared_at_commit_seq: sweep.prepared_at.as_u64(),
                    },
                }),
                ContentReclaimSweepRecordState::Reclaimed => Err(Error::ContentNotFound {
                    storage_domain_id: storage_domain_id.to_string(),
                    content_id: content_id.to_string(),
                }),
            };
        }
        let quarantine_key = content_quarantine_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &quarantine_key)
            .await?
        {
            let quarantine =
                ContentQuarantineRecord::decode(&bytes, storage_domain_id, content_id)?;
            return Err(Error::ContentQuarantined {
                quarantined_at: quarantine.quarantined_at,
            });
        }
        let grace_key = content_reclaim_grace_key(storage_domain_id, content_id);
        if self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &grace_key)
            .await?
            .is_some()
        {
            return Err(Error::Corruption {
                message: "content reclaim grace exists without its quarantine fence".to_owned(),
            });
        }
        let key = content_control_key(storage_domain_id, content_id);
        if let Some(bytes) = self
            .get_internal_bucket(CONTENT_CONTROL_BUCKET, &key)
            .await?
        {
            ContentControlRecord::decode(&bytes, storage_domain_id, content_id)?;
        }
        self.stage_active_content_control(storage_domain_id, content_id, key)
    }

    fn stage_active_content_control(
        &mut self,
        storage_domain_id: crate::StorageDomainId,
        content_id: crate::ContentId,
        key: Vec<u8>,
    ) -> Result<()> {
        let active = ContentControlRecord::active(storage_domain_id, content_id);
        self.put_internal_bucket_with_commit_sequence(
            CONTENT_CONTROL_BUCKET,
            key,
            &active.encode_prefix(),
            &[],
        )
    }
}
