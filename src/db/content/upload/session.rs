use super::super::{
    ContentUpload, ContentUploadOptions, ContentUploadResume, Db, Error, Result, UploadId,
    UploadIdRetirement, UploadSessionState, UploadSessionStatus, UploadToken,
    decode_upload_id_tombstone, initial_upload_reservation,
};

impl Db {
    /// Starts a bounded-memory upload for one immutable `ContentObject`.
    ///
    /// The upload is independent of key/value transactions and is not visible
    /// through [`open_content`](Self::open_content) until
    /// [`ContentUpload::seal`] publishes its descriptor. `options.chunk_bytes()`
    /// bounds retained unsealed payload memory; calls to `write` may use any
    /// input size.
    ///
    /// This storage-layer API does not create a higher-level File or consume an
    /// attachment token. Ordinary Blob values continue to use the key/value
    /// path.
    ///
    /// # Parameters
    ///
    /// - `options`: attachment scope, token lifetime, chunk bound, and optional
    ///   expected original length and `ContentId`. Chunk bounds outside 64 KiB
    ///   through 16 MiB and token lifetimes below one millisecond are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Closed`], [`Error::ReadOnly`],
    /// [`Error::InvalidOptions`] for an invalid chunk bound, or
    /// [`Error::UnsupportedBackend`] when the selected host backend does not
    /// yet implement content objects. Backend failures may also be returned
    /// while creating the initial durable session record.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentUploadOptions, Db, DbOptions, OwnerScopeId,
    ///     StorageDomainId,
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
    ///     upload.write(b"immutable bytes").await?;
    ///     let sealed = upload.seal().await?;
    ///
    ///     let content = db
    ///         .open_content(scope.storage_domain_id(), sealed.content_id())
    ///         .await?;
    ///     assert_eq!(&*content.read_range(0, 9).await?, b"immutable");
    ///     Ok(())
    /// }
    /// ```
    pub async fn begin_content_upload(
        &self,
        options: ContentUploadOptions,
    ) -> Result<ContentUpload> {
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let options = options.validate()?;
        let upload_id = UploadId::generate()?;
        let upload_token = UploadToken::generate()?;
        let _upload = self.lock_content_upload(upload_id).await;
        let state = UploadSessionState::initial(upload_id, options, upload_token)?;
        self.write_upload_state(&state).await?;
        if let Err(error) = self
            .reserve_content_upload_bytes(&state, initial_upload_reservation(&state))
            .await
        {
            let _ = self.discard_open_upload(&state).await;
            return Err(error);
        }
        Ok(ContentUpload::new(
            self.clone(),
            upload_id,
            options,
            Vec::with_capacity(options.chunk_bytes()),
            0,
            0,
            0,
        ))
    }

    /// Begins or resumes a durable upload under a caller-supplied [`UploadId`].
    ///
    /// This is the idempotent counterpart to [`Db::begin_content_upload`].
    /// Generate and persist `upload_id` before a remote or otherwise uncertain
    /// request. The first successful call creates the same bounded-memory
    /// sequential writer as `begin_content_upload`. An exact retry returns the
    /// current open writer at its durable original-byte length, or the exact
    /// prior [`crate::SealedContent`] after sealing.
    ///
    /// An upload identity is permanently bound to its first options. Reusing it
    /// with a different attachment scope, token lifetime, chunk size, expected
    /// length, or expected `ContentId` fails without changing existing state.
    /// Concurrent append, seal, or abort operations remain serialized by the
    /// same upload lock; after the identity/options check this method delegates
    /// recovery to [`Db::resume_content_upload`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] for invalid options or an identity
    /// already bound to different options, [`Error::Closed`] or
    /// [`Error::ReadOnly`] when writes are unavailable, and typed backend,
    /// integrity, or recovery errors. Aborted and sealed identities are
    /// permanently retired and cannot be started again.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentUploadOptions, ContentUploadResume, Db, DbOptions,
    ///     OwnerScopeId, StorageDomainId, UploadId,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let upload_id = UploadId::new()?;
    ///     let options = ContentUploadOptions::new(
    ///         ContentAttachmentScope::new(
    ///             StorageDomainId::from_bytes([1; 16]),
    ///             OwnerScopeId::from_bytes([2; 16]),
    ///         ),
    ///         Duration::from_secs(60),
    ///     );
    ///     let first = db.begin_content_upload_with_id(upload_id, options).await?;
    ///     assert!(matches!(first, ContentUploadResume::Open(_)));
    ///     let retry = db.begin_content_upload_with_id(upload_id, options).await?;
    ///     assert!(matches!(retry, ContentUploadResume::Open(_)));
    ///     Ok(())
    /// }
    /// ```
    pub async fn begin_content_upload_with_id(
        &self,
        upload_id: UploadId,
        options: ContentUploadOptions,
    ) -> Result<ContentUploadResume> {
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let options = options.validate()?;
        let upload_guard = self.lock_content_upload(upload_id).await;
        let object = self.content_upload_state_object(upload_id);
        if let Some(bytes) = self.read_content_object(object).await? {
            if let Some(retirement) = decode_upload_id_tombstone(&bytes, upload_id)? {
                return Err(match retirement {
                    UploadIdRetirement::Sealed => Error::ContentUploadSealed {
                        upload_id: upload_id.to_string(),
                    },
                    UploadIdRetirement::Aborted => Error::ContentUploadNotFound {
                        upload_id: upload_id.to_string(),
                    },
                });
            }
            let state = UploadSessionState::decode(&bytes, upload_id)?;
            if state.status() == UploadSessionStatus::Aborting {
                self.discard_open_upload(&state).await?;
            } else {
                if state.options() != options {
                    return Err(Error::invalid_options(format!(
                        "content upload {upload_id} is already bound to different options"
                    )));
                }
                if state.status() == UploadSessionStatus::Open {
                    self.reserve_content_upload_bytes(&state, initial_upload_reservation(&state))
                        .await?;
                }
                drop(upload_guard);
                return self.resume_content_upload(upload_id).await;
            }
        }

        let upload_token = UploadToken::generate()?;
        let state = UploadSessionState::initial(upload_id, options, upload_token)?;
        self.write_upload_state(&state).await?;
        if let Err(error) = self
            .reserve_content_upload_bytes(&state, initial_upload_reservation(&state))
            .await
        {
            let _ = self.discard_open_upload(&state).await;
            return Err(error);
        }
        Ok(ContentUploadResume::Open(ContentUpload::new(
            self.clone(),
            upload_id,
            options,
            Vec::with_capacity(options.chunk_bytes()),
            0,
            0,
            0,
        )))
    }

    /// Resumes durable upload state by [`UploadId`].
    ///
    /// Open state returns a writer positioned at its durable original-byte
    /// length. A partial chunk is reloaded and verified into a buffer no larger
    /// than the configured chunk bound. A session interrupted while issuing its
    /// attachment token completes seal recovery. Already sealed state returns
    /// the exact prior [`crate::SealedContent`] instead of reopening a writer.
    ///
    /// A write publishes chunk bytes before advancing the session revision. If
    /// a crash leaves a newer partial frame than the session record, resume
    /// verifies that frame and keeps only the prefix named by the durable state.
    /// Callers should therefore continue from `ContentUpload::len()`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentUploadNotFound`] for an unknown or aborted
    /// identity, or a storage/format/integrity error when durable state or its
    /// partial chunk cannot be trusted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::Duration;
    /// use trine_kv::{
    ///     ContentAttachmentScope, ContentUploadOptions, ContentUploadResume, Db, DbOptions,
    ///     OwnerScopeId, StorageDomainId,
    /// };
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let scope = ContentAttachmentScope::new(
    ///         StorageDomainId::from_bytes([1; 16]),
    ///         OwnerScopeId::from_bytes([2; 16]),
    ///     );
    ///     let mut first = db
    ///         .begin_content_upload(ContentUploadOptions::new(
    ///             scope,
    ///             Duration::from_secs(3600),
    ///         ))
    ///         .await?;
    ///     first.write(b"confirmed prefix").await?;
    ///     let upload_id = first.upload_id();
    ///     drop(first);
    ///
    ///     let mut resumed = match db.resume_content_upload(upload_id).await? {
    ///         ContentUploadResume::Open(upload) => upload,
    ///         ContentUploadResume::Sealed(sealed) => {
    ///             assert_eq!(sealed.len(), 16);
    ///             return Ok(());
    ///         }
    ///     };
    ///     assert_eq!(resumed.len(), 16);
    ///     resumed.write(b" and suffix").await?;
    ///     let sealed = resumed.seal().await?;
    ///     assert_eq!(db.seal_content_upload(upload_id).await?, sealed);
    ///     Ok(())
    /// }
    /// ```
    pub async fn resume_content_upload(&self, upload_id: UploadId) -> Result<ContentUploadResume> {
        let _activity = self.inner.publish_barrier.begin_activity()?;
        self.ensure_open()?;
        let upload_guard = self.lock_content_upload(upload_id).await;
        let state = self.require_upload_state(upload_id).await?;
        match state.status() {
            UploadSessionStatus::Sealed(sealed) => Ok(ContentUploadResume::Sealed(sealed)),
            UploadSessionStatus::Sealing(_) => {
                drop(upload_guard);
                self.seal_content_upload(upload_id)
                    .await
                    .map(ContentUploadResume::Sealed)
            }
            UploadSessionStatus::Open => {
                self.reserve_content_upload_bytes(&state, initial_upload_reservation(&state))
                    .await?;
                let mut buffer = Vec::with_capacity(state.options().chunk_bytes());
                if state.partial_len() != 0 {
                    let frame = self
                        .read_content_partial_chunk(
                            upload_id,
                            state.complete_chunks(),
                            state.revision(),
                        )
                        .await?
                        .ok_or_else(|| Error::Corruption {
                            message: format!(
                                "content upload {upload_id} is missing its partial chunk"
                            ),
                        })?;
                    let payload =
                        crate::content::decode_chunk(&frame, upload_id, state.complete_chunks())?;
                    let durable_len =
                        usize::try_from(state.partial_len()).map_err(|_| Error::InvalidFormat {
                            message: "content partial length exceeds usize".to_owned(),
                        })?;
                    let durable = payload
                    .get(..durable_len)
                    .ok_or_else(|| Error::Corruption {
                        message: format!(
                            "content upload {upload_id} partial chunk is shorter than durable state"
                        ),
                    })?;
                    buffer.extend_from_slice(durable);
                }
                Ok(ContentUploadResume::Open(ContentUpload::new(
                    self.clone(),
                    upload_id,
                    state.options(),
                    buffer,
                    state.length(),
                    state.complete_chunks(),
                    state.revision(),
                )))
            }
            UploadSessionStatus::Aborting => {
                self.discard_open_upload(&state).await?;
                Err(Error::ContentUploadNotFound {
                    upload_id: upload_id.to_string(),
                })
            }
        }
    }
}
