use std::{path::PathBuf, sync::Arc};

use futures::lock::MutexGuard;
use sha2::{Digest, Sha256};

use crate::{
    content::{
        ContentDescriptor, ContentHandle, ContentId, ContentUpload, ContentUploadOptions,
        ContentUploadResume, SealedContent, UploadId, UploadSessionState, UploadSessionStatus,
    },
    error::{Error, Result},
    options::{DurabilityMode, HostStorageBackend, StorageMode},
    storage::{
        StorageObjectDeleteBackend, StorageObjectId, StorageObjectKind, StorageObjectReadBackend,
        StorageObjectWriteBackend,
    },
};

use super::Db;

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
    /// - `options`: chunk bound plus optional expected original length and
    ///   `ContentId`. Chunk bounds outside 64 KiB through 16 MiB are rejected.
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
    /// use trine_kv::{ContentUploadOptions, Db, DbOptions};
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let mut upload = db
    ///         .begin_content_upload(ContentUploadOptions::new())
    ///         .await?;
    ///     upload.write(b"immutable bytes").await?;
    ///     let sealed = upload.seal().await?;
    ///
    ///     let content = db.open_content(sealed.content_id()).await?;
    ///     assert_eq!(&*content.read_range(0, 9).await?, b"immutable");
    ///     Ok(())
    /// }
    /// ```
    pub async fn begin_content_upload(
        &self,
        options: ContentUploadOptions,
    ) -> Result<ContentUpload> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        self.ensure_content_backend_supported()?;
        let options = options.validate()?;
        let upload_id = UploadId::generate()?;
        let _upload = self.lock_content_upload(upload_id).await;
        let state = UploadSessionState::initial(upload_id, options)?;
        self.write_upload_state(state).await?;
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

    /// Resumes durable upload state by [`UploadId`].
    ///
    /// Open state returns a writer positioned at its durable original-byte
    /// length. A partial chunk is reloaded and verified into a buffer no larger
    /// than the configured chunk bound. Already sealed state returns the exact
    /// prior [`SealedContent`] instead of reopening a writer.
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
    /// use trine_kv::{ContentUploadOptions, ContentUploadResume, Db, DbOptions};
    ///
    /// async fn example() -> trine_kv::Result<()> {
    ///     let db = Db::open(DbOptions::memory()).await?;
    ///     let mut first = db
    ///         .begin_content_upload(ContentUploadOptions::new())
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
        self.ensure_open()?;
        self.ensure_content_backend_supported()?;
        let _upload = self.lock_content_upload(upload_id).await;
        let state = self.require_upload_state(upload_id).await?;
        match state.status() {
            UploadSessionStatus::Sealed(sealed) => Ok(ContentUploadResume::Sealed(sealed)),
            UploadSessionStatus::Open => {
                let mut buffer = Vec::with_capacity(state.options().chunk_bytes());
                if state.partial_len() != 0 {
                    let frame = self
                        .read_content_chunk(upload_id, state.complete_chunks())
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
                    let durable = payload.get(..durable_len).ok_or_else(|| Error::Corruption {
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
        }
    }

    /// Seals an upload idempotently by durable [`UploadId`].
    ///
    /// The complete SHA-256 is recomputed from verified durable chunks, so this
    /// works after process restart without serializing hash-library internals.
    /// Descriptor publication happens before the session becomes `sealed`; a
    /// retry after a crash in that window observes the descriptor and completes
    /// the same session result.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentUploadNotFound`], a typed expected length/digest
    /// mismatch, or a storage/format/integrity error. An expectation mismatch
    /// aborts the session. Other failures before descriptor publication leave
    /// the upload open and resumable.
    pub async fn seal_content_upload(&self, upload_id: UploadId) -> Result<SealedContent> {
        self.seal_content_upload_at(upload_id, None).await
    }

    /// Opens a sealed immutable `ContentObject` by cryptographic identity.
    ///
    /// The descriptor is read and validated once. The resulting handle returns
    /// original bytes through verified ranges and sequential streaming; it does
    /// not expose chunk paths or upload identity.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ContentNotFound`] when no sealed descriptor exists,
    /// [`Error::Closed`] for a closed database, or a storage/format/integrity
    /// error when the descriptor cannot be trusted.
    pub async fn open_content(&self, content_id: ContentId) -> Result<ContentHandle> {
        self.ensure_open()?;
        self.ensure_content_backend_supported()?;
        let bytes = self
            .read_content_descriptor(content_id)
            .await?
            .ok_or_else(|| Error::ContentNotFound {
                content_id: content_id.to_string(),
            })?;
        let descriptor = ContentDescriptor::decode(&bytes, content_id)?;
        Ok(ContentHandle::new(self.clone(), descriptor))
    }

    pub(crate) async fn seal_content_upload_at(
        &self,
        upload_id: UploadId,
        expected_revision: Option<u64>,
    ) -> Result<SealedContent> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let _upload = self.lock_content_upload(upload_id).await;
        let state = self.require_upload_state(upload_id).await?;
        if let Some(expected_revision) = expected_revision {
            state.require_open_revision(expected_revision)?;
        }
        if let UploadSessionStatus::Sealed(sealed) = state.status() {
            return Ok(sealed);
        }

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

        let descriptor = ContentDescriptor::new(
            content_id,
            upload_id,
            state.length(),
            state.options().chunk_bytes(),
            state.chunk_count(),
        )?;
        let seal_guard = self.lock_content_seal().await;
        let (sealed, reused) = if let Some(existing) =
            self.read_content_descriptor(content_id).await?
        {
            let existing = ContentDescriptor::decode(&existing, content_id)?;
            if existing.length() != state.length() {
                return Err(Error::Corruption {
                    message: format!(
                        "content descriptor {content_id} length {} differs from upload length {}",
                        existing.length(),
                        state.length()
                    ),
                });
            }
            (existing.sealed(), existing.upload_id() != upload_id)
        } else {
            self.write_content_descriptor(content_id, descriptor.encode())
                .await?;
            (descriptor.sealed(), false)
        };
        self.write_upload_state(state.into_sealed(sealed)?).await?;
        drop(seal_guard);

        if reused {
            self.cleanup_upload_chunks(state).await;
        }
        Ok(sealed)
    }

    /// Aborts durable upload state and schedules no content visibility.
    ///
    /// The session record is deleted before chunk cleanup. A crash during
    /// cleanup can therefore leave only unreachable staging chunks, never a
    /// resumable session that references missing bytes. Cleanup deletion is
    /// idempotent and may be retried by maintenance.
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
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let _upload = self.lock_content_upload(upload_id).await;
        let state = self.require_upload_state(upload_id).await?;
        if let Some(expected_revision) = expected_revision {
            state.require_open_revision(expected_revision)?;
        }
        if matches!(state.status(), UploadSessionStatus::Sealed(_)) {
            return Err(Error::ContentUploadSealed {
                upload_id: upload_id.to_string(),
            });
        }
        self.discard_open_upload(state).await
    }

    async fn hash_upload_state(&self, state: UploadSessionState) -> Result<ContentId> {
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
                .read_content_chunk(state.upload_id(), index)
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

    async fn discard_open_upload(&self, state: UploadSessionState) -> Result<()> {
        self.delete_upload_state(state.upload_id()).await?;
        self.cleanup_upload_chunks(state).await;
        Ok(())
    }

    async fn cleanup_upload_chunks(&self, state: UploadSessionState) {
        for index in 0..state.chunk_count() {
            let _ = self.delete_content_chunk(state.upload_id(), index).await;
        }
    }

    pub(crate) async fn write_content_chunk(
        &self,
        upload_id: UploadId,
        index: u64,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        let object = self.content_chunk_object(upload_id, index)?;
        self.write_content_object(object, bytes).await
    }

    pub(crate) async fn read_content_chunk(
        &self,
        upload_id: UploadId,
        index: u64,
    ) -> Result<Option<Arc<[u8]>>> {
        let object = self.content_chunk_object(upload_id, index)?;
        self.read_content_object(object).await
    }

    pub(crate) async fn delete_content_chunk(&self, upload_id: UploadId, index: u64) -> Result<()> {
        let object = self.content_chunk_object(upload_id, index)?;
        self.delete_content_object(object).await
    }

    pub(crate) async fn write_content_descriptor(
        &self,
        content_id: ContentId,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        let object = self.content_descriptor_object(content_id)?;
        self.write_content_object(object, bytes).await
    }

    pub(crate) async fn read_content_descriptor(
        &self,
        content_id: ContentId,
    ) -> Result<Option<Arc<[u8]>>> {
        let object = self.content_descriptor_object(content_id)?;
        self.read_content_object(object).await
    }

    pub(crate) async fn write_upload_state(&self, state: UploadSessionState) -> Result<()> {
        let object = self.content_upload_state_object(state.upload_id())?;
        self.write_content_object(object, state.encode()?).await
    }

    pub(crate) async fn require_upload_state(
        &self,
        upload_id: UploadId,
    ) -> Result<UploadSessionState> {
        let object = self.content_upload_state_object(upload_id)?;
        let bytes = self.read_content_object(object).await?.ok_or_else(|| {
            Error::ContentUploadNotFound {
                upload_id: upload_id.to_string(),
            }
        })?;
        UploadSessionState::decode(&bytes, upload_id)
    }

    async fn delete_upload_state(&self, upload_id: UploadId) -> Result<()> {
        let object = self.content_upload_state_object(upload_id)?;
        self.delete_content_object(object).await
    }

    pub(crate) async fn lock_content_upload(&self, upload_id: UploadId) -> MutexGuard<'_, ()> {
        self.inner.content_upload_locks[upload_id.lock_shard()]
            .lock()
            .await
    }

    pub(crate) async fn lock_content_seal(&self) -> MutexGuard<'_, ()> {
        self.inner.content_seal_lock.lock().await
    }

    fn ensure_content_backend_supported(&self) -> Result<()> {
        match &self.inner.options.storage_mode {
            StorageMode::InMemory
            | StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. } | HostStorageBackend::ObjectStore,
            } => Ok(()),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Err(Error::unsupported_backend(
                "browser content objects are not implemented in this prototype",
            )),
        }
    }

    fn content_root(&self) -> Result<PathBuf> {
        let root = match &self.inner.options.storage_mode {
            StorageMode::InMemory => PathBuf::from("__trine_content_v1"),
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => self
                .persistent_path()
                .ok_or_else(|| Error::Corruption {
                    message: "persistent content backend has no database path".to_owned(),
                })?
                .join(".trine-content-v1"),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => self.object_store_db_path().join("content-v1"),
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => {
                return Err(Error::unsupported_backend(
                    "browser content objects are not implemented in this prototype",
                ));
            }
        };
        Ok(root)
    }

    fn content_chunk_object(&self, upload_id: UploadId, index: u64) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("chunks")
            .join(upload_id.to_string())
            .join(format!("{index:020}.trinec"));
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentChunk,
            path,
        ))
    }

    fn content_descriptor_object(&self, content_id: ContentId) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("descriptors")
            .join("sha256")
            .join(format!("{}.trined", hex_digest(content_id.digest())));
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentDescriptor,
            path,
        ))
    }

    fn content_upload_state_object(&self, upload_id: UploadId) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("uploads")
            .join(format!("{upload_id}.trineu"));
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentUpload,
            path,
        ))
    }

    async fn write_content_object(&self, object: StorageObjectId, bytes: Arc<[u8]>) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let durability = self.content_durability();
        match &self.inner.options.storage_mode {
            StorageMode::InMemory => {
                self.inner
                    .content_memory
                    .write_object(object, bytes, durability)
                    .await
            }
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => {
                self.inner
                    .native_storage
                    .write_object(object, bytes, durability)
                    .await
            }
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => {
                self.object_storage()?
                    .write_object(object, bytes, durability)
                    .await
            }
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Err(Error::unsupported_backend(
                "browser content objects are not implemented in this prototype",
            )),
        }
    }

    async fn read_content_object(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        self.ensure_open()?;
        match &self.inner.options.storage_mode {
            StorageMode::InMemory => self.inner.content_memory.read_object_bytes(object).await,
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => self.inner.native_storage.read_object_bytes(object).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => self.object_storage()?.read_object_bytes(object).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Err(Error::unsupported_backend(
                "browser content objects are not implemented in this prototype",
            )),
        }
    }

    async fn delete_content_object(&self, object: StorageObjectId) -> Result<()> {
        self.ensure_open()?;
        match &self.inner.options.storage_mode {
            StorageMode::InMemory => self.inner.content_memory.delete_object(object).await,
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => self.inner.native_storage.delete_object(object).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => self.object_storage()?.delete_object(object).await,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => Err(Error::unsupported_backend(
                "browser content objects are not implemented in this prototype",
            )),
        }
    }

    fn content_durability(&self) -> DurabilityMode {
        match &self.inner.options.storage_mode {
            StorageMode::Persistent { .. } => self.filesystem_publish_durability(),
            StorageMode::InMemory
            | StorageMode::HostPersistent {
                backend:
                    HostStorageBackend::Wasi { .. }
                    | HostStorageBackend::Browser { .. }
                    | HostStorageBackend::ObjectStore,
            } => DurabilityMode::Flush,
        }
    }
}

fn hex_digest(digest: [u8; 32]) -> String {
    use std::fmt::Write;

    let mut value = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(value, "{byte:02x}");
    }
    value
}
