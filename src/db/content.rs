use std::{path::PathBuf, sync::Arc};

use futures::lock::MutexGuard;

use crate::{
    content::{
        ContentDescriptor, ContentHandle, ContentId, ContentUpload, ContentUploadOptions, UploadId,
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
    /// yet implement content objects.
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
    // This remains async because the full protocol reserves durable quota and
    // resumable state here; the evidence prototype performs only local setup.
    #[allow(clippy::unused_async)]
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
        Ok(ContentUpload::new(
            self.clone(),
            UploadId::generate()?,
            options,
        ))
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
