use super::{
    Arc, ContentAccessBarrierRecord, ContentId, Db, DurabilityMode, Error, HostStorageBackend,
    MutexGuard, PathBuf, Result, StorageDomainId, StorageMode, StorageObjectDeleteBackend,
    StorageObjectId, StorageObjectKind, StorageObjectReadBackend, StorageObjectWriteBackend,
    UploadId, UploadSessionState, content_lock_shard_index,
};

impl Db {
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
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        let object = self.content_descriptor_object(storage_domain_id, content_id)?;
        self.write_content_object(object, bytes).await
    }

    pub(crate) async fn read_content_descriptor(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<Option<Arc<[u8]>>> {
        let object = self.content_descriptor_object(storage_domain_id, content_id)?;
        self.read_content_object(object).await
    }

    pub(super) async fn delete_content_descriptor(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<()> {
        let object = self.content_descriptor_object(storage_domain_id, content_id)?;
        self.delete_content_object(object).await
    }

    pub(crate) async fn read_content_access_barrier_record(
        &self,
        storage_domain_id: StorageDomainId,
    ) -> Result<Option<ContentAccessBarrierRecord>> {
        let object = self.content_access_barrier_object(storage_domain_id)?;
        self.read_content_object(object)
            .await?
            .map(|bytes| ContentAccessBarrierRecord::decode(&bytes, storage_domain_id))
            .transpose()
    }

    #[cfg(test)]
    pub(crate) async fn write_content_access_barrier_bytes_for_test(
        &self,
        storage_domain_id: StorageDomainId,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        let object = self.content_access_barrier_object(storage_domain_id)?;
        self.write_content_object(object, bytes).await
    }

    pub(crate) async fn write_content_access_barrier_record(
        &self,
        record: ContentAccessBarrierRecord,
    ) -> Result<()> {
        let object = self.content_access_barrier_object(record.storage_domain_id)?;
        self.write_content_object(object, record.encode()).await
    }

    pub(crate) async fn write_upload_state(&self, state: &UploadSessionState) -> Result<()> {
        let object = self.content_upload_state_object(state.upload_id())?;
        self.write_content_object(object, (*state).encode()?).await
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

    pub(super) async fn delete_upload_state(&self, upload_id: UploadId) -> Result<()> {
        let object = self.content_upload_state_object(upload_id)?;
        self.delete_content_object(object).await
    }

    pub(crate) async fn lock_content_upload(&self, upload_id: UploadId) -> MutexGuard<'_, ()> {
        let shard = content_lock_shard_index(
            &self.inner.content_lock_hasher,
            &upload_id,
            self.inner.content_upload_locks.len(),
        );
        self.inner.content_upload_locks[shard].lock().await
    }

    pub(crate) async fn lock_content_seal(&self) -> MutexGuard<'_, ()> {
        self.inner.content_seal_lock.lock().await
    }

    pub(super) async fn lock_content_quota(
        &self,
        storage_domain_id: StorageDomainId,
    ) -> MutexGuard<'_, ()> {
        let shard = content_lock_shard_index(
            &self.inner.content_lock_hasher,
            &storage_domain_id,
            self.inner.content_quota_locks.len(),
        );
        self.inner.content_quota_locks[shard].lock().await
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
                backend: HostStorageBackend::Browser { path },
            } => path.join(".trine-content-v1"),
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

    fn content_descriptor_object(
        &self,
        storage_domain_id: StorageDomainId,
        content_id: ContentId,
    ) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("domains")
            .join(hex_identifier(storage_domain_id.to_bytes()))
            .join("descriptors")
            .join("sha256")
            .join(format!("{}.trined", hex_identifier(content_id.digest())));
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentDescriptor,
            path,
        ))
    }

    fn content_access_barrier_object(
        &self,
        storage_domain_id: StorageDomainId,
    ) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("domains")
            .join(hex_identifier(storage_domain_id.to_bytes()))
            .join("access")
            .join("leased-only.trinebarrier");
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentAccessBarrier,
            path,
        ))
    }

    pub(super) fn content_upload_state_object(
        &self,
        upload_id: UploadId,
    ) -> Result<StorageObjectId> {
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
            } => {
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                {
                    self.browser_storage()?
                        .write_object(object, bytes, durability)
                        .await
                }
                #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
                {
                    Err(Error::unsupported_backend(
                        "browser content objects require wasm32-unknown-unknown",
                    ))
                }
            }
        }
    }

    pub(super) async fn read_content_object(
        &self,
        object: StorageObjectId,
    ) -> Result<Option<Arc<[u8]>>> {
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
            } => {
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                {
                    self.browser_storage()?.read_object_bytes(object).await
                }
                #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
                {
                    Err(Error::unsupported_backend(
                        "browser content objects require wasm32-unknown-unknown",
                    ))
                }
            }
        }
    }

    async fn delete_content_object(&self, object: StorageObjectId) -> Result<()> {
        self.ensure_open()?;
        match &self.inner.options.storage_mode {
            StorageMode::InMemory => self.inner.content_memory.delete_object(object).await,
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => {
                self.inner
                    .native_storage
                    .delete_object_durable(object, self.content_durability())
                    .await
            }
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => {
                self.object_storage()?
                    .delete_unversioned_object_verified(object)
                    .await
            }
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => {
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                {
                    self.browser_storage()?.delete_object(object).await
                }
                #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
                {
                    Err(Error::unsupported_backend(
                        "browser content objects require wasm32-unknown-unknown",
                    ))
                }
            }
        }
    }

    pub(super) fn content_durability(&self) -> DurabilityMode {
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

fn hex_identifier<const N: usize>(bytes: [u8; N]) -> String {
    use std::fmt::Write;

    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}
