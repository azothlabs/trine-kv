use super::{
    Arc, ContentAccessBarrierRecord, ContentId, Db, DurabilityMode, Error, HostStorageBackend,
    MutexGuard, PathBuf, Result, StorageDomainId, StorageMode, StorageObjectDeleteBackend,
    StorageObjectId, StorageObjectKind, StorageObjectReadBackend, StorageObjectWriteBackend,
    UploadId, UploadSessionState, content_lock_shard_index,
};
use crate::storage::{StorageObjectListBackend, StorageObjectListRequest};

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
        let state = state.with_updated_at_unix_ms(crate::content::current_epoch_millis()?);
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

    pub(super) async fn list_upload_states(&self) -> Result<Vec<UploadSessionState>> {
        let root = self.content_root()?.join("uploads");
        let request = StorageObjectListRequest::native_file(StorageObjectKind::ContentUpload, root)
            .with_file_extension("trineu");
        let objects = match &self.inner.options.storage_mode {
            StorageMode::InMemory => self.inner.content_memory.list_objects(request).await?,
            StorageMode::Persistent { .. }
            | StorageMode::HostPersistent {
                backend: HostStorageBackend::Wasi { .. },
            } => self.inner.native_storage.list_objects(request).await?,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::ObjectStore,
            } => self.object_storage()?.list_objects(request).await?,
            StorageMode::HostPersistent {
                backend: HostStorageBackend::Browser { .. },
            } => {
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                {
                    self.browser_storage()?.list_objects(request).await?
                }
                #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
                {
                    return Err(Error::unsupported_backend(
                        "browser content objects require wasm32-unknown-unknown",
                    ));
                }
            }
        };

        let mut states = Vec::with_capacity(objects.len());
        for object in objects {
            let upload_id = upload_id_from_state_object(&object)?;
            let Some(bytes) = self.read_content_object(object).await? else {
                // A concurrent abort or maintenance pass can remove a state
                // after listing. Absence is not corruption; a later listing
                // will simply omit the completed cleanup.
                continue;
            };
            states.push(UploadSessionState::decode(&bytes, upload_id)?);
        }
        states.sort_unstable_by_key(|state| state.upload_id());
        Ok(states)
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

fn upload_id_from_state_object(object: &StorageObjectId) -> Result<UploadId> {
    let stem = object
        .path()
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| Error::Corruption {
            message: format!(
                "content upload state object {} has no UTF-8 identity",
                object.path().display()
            ),
        })?;
    if stem.len() != 32 {
        return Err(Error::Corruption {
            message: format!(
                "content upload state object {} has malformed identity length",
                object.path().display()
            ),
        });
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let offset = index * 2;
        let high = decode_hex_nibble(stem.as_bytes()[offset])?;
        let low = decode_hex_nibble(stem.as_bytes()[offset + 1])?;
        *byte = (high << 4) | low;
    }
    Ok(UploadId::from_bytes(bytes))
}

fn decode_hex_nibble(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(Error::Corruption {
            message: "content upload state object has a non-hex identity".to_owned(),
        }),
    }
}
