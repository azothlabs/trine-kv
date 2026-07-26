use super::{
    Arc, ContentAccessBarrierRecord, ContentId, Db, DurabilityMode, Error, HostStorageBackend,
    MutexGuard, PathBuf, Result, StorageDomainId, StorageMode, StorageObjectId, StorageObjectKind,
    UploadId, UploadIdRetirement, UploadSessionState, backend::ContentObjectBackend,
    content_lock_shard_index, decode_upload_id_tombstone, encode_upload_id_tombstone,
};
use crate::content::UploadStatePublish;
use crate::object_store::{Precondition, PutIf};
use crate::storage::StorageObjectListRequest;

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

    pub(crate) async fn write_content_partial_chunk(
        &self,
        upload_id: UploadId,
        index: u64,
        revision: u64,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        let object = self.content_partial_chunk_object(upload_id, index, revision)?;
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

    pub(crate) async fn read_content_partial_chunk(
        &self,
        upload_id: UploadId,
        index: u64,
        revision: u64,
    ) -> Result<Option<Arc<[u8]>>> {
        let object = self.content_partial_chunk_object(upload_id, index, revision)?;
        self.read_content_object(object).await
    }

    pub(crate) async fn delete_content_chunk(&self, upload_id: UploadId, index: u64) -> Result<()> {
        let object = self.content_chunk_object(upload_id, index)?;
        self.delete_content_object(object).await
    }

    pub(crate) async fn delete_content_partial_chunk(
        &self,
        upload_id: UploadId,
        index: u64,
        revision: u64,
    ) -> Result<()> {
        let object = self.content_partial_chunk_object(upload_id, index, revision)?;
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
        let bytes = state.encode()?;
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return self
                .write_upload_state_object_store_cas(object, &state, bytes)
                .await;
        }
        self.write_content_object(object, bytes).await
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
        if let Some(retirement) = decode_upload_id_tombstone(&bytes, upload_id)? {
            return Err(upload_retirement_error(upload_id, retirement));
        }
        UploadSessionState::decode(&bytes, upload_id)
    }

    pub(super) async fn retire_upload_id(
        &self,
        upload_id: UploadId,
        retirement: UploadIdRetirement,
    ) -> Result<()> {
        let object = self.content_upload_state_object(upload_id)?;
        let bytes = encode_upload_id_tombstone(upload_id, retirement);
        if self.inner.options.storage_mode.is_object_store_persistent() {
            return self
                .retire_upload_id_object_store_cas(object, upload_id, retirement, bytes)
                .await;
        }
        self.write_content_object(object, bytes).await
    }

    pub(super) async fn delete_upload_state(&self, upload_id: UploadId) -> Result<()> {
        self.retire_upload_id(upload_id, UploadIdRetirement::Aborted)
            .await
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

    fn content_partial_chunk_object(
        &self,
        upload_id: UploadId,
        index: u64,
        revision: u64,
    ) -> Result<StorageObjectId> {
        let path = self
            .content_root()?
            .join("chunks")
            .join(upload_id.to_string())
            .join(format!("partial-{index:020}-{revision:020}.trinec"));
        Ok(StorageObjectId::native_file(
            StorageObjectKind::ContentChunk,
            path,
        ))
    }

    pub(super) async fn list_content_upload_chunk_objects(
        &self,
        upload_id: UploadId,
    ) -> Result<Vec<StorageObjectId>> {
        let root = self
            .content_root()?
            .join("chunks")
            .join(upload_id.to_string());
        let request = StorageObjectListRequest::native_file(StorageObjectKind::ContentChunk, root)
            .with_file_extension("trinec");
        ContentObjectBackend::for_db(self)?.list(request).await
    }

    pub(super) async fn delete_content_chunk_object(&self, object: StorageObjectId) -> Result<()> {
        if object.kind() != StorageObjectKind::ContentChunk {
            return Err(Error::Corruption {
                message: "content chunk cleanup received a non-chunk object".to_owned(),
            });
        }
        self.delete_content_object(object).await
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
        let objects = self.list_upload_state_objects().await?;
        let mut states = Vec::with_capacity(objects.len());
        for object in objects {
            let upload_id = upload_id_from_state_object(&object)?;
            let Some(bytes) = self.read_content_object(object).await? else {
                // A concurrent maintenance pass can change the listing between
                // requests. Absence is not corruption; a later listing will
                // simply omit that entry.
                continue;
            };
            if decode_upload_id_tombstone(&bytes, upload_id)?.is_some() {
                continue;
            }
            states.push(UploadSessionState::decode(&bytes, upload_id)?);
        }
        states.sort_unstable_by_key(|state| state.upload_id());
        Ok(states)
    }

    pub(super) async fn cleanup_retired_upload_chunks(&self) -> Result<()> {
        for state_object in self.list_upload_state_objects().await? {
            let upload_id = upload_id_from_state_object(&state_object)?;
            let Some(bytes) = self.read_content_object(state_object).await? else {
                continue;
            };
            let Some(retirement) = decode_upload_id_tombstone(&bytes, upload_id)? else {
                continue;
            };
            for chunk in self.list_content_upload_chunk_objects(upload_id).await? {
                let is_partial = chunk
                    .path()
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("partial-"));
                if retirement == UploadIdRetirement::Aborted || is_partial {
                    self.delete_content_chunk_object(chunk).await?;
                }
            }
        }
        Ok(())
    }

    async fn list_upload_state_objects(&self) -> Result<Vec<StorageObjectId>> {
        let root = self.content_root()?.join("uploads");
        let request = StorageObjectListRequest::native_file(StorageObjectKind::ContentUpload, root)
            .with_file_extension("trineu");
        ContentObjectBackend::for_db(self)?.list(request).await
    }

    async fn write_content_object(&self, object: StorageObjectId, bytes: Arc<[u8]>) -> Result<()> {
        self.ensure_open()?;
        if self.inner.options.read_only {
            return Err(Error::ReadOnly);
        }
        let durability = self.content_durability();
        let backend = ContentObjectBackend::for_db(self)?;
        if backend.requires_lease_fence() {
            self.fence_object_mutation_or_close("before content-object write")
                .await?;
        }
        let result = backend.write(object, bytes, durability).await;
        if backend.requires_lease_fence() {
            self.fence_object_mutation_or_close("after content-object write")
                .await?;
        }
        result
    }

    async fn write_upload_state_object_store_cas(
        &self,
        object: StorageObjectId,
        intended: &UploadSessionState,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        self.fence_object_mutation_or_close("before upload-state CAS")
            .await?;
        let backend = self.object_storage()?;
        let observed = backend.read_object_versioned(&object).await?;
        let precondition = match observed {
            None => match intended.plan_publish_against(None)? {
                UploadStatePublish::Create => Precondition::IfNoneMatch,
                UploadStatePublish::Replace { .. } | UploadStatePublish::AlreadyApplied => {
                    return Err(Error::Corruption {
                        message: "absent upload state produced a non-create transition".to_owned(),
                    });
                }
            },
            Some((current_bytes, etag)) => {
                if let Some(retirement) =
                    decode_upload_id_tombstone(&current_bytes, intended.upload_id())?
                {
                    return Err(upload_retirement_error(intended.upload_id(), retirement));
                }
                let current = UploadSessionState::decode(&current_bytes, intended.upload_id())?;
                match intended.plan_publish_against(Some(&current))? {
                    UploadStatePublish::Replace { .. } => Precondition::IfMatch(etag),
                    UploadStatePublish::AlreadyApplied => {
                        self.fence_object_mutation_or_close("after idempotent upload-state CAS")
                            .await?;
                        return Ok(());
                    }
                    UploadStatePublish::Create => {
                        return Err(Error::Corruption {
                            message: "present upload state produced a create transition".to_owned(),
                        });
                    }
                }
            }
        };

        let publish = backend
            .put_object_if(&object, Arc::clone(&bytes), precondition)
            .await;
        let result = match publish {
            Ok(PutIf::Stored { .. }) => Ok(()),
            Ok(PutIf::PreconditionFailed { .. }) => {
                self.reconcile_upload_state_cas(&backend, &object, intended)
                    .await
            }
            Err(error) => match self
                .reconcile_upload_state_cas(&backend, &object, intended)
                .await
            {
                Ok(()) => Ok(()),
                Err(_) => Err(error),
            },
        };
        self.fence_object_mutation_or_close("after upload-state CAS")
            .await?;
        result
    }

    async fn reconcile_upload_state_cas(
        &self,
        backend: &crate::object_store::ObjectStoreBackend,
        object: &StorageObjectId,
        intended: &UploadSessionState,
    ) -> Result<()> {
        let Some((current_bytes, _)) = backend.read_object_versioned(object).await? else {
            return Err(Error::ContentUploadConflict {
                upload_id: intended.upload_id().to_string(),
                expected_revision: intended.revision(),
                actual_revision: 0,
            });
        };
        if let Some(retirement) = decode_upload_id_tombstone(&current_bytes, intended.upload_id())?
        {
            return Err(upload_retirement_error(intended.upload_id(), retirement));
        }
        let current = UploadSessionState::decode(&current_bytes, intended.upload_id())?;
        if current.logically_eq_ignoring_updated_at(intended) {
            Ok(())
        } else {
            Err(Error::ContentUploadConflict {
                upload_id: intended.upload_id().to_string(),
                expected_revision: intended.revision(),
                actual_revision: current.revision(),
            })
        }
    }

    async fn retire_upload_id_object_store_cas(
        &self,
        object: StorageObjectId,
        upload_id: UploadId,
        retirement: UploadIdRetirement,
        bytes: Arc<[u8]>,
    ) -> Result<()> {
        self.fence_object_mutation_or_close("before upload-id retirement")
            .await?;
        let backend = self.object_storage()?;
        let Some((current_bytes, etag)) = backend.read_object_versioned(&object).await? else {
            return Err(Error::ContentUploadNotFound {
                upload_id: upload_id.to_string(),
            });
        };
        if let Some(current) = decode_upload_id_tombstone(&current_bytes, upload_id)? {
            let result = if current == retirement {
                Ok(())
            } else {
                Err(upload_retirement_error(upload_id, current))
            };
            self.fence_object_mutation_or_close("after idempotent upload-id retirement")
                .await?;
            return result;
        }
        let current = UploadSessionState::decode(&current_bytes, upload_id)?;
        current.require_retirement(retirement)?;
        let publish = backend
            .put_object_if(&object, bytes, Precondition::IfMatch(etag))
            .await;
        let result: Result<()> = match publish {
            Ok(PutIf::Stored { .. }) => Ok(()),
            Ok(PutIf::PreconditionFailed { .. }) | Err(_) => {
                match backend.read_object_versioned(&object).await {
                    Ok(Some((current_bytes, _))) => {
                        match decode_upload_id_tombstone(&current_bytes, upload_id) {
                            Ok(Some(current)) if current == retirement => Ok(()),
                            Ok(Some(current)) => Err(upload_retirement_error(upload_id, current)),
                            Ok(None) => {
                                match UploadSessionState::decode(&current_bytes, upload_id) {
                                    Ok(current_state) => Err(Error::ContentUploadConflict {
                                        upload_id: upload_id.to_string(),
                                        expected_revision: current.revision(),
                                        actual_revision: current_state.revision(),
                                    }),
                                    Err(error) => Err(error),
                                }
                            }
                            Err(error) => Err(error),
                        }
                    }
                    Ok(None) => Err(Error::ContentUploadNotFound {
                        upload_id: upload_id.to_string(),
                    }),
                    Err(error) => Err(error),
                }
            }
        };
        self.fence_object_mutation_or_close("after upload-id retirement")
            .await?;
        result
    }

    async fn fence_object_mutation_or_close(&self, stage: &'static str) -> Result<()> {
        self.inner
            .substrate
            .fence_object_mutation_async()
            .await
            .map_err(|error| {
                let message =
                    format!("object mutation lease fence failed {stage}: {error}; database closed");
                self.stop_writes_after_fatal_error(
                    crate::db::FatalWriteStopReason::Fenced,
                    Error::Corruption {
                        message: message.clone(),
                    },
                );
                Error::Corruption { message }
            })
    }

    pub(super) async fn read_content_object(
        &self,
        object: StorageObjectId,
    ) -> Result<Option<Arc<[u8]>>> {
        self.ensure_open()?;
        ContentObjectBackend::for_db(self)?.read(object).await
    }

    async fn delete_content_object(&self, object: StorageObjectId) -> Result<()> {
        self.ensure_open()?;
        let backend = ContentObjectBackend::for_db(self)?;
        if backend.requires_lease_fence() {
            self.fence_object_mutation_or_close("before content-object deletion")
                .await?;
        }
        let result = backend.delete(object, self.content_durability()).await;
        if backend.requires_lease_fence() {
            self.fence_object_mutation_or_close("after content-object deletion")
                .await?;
        }
        result
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

fn upload_retirement_error(upload_id: UploadId, retirement: UploadIdRetirement) -> Error {
    match retirement {
        UploadIdRetirement::Aborted => Error::ContentUploadNotFound {
            upload_id: upload_id.to_string(),
        },
        UploadIdRetirement::Sealed => Error::ContentUploadSealed {
            upload_id: upload_id.to_string(),
        },
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
