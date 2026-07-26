use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{
    ETag, ObjectClient, ObjectMeta, Precondition, PutIf, canonical_object_key,
    canonical_object_prefix,
};
use crate::error::{Error, Result};
use crate::invariants::immutable_retry_allowed;
use crate::object_store::contract::{
    validate_object_list_page, verify_object_store_reclamation_absent,
};
use crate::options::DurabilityMode;
use crate::storage::{
    StorageCapabilities, StorageFuture, StorageObjectDeleteBackend, StorageObjectId,
    StorageObjectKind, StorageObjectListBackend, StorageObjectListPage as StorageListPage,
    StorageObjectListRequest, StorageObjectReadBackend, StorageObjectWriteBackend,
    StorageReadBackend, StorageReadFuture, StorageReadObject, ensure_whole_object_read_len,
};

/// An object-storage **byte** backend: `SSTable` and blob object IO over an
/// [`ObjectClient`].
///
/// It implements the async `Storage*Backend` byte traits the generic
/// table/blob async helpers use. The WAL, manifest CAS, and writer lease live
/// in the object-storage durability substrate.
///
/// A [`StorageObjectId`]'s path is used directly as the object key, so keys are
/// consistent across read / write / list / delete (the open path joins file
/// names under the database's key prefix, mirroring the filesystem layout).
#[derive(Clone)]
pub(crate) struct ObjectStoreBackend {
    client: Arc<dyn ObjectClient>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreBackend")
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    pub(crate) fn new(client: Arc<dyn ObjectClient>) -> Self {
        Self { client }
    }

    pub(crate) fn client(&self) -> Arc<dyn ObjectClient> {
        Arc::clone(&self.client)
    }

    fn object_key(object: &StorageObjectId) -> Result<String> {
        canonical_object_key(object.path())
    }

    pub(crate) async fn delete_unversioned_object_verified(
        &self,
        object: StorageObjectId,
    ) -> Result<()> {
        let key = Self::object_key(&object)?;
        match self.client.head(&key).await? {
            Some(meta) if meta.version.is_some() => {
                return Err(Error::unsupported_backend(
                    "object-store content key has a provider version",
                ));
            }
            Some(_) => {
                self.client.delete(&key).await?;
            }
            None => {}
        }
        verify_object_store_reclamation_absent(&self.client, &key).await
    }

    pub(crate) async fn read_object_versioned(
        &self,
        object: &StorageObjectId,
    ) -> Result<Option<(Arc<[u8]>, ETag)>> {
        let key = Self::object_key(object)?;
        let Some(meta) = self.client.head(&key).await? else {
            return Ok(None);
        };
        let bytes = read_object_bytes_by_meta(self.client.as_ref(), &key, object, &meta).await?;
        Ok(Some((bytes, meta.etag)))
    }

    pub(crate) async fn put_object_if(
        &self,
        object: &StorageObjectId,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> Result<PutIf> {
        let key = Self::object_key(object)?;
        self.client.put_if(&key, bytes, precondition).await
    }
}

/// A bounded random-access object handle.
///
/// Opening performs only `HEAD`; table/blob sections are fetched with range
/// requests on demand. This keeps database open memory proportional to metadata
/// rather than to the sum of every referenced immutable object.
#[derive(Clone)]
pub(crate) struct ObjectStoreReadObject {
    object: StorageObjectId,
    client: Arc<dyn ObjectClient>,
    key: String,
    len: u64,
    etag: ETag,
}

impl std::fmt::Debug for ObjectStoreReadObject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreReadObject")
            .field("object", &self.object)
            .field("key", &self.key)
            .field("len", &self.len)
            .field("etag", &self.etag)
            .finish_non_exhaustive()
    }
}

impl StorageReadObject for ObjectStoreReadObject {
    fn object(&self) -> &StorageObjectId {
        &self.object
    }

    fn len(&self) -> StorageReadFuture<'_, u64> {
        let len = self.len;
        Box::pin(async move { Ok(len) })
    }

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()> {
        Box::pin(async move {
            let offset = u64::try_from(offset)
                .map_err(|_| Error::invalid_options("object read offset overflow"))?;
            let len = u64::try_from(bytes.len())
                .map_err(|_| Error::invalid_options("object read length overflow"))?;
            let end = offset
                .checked_add(len)
                .ok_or_else(|| Error::invalid_options("object read range overflow"))?;
            if end > self.len {
                return Err(Error::Corruption {
                    message: format!("object {} short read", self.object.path().display()),
                });
            }
            if bytes.is_empty() {
                return Ok(());
            }
            let read = self
                .client
                .get_range(&self.key, offset, len, &self.etag)
                .await
                .map_err(|error| immutable_object_read_error(&self.object, error))?;
            if read.len() != bytes.len() {
                return Err(Error::Corruption {
                    message: format!(
                        "object {} range read returned {} bytes for requested length {}",
                        self.object.path().display(),
                        read.len(),
                        bytes.len()
                    ),
                });
            }
            bytes.copy_from_slice(&read);
            Ok(())
        })
    }
}

impl StorageReadBackend for ObjectStoreBackend {
    type ReadObject = ObjectStoreReadObject;

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities::object_store()
    }

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject> {
        Box::pin(async move {
            let key = Self::object_key(&object)?;
            let meta = self
                .client
                .head(&key)
                .await?
                .ok_or_else(|| Error::Corruption {
                    message: format!("referenced object {key} cannot be opened"),
                })?;
            ensure_object_meta_read_len(&object, &meta)?;
            Ok(ObjectStoreReadObject {
                object,
                client: Arc::clone(&self.client),
                key,
                len: meta.size,
                etag: meta.etag,
            })
        })
    }
}

impl StorageObjectReadBackend for ObjectStoreBackend {
    fn read_object_bytes(&self, object: StorageObjectId) -> StorageFuture<'_, Option<Arc<[u8]>>> {
        Box::pin(async move {
            let key = Self::object_key(&object)?;
            let Some(meta) = self.client.head(&key).await? else {
                return Ok(None);
            };
            read_object_bytes_by_meta(self.client.as_ref(), &key, &object, &meta)
                .await
                .map(Some)
        })
    }
}

impl StorageObjectWriteBackend for ObjectStoreBackend {
    fn write_object(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        _durability: DurabilityMode,
    ) -> StorageFuture<'_, ()> {
        // A PUT is durable once the store acknowledges it, so durability hints do
        // not apply (there is no separate flush/fsync step).
        Box::pin(async move {
            let key = Self::object_key(&object)?;
            if matches!(
                object.kind(),
                StorageObjectKind::Table
                    | StorageObjectKind::Blob
                    | StorageObjectKind::ContentAccessBarrier
                    | StorageObjectKind::ContentChunk
                    | StorageObjectKind::ContentDescriptor
            ) {
                let intended = Arc::clone(&bytes);
                return match self
                    .client
                    .put_if(&key, bytes, Precondition::IfNoneMatch)
                    .await
                {
                    Ok(PutIf::Stored { .. }) => Ok(()),
                    Ok(PutIf::PreconditionFailed { .. }) => {
                        let current = self.client.get(&key).await?;
                        if immutable_retry_allowed(
                            current.is_some(),
                            current.as_ref().is_some_and(|bytes| bytes == &intended),
                        ) {
                            Ok(())
                        } else {
                            Err(Error::Corruption {
                                message: format!(
                                    "immutable object {key} already exists with different bytes"
                                ),
                            })
                        }
                    }
                    Err(error) => {
                        let observed = self.client.get(&key).await;
                        if immutable_retry_allowed(
                            observed.as_ref().is_ok_and(Option::is_some),
                            observed
                                .as_ref()
                                .is_ok_and(|current| current.as_ref() == Some(&intended)),
                        ) {
                            Ok(())
                        } else if observed.as_ref().is_ok_and(Option::is_some) {
                            Err(Error::Corruption {
                                message: format!(
                                    "immutable object {key} appeared with different bytes after an uncertain create"
                                ),
                            })
                        } else {
                            Err(error)
                        }
                    }
                };
            }
            self.client.put(&key, bytes).await.map(|_| ())
        })
    }
}

impl StorageObjectDeleteBackend for ObjectStoreBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()> {
        Box::pin(async move {
            if matches!(
                object.kind(),
                StorageObjectKind::Table | StorageObjectKind::Blob
            ) {
                return Err(Error::unsupported_backend(
                    "immutable object-store table/blob deletion requires a durable reader-retirement protocol",
                ));
            }
            let key = Self::object_key(&object)?;
            self.client.delete(&key).await
        })
    }
}

impl StorageObjectListBackend for ObjectStoreBackend {
    fn list_objects(
        &self,
        request: StorageObjectListRequest,
    ) -> StorageFuture<'_, Vec<StorageObjectId>> {
        Box::pin(async move {
            let mut objects = Vec::new();
            let mut after = None;
            loop {
                let page = self
                    .list_objects_page(request.clone(), after.as_deref(), 1_024)
                    .await?;
                objects.extend(page.objects);
                let Some(next_after) = page.next_after else {
                    break;
                };
                after = Some(next_after);
            }
            objects.sort_unstable();
            Ok(objects)
        })
    }

    fn list_objects_page(
        &self,
        request: StorageObjectListRequest,
        after: Option<&str>,
        limit: usize,
    ) -> StorageFuture<'_, StorageListPage> {
        let after = after.map(str::to_owned);
        Box::pin(async move {
            let kind = request.kind();
            let extension = request.file_extension();
            let prefix = canonical_object_key(request.root())?;
            let root = PathBuf::from(&prefix);
            let listing_prefix = if prefix.is_empty() {
                prefix.clone()
            } else {
                format!("{prefix}/")
            };
            let page = self
                .client
                .list_page(&listing_prefix, after.as_deref(), limit)
                .await?;
            validate_object_list_page(&listing_prefix, after.as_deref(), limit, &page)?;
            let mut objects = Vec::new();
            for meta in page.objects {
                let canonical = canonical_object_prefix(&meta.key)?;
                if canonical != meta.key {
                    return Err(Error::Corruption {
                        message: format!("object store returned non-canonical key {:?}", meta.key),
                    });
                }
                let path = PathBuf::from(canonical);
                if path.parent() == Some(root.as_path()) && path_matches_extension(&path, extension)
                {
                    objects.push(StorageObjectId::native_file(kind, path));
                }
            }
            objects.sort_unstable();
            Ok(StorageListPage {
                objects,
                next_after: page.next_after,
            })
        })
    }
}

fn path_matches_extension(path: &Path, expected: Option<&str>) -> bool {
    expected.is_none_or(|expected| {
        path.extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
    })
}

fn ensure_object_meta_read_len(object: &StorageObjectId, meta: &ObjectMeta) -> Result<()> {
    let len = usize::try_from(meta.size).map_err(|_| Error::Corruption {
        message: format!("object {} length exceeds usize", object.path().display()),
    })?;
    ensure_whole_object_read_len(object, len)
}

async fn read_object_bytes_by_meta(
    client: &dyn ObjectClient,
    key: &str,
    object: &StorageObjectId,
    meta: &ObjectMeta,
) -> Result<Arc<[u8]>> {
    ensure_object_meta_read_len(object, meta)?;
    let expected_len = usize::try_from(meta.size).map_err(|_| Error::Corruption {
        message: format!("object {} length exceeds usize", object.path().display()),
    })?;
    if expected_len == 0 {
        let current = client.head(key).await?.ok_or_else(|| Error::Corruption {
            message: format!("object {} disappeared after HEAD", object.path().display()),
        })?;
        if current.size != 0 || current.etag != meta.etag {
            return Err(Error::Corruption {
                message: format!(
                    "object {} changed after metadata was read",
                    object.path().display()
                ),
            });
        }
        return Ok(Arc::from([]));
    }
    let bytes = client
        .get_range(key, 0, meta.size, &meta.etag)
        .await
        .map_err(|error| immutable_object_read_error(object, error))?;
    if bytes.len() != expected_len {
        return Err(Error::Corruption {
            message: format!(
                "object {} range read returned {} bytes for declared length {expected_len}",
                object.path().display(),
                bytes.len()
            ),
        });
    }
    ensure_whole_object_read_len(object, bytes.len())?;
    Ok(bytes)
}

fn immutable_object_read_error(object: &StorageObjectId, error: Error) -> Error {
    match error {
        Error::ObjectVersionChanged { .. } => Error::Corruption {
            message: format!(
                "immutable object {} changed or disappeared while being read",
                object.path().display()
            ),
        },
        other => other,
    }
}
