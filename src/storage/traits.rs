//! Fine-grained storage capabilities consumed by the engine.

use std::sync::Arc;

use crate::{
    error::{Error, Result},
    options::DurabilityMode,
};

use super::{
    StorageCapabilities, StorageDirectoryFile, StorageDirectoryId, StorageFuture, StorageObjectId,
    StorageObjectListPage, StorageObjectListRequest, StorageReadBuffer, StorageReadFuture,
    StorageSharedBound, StorageThreadBound, allocate_read_buffer, poll_ready_storage_future,
};

pub(crate) trait StorageReadObject: StorageSharedBound {
    #[allow(dead_code)]
    fn object(&self) -> &StorageObjectId;

    fn len(&self) -> StorageReadFuture<'_, u64>;

    fn read_exact_at<'op>(
        &'op self,
        offset: usize,
        bytes: &'op mut [u8],
    ) -> StorageReadFuture<'op, ()>;

    fn read_exact_at_owned(
        &self,
        offset: usize,
        len: usize,
    ) -> StorageReadFuture<'_, StorageReadBuffer> {
        Box::pin(async move {
            let mut bytes = allocate_read_buffer(len)?;
            self.read_exact_at(offset, &mut bytes).await?;
            Ok(StorageReadBuffer::from_vec(offset, bytes))
        })
    }
}

pub(crate) trait BlockingStorageReadObject: StorageReadObject {
    fn len_blocking(&self) -> Result<u64>;

    fn read_exact_at_blocking(&self, offset: usize, bytes: &mut [u8]) -> Result<()>;

    fn read_exact_at_owned_blocking(&self, offset: usize, len: usize) -> Result<StorageReadBuffer> {
        poll_ready_storage_future(StorageReadObject::read_exact_at_owned(self, offset, len))
    }
}

pub(crate) trait StorageReadBackend: StorageSharedBound {
    type ReadObject: StorageReadObject;

    fn capabilities(&self) -> StorageCapabilities;

    fn open_read(&self, object: StorageObjectId) -> StorageReadFuture<'_, Self::ReadObject>;
}

pub(crate) trait BlockingStorageReadBackend: StorageReadBackend
where
    Self::ReadObject: BlockingStorageReadObject,
{
    fn open_read_blocking(&self, object: StorageObjectId) -> Result<Self::ReadObject>;
}

pub(crate) trait StorageObjectReadBackend: StorageReadBackend {
    fn read_object_bytes(&self, object: StorageObjectId) -> StorageFuture<'_, Option<Arc<[u8]>>>;
}

pub(crate) trait BlockingStorageObjectReadBackend: StorageObjectReadBackend {
    fn read_object_bytes_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>>;
}

pub(crate) trait StorageAppendObject: StorageThreadBound {
    fn append<'op>(
        &'op mut self,
        bytes: &'op [u8],
        durability: DurabilityMode,
    ) -> StorageFuture<'op, ()>;

    fn persist(&mut self, durability: DurabilityMode) -> StorageFuture<'_, ()>;
}

#[allow(dead_code)]
pub(crate) trait BlockingStorageAppendObject: StorageAppendObject {
    fn append_blocking(&mut self, bytes: &[u8], durability: DurabilityMode) -> Result<()> {
        poll_ready_storage_future(StorageAppendObject::append(self, bytes, durability))
    }

    fn persist_blocking(&mut self, durability: DurabilityMode) -> Result<()> {
        poll_ready_storage_future(StorageAppendObject::persist(self, durability))
    }
}

pub(crate) trait StorageAppendBackend: StorageReadBackend {
    type AppendObject: StorageAppendObject;

    fn open_append(&self, object: StorageObjectId) -> StorageFuture<'_, Self::AppendObject>;
}

pub(crate) trait BlockingStorageAppendBackend: StorageAppendBackend
where
    Self::AppendObject: BlockingStorageAppendObject,
{
    fn open_append_blocking(&self, object: StorageObjectId) -> Result<Self::AppendObject> {
        poll_ready_storage_future(self.open_append(object))
    }
}

pub(crate) trait StorageWalRewriteBackend: StorageReadBackend {
    fn rewrite_wal(
        &self,
        object: StorageObjectId,
        temporary_object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()>;
}

#[allow(dead_code)]
pub(crate) trait BlockingStorageWalRewriteBackend: StorageWalRewriteBackend {
    fn rewrite_wal_blocking(
        &self,
        object: StorageObjectId,
        temporary_object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        poll_ready_storage_future(self.rewrite_wal(object, temporary_object, bytes, durability))
    }
}

pub(crate) trait StorageWriterLeaseBackend: StorageReadBackend {
    type WriterLease: StorageThreadBound;

    fn acquire_writer_lease(&self, object: StorageObjectId)
    -> StorageFuture<'_, Self::WriterLease>;
}

pub(crate) trait BlockingStorageWriterLeaseBackend: StorageWriterLeaseBackend {
    fn acquire_writer_lease_blocking(&self, object: StorageObjectId) -> Result<Self::WriterLease> {
        poll_ready_storage_future(self.acquire_writer_lease(object))
    }
}

pub(crate) trait StorageDirectoryCreateBackend: StorageReadBackend {
    fn create_directory_all(&self, directory: StorageDirectoryId) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageDirectoryCreateBackend:
    StorageDirectoryCreateBackend
{
    fn create_directory_all_blocking(&self, directory: StorageDirectoryId) -> Result<()> {
        poll_ready_storage_future(self.create_directory_all(directory))
    }
}

pub(crate) trait StorageDirectoryListBackend: StorageReadBackend {
    fn list_directory_files(
        &self,
        directory: StorageDirectoryId,
    ) -> StorageFuture<'_, Vec<StorageDirectoryFile>>;
}

pub(crate) trait BlockingStorageDirectoryListBackend: StorageDirectoryListBackend {
    fn list_directory_files_blocking(
        &self,
        directory: StorageDirectoryId,
    ) -> Result<Vec<StorageDirectoryFile>> {
        poll_ready_storage_future(self.list_directory_files(directory))
    }
}

pub(crate) trait StorageDirectorySyncBackend: StorageReadBackend {
    fn sync_directory_after_renames(&self, directory: StorageDirectoryId) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageDirectorySyncBackend: StorageDirectorySyncBackend {
    fn sync_directory_after_renames_blocking(&self, directory: StorageDirectoryId) -> Result<()> {
        poll_ready_storage_future(self.sync_directory_after_renames(directory))
    }
}

pub(crate) trait StorageManifestReadBackend: StorageReadBackend {
    fn read_current_manifest(
        &self,
        object: StorageObjectId,
    ) -> StorageFuture<'_, Option<Arc<[u8]>>>;
}

pub(crate) trait BlockingStorageManifestReadBackend: StorageManifestReadBackend {
    fn read_current_manifest_blocking(&self, object: StorageObjectId) -> Result<Option<Arc<[u8]>>> {
        poll_ready_storage_future(self.read_current_manifest(object))
    }
}

pub(crate) trait StorageManifestPublishBackend: StorageReadBackend {
    fn publish_manifest(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageManifestPublishBackend:
    StorageManifestPublishBackend
{
    fn publish_manifest_blocking(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        poll_ready_storage_future(self.publish_manifest(object, bytes, durability))
    }
}

pub(crate) trait StorageObjectWriteBackend: StorageReadBackend {
    fn write_object(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageObjectWriteBackend: StorageObjectWriteBackend {
    fn write_object_blocking(
        &self,
        object: StorageObjectId,
        bytes: Arc<[u8]>,
        durability: DurabilityMode,
    ) -> Result<()> {
        poll_ready_storage_future(self.write_object(object, bytes, durability))
    }
}

pub(crate) trait StorageObjectDeleteBackend: StorageReadBackend {
    fn delete_object(&self, object: StorageObjectId) -> StorageFuture<'_, ()>;
}

pub(crate) trait BlockingStorageObjectDeleteBackend: StorageObjectDeleteBackend {
    fn delete_object_blocking(&self, object: StorageObjectId) -> Result<()> {
        poll_ready_storage_future(self.delete_object(object))
    }
}

pub(crate) trait StorageObjectListBackend: StorageReadBackend {
    fn list_objects(
        &self,
        request: StorageObjectListRequest,
    ) -> StorageFuture<'_, Vec<StorageObjectId>>;

    fn list_objects_page(
        &self,
        request: StorageObjectListRequest,
        after: Option<&str>,
        limit: usize,
    ) -> StorageFuture<'_, StorageObjectListPage>;
}

pub(super) fn paginate_storage_objects(
    objects: Vec<StorageObjectId>,
    after: Option<&str>,
    limit: usize,
) -> Result<StorageObjectListPage> {
    if limit == 0 {
        return Err(Error::invalid_options(
            "storage object listing page limit must be non-zero",
        ));
    }
    let start = match after {
        Some(after) => objects
            .iter()
            .position(|object| object.path().to_string_lossy().as_ref() > after)
            .unwrap_or(objects.len()),
        None => 0,
    };
    let mut eligible = objects.into_iter().skip(start);
    let page_objects = eligible.by_ref().take(limit).collect::<Vec<_>>();
    let has_more = eligible.next().is_some();
    let next_after = has_more.then(|| {
        page_objects
            .last()
            .expect("a page with more objects cannot be empty")
            .path()
            .to_string_lossy()
            .into_owned()
    });
    Ok(StorageObjectListPage {
        objects: page_objects,
        next_after,
    })
}

pub(crate) trait BlockingStorageObjectListBackend: StorageObjectListBackend {
    fn list_objects_blocking(
        &self,
        request: StorageObjectListRequest,
    ) -> Result<Vec<StorageObjectId>> {
        poll_ready_storage_future(self.list_objects(request))
    }
}
