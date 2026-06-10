//! Real object-storage [`ObjectClient`] (S3 and compatible) via the
//! [`object_store`] crate, behind the `s3` feature.
//!
//! `ObjectStoreClient` adapts any `object_store::ObjectStore` to trine's
//! `ObjectClient`, so it works with S3, GCS, Azure, MinIO/R2/Ceph, local
//! files, or object_store's in-memory store. Open a database with
//! `Db::open_object_store(Arc::new(ObjectStoreClient::new(store)), opts)`.
//!
//! The load-bearing conditional write maps directly onto object_store's
//! conditional put: `If-None-Match` → `PutMode::Create`, `If-Match` →
//! `PutMode::Update(UpdateVersion { e_tag })`. A failed precondition is reported
//! to the caller (the manifest commit) as [`PutIf::PreconditionFailed`], not an
//! error.

use std::sync::Arc;

use futures::TryStreamExt;
use object_store::{
    Error as OsError, GetOptions, GetRange, ObjectStore, PutMode, PutOptions, PutPayload,
    UpdateVersion, path::Path as OsPath,
};

use crate::error::{Error, Result};
use crate::object_store::{ETag, ObjectClient, ObjectFuture, ObjectMeta, Precondition, PutIf};

/// Adapts any [`object_store::ObjectStore`] to trine's [`ObjectClient`].
#[derive(Clone)]
pub struct ObjectStoreClient {
    store: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for ObjectStoreClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreClient")
            .field("store", &self.store.to_string())
            .finish()
    }
}

impl ObjectStoreClient {
    /// Wraps an existing object store (S3/GCS/Azure/local/in-memory).
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Convenience constructor for S3 (and S3-compatible) storage.
    ///
    /// Credentials are read from the environment (`AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY`, …). Pass `endpoint` to target an S3-compatible
    /// service (MinIO/R2/Ceph); leave it `None` for AWS S3.
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 client cannot be configured.
    pub fn s3(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: Option<String>,
    ) -> Result<Self> {
        use object_store::aws::AmazonS3Builder;

        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket.into())
            .with_region(region.into());
        if let Some(endpoint) = endpoint {
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }
        let store = builder.build().map_err(map_object_store_error)?;
        Ok(Self::new(Arc::new(store)))
    }
}

fn map_object_store_error(error: OsError) -> Error {
    Error::Io(std::io::Error::other(error))
}

/// Resolve the `ETag` for a write, fetching it via `head` if the store did not
/// return one on the put.
async fn resolve_put_etag(
    e_tag: Option<String>,
    store: &Arc<dyn ObjectStore>,
    path: &OsPath,
) -> Result<ETag> {
    if let Some(e_tag) = e_tag {
        return Ok(ETag::new(e_tag));
    }
    let meta = store.head(path).await.map_err(map_object_store_error)?;
    meta.e_tag.map(ETag::new).ok_or_else(|| Error::Corruption {
        message: "object store did not return an ETag (required for manifest CAS)".to_owned(),
    })
}

impl ObjectClient for ObjectStoreClient {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        let path = OsPath::from(key);
        Box::pin(async move {
            match self.store.get(&path).await {
                Ok(result) => {
                    let bytes = result.bytes().await.map_err(map_object_store_error)?;
                    Ok(Some(Arc::from(bytes.as_ref())))
                }
                Err(OsError::NotFound { .. }) => Ok(None),
                Err(error) => Err(map_object_store_error(error)),
            }
        })
    }

    fn get_range<'op>(&'op self, key: &str, offset: u64, len: u64) -> ObjectFuture<'op, Arc<[u8]>> {
        let path = OsPath::from(key);
        Box::pin(async move {
            let end = offset
                .checked_add(len)
                .ok_or_else(|| Error::invalid_options("object range end overflow"))?;
            let options = GetOptions {
                range: Some(GetRange::Bounded(offset..end)),
                ..GetOptions::default()
            };
            let result = self
                .store
                .get_opts(&path, options)
                .await
                .map_err(map_object_store_error)?;
            let bytes = result.bytes().await.map_err(map_object_store_error)?;
            Ok(Arc::from(bytes.as_ref()))
        })
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        let path = OsPath::from(key);
        Box::pin(async move {
            let payload = PutPayload::from(bytes.to_vec());
            let result = self
                .store
                .put(&path, payload)
                .await
                .map_err(map_object_store_error)?;
            resolve_put_etag(result.e_tag, &self.store, &path).await
        })
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        let path = OsPath::from(key);
        Box::pin(async move {
            match self.store.delete(&path).await {
                // Idempotent: deleting an absent key succeeds.
                Ok(()) | Err(OsError::NotFound { .. }) => Ok(()),
                Err(error) => Err(map_object_store_error(error)),
            }
        })
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        let os_prefix = OsPath::from(prefix);
        Box::pin(async move {
            let metas: Vec<object_store::ObjectMeta> = self
                .store
                .list(Some(&os_prefix))
                .try_collect()
                .await
                .map_err(map_object_store_error)?;
            // object_store yields keys in lexicographic order, matching the
            // ObjectClient contract. List ETags are not used for CAS (only the
            // key is read by the table/blob listers), so an absent ETag is fine.
            Ok(metas
                .into_iter()
                .map(|meta| ObjectMeta {
                    key: meta.location.as_ref().to_owned(),
                    size: meta.size,
                    etag: ETag::new(meta.e_tag.unwrap_or_default()),
                })
                .collect())
        })
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        let path = OsPath::from(key);
        Box::pin(async move {
            match self.store.head(&path).await {
                Ok(meta) => {
                    let etag = meta.e_tag.map(ETag::new).ok_or_else(|| Error::Corruption {
                        message: "object store did not return an ETag (required for manifest CAS)"
                            .to_owned(),
                    })?;
                    Ok(Some(ObjectMeta {
                        key: meta.location.as_ref().to_owned(),
                        size: meta.size,
                        etag,
                    }))
                }
                Err(OsError::NotFound { .. }) => Ok(None),
                Err(error) => Err(map_object_store_error(error)),
            }
        })
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        let path = OsPath::from(key);
        Box::pin(async move {
            let mode = match precondition {
                Precondition::IfNoneMatch => PutMode::Create,
                Precondition::IfMatch(etag) => PutMode::Update(UpdateVersion {
                    e_tag: Some(etag.as_str().to_owned()),
                    version: None,
                }),
            };
            let options = PutOptions {
                mode,
                ..PutOptions::default()
            };
            let payload = PutPayload::from(bytes.to_vec());
            match self.store.put_opts(&path, payload, options).await {
                Ok(result) => Ok(PutIf::Stored {
                    etag: resolve_put_etag(result.e_tag, &self.store, &path).await?,
                }),
                // A lost CAS race is the expected, retryable outcome: report the
                // current ETag so the manifest commit can rebase and retry.
                Err(OsError::AlreadyExists { .. } | OsError::Precondition { .. }) => {
                    let current = match self.store.head(&path).await {
                        Ok(meta) => meta.e_tag.map(ETag::new),
                        Err(OsError::NotFound { .. }) => None,
                        Err(error) => return Err(map_object_store_error(error)),
                    };
                    Ok(PutIf::PreconditionFailed { current })
                }
                Err(error) => Err(map_object_store_error(error)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Db;
    use crate::options::DbOptions;

    fn memory_client() -> ObjectStoreClient {
        ObjectStoreClient::new(Arc::new(object_store::memory::InMemory::new()))
    }

    fn bytes(data: &[u8]) -> Arc<[u8]> {
        Arc::from(data)
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        futures::executor::block_on(future)
    }

    #[test]
    fn adapter_round_trips_objects_and_lists_by_prefix() {
        let client = memory_client();
        assert!(block_on(client.get("missing")).unwrap().is_none());
        assert!(block_on(client.head("missing")).unwrap().is_none());

        let etag = block_on(client.put("db/0001.trinet", bytes(b"hello world"))).unwrap();
        assert_eq!(
            block_on(client.get("db/0001.trinet")).unwrap().as_deref(),
            Some(b"hello world".as_slice())
        );
        assert_eq!(
            block_on(client.get_range("db/0001.trinet", 6, 5))
                .unwrap()
                .as_ref(),
            b"world"
        );
        let meta = block_on(client.head("db/0001.trinet"))
            .unwrap()
            .expect("present");
        assert_eq!(meta.size, 11);
        assert_eq!(meta.etag, etag);

        block_on(client.put("db/0002.trinet", bytes(b"x"))).unwrap();
        let listed = block_on(client.list("db/")).unwrap();
        let keys: Vec<&str> = listed.iter().map(|m| m.key.as_str()).collect();
        assert_eq!(keys, ["db/0001.trinet", "db/0002.trinet"]);

        block_on(client.delete("db/0001.trinet")).unwrap();
        assert!(block_on(client.get("db/0001.trinet")).unwrap().is_none());
        block_on(client.delete("db/0001.trinet")).unwrap(); // idempotent
    }

    #[test]
    fn adapter_put_if_is_a_real_compare_and_swap() {
        let client = memory_client();

        // If-None-Match creates only when absent.
        let created = match block_on(client.put_if("LOCK", bytes(b"v1"), Precondition::IfNoneMatch))
            .unwrap()
        {
            PutIf::Stored { etag } => etag,
            PutIf::PreconditionFailed { .. } => panic!("create should succeed when absent"),
        };
        match block_on(client.put_if("LOCK", bytes(b"v2"), Precondition::IfNoneMatch)).unwrap() {
            PutIf::PreconditionFailed { current } => assert_eq!(current.as_ref(), Some(&created)),
            PutIf::Stored { .. } => panic!("second create must lose the CAS"),
        }

        // If-Match advances only when the ETag still matches.
        let advanced = match block_on(client.put_if(
            "LOCK",
            bytes(b"v3"),
            Precondition::IfMatch(created.clone()),
        ))
        .unwrap()
        {
            PutIf::Stored { etag } => etag,
            PutIf::PreconditionFailed { .. } => panic!("If-Match with current ETag should win"),
        };
        // The stale ETag now loses.
        match block_on(client.put_if("LOCK", bytes(b"v4"), Precondition::IfMatch(created))).unwrap()
        {
            PutIf::PreconditionFailed { current } => assert_eq!(current.as_ref(), Some(&advanced)),
            PutIf::Stored { .. } => panic!("stale If-Match must lose the CAS"),
        }
    }

    #[test]
    fn database_round_trips_over_object_store_adapter() {
        let client: Arc<dyn ObjectClient> = Arc::new(memory_client());

        {
            let db = block_on(Db::open_object_store(
                Arc::clone(&client),
                DbOptions::object_store(),
            ))
            .expect("open over object_store adapter");
            db.put_sync(b"k", b"v").expect("put");
            block_on(db.flush()).expect("flush");
        }

        let db = block_on(Db::open_object_store(client, DbOptions::object_store()))
            .expect("reopen over object_store adapter");
        assert_eq!(
            db.get_sync(b"k").expect("get after reopen").as_deref(),
            Some(b"v".as_slice())
        );
    }
}
