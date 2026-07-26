use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "s3")]
use std::sync::Arc;

use trine_kv::{
    Bucket, ContentId, Db, DbOptions, Error, Iter, ReadVersion, Snapshot, StorageDomainId,
};

#[cfg(feature = "s3")]
use trine_kv::{ObjectClient, s3::ObjectStoreClient};

static ACCEPTANCE_DB_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) enum DurableLocation {
    Native(PathBuf),
    #[cfg(feature = "s3")]
    ObjectStore {
        client: Arc<dyn ObjectClient>,
        prefix: String,
    },
}

impl std::fmt::Debug for DurableLocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Native(path) => formatter.debug_tuple("Native").field(path).finish(),
            #[cfg(feature = "s3")]
            Self::ObjectStore { prefix, .. } => formatter
                .debug_struct("ObjectStore")
                .field("prefix", prefix)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Debug, Default, cucumber::World)]
pub(crate) struct TrineWorld {
    pub(crate) db: Option<Db>,
    pub(crate) location: Option<DurableLocation>,
    pub(crate) keep_last_read_versions: u64,
    pub(crate) snapshot: Option<Snapshot>,
    pub(crate) checkpoint_version: Option<ReadVersion>,
    pub(crate) remembered_version: Option<ReadVersion>,
    pub(crate) retained_bucket: Option<Bucket>,
    pub(crate) retained_cursor: Option<Iter>,
    pub(crate) last_value: Option<Vec<u8>>,
    pub(crate) rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub(crate) last_error: Option<Error>,
    pub(crate) first_content_id: Option<ContentId>,
    pub(crate) second_content_id: Option<ContentId>,
    pub(crate) sealed_content_bytes: Option<Vec<u8>>,
    pub(crate) content_domain: Option<StorageDomainId>,
    pub(crate) expected_content_id: Option<ContentId>,
}

impl TrineWorld {
    pub(crate) fn db(&self) -> &Db {
        self.db.as_ref().expect("scenario database is initialized")
    }

    pub(crate) async fn open_new(&mut self, keep_last_read_versions: u64) -> trine_kv::Result<()> {
        assert!(
            self.db.is_none(),
            "scenario database may only be created once"
        );
        self.keep_last_read_versions = keep_last_read_versions;
        let location = new_location()?;
        let db = open_location(&location, keep_last_read_versions, false).await?;
        self.location = Some(location);
        self.db = Some(db);
        Ok(())
    }

    pub(crate) async fn reopen(&mut self, read_only: bool) -> trine_kv::Result<()> {
        self.snapshot.take();
        self.retained_cursor.take();
        self.retained_bucket.take();
        if let Some(db) = self.db.take() {
            db.close().await?;
        }
        let location = self
            .location
            .as_ref()
            .expect("durable location exists before reopen");
        self.db = Some(open_location(location, self.keep_last_read_versions, read_only).await?);
        Ok(())
    }

    pub(crate) async fn try_open_second_writer(&self) -> trine_kv::Result<Db> {
        open_location(
            self.location
                .as_ref()
                .expect("durable location exists before second open"),
            self.keep_last_read_versions,
            false,
        )
        .await
    }

    pub(crate) fn record_error<T>(&mut self, result: trine_kv::Result<T>) {
        match result {
            Ok(_) => self.last_error = None,
            Err(error) => self.last_error = Some(error),
        }
    }

    pub(crate) async fn cleanup(&mut self) -> trine_kv::Result<()> {
        self.snapshot.take();
        self.retained_cursor.take();
        self.retained_bucket.take();
        if let Some(db) = self.db.take() {
            db.close().await?;
        }
        let Some(location) = self.location.take() else {
            return Ok(());
        };
        match location {
            DurableLocation::Native(path) => {
                if path.exists() {
                    fs::remove_dir_all(path).map_err(Error::Io)?;
                }
                Ok(())
            }
            #[cfg(feature = "s3")]
            DurableLocation::ObjectStore { client, prefix } => {
                cleanup_object_prefix(&client, &prefix).await
            }
        }
    }
}

fn unique_suffix() -> String {
    let id = ACCEPTANCE_DB_ID.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after Unix epoch")
        .as_nanos();
    format!("{}-{nanos}-{id}", std::process::id())
}

fn new_location() -> trine_kv::Result<DurableLocation> {
    match std::env::var("TRINE_ACCEPTANCE_BACKEND").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("native") => Ok(DurableLocation::Native(
            std::env::temp_dir().join(format!("trine-gherkin-{}", unique_suffix())),
        )),
        Ok("s3") => new_object_store_location(),
        Ok(other) => Err(Error::invalid_options(format!(
            "unsupported TRINE_ACCEPTANCE_BACKEND {other:?}; expected native or s3"
        ))),
        Err(error) => Err(Error::invalid_options(format!(
            "TRINE_ACCEPTANCE_BACKEND is not valid Unicode: {error}"
        ))),
    }
}

#[cfg(feature = "s3")]
fn new_object_store_location() -> trine_kv::Result<DurableLocation> {
    let bucket = std::env::var("TRINE_S3_BUCKET")
        .map_err(|_| Error::invalid_options("TRINE_S3_BUCKET is required for S3 acceptance"))?;
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".to_owned());
    let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
    let client: Arc<dyn ObjectClient> = Arc::new(ObjectStoreClient::s3(bucket, region, endpoint)?);
    Ok(DurableLocation::ObjectStore {
        client,
        prefix: format!("trine-gherkin/{}", unique_suffix()),
    })
}

#[cfg(not(feature = "s3"))]
fn new_object_store_location() -> trine_kv::Result<DurableLocation> {
    Err(Error::unsupported(
        "the s3 acceptance backend requires cargo test --features s3",
    ))
}

async fn open_location(
    location: &DurableLocation,
    keep_last_read_versions: u64,
    read_only: bool,
) -> trine_kv::Result<Db> {
    match location {
        DurableLocation::Native(path) => {
            let mut options =
                DbOptions::persistent(path).with_keep_last_read_versions(keep_last_read_versions);
            if read_only {
                options = options.read_only();
            }
            Db::open(options).await
        }
        #[cfg(feature = "s3")]
        DurableLocation::ObjectStore { client, prefix } => {
            let mut options =
                DbOptions::object_store().with_keep_last_read_versions(keep_last_read_versions);
            if read_only {
                options = options.read_only();
            }
            Db::open_object_store_at(Arc::clone(client), prefix.clone(), options).await
        }
    }
}

#[cfg(feature = "s3")]
async fn cleanup_object_prefix(
    client: &Arc<dyn ObjectClient>,
    prefix: &str,
) -> trine_kv::Result<()> {
    for _ in 0..8 {
        let objects = client.list(prefix).await?;
        if objects.is_empty() {
            return Ok(());
        }
        for object in objects {
            client.delete(&object.key).await?;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let remaining = client.list(prefix).await?;
    if remaining.is_empty() {
        Ok(())
    } else {
        Err(Error::Corruption {
            message: format!(
                "acceptance cleanup left {} objects under isolated prefix {prefix}",
                remaining.len()
            ),
        })
    }
}
