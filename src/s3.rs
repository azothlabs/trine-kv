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
    /// service (Cloudflare R2/MinIO/Ceph); leave it `None` for AWS S3. For R2 use
    /// region `"auto"` and the `https://<account>.r2.cloudflarestorage.com`
    /// endpoint.
    ///
    /// Conditional PUT (`ETagMatch`) is enabled explicitly: `object_store`
    /// disables conditional writes for non-AWS endpoints by default, but the
    /// manifest commit requires a real CAS, so a backend that cannot honor
    /// `If-Match` / `If-None-Match` is unsupported.
    ///
    /// # Errors
    ///
    /// Returns an error if the S3 client cannot be configured.
    pub fn s3(
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: Option<String>,
    ) -> Result<Self> {
        use object_store::aws::{AmazonS3Builder, S3ConditionalPut};

        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket.into())
            .with_region(region.into())
            .with_conditional_put(S3ConditionalPut::ETagMatch);
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
    use std::collections::BTreeMap;
    use std::sync::{Barrier, Mutex};
    use std::time::{Duration, Instant};

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

    #[derive(Debug, Default)]
    struct LiveMetrics {
        samples: Mutex<BTreeMap<&'static str, Vec<Duration>>>,
    }

    impl LiveMetrics {
        fn record(&self, operation: &'static str, elapsed: Duration) {
            self.samples
                .lock()
                .expect("metrics mutex poisoned")
                .entry(operation)
                .or_default()
                .push(elapsed);
        }

        fn summary(&self) -> Vec<MetricSummary> {
            let samples = self.samples.lock().expect("metrics mutex poisoned");
            samples
                .iter()
                .map(|(operation, durations)| MetricSummary::new(operation, durations))
                .collect()
        }

        fn snapshot(&self) -> RequestCounts {
            let samples = self.samples.lock().expect("metrics mutex poisoned");
            let count = |name| samples.get(name).map_or(0, Vec::len);
            RequestCounts {
                get: count("get"),
                get_range: count("get_range"),
                put: count("put"),
                delete: count("delete"),
                list: count("list"),
                head: count("head"),
                put_if: count("put_if"),
            }
        }

        fn cost_estimate(&self) -> R2CostEstimate {
            R2CostEstimate::from_counts(self.snapshot())
        }
    }

    #[derive(Debug)]
    struct MetricSummary {
        operation: &'static str,
        count: usize,
        total: Duration,
        min: Duration,
        p50: Duration,
        p95: Duration,
        max: Duration,
    }

    impl MetricSummary {
        fn new(operation: &'static str, durations: &[Duration]) -> Self {
            let mut sorted = durations.to_vec();
            sorted.sort_unstable();
            let total = sorted.iter().copied().sum();
            Self {
                operation,
                count: sorted.len(),
                total,
                min: sorted.first().copied().unwrap_or_default(),
                p50: percentile(&sorted, 50),
                p95: percentile(&sorted, 95),
                max: sorted.last().copied().unwrap_or_default(),
            }
        }
    }

    #[derive(Debug, Clone, Copy, Default)]
    struct RequestCounts {
        get: usize,
        get_range: usize,
        put: usize,
        delete: usize,
        list: usize,
        head: usize,
        put_if: usize,
    }

    impl RequestCounts {
        fn delta_since(self, previous: Self) -> Self {
            Self {
                get: self.get.saturating_sub(previous.get),
                get_range: self.get_range.saturating_sub(previous.get_range),
                put: self.put.saturating_sub(previous.put),
                delete: self.delete.saturating_sub(previous.delete),
                list: self.list.saturating_sub(previous.list),
                head: self.head.saturating_sub(previous.head),
                put_if: self.put_if.saturating_sub(previous.put_if),
            }
        }

        fn class_a(self) -> usize {
            self.list + self.put + self.put_if
        }

        fn class_b(self) -> usize {
            self.get + self.get_range + self.head
        }

        fn free(self) -> usize {
            self.delete
        }
    }

    #[derive(Debug)]
    struct R2CostEstimate {
        class_a: usize,
        class_b: usize,
        free: usize,
    }

    impl R2CostEstimate {
        fn from_counts(counts: RequestCounts) -> Self {
            Self {
                class_a: counts.class_a(),
                class_b: counts.class_b(),
                free: counts.free(),
            }
        }

        fn standard_usd_before_free_tier(&self) -> f64 {
            let class_a = self.class_a as f64 * 4.50 / 1_000_000.0;
            let class_b = self.class_b as f64 * 0.36 / 1_000_000.0;
            class_a + class_b
        }
    }

    struct MeasuredClient {
        inner: Arc<dyn ObjectClient>,
        metrics: Arc<LiveMetrics>,
    }

    impl std::fmt::Debug for MeasuredClient {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("MeasuredClient")
                .finish_non_exhaustive()
        }
    }

    impl MeasuredClient {
        fn new(inner: Arc<dyn ObjectClient>, metrics: Arc<LiveMetrics>) -> Self {
            Self { inner, metrics }
        }
    }

    impl ObjectClient for MeasuredClient {
        fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
            let inner = Arc::clone(&self.inner);
            let metrics = Arc::clone(&self.metrics);
            let key = key.to_owned();
            Box::pin(async move {
                let started = Instant::now();
                let result = inner.get(&key).await;
                metrics.record("get", started.elapsed());
                result
            })
        }

        fn get_range<'op>(
            &'op self,
            key: &str,
            offset: u64,
            len: u64,
        ) -> ObjectFuture<'op, Arc<[u8]>> {
            let inner = Arc::clone(&self.inner);
            let metrics = Arc::clone(&self.metrics);
            let key = key.to_owned();
            Box::pin(async move {
                let started = Instant::now();
                let result = inner.get_range(&key, offset, len).await;
                metrics.record("get_range", started.elapsed());
                result
            })
        }

        fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
            let inner = Arc::clone(&self.inner);
            let metrics = Arc::clone(&self.metrics);
            let key = key.to_owned();
            Box::pin(async move {
                let started = Instant::now();
                let result = inner.put(&key, bytes).await;
                metrics.record("put", started.elapsed());
                result
            })
        }

        fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
            let inner = Arc::clone(&self.inner);
            let metrics = Arc::clone(&self.metrics);
            let key = key.to_owned();
            Box::pin(async move {
                let started = Instant::now();
                let result = inner.delete(&key).await;
                metrics.record("delete", started.elapsed());
                result
            })
        }

        fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
            let inner = Arc::clone(&self.inner);
            let metrics = Arc::clone(&self.metrics);
            let prefix = prefix.to_owned();
            Box::pin(async move {
                let started = Instant::now();
                let result = inner.list(&prefix).await;
                metrics.record("list", started.elapsed());
                result
            })
        }

        fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
            let inner = Arc::clone(&self.inner);
            let metrics = Arc::clone(&self.metrics);
            let key = key.to_owned();
            Box::pin(async move {
                let started = Instant::now();
                let result = inner.head(&key).await;
                metrics.record("head", started.elapsed());
                result
            })
        }

        fn put_if<'op>(
            &'op self,
            key: &str,
            bytes: Arc<[u8]>,
            precondition: Precondition,
        ) -> ObjectFuture<'op, PutIf> {
            let inner = Arc::clone(&self.inner);
            let metrics = Arc::clone(&self.metrics);
            let key = key.to_owned();
            Box::pin(async move {
                let started = Instant::now();
                let result = inner.put_if(&key, bytes, precondition).await;
                metrics.record("put_if", started.elapsed());
                result
            })
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct WalObjectSummary {
        count: usize,
        bytes: u64,
        largest_object: u64,
    }

    impl WalObjectSummary {
        fn from_objects(objects: &[ObjectMeta]) -> Self {
            objects
                .iter()
                .filter(|meta| crate::is_wal_object_key(&meta.key))
                .fold(
                    Self {
                        count: 0,
                        bytes: 0,
                        largest_object: 0,
                    },
                    |mut summary, meta| {
                        summary.count += 1;
                        summary.bytes = summary.bytes.saturating_add(meta.size);
                        summary.largest_object = summary.largest_object.max(meta.size);
                        summary
                    },
                )
        }
    }

    fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
        if sorted.is_empty() {
            return Duration::ZERO;
        }
        let index = (sorted.len() - 1) * percentile / 100;
        sorted[index]
    }

    fn duration_ms(duration: Duration) -> f64 {
        duration.as_secs_f64() * 1_000.0
    }

    fn unique_prefix(label: &str) -> String {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time after UNIX_EPOCH")
            .as_nanos();
        format!("trine-it/{label}/{}-{nonce}", std::process::id())
    }

    fn live_r2_client() -> Option<Arc<dyn ObjectClient>> {
        let Ok(bucket) = std::env::var("TRINE_S3_BUCKET") else {
            eprintln!("skipping live R2 test: set TRINE_S3_BUCKET (+ AWS_* / AWS_ENDPOINT_URL)");
            return None;
        };
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".to_owned());
        let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
        Some(Arc::new(
            ObjectStoreClient::s3(bucket, region, endpoint).expect("build R2/S3 client"),
        ))
    }

    async fn cleanup_prefix(client: &Arc<dyn ObjectClient>, prefix: &str) -> Result<()> {
        for meta in client.list(prefix).await? {
            client.delete(&meta.key).await?;
        }
        Ok(())
    }

    async fn wait_for_list_condition(
        client: &Arc<dyn ObjectClient>,
        prefix: &str,
        condition: impl Fn(&[ObjectMeta]) -> bool,
    ) -> Result<(Vec<ObjectMeta>, usize, Duration)> {
        let started = Instant::now();
        for attempt in 1..=12 {
            let objects = client.list(prefix).await?;
            if condition(&objects) {
                return Ok((objects, attempt, started.elapsed()));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Err(Error::Corruption {
            message: format!("R2 listing for prefix {prefix} did not reach expected state"),
        })
    }

    fn report_metric_summaries(metrics: &LiveMetrics) {
        for summary in metrics.summary() {
            eprintln!(
                "r2_metric op={} count={} total_ms={:.2} min_ms={:.2} p50_ms={:.2} \
                 p95_ms={:.2} max_ms={:.2}",
                summary.operation,
                summary.count,
                duration_ms(summary.total),
                duration_ms(summary.min),
                duration_ms(summary.p50),
                duration_ms(summary.p95),
                duration_ms(summary.max)
            );
        }
        let cost = metrics.cost_estimate();
        eprintln!(
            "r2_cost_estimate class_a={} class_b={} free_delete={} \
             standard_usd_before_free_tier={:.8}",
            cost.class_a,
            cost.class_b,
            cost.free,
            cost.standard_usd_before_free_tier()
        );
    }

    fn report_billing_scenario(name: &str, counts: RequestCounts) {
        let cost = R2CostEstimate::from_counts(counts);
        eprintln!(
            "r2_billing_scenario name={} class_a={} class_b={} free_delete={} \
             put={} put_if={} list={} get={} get_range={} head={} delete={} \
             standard_usd_before_free_tier={:.8}",
            name,
            cost.class_a,
            cost.class_b,
            cost.free,
            counts.put,
            counts.put_if,
            counts.list,
            counts.get,
            counts.get_range,
            counts.head,
            counts.delete,
            cost.standard_usd_before_free_tier()
        );
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

    /// Live integration test against real S3-compatible storage (Cloudflare R2).
    ///
    /// Ignored by default — it makes real, billable network writes. Provide
    /// credentials + target via the environment and run explicitly:
    ///
    /// ```text
    /// export AWS_ACCESS_KEY_ID=...        # R2 API token access key
    /// export AWS_SECRET_ACCESS_KEY=...    # R2 API token secret
    /// export AWS_REGION=auto              # R2 uses "auto"
    /// export AWS_ENDPOINT_URL=https://<account>.r2.cloudflarestorage.com
    /// export TRINE_S3_BUCKET=<your-r2-bucket>
    /// cargo test --features s3 -- --ignored s3_live_database_round_trip --nocapture
    /// ```
    ///
    /// The run is isolated under a unique `trine-it/<pid>-<nonce>/` key prefix
    /// (via `object_store`'s `PrefixStore`) and cleans up afterward, so it can
    /// safely share a bucket. It exercises the one thing only a real backend can
    /// confirm: that R2's conditional PUT (`If-None-Match` / `If-Match`) actually
    /// backs the manifest commit CAS.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires real S3/R2 credentials; run with --features s3 -- --ignored"]
    async fn s3_live_database_round_trip() {
        let Some(client) = live_r2_client() else {
            return;
        };

        // Isolate this run under a unique key prefix (also exercises the native
        // prefix feature) so it can safely share a real bucket.
        let prefix = unique_prefix("round-trip");

        // open -> put (default + named bucket) -> flush -> reopen -> read, all
        // against real R2.
        {
            let db = Db::open_object_store_at(
                Arc::clone(&client),
                prefix.clone(),
                DbOptions::object_store(),
            )
            .await
            .expect("open over R2");
            db.put_sync(b"alpha", b"one").expect("put alpha");
            let docs = db
                .bucket_with_options("docs", crate::options::BucketOptions::default())
                .await
                .expect("create docs bucket (manifest CAS)");
            docs.put_sync(b"title", b"trine").expect("put into docs");
            db.flush()
                .await
                .expect("flush to R2 (objects + manifest CAS)");
        }

        let db = Db::open_object_store_at(
            Arc::clone(&client),
            prefix.clone(),
            DbOptions::object_store(),
        )
        .await
        .expect("reopen over R2");
        assert_eq!(
            db.get_sync(b"alpha").expect("get alpha").as_deref(),
            Some(b"one".as_slice()),
            "value recovered from R2 after reopen"
        );
        let docs = db
            .bucket_with_options("docs", crate::options::BucketOptions::default())
            .await
            .expect("reopen docs bucket");
        assert_eq!(
            docs.get_sync(b"title").expect("get docs title").as_deref(),
            Some(b"trine".as_slice())
        );
        drop(db);

        // Best-effort cleanup: remove everything this run wrote under the prefix.
        if let Ok(metas) = client.list(&prefix).await {
            for meta in metas {
                let _ = client.delete(&meta.key).await;
            }
        }
    }

    /// Live R2 measurement/fault suite for the object-store compute/storage
    /// split. Ignored by default because it performs billable requests.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires real R2 credentials; run with --features s3 -- --ignored"]
    async fn s3_live_measurement_and_fault_suite() {
        let Some(raw_client) = live_r2_client() else {
            return;
        };
        let metrics = Arc::new(LiveMetrics::default());
        let client: Arc<dyn ObjectClient> = Arc::new(MeasuredClient::new(
            Arc::clone(&raw_client),
            Arc::clone(&metrics),
        ));
        let prefix = unique_prefix("measure");

        let result =
            run_live_measurement_and_fault_suite(Arc::clone(&client), &prefix, &metrics).await;
        let cleanup = cleanup_prefix(&client, &prefix).await;
        report_metric_summaries(&metrics);
        cleanup.expect("cleanup R2 measurement prefix");
        result.expect("R2 measurement/fault suite");
    }

    async fn run_live_measurement_and_fault_suite(
        client: Arc<dyn ObjectClient>,
        prefix: &str,
        metrics: &LiveMetrics,
    ) -> Result<()> {
        const WRITE_COUNT: usize = 12;
        const GROUP_WRITE_COUNT: usize = 12;
        const CLEANUP_OBJECTS: usize = 5;

        let writer = Db::open_object_store_at(
            Arc::clone(&client),
            prefix.to_owned(),
            DbOptions::object_store(),
        )
        .await?;
        let reader = Db::open_object_store_at(
            Arc::clone(&client),
            prefix.to_owned(),
            DbOptions::object_store().read_only(),
        )
        .await?;

        let sequential_counts_before = metrics.snapshot();
        let mut write_latencies = Vec::with_capacity(WRITE_COUNT);
        for index in 0..WRITE_COUNT {
            let key = format!("key-{index:02}");
            let value = format!("value-{index:02}");
            let started = Instant::now();
            writer.put(key.as_bytes(), value.as_bytes()).await?;
            write_latencies.push(started.elapsed());
        }
        let sequential_write_counts = metrics.snapshot().delta_since(sequential_counts_before);
        assert!(
            sequential_write_counts.class_a() <= WRITE_COUNT * 2,
            "sequential durable writes should not exceed one WAL PUT plus one head CAS per write"
        );

        let (objects_before_flush, wal_list_attempts, wal_list_latency) =
            wait_for_list_condition(&client, prefix, |objects| {
                WalObjectSummary::from_objects(objects).count == 1
            })
            .await?;
        let wal_before_flush = WalObjectSummary::from_objects(&objects_before_flush);

        let refresh_counts_before = metrics.snapshot();
        let started = Instant::now();
        let refreshed = reader.refresh_object_store().await?;
        let refresh_latency = started.elapsed();
        assert_eq!(
            reader.get(b"key-11").await?.as_deref(),
            Some(b"value-11".as_slice())
        );
        let refresh_counts = metrics.snapshot().delta_since(refresh_counts_before);

        let flush_counts_before = metrics.snapshot();
        let started = Instant::now();
        writer.flush().await?;
        let flush_latency = started.elapsed();
        let flush_counts = metrics.snapshot().delta_since(flush_counts_before);

        let wal_cleanup_counts_before = metrics.snapshot();
        let (objects_after_flush, wal_cleanup_attempts, wal_cleanup_latency) =
            wait_for_list_condition(&client, prefix, |objects| {
                WalObjectSummary::from_objects(objects).count == 0
            })
            .await?;
        let wal_after_flush = WalObjectSummary::from_objects(&objects_after_flush);
        let wal_cleanup_counts = metrics.snapshot().delta_since(wal_cleanup_counts_before);

        let group_prefix = format!("{prefix}/group");
        let group_writer = Db::open_object_store_at(
            Arc::clone(&client),
            group_prefix.clone(),
            DbOptions::object_store(),
        )
        .await?;
        let group_counts_before = metrics.snapshot();
        let group_latency =
            run_concurrent_puts(&group_writer, GROUP_WRITE_COUNT, "group-key").await?;
        let group_counts = metrics.snapshot().delta_since(group_counts_before);
        if group_counts.put != 1 || group_counts.put_if != 1 {
            return Err(Error::Corruption {
                message: format!(
                    "R2 group commit billing regression: expected 1 WAL PUT + 1 head CAS, got \
                     put={} put_if={}",
                    group_counts.put, group_counts.put_if
                ),
            });
        }
        let (group_objects, group_wal_attempts, group_wal_latency) =
            wait_for_list_condition(&client, &group_prefix, |objects| {
                WalObjectSummary::from_objects(objects).count == 1
            })
            .await?;
        let group_wal = WalObjectSummary::from_objects(&group_objects);
        let group_reader = Db::open_object_store_at(
            Arc::clone(&client),
            group_prefix.clone(),
            DbOptions::object_store().read_only(),
        )
        .await?;
        let _ = group_reader.refresh_object_store().await?;
        assert_eq!(
            group_reader.get(b"group-key-11").await?.as_deref(),
            Some(b"value-11".as_slice())
        );
        drop(group_reader);
        drop(group_writer);

        let split_prefix = format!("{prefix}/split");
        let split_counts_before = metrics.snapshot();
        let split_started = Instant::now();
        {
            let split_writer = Db::open_object_store_with_wal_at(
                Arc::clone(&client),
                Arc::clone(&client),
                split_prefix.clone(),
                DbOptions::object_store(),
            )
            .await?;
            split_writer.put(b"split-key", b"split-value").await?;
        }
        let split_reopen = Db::open_object_store_with_wal_at(
            Arc::clone(&client),
            Arc::clone(&client),
            split_prefix,
            DbOptions::object_store(),
        )
        .await?;
        assert_eq!(
            split_reopen.get(b"split-key").await?.as_deref(),
            Some(b"split-value".as_slice())
        );
        let split_latency = split_started.elapsed();
        let split_counts = metrics.snapshot().delta_since(split_counts_before);

        let cas_key = format!("{prefix}/cas-conflict");
        let started = Instant::now();
        let created = match client
            .put_if(&cas_key, bytes(b"v1"), Precondition::IfNoneMatch)
            .await?
        {
            PutIf::Stored { etag } => etag,
            PutIf::PreconditionFailed { .. } => {
                return Err(Error::Corruption {
                    message: "fresh R2 CAS key unexpectedly existed".to_owned(),
                });
            }
        };
        let cas_create_latency = started.elapsed();

        let started = Instant::now();
        match client
            .put_if(&cas_key, bytes(b"v2"), Precondition::IfNoneMatch)
            .await?
        {
            PutIf::PreconditionFailed { current } => assert_eq!(current.as_ref(), Some(&created)),
            PutIf::Stored { .. } => {
                return Err(Error::Corruption {
                    message: "R2 If-None-Match unexpectedly overwrote existing key".to_owned(),
                });
            }
        }
        let cas_create_conflict_latency = started.elapsed();

        let advanced = match client
            .put_if(
                &cas_key,
                bytes(b"v3"),
                Precondition::IfMatch(created.clone()),
            )
            .await?
        {
            PutIf::Stored { etag } => etag,
            PutIf::PreconditionFailed { .. } => {
                return Err(Error::Corruption {
                    message: "R2 If-Match with current ETag failed".to_owned(),
                });
            }
        };
        let started = Instant::now();
        match client
            .put_if(&cas_key, bytes(b"v4"), Precondition::IfMatch(created))
            .await?
        {
            PutIf::PreconditionFailed { current } => assert_eq!(current.as_ref(), Some(&advanced)),
            PutIf::Stored { .. } => {
                return Err(Error::Corruption {
                    message: "R2 stale If-Match unexpectedly succeeded".to_owned(),
                });
            }
        }
        let cas_stale_conflict_latency = started.elapsed();

        let cleanup_prefix = format!("{prefix}/manual-cleanup/");
        let manual_cleanup_counts_before = metrics.snapshot();
        let cleanup_started = Instant::now();
        for index in 0..CLEANUP_OBJECTS {
            let key = format!("{cleanup_prefix}{index:02}.tmp");
            client.put(&key, bytes(b"cleanup")).await?;
        }
        let cleanup_put_latency = cleanup_started.elapsed();
        let (cleanup_objects, cleanup_list_attempts, cleanup_list_latency) =
            wait_for_list_condition(&client, &cleanup_prefix, |objects| {
                objects.len() >= CLEANUP_OBJECTS
            })
            .await?;

        let delete_started = Instant::now();
        for meta in cleanup_objects {
            client.delete(&meta.key).await?;
        }
        let cleanup_delete_latency = delete_started.elapsed();
        let (_cleanup_after_delete, cleanup_delete_attempts, cleanup_delete_visible_latency) =
            wait_for_list_condition(&client, &cleanup_prefix, |objects| objects.is_empty()).await?;
        let manual_cleanup_counts = metrics.snapshot().delta_since(manual_cleanup_counts_before);

        eprintln!(
            "r2_write_latency count={} total_ms={:.2} min_ms={:.2} p50_ms={:.2} \
             p95_ms={:.2} max_ms={:.2}",
            write_latencies.len(),
            duration_ms(write_latencies.iter().copied().sum()),
            duration_ms(write_latencies.iter().copied().min().unwrap_or_default()),
            duration_ms({
                let mut sorted = write_latencies.clone();
                sorted.sort_unstable();
                percentile(&sorted, 50)
            }),
            duration_ms({
                let mut sorted = write_latencies.clone();
                sorted.sort_unstable();
                percentile(&sorted, 95)
            }),
            duration_ms(write_latencies.iter().copied().max().unwrap_or_default())
        );
        eprintln!(
            "r2_wal_growth writes={} before_flush_count={} before_flush_bytes={} \
             before_flush_largest_bytes={} list_attempts={} list_visible_ms={:.2}",
            WRITE_COUNT,
            wal_before_flush.count,
            wal_before_flush.bytes,
            wal_before_flush.largest_object,
            wal_list_attempts,
            duration_ms(wal_list_latency)
        );
        eprintln!(
            "r2_refresh version={:?} latency_ms={:.2}",
            refreshed,
            duration_ms(refresh_latency)
        );
        eprintln!("r2_flush latency_ms={:.2}", duration_ms(flush_latency));
        eprintln!(
            "r2_wal_cleanup after_flush_count={} after_flush_bytes={} attempts={} \
             visible_ms={:.2}",
            wal_after_flush.count,
            wal_after_flush.bytes,
            wal_cleanup_attempts,
            duration_ms(wal_cleanup_latency)
        );
        eprintln!(
            "r2_group_commit writes={} total_ms={:.2} wal_puts={} head_put_ifs={} \
             before_flush_count={} before_flush_bytes={} list_attempts={} list_visible_ms={:.2}",
            GROUP_WRITE_COUNT,
            duration_ms(group_latency),
            group_counts.put,
            group_counts.put_if,
            group_wal.count,
            group_wal.bytes,
            group_wal_attempts,
            duration_ms(group_wal_latency)
        );
        eprintln!(
            "r2_split_wal_tier reopen_after_unflushed_write_ms={:.2}",
            duration_ms(split_latency)
        );
        eprintln!(
            "r2_cas create_ms={:.2} create_conflict_ms={:.2} stale_conflict_ms={:.2}",
            duration_ms(cas_create_latency),
            duration_ms(cas_create_conflict_latency),
            duration_ms(cas_stale_conflict_latency)
        );
        eprintln!(
            "r2_manual_cleanup put_count={} put_total_ms={:.2} list_attempts={} \
             list_visible_ms={:.2} delete_total_ms={:.2} delete_visible_attempts={} \
             delete_visible_ms={:.2}",
            CLEANUP_OBJECTS,
            duration_ms(cleanup_put_latency),
            cleanup_list_attempts,
            duration_ms(cleanup_list_latency),
            duration_ms(cleanup_delete_latency),
            cleanup_delete_attempts,
            duration_ms(cleanup_delete_visible_latency)
        );
        report_billing_scenario("sequential_writes", sequential_write_counts);
        report_billing_scenario("refresh", refresh_counts);
        report_billing_scenario("flush", flush_counts);
        report_billing_scenario("wal_cleanup_poll", wal_cleanup_counts);
        report_billing_scenario("group_commit_writes", group_counts);
        report_billing_scenario("split_wal_reopen_smoke", split_counts);
        report_billing_scenario("manual_cleanup", manual_cleanup_counts);

        Ok(())
    }

    async fn run_concurrent_puts(db: &Db, count: usize, key_prefix: &str) -> Result<Duration> {
        tokio::task::block_in_place(|| {
            let barrier = Arc::new(Barrier::new(count + 1));
            let mut handles = Vec::with_capacity(count);
            for index in 0..count {
                let db = db.clone();
                let barrier = Arc::clone(&barrier);
                let key = format!("{key_prefix}-{index:02}").into_bytes();
                let value = format!("value-{index:02}").into_bytes();
                handles.push(std::thread::spawn(move || {
                    barrier.wait();
                    db.put_sync(key, value)
                }));
            }

            let started = Instant::now();
            barrier.wait();
            for handle in handles {
                let result = handle.join().map_err(|_| Error::Corruption {
                    message: "R2 group commit worker thread panicked".to_owned(),
                })?;
                result?;
            }
            Ok(started.elapsed())
        })
    }
}
