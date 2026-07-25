use std::{
    fs,
    future::Future,
    path::PathBuf,
    task::{Context, Poll, Waker},
    time::{SystemTime, UNIX_EPOCH},
};

use super::{ManifestStore, decode_manifest, decode_state, manifest_path};
use crate::{
    limits,
    options::{
        BlobLevelMergePolicy, BucketOptions, CompressionProfile, FilterDepthCurve, FilterPolicy,
        IndexSearchPolicy, PrefixFilterPolicy,
    },
    prefix::PrefixExtractor,
    storage::NativeFileBackend,
};

struct AppliedManifestThenError {
    inner: std::sync::Arc<dyn crate::object_store::ObjectClient>,
    fail_once: std::sync::atomic::AtomicBool,
}

impl crate::object_store::ObjectClient for AppliedManifestThenError {
    fn get<'op>(
        &'op self,
        key: &str,
    ) -> crate::object_store::ObjectFuture<'op, Option<std::sync::Arc<[u8]>>> {
        self.inner.get(key)
    }

    fn get_range<'op>(
        &'op self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> crate::object_store::ObjectFuture<'op, std::sync::Arc<[u8]>> {
        self.inner.get_range(key, offset, len)
    }

    fn put<'op>(
        &'op self,
        key: &str,
        bytes: std::sync::Arc<[u8]>,
    ) -> crate::object_store::ObjectFuture<'op, crate::object_store::ETag> {
        self.inner.put(key, bytes)
    }

    fn delete<'op>(&'op self, key: &str) -> crate::object_store::ObjectFuture<'op, ()> {
        self.inner.delete(key)
    }

    fn list<'op>(
        &'op self,
        prefix: &str,
    ) -> crate::object_store::ObjectFuture<'op, Vec<crate::object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn head<'op>(
        &'op self,
        key: &str,
    ) -> crate::object_store::ObjectFuture<'op, Option<crate::object_store::ObjectMeta>> {
        self.inner.head(key)
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: std::sync::Arc<[u8]>,
        precondition: crate::object_store::Precondition,
    ) -> crate::object_store::ObjectFuture<'op, crate::object_store::PutIf> {
        let future = self.inner.put_if(key, bytes, precondition);
        let fail = self
            .fail_once
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        Box::pin(async move {
            let outcome = future.await?;
            if fail && matches!(outcome, crate::object_store::PutIf::Stored { .. }) {
                return Err(crate::Error::Io(std::io::Error::other(
                    "injected response loss after manifest CAS applied",
                )));
            }
            Ok(outcome)
        })
    }
}

#[test]
fn manifest_decode_rejects_table_count_before_large_allocation() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&u32::MAX.to_le_bytes());

    let error = decode_state(&payload).expect_err("impossible table count should fail");
    assert!(
        error
            .to_string()
            .contains("table count exceeds payload bytes"),
        "unexpected error: {error}"
    );
}

#[test]
fn manifest_decode_rejects_payload_len_before_large_allocation() {
    let payload_len = u32::try_from(limits::MAX_MANIFEST_PAYLOAD_BYTES + 1)
        .expect("test payload length fits u32");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&super::MANIFEST_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&super::MANIFEST_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());

    let error = decode_manifest(&bytes).expect_err("oversized manifest payload should fail");

    assert!(error.to_string().contains("manifest payload length"));
}

fn put_bucket_options_header(payload: &mut Vec<u8>) {
    super::put_u64(payload, 0);
    super::put_u32(payload, 1);
    super::put_bytes(payload, b"users").expect("bucket name encodes");
    super::put_bool(payload, true);
    super::put_compression_profile(payload, CompressionProfile::Fast);
    super::put_usize(payload, 4096).expect("block size encodes");
    super::put_filter_policy(payload, FilterPolicy::Bloom { bits_per_key: 12 });
    super::put_prefix_extractor(payload, &PrefixExtractor::Disabled)
        .expect("prefix extractor encodes");
    super::put_prefix_filter_policy(payload, PrefixFilterPolicy::Bloom { bits_per_prefix: 8 });
    super::put_index_search_policy(payload, IndexSearchPolicy::Auto);
    super::put_usize(payload, 128 * 1024).expect("threshold encodes");
    super::put_blob_level_merge_policy(payload, BlobLevelMergePolicy::Auto);
}

#[test]
fn manifest_round_trips_custom_filter_depth_curve() {
    // The current format persists a custom curve and decodes it back exactly.
    let mut payload = Vec::new();
    put_bucket_options_header(&mut payload);
    super::put_filter_depth_curve(&mut payload, FilterDepthCurve::Custom { step: 3, floor: 6 });
    super::put_u32(&mut payload, 0); // tables
    super::put_u32(&mut payload, 0); // pending blob deletions
    super::put_u32(&mut payload, 0); // checkpoints
    super::put_u64(&mut payload, 0); // writer_epoch

    let state = decode_state(&payload).expect("manifest decodes");
    let options = state.buckets().get("users").expect("bucket options exist");
    assert_eq!(
        options.filter_depth_curve,
        FilterDepthCurve::Custom { step: 3, floor: 6 }
    );
}

#[test]
fn manifest_round_trips_cost_weighted_filter_depth_curve() {
    // The ascending remote curve persists and decodes back exactly.
    let mut payload = Vec::new();
    put_bucket_options_header(&mut payload);
    super::put_filter_depth_curve(
        &mut payload,
        FilterDepthCurve::CostWeighted { step: 4, ceil: 24 },
    );
    super::put_u32(&mut payload, 0); // tables
    super::put_u32(&mut payload, 0); // pending blob deletions
    super::put_u32(&mut payload, 0); // checkpoints
    super::put_u64(&mut payload, 0); // writer_epoch

    let state = decode_state(&payload).expect("manifest decodes");
    let options = state.buckets().get("users").expect("bucket options exist");
    assert_eq!(
        options.filter_depth_curve,
        FilterDepthCurve::CostWeighted { step: 4, ceil: 24 }
    );
}

#[test]
fn manifest_state_stays_put_when_publish_fails() {
    let dir = temp_manifest_dir("publish-fails");
    fs::create_dir_all(&dir).expect("create manifest test dir");
    let path = manifest_path(&dir);
    let mut store = ManifestStore::open_or_create(path, true).expect("manifest opens");

    fs::remove_dir_all(&dir).expect("remove manifest parent to force publish failure");
    let error = store
        .create_bucket("users".to_owned(), BucketOptions::default())
        .expect_err("publish should fail");
    assert!(
        error.to_string().contains("io error"),
        "unexpected error: {error}"
    );
    assert!(
        !store.state().buckets().contains_key("users"),
        "failed publish must not advance in-memory manifest state"
    );
}

#[test]
fn async_manifest_open_create_and_bucket_publish_round_trip() {
    let dir = temp_manifest_dir("async-round-trip");
    fs::create_dir_all(&dir).expect("create manifest test dir");
    let path = manifest_path(&dir);
    let mut store = poll_ready(ManifestStore::open_or_create_with_backend_async(
        path.clone(),
        true,
        NativeFileBackend::new(),
    ))
    .expect("manifest opens through async helper");

    poll_ready(store.create_bucket_async("users".to_owned(), BucketOptions::default()))
        .expect("bucket publishes through async helper");

    let reopened = poll_ready(ManifestStore::open_or_create_with_backend_async(
        path,
        false,
        NativeFileBackend::new(),
    ))
    .expect("manifest reopens through async helper");
    assert!(reopened.state().buckets().contains_key("users"));
    assert!(reopened.state().tables().contains_key("users"));
}

#[test]
fn manifest_checkpoint_publish_round_trip() {
    let dir = temp_manifest_dir("checkpoint-round-trip");
    fs::create_dir_all(&dir).expect("create manifest test dir");
    let path = manifest_path(&dir);
    let mut store = ManifestStore::open_or_create(path.clone(), true).expect("manifest opens");

    store
        .create_checkpoint("cp".to_owned(), crate::types::Sequence::new(7))
        .expect("checkpoint publishes");
    assert_eq!(
        store.checkpoint_sequence("cp"),
        Some(crate::types::Sequence::new(7))
    );

    let reopened = ManifestStore::open_or_create(path, false).expect("manifest reopens");
    assert_eq!(
        reopened.checkpoint_sequence("cp"),
        Some(crate::types::Sequence::new(7))
    );
    assert_eq!(
        reopened.state().checkpoints().get("cp"),
        Some(&crate::types::Sequence::new(7))
    );
}

#[test]
fn async_manifest_publish_failure_does_not_advance_state() {
    let dir = temp_manifest_dir("async-publish-fails");
    fs::create_dir_all(&dir).expect("create manifest test dir");
    let path = manifest_path(&dir);
    let mut store = poll_ready(ManifestStore::open_or_create_with_backend_async(
        path,
        true,
        NativeFileBackend::new(),
    ))
    .expect("manifest opens through async helper");

    fs::remove_dir_all(&dir).expect("remove manifest parent to force publish failure");
    let error = poll_ready(store.create_bucket_async("users".to_owned(), BucketOptions::default()))
        .expect_err("publish should fail");
    assert!(
        error.to_string().contains("io error"),
        "unexpected error: {error}"
    );
    assert!(
        !store.state().buckets().contains_key("users"),
        "failed publish must not advance in-memory manifest state"
    );
}

fn object_manifest_state(floor: u64) -> super::ManifestState {
    let mut state = super::ManifestState::empty();
    state.wal_replay_floor = crate::types::Sequence::new(floor);
    state
}

#[test]
fn object_manifest_creates_then_advances_via_cas() {
    use crate::object_store::InMemoryObjectStore;

    let store = std::sync::Arc::new(InMemoryObjectStore::new());
    let mut manifest =
        poll_ready(super::ObjectManifestStore::open(store, "MANIFEST", 1)).expect("open empty");
    assert_eq!(
        manifest.state().wal_replay_floor(),
        crate::types::Sequence::ZERO,
        "absent manifest opens empty"
    );

    // First publish creates the object (If-None-Match).
    assert!(matches!(
        poll_ready(manifest.try_publish(object_manifest_state(5))).expect("create"),
        super::PublishOutcome::Published
    ));
    assert_eq!(
        manifest.state().wal_replay_floor(),
        crate::types::Sequence::new(5)
    );

    // Second publish advances the existing object (If-Match).
    assert!(matches!(
        poll_ready(manifest.try_publish(object_manifest_state(9))).expect("advance"),
        super::PublishOutcome::Published
    ));
    assert_eq!(
        manifest.state().wal_replay_floor(),
        crate::types::Sequence::new(9)
    );
}

#[test]
fn object_manifest_reconciles_applied_then_errored_cas() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let inner: std::sync::Arc<dyn ObjectClient> = std::sync::Arc::new(InMemoryObjectStore::new());
    let client = std::sync::Arc::new(AppliedManifestThenError {
        inner,
        fail_once: std::sync::atomic::AtomicBool::new(true),
    });
    let mut manifest =
        poll_ready(super::ObjectManifestStore::open(client, "MANIFEST", 1)).expect("open");

    assert!(matches!(
        poll_ready(manifest.try_publish(object_manifest_state(5)))
            .expect("read-after-error reconciles exact intended state"),
        super::PublishOutcome::Published
    ));
    assert_eq!(
        manifest.state().wal_replay_floor(),
        crate::types::Sequence::new(5)
    );
}

#[test]
fn object_manifest_reports_conflict_then_rebases() {
    use crate::object_store::InMemoryObjectStore;

    let store = std::sync::Arc::new(InMemoryObjectStore::new());
    // Two writers that both observed the (empty) manifest.
    let mut writer_a = poll_ready(super::ObjectManifestStore::open(
        std::sync::Arc::clone(&store),
        "MANIFEST",
        1,
    ))
    .expect("open A");
    let mut writer_b = poll_ready(super::ObjectManifestStore::open(
        std::sync::Arc::clone(&store),
        "MANIFEST",
        1,
    ))
    .expect("open B");

    // A wins the create race.
    assert!(matches!(
        poll_ready(writer_a.try_publish(object_manifest_state(5))).expect("A creates"),
        super::PublishOutcome::Published
    ));

    // B's create loses the CAS; it learns A's winning state and does not
    // advance past it.
    match poll_ready(writer_b.try_publish(object_manifest_state(7))).expect("B conflicts") {
        super::PublishOutcome::Conflict { current } => {
            assert_eq!(current.wal_replay_floor(), crate::types::Sequence::new(5));
        }
        super::PublishOutcome::Published => panic!("B must lose the create race"),
    }
    assert_eq!(
        writer_b.state().wal_replay_floor(),
        crate::types::Sequence::new(5),
        "B refreshed to the winning state, ready to rebase"
    );

    // After rebasing onto A's state, B's If-Match publish now succeeds.
    assert!(matches!(
        poll_ready(writer_b.try_publish(object_manifest_state(7))).expect("B retries"),
        super::PublishOutcome::Published
    ));
    assert_eq!(
        writer_b.state().wal_replay_floor(),
        crate::types::Sequence::new(7)
    );
}

#[test]
fn object_manifest_fences_a_stale_writer_after_takeover() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: std::sync::Arc<dyn ObjectClient> = std::sync::Arc::new(InMemoryObjectStore::new());

    // Writer A holds epoch 1 and publishes.
    let mut a = poll_ready(ManifestStore::open_object_store_async(
        std::sync::Arc::clone(&client),
        "MANIFEST",
        1,
    ))
    .expect("open A");
    poll_ready(a.create_bucket_async("alpha".to_owned(), BucketOptions::default()))
        .expect("A publishes");

    // Writer B takes over with a higher epoch (2) and claims it on open,
    // stamping the manifest immediately.
    let mut b = poll_ready(ManifestStore::open_object_store_async(
        std::sync::Arc::clone(&client),
        "MANIFEST",
        2,
    ))
    .expect("open B");
    poll_ready(b.claim_object_epoch_async()).expect("B claims epoch 2");

    // A is still alive (partitioned) and tries to publish again: it loses the
    // CAS, refreshes onto B's manifest (epoch 2 > 1), and is fenced — it does
    // NOT retry into a clobber.
    let fenced = poll_ready(a.create_bucket_async("beta".to_owned(), BucketOptions::default()))
        .expect_err("A must be fenced after B's takeover");
    assert!(
        matches!(
            fenced,
            crate::error::Error::Fenced {
                held_epoch: 1,
                current_epoch: 2
            }
        ),
        "expected Fenced{{1,2}}, got {fenced:?}"
    );

    // B, the legitimate owner, still publishes.
    poll_ready(b.create_bucket_async("gamma".to_owned(), BucketOptions::default()))
        .expect("B still writes");
}

#[test]
fn object_store_manifest_create_bucket_rebases_on_conflict() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: std::sync::Arc<dyn ObjectClient> = std::sync::Arc::new(InMemoryObjectStore::new());
    let mut writer_a = poll_ready(ManifestStore::open_object_store_async(
        std::sync::Arc::clone(&client),
        "MANIFEST",
        1,
    ))
    .expect("open A");
    let mut writer_b = poll_ready(ManifestStore::open_object_store_async(
        std::sync::Arc::clone(&client),
        "MANIFEST",
        1,
    ))
    .expect("open B");

    // A creates "alpha" (first publish = If-None-Match create).
    poll_ready(writer_a.create_bucket_async("alpha".to_owned(), BucketOptions::default()))
        .expect("A creates alpha");
    assert!(writer_a.state().buckets().contains_key("alpha"));

    // B still believes the manifest is empty; creating "beta" loses the
    // initial CAS, rebases onto A's winning state (which has alpha), re-runs
    // the edit, and retries — ending with both buckets.
    poll_ready(writer_b.create_bucket_async("beta".to_owned(), BucketOptions::default()))
        .expect("B creates beta after rebase");
    assert!(
        writer_b.state().buckets().contains_key("alpha"),
        "B rebased onto A's winning state"
    );
    assert!(writer_b.state().buckets().contains_key("beta"));

    // A fresh open sees both — the durable manifest holds the merged result.
    let reopened = poll_ready(ManifestStore::open_object_store_async(
        client, "MANIFEST", 1,
    ))
    .expect("reopen");
    assert!(reopened.state().buckets().contains_key("alpha"));
    assert!(reopened.state().buckets().contains_key("beta"));
}

#[test]
fn object_store_manifest_create_bucket_is_idempotent() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: std::sync::Arc<dyn ObjectClient> = std::sync::Arc::new(InMemoryObjectStore::new());
    let mut manifest = poll_ready(ManifestStore::open_object_store_async(
        client, "MANIFEST", 1,
    ))
    .expect("open");
    poll_ready(manifest.create_bucket_async("alpha".to_owned(), BucketOptions::default()))
        .expect("create");
    // Re-creating with identical options is a no-op (the edit returns None).
    poll_ready(manifest.create_bucket_async("alpha".to_owned(), BucketOptions::default()))
        .expect("idempotent re-create");
    assert_eq!(manifest.state().buckets().len(), 1);
}

#[test]
fn object_store_manifest_rejects_sync_publish() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: std::sync::Arc<dyn ObjectClient> = std::sync::Arc::new(InMemoryObjectStore::new());
    let mut manifest = poll_ready(ManifestStore::open_object_store_async(
        client, "MANIFEST", 1,
    ))
    .expect("open");
    // The sync API is unsupported for object storage (it cannot await a CAS).
    let error = manifest
        .create_bucket("alpha".to_owned(), BucketOptions::default())
        .expect_err("sync create must be rejected");
    assert!(
        error.to_string().contains("async API"),
        "unexpected error: {error}"
    );
}

fn poll_ready<T>(future: impl Future<Output = crate::Result<T>>) -> crate::Result<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("manifest storage future unexpectedly pending"),
    }
}

fn temp_manifest_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "trine-kv-manifest-{name}-{}-{nonce}",
        std::process::id()
    ))
}
