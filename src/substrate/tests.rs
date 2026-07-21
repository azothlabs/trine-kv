use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use crate::object_store::{ObjectFuture, ObjectMeta};
use crate::storage::NativeFileBackend;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "trine-kv-substrate-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn put(key: &str, value: &str) -> BatchOperation {
    BatchOperation::Put {
        bucket: "default".to_owned(),
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

struct CountingObjectClient {
    inner: Arc<dyn ObjectClient>,
    puts: AtomicUsize,
    put_ifs: AtomicUsize,
}

impl std::fmt::Debug for CountingObjectClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CountingObjectClient")
            .finish_non_exhaustive()
    }
}

impl CountingObjectClient {
    fn new(inner: Arc<dyn ObjectClient>) -> Self {
        Self {
            inner,
            puts: AtomicUsize::new(0),
            put_ifs: AtomicUsize::new(0),
        }
    }

    fn puts(&self) -> usize {
        self.puts.load(Ordering::Acquire)
    }

    fn put_ifs(&self) -> usize {
        self.put_ifs.load(Ordering::Acquire)
    }
}

impl ObjectClient for CountingObjectClient {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        self.inner.get(key)
    }

    fn get_range<'op>(&'op self, key: &str, offset: u64, len: u64) -> ObjectFuture<'op, Arc<[u8]>> {
        self.inner.get_range(key, offset, len)
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
        self.puts.fetch_add(1, Ordering::AcqRel);
        self.inner.put(key, bytes)
    }

    fn delete<'op>(&'op self, key: &str) -> ObjectFuture<'op, ()> {
        self.inner.delete(key)
    }

    fn list<'op>(&'op self, prefix: &str) -> ObjectFuture<'op, Vec<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn head<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<ObjectMeta>> {
        self.inner.head(key)
    }

    fn put_if<'op>(
        &'op self,
        key: &str,
        bytes: Arc<[u8]>,
        precondition: Precondition,
    ) -> ObjectFuture<'op, PutIf> {
        self.put_ifs.fetch_add(1, Ordering::AcqRel);
        self.inner.put_if(key, bytes, precondition)
    }
}

#[test]
fn filesystem_substrate_drives_wal_and_lease() {
    let dir = temp_dir("wal-and-lease");
    fs::create_dir_all(&dir).expect("create substrate test dir");
    let backend = NativeFileBackend::new();

    let lease = ProcessLock::acquire_with_backend(&backend, &dir).expect("acquire writer lease");
    let wal = WalFrontDoor::open_sharded_with_backend(&backend, &dir, 1).expect("open sharded WAL");
    let substrate =
        DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(Some(wal), Some(lease)));

    // Drive it exactly as the commit / flush / close paths would.
    assert!(substrate.wal_is_present());
    substrate
        .accept_commit(Sequence::new(1), &[put("k", "v")], DurabilityMode::Flush)
        .expect("accept commit");
    substrate
        .persist_wal(DurabilityMode::Flush)
        .expect("persist WAL");
    let stats = substrate.wal_stats().expect("WAL present");
    assert_eq!(stats.records_accepted, 1);
    assert_eq!(stats.open_shards, 1);
    substrate
        .rewrite_wal_after_replay_floor(Sequence::new(1))
        .expect("rewrite WAL after replay floor");

    // Releasing the lease is idempotent.
    substrate.release_writer_lease();
    substrate.release_writer_lease();

    drop(substrate);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn filesystem_substrate_without_wal_is_inert() {
    let substrate = DurabilitySubstrate::Filesystem(FilesystemSubstrate::new(None, None));
    assert!(!substrate.wal_is_present());
    substrate
        .accept_commit(Sequence::new(1), &[put("k", "v")], DurabilityMode::Flush)
        .expect("no-op accept");
    substrate
        .persist_wal(DurabilityMode::Flush)
        .expect("no-op persist");
    assert!(substrate.wal_stats().is_none());
    substrate
        .rewrite_wal_after_replay_floor(Sequence::new(1))
        .expect("no-op rewrite");
    substrate.release_writer_lease();
}

fn poll_ready<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(result) => result,
        std::task::Poll::Pending => panic!("in-memory object future unexpectedly pending"),
    }
}

#[test]
fn object_store_substrate_publishes_remote_wal_head() {
    use crate::object_store::InMemoryObjectStore;

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let lease =
        poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK")).expect("acquire lease");
    let substrate = DurabilitySubstrate::ObjectStore(
        ObjectStoreSubstrate::new(lease, PathBuf::from("db")).expect("open object WAL lane"),
    );

    assert!(substrate.wal_is_present());
    substrate
        .accept_commit(Sequence::new(1), &[put("k", "v")], DurabilityMode::Flush)
        .expect("accept commit");
    substrate
        .persist_wal(DurabilityMode::Flush)
        .expect("persist WAL");
    assert_eq!(
        substrate
            .wal_stats()
            .expect("object WAL stats")
            .records_accepted,
        1
    );
    substrate
        .rewrite_wal_after_replay_floor(Sequence::new(1))
        .expect("rewrite WAL after replay floor");
    let head = poll_ready(ObjectWriterLease::read_current(client, "LOCK"))
        .expect("read lease")
        .expect("lease exists");
    assert_eq!(head.committed_sequence, Sequence::new(1));
    substrate.release_writer_lease(); // no-op; idempotent
}

#[test]
fn object_wal_lane_group_commits_queued_accepts() {
    use crate::object_store::InMemoryObjectStore;

    const COMMITS: usize = 8;
    let counted = Arc::new(CountingObjectClient::new(Arc::new(
        InMemoryObjectStore::new(),
    )));
    let client: Arc<dyn ObjectClient> = counted.clone();
    let lease =
        poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK")).expect("acquire lease");
    let lane = ObjectWalLane::spawn(lease, PathBuf::from("db")).expect("open object WAL lane");
    let mut completions = Vec::with_capacity(COMMITS);

    for index in 0..COMMITS {
        let sequence = Sequence::new((index + 1) as u64);
        let frame = wal::encode_batch_frame(
            sequence,
            &[put(
                &format!("key-{index:02}"),
                &format!("value-{index:02}"),
            )],
        )
        .expect("encode WAL frame");
        let completion = Arc::new(ObjectWalCompletion::new());
        lane.send(ObjectWalCommand::Accept(ObjectWalAccept {
            sequence,
            frame: frame.into(),
            completion: Arc::clone(&completion),
        }))
        .expect("queue WAL accept");
        completions.push(completion);
    }

    for completion in completions {
        completion.wait().expect("WAL accept completed");
    }

    let head = poll_ready(ObjectWriterLease::read_current(Arc::clone(&client), "LOCK"))
        .expect("read lease")
        .expect("lease exists");
    assert_eq!(head.committed_sequence, Sequence::new(COMMITS as u64));
    let segment_key = head.current_wal_key.expect("segment key");
    let segment = poll_ready(client.get(&segment_key))
        .expect("read WAL segment")
        .expect("segment exists");
    let batches = wal::decode_frames_after(segment.as_ref(), Sequence::ZERO)
        .expect("decode grouped WAL segment");
    assert_eq!(batches.len(), COMMITS);
    assert_eq!(
        counted.puts(),
        1,
        "all accepts should share one segment PUT"
    );
    assert_eq!(
        counted.put_ifs(),
        2,
        "lease acquire and grouped head publish should each use one CAS"
    );
}

#[test]
fn object_writer_discards_unconfirmed_wal_tail_before_retrying_sequence() {
    use crate::object_store::InMemoryObjectStore;

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let mut first =
        poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK")).expect("first lease");
    let first_frame = wal::encode_batch_frame(Sequence::new(1), &[put("first", "one")])
        .expect("encode first frame");
    let first_accept = ObjectWalAccept {
        sequence: Sequence::new(1),
        frame: first_frame.into(),
        completion: Arc::new(ObjectWalCompletion::new()),
    };
    poll_ready(first.publish_commit_batch(PathBuf::from("db").as_path(), &[first_accept]))
        .expect("publish first commit");

    let head = first.lease_state();
    let segment_key = head.current_wal_key.expect("current WAL key");
    let mut segment = poll_ready(client.get(&segment_key))
        .expect("read current WAL")
        .expect("current WAL exists")
        .to_vec();
    segment.extend_from_slice(
        &wal::encode_batch_frame(Sequence::new(2), &[put("retry", "unconfirmed")])
            .expect("encode unconfirmed tail"),
    );
    poll_ready(client.put(&segment_key, Arc::from(segment.as_slice())))
        .expect("persist WAL before head confirmation");
    poll_ready(first.release()).expect("release first lease");

    let mut retry =
        poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK")).expect("retry lease");
    let retry_frame = wal::encode_batch_frame(Sequence::new(2), &[put("retry", "confirmed")])
        .expect("encode retry frame");
    let retry_accept = ObjectWalAccept {
        sequence: Sequence::new(2),
        frame: retry_frame.into(),
        completion: Arc::new(ObjectWalCompletion::new()),
    };
    poll_ready(retry.publish_commit_batch(PathBuf::from("db").as_path(), &[retry_accept]))
        .expect("publish retried sequence");

    let retried_head = retry.lease_state();
    assert_eq!(retried_head.committed_sequence, Sequence::new(2));
    let retried_key = retried_head.current_wal_key.expect("retried WAL key");
    let retried_segment = poll_ready(client.get(&retried_key))
        .expect("read retried WAL")
        .expect("retried WAL exists");
    let batches = wal::decode_frames_after(retried_segment.as_ref(), Sequence::ZERO)
        .expect("retried WAL has increasing sequences");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>(),
        vec![Sequence::new(1), Sequence::new(2)]
    );
    assert_eq!(batches[1].operations, vec![put("retry", "confirmed")]);
}

#[test]
fn object_writer_lease_rejects_live_second_writer_and_takes_expired() {
    use crate::object_store::InMemoryObjectStore;

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());

    // First acquire creates the lease object at epoch 1.
    let first =
        poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK")).expect("first acquire");
    assert_eq!(first.epoch(), 1);
    assert_eq!(first.committed_sequence(), Sequence::ZERO);

    let error = poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK"))
        .expect_err("live lease rejects a second writer");
    assert!(error.to_string().contains("writer lease LOCK is held"));

    let expired = ObjectLeaseState {
        epoch: first.epoch(),
        committed_sequence: Sequence::ZERO,
        current_wal_key: None,
        lease_expires_at_ms: 0,
    };
    match poll_ready(client.put_if(
        "LOCK",
        encode_lease_state(expired).expect("encode expired lease"),
        Precondition::IfMatch(first.etag.clone()),
    ))
    .expect("store expired lease")
    {
        PutIf::Stored { .. } => {}
        PutIf::PreconditionFailed { .. } => {
            panic!("expired lease rewrite should match the observed ETag");
        }
    }

    let second = poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK"))
        .expect("expired lease can be acquired");
    assert_eq!(second.epoch(), 2);
    let error = poll_ready(ObjectWriterLease::acquire(client, "LOCK"))
        .expect_err("new live lease rejects another writer");
    assert!(error.to_string().contains("writer lease LOCK is held"));
}
