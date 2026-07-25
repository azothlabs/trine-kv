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

#[test]
fn object_wal_segments_require_chain_header() {
    assert!(matches!(
        decode_object_wal_segment("db/wal", b"raw WAL frames"),
        Err(Error::Corruption { .. })
    ));
}

struct CountingObjectClient {
    inner: Arc<dyn ObjectClient>,
    puts: AtomicUsize,
    put_ifs: AtomicUsize,
}

struct AppliedThenErroredClient {
    inner: Arc<dyn ObjectClient>,
    fail_put_if_call: usize,
    put_if_calls: AtomicUsize,
}

impl std::fmt::Debug for AppliedThenErroredClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppliedThenErroredClient")
            .field("fail_put_if_call", &self.fail_put_if_call)
            .finish_non_exhaustive()
    }
}

impl AppliedThenErroredClient {
    fn new(inner: Arc<dyn ObjectClient>, fail_put_if_call: usize) -> Self {
        Self {
            inner,
            fail_put_if_call,
            put_if_calls: AtomicUsize::new(0),
        }
    }
}

impl ObjectClient for AppliedThenErroredClient {
    fn get<'op>(&'op self, key: &str) -> ObjectFuture<'op, Option<Arc<[u8]>>> {
        self.inner.get(key)
    }

    fn get_range<'op>(&'op self, key: &str, offset: u64, len: u64) -> ObjectFuture<'op, Arc<[u8]>> {
        self.inner.get_range(key, offset, len)
    }

    fn put<'op>(&'op self, key: &str, bytes: Arc<[u8]>) -> ObjectFuture<'op, ETag> {
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
        let call = self.put_if_calls.fetch_add(1, Ordering::AcqRel) + 1;
        let applied = self.inner.put_if(key, bytes, precondition);
        Box::pin(async move {
            let outcome = applied.await?;
            if call == self.fail_put_if_call && matches!(outcome, PutIf::Stored { .. }) {
                return Err(Error::Io(std::io::Error::other(
                    "injected response loss after conditional write applied",
                )));
            }
            Ok(outcome)
        })
    }
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
fn object_wal_reconciles_applied_writes_after_response_loss() {
    use crate::object_store::InMemoryObjectStore;

    // Conditional writes are: lease acquisition, immutable segment creation,
    // then WAL-head publication. Each can be durably applied even when the
    // client receives an I/O error instead of the success response.
    for fail_put_if_call in 1..=3 {
        let client: Arc<dyn ObjectClient> = Arc::new(AppliedThenErroredClient::new(
            Arc::new(InMemoryObjectStore::new()),
            fail_put_if_call,
        ));
        let mut lease = poll_ready(ObjectWriterLease::acquire(Arc::clone(&client), "LOCK"))
            .expect("lease acquisition reconciles an applied response loss");
        let frame =
            wal::encode_batch_frame(Sequence::new(1), &[put("k", "v")]).expect("encode WAL frame");
        let accept = ObjectWalAccept {
            sequence: Sequence::new(1),
            frame: frame.into(),
            completion: Arc::new(ObjectWalCompletion::new()),
        };
        poll_ready(lease.publish_commit_batch(PathBuf::from("db").as_path(), &[accept]))
            .expect("WAL publication reconciles an applied response loss");
        let state = lease.lease_state();
        assert_eq!(state.committed_sequence, Sequence::new(1));
        let batches = poll_ready(object_store_wal_batches_after_replay_floor(
            Arc::clone(&client),
            PathBuf::from("db").as_path(),
            &state,
            Sequence::ZERO,
        ))
        .expect("reconciled WAL chain reads");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].operations, vec![put("k", "v")]);
    }
}

#[test]
fn object_wal_chain_rejects_cross_database_keys_and_sequence_holes() {
    use crate::object_store::InMemoryObjectStore;

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let cross_database = ObjectLeaseState {
        epoch: 1,
        owner_id: [1; 16],
        committed_sequence: Sequence::new(1),
        current_wal_key: Some(
            "other/trine.wal.epoch-00000000000000000001.commit-00000000000000000001.trinewal"
                .to_owned(),
        ),
        lease_expires_at_ms: u64::MAX,
    };
    let error = poll_ready(object_store_wal_batches_after_replay_floor(
        Arc::clone(&client),
        PathBuf::from("db").as_path(),
        &cross_database,
        Sequence::ZERO,
    ))
    .expect_err("WAL predecessor cannot escape the database root");
    assert!(matches!(error, Error::Corruption { .. }));

    let frame =
        wal::encode_batch_frame(Sequence::new(2), &[put("k", "v")]).expect("encode hole frame");
    let segment = encode_object_wal_segment(None, &frame).expect("encode hole segment");
    let identity = object_wal_segment_identity(&segment);
    let key = canonical_object_key(&wal::object_wal_commit_path(
        PathBuf::from("db").as_path(),
        1,
        Sequence::new(2),
        &identity,
    ))
    .expect("canonical WAL key");
    assert!(matches!(
        poll_ready(client.put_if(&key, Arc::from(segment), Precondition::IfNoneMatch))
            .expect("store malformed chain"),
        PutIf::Stored { .. }
    ));
    let hole = ObjectLeaseState {
        epoch: 1,
        owner_id: [1; 16],
        committed_sequence: Sequence::new(2),
        current_wal_key: Some(key),
        lease_expires_at_ms: u64::MAX,
    };
    let error = poll_ready(object_store_wal_batches_after_replay_floor(
        client,
        PathBuf::from("db").as_path(),
        &hole,
        Sequence::ZERO,
    ))
    .expect_err("WAL chain cannot skip an acknowledged sequence");
    assert!(matches!(error, Error::Corruption { .. }));
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
    let batches = poll_ready(object_store_wal_batches_after_replay_floor(
        Arc::clone(&client),
        PathBuf::from("db").as_path(),
        &head,
        Sequence::ZERO,
    ))
    .expect("decode grouped WAL chain");
    assert_eq!(batches.len(), COMMITS);
    assert_eq!(
        counted.puts(),
        0,
        "immutable segments must never use overwrite PUT"
    );
    assert_eq!(
        counted.put_ifs(),
        3,
        "lease acquire, one grouped segment, and grouped head publish use CAS"
    );
}

#[test]
fn object_writer_ignores_orphan_segment_before_retrying_sequence() {
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
    let orphan_frame = wal::encode_batch_frame(Sequence::new(2), &[put("retry", "unconfirmed")])
        .expect("encode unconfirmed frame");
    let orphan = encode_object_wal_segment(Some(&segment_key), &orphan_frame)
        .expect("encode orphan segment");
    let orphan_identity = object_wal_segment_identity(&orphan);
    let orphan_key = canonical_object_key(&wal::object_wal_commit_path(
        PathBuf::from("db").as_path(),
        first.epoch(),
        Sequence::new(2),
        &orphan_identity,
    ))
    .expect("canonical orphan key");
    assert!(matches!(
        poll_ready(client.put_if(&orphan_key, Arc::from(orphan), Precondition::IfNoneMatch,))
            .expect("persist WAL before head confirmation"),
        PutIf::Stored { .. }
    ));
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
    let retried_key = retried_head
        .current_wal_key
        .clone()
        .expect("retried WAL key");
    assert_ne!(retried_key, orphan_key);
    let batches = poll_ready(object_store_wal_batches_after_replay_floor(
        Arc::clone(&client),
        PathBuf::from("db").as_path(),
        &retried_head,
        Sequence::ZERO,
    ))
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
        owner_id: first.state.owner_id,
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
