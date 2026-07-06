use std::{
    fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    Db, KeyRange, WriteBatch,
    db::CommitTracker,
    error::Error,
    lsm::LsmTree,
    options::{DbOptions, WriteOptions},
    transaction::{ReadKey, TransactionReadSet},
    types::Sequence,
    wal,
    write_batch::BatchOperation,
};

use super::{
    AcceptedWrite, AcceptedWriteState, DurabilityMode, PreparedShardId, WalAcceptState,
    WriteRequest, effective_durability,
};

fn publish_writer_state_for_test(
    db: &Db,
    writer_state: super::WriterLocalWriteState,
    publish: &super::PublishSequenceGuard<'_>,
) -> super::PublishedWrite {
    let sequenced = db
        .sequence_writer_local_state_under_barrier(writer_state, publish)
        .expect("sequence writer-local state");
    let sequenced = match sequenced {
        super::SequencedWriteState::Noop(_) => {
            panic!("test write should need a commit sequence")
        }
        super::SequencedWriteState::Pending(sequenced) => sequenced,
    };
    let durable = db
        .accept_deferred_wal_for_sequenced_write(sequenced)
        .expect("accept deferred WAL if needed");
    let _memtable_publish = db
        .inner
        .memtable_publish_lock
        .lock()
        .expect("memtable publish lock");
    db.publish_durable_writer_local_state_under_memtable_lock(durable)
        .expect("publish durable writer-local state")
}

#[test]
fn database_durability_is_a_write_floor() {
    assert_eq!(
        effective_durability(DurabilityMode::Buffered, DurabilityMode::SyncData),
        DurabilityMode::SyncData
    );
    assert_eq!(
        effective_durability(DurabilityMode::SyncAll, DurabilityMode::Buffered),
        DurabilityMode::SyncAll
    );
    // A database opened with the strict tier as its default makes every write
    // strict: a per-write request cannot quietly drop below F_FULLFSYNC.
    assert_eq!(
        effective_durability(DurabilityMode::SyncAllStrict, DurabilityMode::SyncAll),
        DurabilityMode::SyncAllStrict
    );
    // And a per-write request can still ask for the strict tier above a
    // weaker database default.
    assert_eq!(
        effective_durability(DurabilityMode::SyncData, DurabilityMode::SyncAllStrict),
        DurabilityMode::SyncAllStrict
    );
}

#[test]
fn commit_tracker_waits_for_prior_terminal_slot() {
    let tracker = CommitTracker::new(Sequence::ZERO);

    let first = tracker.reserve_slot().expect("reserve first slot");
    let second = tracker.reserve_slot().expect("reserve second slot");
    assert_eq!(first.sequence(), Sequence::new(1));
    assert_eq!(second.sequence(), Sequence::new(2));

    tracker.mark_visible(second).expect("mark second visible");
    assert_eq!(tracker.visible_sequence(), Sequence::ZERO);

    tracker.mark_skipped(first).expect("mark first skipped");
    assert_eq!(tracker.visible_sequence(), Sequence::new(2));

    let third = tracker.reserve_slot().expect("reserve third slot");
    assert_eq!(third.sequence(), Sequence::new(3));
}

#[test]
fn commit_tracker_rejects_second_terminal_transition() {
    let tracker = CommitTracker::new(Sequence::ZERO);
    let slot = tracker.reserve_slot().expect("reserve slot");

    tracker.mark_visible(slot).expect("mark slot visible");

    assert!(tracker.mark_skipped(slot).is_err());
    assert_eq!(tracker.visible_sequence(), Sequence::new(1));
}

#[test]
fn accepted_write_completion_delivers_success_result() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"k".to_vec(), b"v".to_vec());
    let request = WriteRequest::batch(batch, WriteOptions::default());
    let (accepted_write, waiter) = AcceptedWrite::accept(request);

    accepted_write.execute(&db);
    let commit = waiter.wait().expect("waiter receives commit result");

    assert_eq!(commit.sequence(), db.last_committed_sequence());
    assert_eq!(
        db.get_sync(b"k").expect("read committed key"),
        Some(b"v".to_vec())
    );
}

#[test]
fn accepted_write_completion_delivers_error_result() {
    let mut options = DbOptions::memory();
    options.read_only = true;
    let db = Db::open_sync(options).expect("read-only memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"k".to_vec(), b"v".to_vec());
    let request = WriteRequest::batch(batch, WriteOptions::default());
    let (accepted_write, waiter) = AcceptedWrite::accept(request);

    accepted_write.execute(&db);
    let error = waiter.wait().expect_err("waiter receives commit error");

    assert!(matches!(error, Error::ReadOnly));
    assert_eq!(db.get_sync(b"k").expect("read missing key"), None);
}

#[test]
fn write_rejects_key_over_configured_limit() {
    let options = DbOptions::memory().with_max_key_bytes(3);
    let db = Db::open_sync(options).expect("memory db opens");

    let error = db
        .put_sync(b"long", b"value")
        .expect_err("oversized key is rejected");

    assert!(
        matches!(error, Error::InvalidOptions { ref message } if message.contains("write key length 4 exceeds maximum 3")),
        "expected configured key limit error, got {error:?}"
    );
    assert_eq!(db.get_sync(b"long").expect("read missing key"), None);
}

#[test]
fn write_rejects_value_over_configured_limit() {
    let options = DbOptions::memory().with_max_value_bytes(3);
    let db = Db::open_sync(options).expect("memory db opens");

    let error = db
        .put_sync(b"key", b"value")
        .expect_err("oversized value is rejected");

    assert!(
        matches!(error, Error::InvalidOptions { ref message } if message.contains("write value length 5 exceeds maximum 3")),
        "expected configured value limit error, got {error:?}"
    );
    assert_eq!(db.get_sync(b"key").expect("read missing value"), None);
}

#[test]
fn range_delete_rejects_bound_over_configured_key_limit() {
    let options = DbOptions::memory().with_max_key_bytes(3);
    let db = Db::open_sync(options).expect("memory db opens");

    let error = db
        .delete_range_sync(KeyRange::half_open(b"a".to_vec(), b"long".to_vec()))
        .expect_err("oversized range bound is rejected");

    assert!(
        matches!(error, Error::InvalidOptions { ref message } if message.contains("write range bound length 4 exceeds maximum 3")),
        "expected configured range-bound limit error, got {error:?}"
    );
}

#[test]
fn open_rejects_invalid_write_byte_limits() {
    let error = Db::open_sync(DbOptions::memory().with_max_key_bytes(0))
        .expect_err("zero key limit is rejected");
    assert!(
        matches!(error, Error::InvalidOptions { ref message } if message.contains("max key size must be non-zero")),
        "expected zero key limit error, got {error:?}"
    );

    let error = Db::open_sync(
        DbOptions::memory().with_max_value_bytes(DbOptions::MAX_WRITE_FIELD_BYTES + 1),
    )
    .expect_err("oversized value limit is rejected");
    assert!(
        matches!(error, Error::InvalidOptions { ref message } if message.contains("max value size")),
        "expected value limit ceiling error, got {error:?}"
    );
}

#[test]
fn accepted_write_preflight_creates_writer_local_state_without_publication() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    db.bucket_sync("events").expect("named bucket opens");
    let mut batch = WriteBatch::new();
    batch.put(b"default".to_vec(), b"v1".to_vec());
    batch
        .put_bucket("events", b"event".to_vec(), b"v2".to_vec())
        .expect("stage named bucket write");
    let request = WriteRequest::batch(batch, WriteOptions::default());

    let accepted_state = db
        .accept_write_request(request)
        .expect("write request is accepted");
    let AcceptedWriteState::Pending(writer_state) = accepted_state else {
        panic!("non-empty write must produce writer-local state");
    };

    let prepared = writer_state.prepared;
    assert_eq!(prepared.operation_count(), 2);
    assert!(prepared.transaction_reads.is_none());
    assert_eq!(
        prepared
            .wal_operations
            .iter()
            .map(BatchOperation::bucket)
            .collect::<Vec<_>>(),
        ["default", "events"]
    );
    assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
    assert_eq!(
        db.get_sync(b"default").expect("preflight is not visible"),
        None
    );

    assert_eq!(prepared.deltas.len(), 2);
    assert_eq!(prepared.touched_states.len(), 2);
    assert_eq!(prepared.deltas[0].bucket, "default");
    assert_eq!(
        prepared.deltas[0].shard,
        PreparedShardId::CURRENT_SINGLE_SHARD
    );
    assert_eq!(prepared.deltas[0].operations[0].batch_index, 0);
    assert_eq!(
        prepared.deltas[0].operations[0].operation.bucket(),
        "default"
    );
    assert_eq!(
        prepared.deltas[0].key_bounds.lower.as_deref(),
        Some(b"default".as_slice())
    );
    assert_eq!(
        prepared.deltas[0].key_bounds.upper.as_deref(),
        Some(b"default".as_slice())
    );
    assert_eq!(prepared.deltas[1].bucket, "events");
    assert_eq!(prepared.deltas[1].operations[0].batch_index, 1);
    assert_eq!(
        prepared.deltas[1].operations[0].operation.bucket(),
        "events"
    );
    assert!(prepared.estimated_bytes > 0);
    assert!(
        prepared
            .deltas
            .iter()
            .all(|delta| delta.estimated_bytes > 0)
    );
    assert!(!Arc::ptr_eq(
        &prepared.deltas[0].state,
        &prepared.deltas[1].state
    ));
}

#[test]
fn partial_memtable_publication_failure_closes_database_handle() {
    let path = temp_db_path("partial-publish-failure");
    let db = Db::open_sync(DbOptions::new(&path)).expect("persistent db opens");
    db.bucket_sync("events").expect("named bucket opens");
    let events_state = db.bucket_state("events").expect("events bucket state");
    poison_active_memtable_entries(&events_state);

    let mut batch = WriteBatch::new();
    batch.put(b"default".to_vec(), b"v1".to_vec());
    batch
        .put_bucket("events", b"event".to_vec(), b"v2".to_vec())
        .expect("stage named bucket write");

    let error = db
        .write_sync(batch, WriteOptions::default())
        .expect_err("second bucket publish should fail");
    match error {
        Error::Corruption { message } => {
            assert!(message.contains("partially publishing in-memory state"));
            assert!(message.contains("database handle closed"));
        }
        other => panic!("expected corruption error, got {other:?}"),
    }

    assert!(matches!(db.get_sync(b"default"), Err(Error::Closed)));

    drop(db);
    cleanup_dir(&path);
}

#[test]
fn writer_local_preparation_groups_same_bucket_delta_with_bounds() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"b".to_vec(), b"v".to_vec());
    batch.delete(b"a".to_vec());
    batch.delete_range(crate::types::KeyRange::half_open(
        b"c".to_vec(),
        b"e".to_vec(),
    ));
    let request = WriteRequest::batch(batch, WriteOptions::default());

    let accepted_state = db
        .accept_write_request(request)
        .expect("write request is accepted");
    let AcceptedWriteState::Pending(writer_state) = accepted_state else {
        panic!("non-empty write must produce writer-local state");
    };
    let prepared = writer_state.prepared;

    assert_eq!(prepared.operation_count(), 3);
    assert_eq!(prepared.deltas.len(), 1);
    assert_eq!(prepared.touched_states.len(), 1);
    assert_eq!(prepared.deltas[0].bucket, "default");
    assert_eq!(
        prepared.deltas[0].shard,
        PreparedShardId::CURRENT_SINGLE_SHARD
    );
    assert_eq!(
        prepared.deltas[0]
            .operations
            .iter()
            .map(|operation| operation.batch_index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        prepared.deltas[0].key_bounds.lower.as_deref(),
        Some(b"a".as_slice())
    );
    assert_eq!(
        prepared.deltas[0].key_bounds.upper.as_deref(),
        Some(b"e".as_slice())
    );
    assert!(!prepared.deltas[0].key_bounds.lower_unbounded);
    assert!(!prepared.deltas[0].key_bounds.upper_unbounded);
    assert!(prepared.deltas[0].estimated_bytes > 0);
}

#[test]
fn persistent_blind_write_accepts_wal_before_publish_barrier() {
    let path = temp_db_path("preaccept-wal");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("persistent db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"k".to_vec(), b"v".to_vec());
    let request = WriteRequest::batch(batch, WriteOptions::flush());

    let blocked_publish = db
        .inner
        .publish_barrier
        .enter_sequence()
        .expect("enter publish barrier");
    let accepted_state = db
        .accept_write_request(request)
        .expect("write request is accepted");
    let AcceptedWriteState::Pending(writer_state) = accepted_state else {
        panic!("non-empty write must produce writer-local state");
    };

    assert!(matches!(
        writer_state.wal_accept,
        WalAcceptState::Accepted(_)
    ));
    assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
    assert_eq!(
        db.get_sync(b"k").expect("preaccepted write is not visible"),
        None
    );
    let wal_batches = wal::read_all_batches(&path).expect("WAL reads");
    assert_eq!(wal_batches.len(), 1);
    assert_eq!(wal_batches[0].sequence, Sequence::new(1));

    drop(blocked_publish);
    let publish = db
        .inner
        .publish_barrier
        .enter_sequence()
        .expect("enter publish barrier");
    let published = publish_writer_state_for_test(&db, writer_state, &publish);
    assert_eq!(published.commit_info.sequence(), Sequence::new(1));
    let visible_slot = published.visible_slot.expect("preaccepted commit has slot");
    db.inner
        .commit_tracker
        .mark_visible(visible_slot)
        .expect("mark preaccepted commit visible");
    assert_eq!(db.last_committed_sequence(), Sequence::new(1));
    assert_eq!(
        db.get_sync(b"k").expect("published write reads"),
        Some(b"v".to_vec())
    );

    drop(publish);
    drop(db);
    cleanup_dir(&path);
}

#[test]
fn persistent_transaction_accepts_wal_after_sequence_barrier_before_memory_publish() {
    let path = temp_db_path("transaction-wal-after-sequence");
    let mut options = DbOptions::persistent(&path).with_durability(DurabilityMode::Flush);
    options.background_worker_count = 0;
    let db = Db::open_sync(options).expect("persistent db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"k".to_vec(), b"txn".to_vec());
    let request = WriteRequest::transaction(
        Sequence::ZERO,
        TransactionReadSet {
            point_reads: vec![ReadKey {
                bucket: "default".to_owned(),
                key: b"k".to_vec(),
            }],
            range_reads: Vec::new(),
        },
        batch,
        WriteOptions::flush(),
    );
    let accepted_state = db
        .accept_write_request(request)
        .expect("transaction write request is accepted");
    let AcceptedWriteState::Pending(writer_state) = accepted_state else {
        panic!("transaction write must produce writer-local state");
    };
    assert!(matches!(writer_state.wal_accept, WalAcceptState::Deferred));
    assert!(
        wal::read_all_batches(&path).expect("WAL reads").is_empty(),
        "transaction WAL should wait for serialized read validation"
    );

    let publish = db
        .inner
        .publish_barrier
        .enter_sequence()
        .expect("enter publish barrier");
    let sequenced = db
        .sequence_writer_local_state_under_barrier(writer_state, &publish)
        .expect("transaction read set validates and slot reserves");
    let super::SequencedWriteState::Pending(sequenced) = sequenced else {
        panic!("transaction write should reserve a slot");
    };
    assert_eq!(db.stats().commit_open_slots, 1);
    assert!(
        wal::read_all_batches(&path).expect("WAL reads").is_empty(),
        "sequence reservation should not append WAL while barrier is held"
    );
    drop(publish);

    let durable = db
        .accept_deferred_wal_for_sequenced_write(sequenced)
        .expect("transaction WAL accepts outside publish barrier");
    let wal_batches = wal::read_all_batches(&path).expect("WAL reads");
    assert_eq!(wal_batches.len(), 1);
    assert_eq!(wal_batches[0].sequence, Sequence::new(1));
    assert_eq!(
        db.get_sync(b"k")
            .expect("WAL-accepted transaction is not visible"),
        None
    );

    let memtable_publish = db
        .inner
        .memtable_publish_lock
        .lock()
        .expect("memtable publish lock");
    let published = db
        .publish_durable_writer_local_state_under_memtable_lock(durable)
        .expect("publish transaction state");
    let visible_slot = published
        .visible_slot
        .expect("transaction has visible slot");
    db.inner
        .commit_tracker
        .mark_visible(visible_slot)
        .expect("mark transaction visible");
    assert_eq!(
        db.get_sync(b"k").expect("transaction write is visible"),
        Some(b"txn".to_vec())
    );

    drop(memtable_publish);
    drop(db);
    cleanup_dir(&path);
}

#[test]
fn writer_local_state_publishes_under_memtable_lock_after_sequence() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let mut batch = WriteBatch::new();
    batch.put(b"k".to_vec(), b"v".to_vec());
    let request = WriteRequest::batch(batch, WriteOptions::default());
    let accepted_state = db
        .accept_write_request(request)
        .expect("write request is accepted");
    let AcceptedWriteState::Pending(writer_state) = accepted_state else {
        panic!("non-empty write must produce writer-local state");
    };

    let publish = db
        .inner
        .publish_barrier
        .enter_sequence()
        .expect("enter publish barrier");
    let published = publish_writer_state_for_test(&db, writer_state, &publish);

    assert!(!published.request_flush);
    assert_eq!(published.commit_info.sequence(), Sequence::new(1));
    assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
    assert_eq!(
        db.get_sync(b"k").expect("published slot is not visible"),
        None
    );
    let visible_slot = published.visible_slot.expect("published write has slot");
    db.inner
        .commit_tracker
        .mark_visible(visible_slot)
        .expect("mark published write visible");
    assert_eq!(db.last_committed_sequence(), Sequence::new(1));
    assert_eq!(
        db.get_sync(b"k").expect("read committed key"),
        Some(b"v".to_vec())
    );
    let state = db.bucket_state("default").expect("default bucket state");
    assert_eq!(
        state
            .active_memtable_bytes()
            .expect("active memtable bytes"),
        0
    );
    let (delta_count, point_delta_count, range_tombstone_count) =
        state.delta_debug_counts().expect("delta counts");
    assert_eq!(delta_count, 1);
    assert_eq!(point_delta_count, 1);
    assert_eq!(range_tombstone_count, 0);
    let db_stats = db.stats();
    assert!(db_stats.memtable_bytes > 0);
    assert_eq!(db_stats.immutable_memtables, 0);
}

#[test]
fn visible_sequence_waits_for_earlier_published_slot_completion() {
    let db = Db::open_sync(DbOptions::memory()).expect("memory db opens");
    let mut first_batch = WriteBatch::new();
    first_batch.put(b"k".to_vec(), b"v1".to_vec());
    let first_request = WriteRequest::batch(first_batch, WriteOptions::default());
    let AcceptedWriteState::Pending(first_state) = db
        .accept_write_request(first_request)
        .expect("first write request is accepted")
    else {
        panic!("first write must produce writer-local state");
    };

    let mut second_batch = WriteBatch::new();
    second_batch.put(b"k".to_vec(), b"v2".to_vec());
    let second_request = WriteRequest::batch(second_batch, WriteOptions::default());
    let AcceptedWriteState::Pending(second_state) = db
        .accept_write_request(second_request)
        .expect("second write request is accepted")
    else {
        panic!("second write must produce writer-local state");
    };

    let publish = db
        .inner
        .publish_barrier
        .enter_sequence()
        .expect("enter publish barrier");
    let first_published = publish_writer_state_for_test(&db, first_state, &publish);
    let second_published = publish_writer_state_for_test(&db, second_state, &publish);
    assert_eq!(first_published.commit_info.sequence(), Sequence::new(1));
    assert_eq!(second_published.commit_info.sequence(), Sequence::new(2));

    assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
    assert_eq!(db.get_sync(b"k").expect("published writes are gated"), None);

    db.inner
        .commit_tracker
        .mark_visible(second_published.visible_slot.expect("second slot"))
        .expect("mark second slot visible");
    assert_eq!(db.last_committed_sequence(), Sequence::ZERO);
    assert_eq!(
        db.get_sync(b"k").expect("visible sequence waits for first"),
        None
    );

    db.inner
        .commit_tracker
        .mark_visible(first_published.visible_slot.expect("first slot"))
        .expect("mark first slot visible");
    assert_eq!(db.last_committed_sequence(), Sequence::new(2));
    assert_eq!(
        db.get_sync(b"k").expect("latest visible key"),
        Some(b"v2".to_vec())
    );
}

#[test]
fn in_memory_write_budget_merges_deltas_without_active_mirror() {
    let mut options = DbOptions::memory();
    options.write_buffer_bytes = 1;
    let db = Db::open_sync(options).expect("memory db opens");

    db.put_sync(b"k", b"v1").expect("first write");
    let snapshot = db.snapshot();
    db.put_sync(b"k", b"v2").expect("second write");

    assert_eq!(
        db.get_sync(b"k").expect("current read"),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        snapshot
            .get_sync(&db.default_bucket_sync().expect("default bucket"), b"k")
            .expect("snapshot read"),
        Some(b"v1".to_vec())
    );

    let state = db.bucket_state("default").expect("default bucket state");
    assert_eq!(
        state
            .active_memtable_bytes()
            .expect("active memtable bytes"),
        0
    );
    let delta_stats = state.delta_debug_stats().expect("delta stats");
    assert_eq!(delta_stats.merged_epoch_count, 1);
    assert_eq!(delta_stats.max_shard_chain_len, 1);
    assert!(delta_stats.open_epoch_bytes > 0);
}

fn poison_active_memtable_entries(state: &LsmTree) {
    let active_memtable = state
        .active_memtable
        .read()
        .expect("active memtable pointer lock is not poisoned")
        .clone();
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _entries = active_memtable
            .write_entries()
            .expect("memtable entries lock starts healthy");
        panic!("poison memtable entries for commit failure test");
    }));
    assert!(result.is_err());
}

fn temp_db_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "trine-kv-commit-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn cleanup_dir(path: &std::path::Path) {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to remove {}: {error}", path.display()),
    }
}
