use super::*;

#[test]
fn fatal_write_stop_reason_has_one_exact_public_classification() {
    for (reason, corruption, fencing, outcome_unknown) in [
        (FatalWriteStopReason::Corruption, 1, 0, 0),
        (FatalWriteStopReason::Fenced, 0, 1, 0),
        (FatalWriteStopReason::OutcomeUnknown, 0, 0, 1),
    ] {
        let stats = FatalWriteStopReason::stats(reason.code());
        assert!(stats.stopped);
        assert_eq!(stats.total, 1);
        assert_eq!(stats.corruption, corruption);
        assert_eq!(stats.fencing, fencing);
        assert_eq!(stats.outcome_unknown, outcome_unknown);
    }
    assert_eq!(
        FatalWriteStopReason::stats(FatalWriteStopReason::NONE),
        crate::stats::FatalWriteStopStats::default()
    );
}

#[test]
fn snapshot_is_rejected_by_a_different_database_lineage() {
    let first = Db::open_sync(DbOptions::memory()).expect("open first db");
    let second = Db::open_sync(DbOptions::memory()).expect("open second db");
    first.put_sync(b"key", b"first").expect("write first db");
    second.put_sync(b"key", b"second").expect("write second db");
    let foreign = first.snapshot();

    assert!(matches!(
        second.get_at_sync(&foreign, b"key"),
        Err(Error::SnapshotDatabaseMismatch)
    ));
    let bucket = second.default_bucket_sync().expect("default bucket");
    assert!(matches!(
        bucket.range_at_sync(&foreign, &KeyRange::all()),
        Err(Error::SnapshotDatabaseMismatch)
    ));
}
use crate::{TransactionOptions, WriteBatch};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

#[test]
fn open_rejects_configuration_that_storage_formats_cannot_encode() {
    assert!(matches!(
        Db::open_sync(
            DbOptions::memory().with_max_key_bytes(
                DbOptions::MAX_KEY_BYTES
                    .checked_add(1)
                    .expect("test key bound increments"),
            )
        ),
        Err(Error::InvalidOptions { .. })
    ));
    assert!(matches!(
        Db::open_sync(
            DbOptions::memory().with_max_value_bytes(
                DbOptions::MAX_WRITE_FIELD_BYTES
                    .checked_add(1)
                    .expect("test value bound increments"),
            )
        ),
        Err(Error::InvalidOptions { .. })
    ));
    let oversized_block = BucketOptions {
        block_bytes: crate::limits::MAX_DECODED_BLOCK_BYTES + 1,
        ..BucketOptions::default()
    };
    assert!(matches!(
        Db::open_sync(DbOptions::memory().with_default_bucket_options(oversized_block)),
        Err(Error::InvalidOptions { .. })
    ));
}

#[test]
fn public_bucket_apis_reject_the_internal_namespace() {
    let db = Db::open_sync(DbOptions::memory()).expect("open memory database");
    let reserved = format!("{}test", crate::bucket::INTERNAL_BUCKET_PREFIX);

    assert!(matches!(
        db.bucket_sync(reserved.as_str()),
        Err(Error::InvalidOptions { .. })
    ));
    assert!(matches!(
        db.drop_bucket_sync(reserved.as_str()),
        Err(Error::InvalidOptions { .. })
    ));
    assert!(matches!(
        db.create_checkpoint_sync(reserved.as_str()),
        Err(Error::InvalidOptions { .. })
    ));

    let mut batch = WriteBatch::new();
    assert!(matches!(
        batch.put_bucket(&reserved, b"k", b"v".to_vec()),
        Err(Error::InvalidOptions { .. })
    ));

    let mut transaction = db.transaction(TransactionOptions::default());
    assert!(matches!(
        transaction.get_bucket_sync(&reserved, b"k"),
        Err(Error::InvalidOptions { .. })
    ));
    assert!(matches!(
        transaction.put_bucket(&reserved, b"k", b"v".to_vec()),
        Err(Error::InvalidOptions { .. })
    ));
    assert!(matches!(
        transaction.delete_bucket(&reserved, b"k"),
        Err(Error::InvalidOptions { .. })
    ));

    let mut branch = db.branch_from_latest().expect("ephemeral branch opens");
    assert!(matches!(
        branch.put(reserved.as_str(), b"k", b"v".to_vec()),
        Err(Error::InvalidOptions { .. })
    ));
}

#[test]
fn dropped_bucket_handle_cannot_cross_into_recreated_generation() {
    let db = Db::open_sync(DbOptions::memory()).expect("open db");
    let stale = db.bucket_sync("scratch").expect("create first generation");
    stale
        .put_sync(b"old", b"value")
        .expect("write first generation");
    db.drop_bucket_sync("scratch")
        .expect("drop first generation");
    let current = db.bucket_sync("scratch").expect("create second generation");
    current
        .put_sync(b"new", b"value")
        .expect("write second generation");

    assert!(matches!(
        stale.get_sync(b"new"),
        Err(Error::BucketStale { .. })
    ));
    assert!(matches!(
        stale.put_sync(b"cross-generation", b"blocked"),
        Err(Error::BucketStale { .. })
    ));
    assert_eq!(
        current
            .get_sync(b"cross-generation")
            .expect("read current generation"),
        None
    );
}

#[test]
fn object_store_prefix_isolates_databases_in_one_bucket() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());

    // Two databases sharing one object store under different key prefixes.
    let a = block_on_test_future(Db::open_object_store_at(
        Arc::clone(&client),
        "app/a",
        DbOptions::object_store(),
    ))
    .expect("open a");
    let b = block_on_test_future(Db::open_object_store_at(
        Arc::clone(&client),
        "app/b",
        DbOptions::object_store(),
    ))
    .expect("open b");
    a.put_sync(b"k", b"from-a").expect("put a");
    b.put_sync(b"k", b"from-b").expect("put b");
    block_on_test_future(a.flush()).expect("flush a");
    block_on_test_future(b.flush()).expect("flush b");
    drop(a);
    drop(b);

    // Reopen each by prefix: fully isolated, each sees only its own value.
    let a = block_on_test_future(Db::open_object_store_at(
        Arc::clone(&client),
        "app/a",
        DbOptions::object_store(),
    ))
    .expect("reopen a");
    let b = block_on_test_future(Db::open_object_store_at(
        client,
        "app/b",
        DbOptions::object_store(),
    ))
    .expect("reopen b");
    assert_eq!(
        a.get_sync(b"k").expect("get a").as_deref(),
        Some(b"from-a".as_slice())
    );
    assert_eq!(
        b.get_sync(b"k").expect("get b").as_deref(),
        Some(b"from-b".as_slice())
    );
}

#[test]
fn object_store_prefix_must_fit_every_durable_lease_record() {
    let client = Arc::new(crate::InMemoryObjectStore::new());
    let prefix = "x".repeat(70 * 1024);
    let error = block_on_test_future(Db::open_object_store_at(
        client,
        prefix,
        DbOptions::object_store(),
    ))
    .expect_err("oversized prefix must fail before writing a lease");
    assert!(matches!(error, Error::InvalidOptions { .. }));
}

#[test]
fn object_store_rejects_live_second_writer() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());

    // A opens the prefix (fencing epoch 1) and is the sole writer.
    let a = block_on_test_future(Db::open_object_store_at(
        Arc::clone(&client),
        "db",
        DbOptions::object_store(),
    ))
    .expect("open a");
    a.put_sync(b"k", b"a1").expect("put a");
    block_on_test_future(a.flush()).expect("flush a");

    // B cannot take over the SAME prefix while A's lease is live.
    let error = block_on_test_future(Db::open_object_store_at(
        Arc::clone(&client),
        "db",
        DbOptions::object_store(),
    ))
    .expect_err("open b while A is live");
    assert!(
        matches!(error, Error::LeaseUnavailable { .. }),
        "expected LeaseUnavailable, got {error:?}"
    );

    // A remains the legitimate owner and can keep writing.
    a.put_sync(b"k", b"a2").expect("put a again");
    block_on_test_future(a.flush()).expect("A flushes as the owner");
}

#[test]
fn object_store_database_persists_across_reopen() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());

    {
        let db = block_on_test_future(Db::open_object_store(
            Arc::clone(&client),
            DbOptions::object_store(),
        ))
        .expect("open object-store database");
        db.put_sync(b"alpha", b"one").expect("put alpha");
        db.put_sync(b"beta", b"two").expect("put beta");
        assert_eq!(
            db.get_sync(b"alpha").expect("get alpha").as_deref(),
            Some(b"one".as_slice())
        );
        // A non-default bucket created post-open (manifest CAS create).
        let docs = block_on_test_future(db.bucket_with_options("docs", BucketOptions::default()))
            .expect("create docs bucket");
        docs.put_sync(b"title", b"trine").expect("put into docs");
    }

    // Reopen from the same object store. Durable writes recover from the
    // manifest plus the remote WAL head even without a flush.
    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("reopen object-store database");
    assert_eq!(
        db.get_sync(b"alpha")
            .expect("get alpha after reopen")
            .as_deref(),
        Some(b"one".as_slice())
    );
    assert_eq!(
        db.get_sync(b"beta")
            .expect("get beta after reopen")
            .as_deref(),
        Some(b"two".as_slice())
    );
    let docs = block_on_test_future(db.bucket_with_options("docs", BucketOptions::default()))
        .expect("reopen docs bucket");
    assert_eq!(
        docs.get_sync(b"title").expect("get docs title").as_deref(),
        Some(b"trine".as_slice())
    );
}

#[test]
fn object_store_open_rejects_file_sync_durability() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    for durability in [
        DurabilityMode::SyncData,
        DurabilityMode::SyncAll,
        DurabilityMode::SyncAllStrict,
    ] {
        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let error = block_on_test_future(Db::open_object_store(
            client,
            DbOptions::object_store().with_durability(durability),
        ))
        .expect_err("object-store open rejects file sync durability");
        assert!(
            matches!(error, Error::UnsupportedDurability { requested } if requested == durability),
            "expected UnsupportedDurability({durability:?}), got {error:?}"
        );
    }
}

#[test]
fn object_client_health_check_rejects_unsafe_put_if_client() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let inner: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let client: Arc<dyn ObjectClient> = Arc::new(UnsafePutIfObjectStore { inner });
    let error = block_on_test_future(verify_object_client_contract(client, "health"))
        .expect_err("unsafe object client must be rejected by health check");

    assert!(
        matches!(error, Error::Corruption { ref message } if message.contains("contract probe")),
        "expected contract probe corruption, got {error:?}"
    );
}

#[test]
fn object_store_open_trusts_object_client_by_default() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let inner: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let client: Arc<dyn ObjectClient> = Arc::new(UnsafePutIfObjectStore { inner });
    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("trusted open does not run the object-client health check");

    assert_eq!(
        db.options().object_client_trust,
        ObjectClientTrustMode::Trusted
    );
}

#[test]
fn object_store_verify_on_open_rejects_unsafe_put_if_client() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let inner: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let client: Arc<dyn ObjectClient> = Arc::new(UnsafePutIfObjectStore { inner });
    let error = block_on_test_future(Db::open_object_store(
        client,
        DbOptions::object_store().with_object_client_trust(ObjectClientTrustMode::VerifyOnOpen),
    ))
    .expect_err("verify-on-open must reject unsafe object clients");

    assert!(
        matches!(error, Error::Corruption { ref message } if message.contains("contract probe")),
        "expected contract probe corruption, got {error:?}"
    );
}

#[test]
fn object_store_buffered_write_is_recovered_after_explicit_persist() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());

    {
        let db = block_on_test_future(Db::open_object_store(
            Arc::clone(&client),
            DbOptions::object_store().with_durability(DurabilityMode::Buffered),
        ))
        .expect("open object-store database");
        db.put_sync(b"k", b"buffered").expect("buffered put");
        assert_eq!(
            db.get_sync(b"k").expect("in-process read").as_deref(),
            Some(b"buffered".as_slice())
        );
        block_on_test_future(db.persist(DurabilityMode::Flush))
            .expect("explicit persist flushes buffered remote WAL");
    }

    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("reopen object-store database");
    assert_eq!(
        db.get_sync(b"k").expect("reopen read").as_deref(),
        Some(b"buffered".as_slice())
    );
}

#[test]
fn object_store_concurrent_commits_publish_one_contiguous_wal_order() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    const WRITERS: u32 = 24;
    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let db = Arc::new(
        block_on_test_future(Db::open_object_store(
            Arc::clone(&client),
            DbOptions::object_store(),
        ))
        .expect("open object-store database"),
    );

    let mut writers = Vec::new();
    for index in 0..WRITERS {
        let db = Arc::clone(&db);
        writers.push(std::thread::spawn(move || {
            let key = format!("concurrent-{index:02}");
            let value = index.to_le_bytes();
            db.put_sync(key.as_bytes(), value)
        }));
    }
    for writer in writers {
        writer
            .join()
            .expect("writer thread does not panic")
            .expect("concurrent object-store commit succeeds");
    }

    let lease = block_on_test_future(ObjectWriterLease::read_current(Arc::clone(&client), "LOCK"))
        .expect("read WAL head")
        .expect("writer lease exists");
    assert_eq!(lease.committed_sequence, Sequence::new(u64::from(WRITERS)));
    let batches = block_on_test_future(
        crate::substrate::object_store_wal_batches_after_replay_floor(
            Arc::clone(&client),
            std::path::Path::new(""),
            &lease,
            Sequence::ZERO,
        ),
    )
    .expect("replay concurrent WAL chain");
    assert_eq!(batches.len(), WRITERS as usize);
    assert!(
        batches
            .windows(2)
            .all(|pair| pair[0].sequence.next() == Some(pair[1].sequence)),
        "remote WAL sequences must be strictly contiguous"
    );

    drop(db);
    let reopened = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("reopen object-store database");
    for index in 0..WRITERS {
        let key = format!("concurrent-{index:02}");
        assert_eq!(
            reopened
                .get_sync(key.as_bytes())
                .expect("read concurrent value")
                .as_deref(),
            Some(index.to_le_bytes().as_slice())
        );
    }
}

#[test]
fn object_store_wal_head_points_to_an_immutable_segment_chain() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let db = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store(),
    ))
    .expect("open object-store database");
    db.put_sync(b"a", b"one").expect("put a");
    db.put_sync(b"b", b"two").expect("put b");
    db.put_sync(b"c", b"three").expect("put c");

    let state = block_on_test_future(ObjectWriterLease::read_current(Arc::clone(&client), "LOCK"))
        .expect("read WAL head")
        .expect("WAL head exists");
    let batches = block_on_test_future(
        crate::substrate::object_store_wal_batches_after_replay_floor(
            Arc::clone(&client),
            std::path::Path::new(""),
            &state,
            Sequence::ZERO,
        ),
    )
    .expect("decode WAL chain");
    assert_eq!(batches.len(), 3);
    assert_eq!(state.committed_sequence, Sequence::new(3));
    let objects = block_on_test_future(client.list("")).expect("list objects");
    let wal_objects = objects
        .iter()
        .filter(|meta| crate::is_wal_object_key(&meta.key))
        .collect::<Vec<_>>();
    assert_eq!(
        wal_objects.len(),
        3,
        "each acknowledged commit has one immutable WAL segment"
    );
    assert!(
        wal_objects.iter().all(|meta| meta.size < 1_024),
        "new commits must not copy the complete preceding WAL history"
    );

    block_on_test_future(db.flush()).expect("flush");
    let state = block_on_test_future(ObjectWriterLease::read_current(client, "LOCK"))
        .expect("read WAL head after flush")
        .expect("WAL head exists after flush");
    assert_eq!(state.current_wal_key, None);
    assert_eq!(state.committed_sequence, Sequence::new(3));
}

#[test]
fn object_store_recovery_ignores_frames_beyond_remote_wal_head() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    {
        let db = block_on_test_future(Db::open_object_store(
            Arc::clone(&client),
            DbOptions::object_store(),
        ))
        .expect("open object-store database");
        db.put_sync(b"committed", b"yes").expect("put committed");

        let state =
            block_on_test_future(ObjectWriterLease::read_current(Arc::clone(&client), "LOCK"))
                .expect("read WAL head")
                .expect("WAL head exists");
        assert_eq!(state.committed_sequence, Sequence::new(1));
        let segment_key = state.current_wal_key.expect("segment key");
        let mut segment = block_on_test_future(client.get(&segment_key))
            .expect("read WAL segment")
            .expect("segment exists")
            .to_vec();
        let uncommitted = crate::wal::encode_batch_frame(
            Sequence::new(2),
            &[BatchOperation::Put {
                bucket: DEFAULT_BUCKET_NAME.to_owned(),
                key: b"uncommitted".to_vec(),
                value: b"no".to_vec(),
            }],
        )
        .expect("encode uncommitted frame");
        segment.extend_from_slice(&uncommitted);
        let digest = Sha256::digest(&segment);
        let mut identity = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(&mut identity, "{byte:02x}").expect("writing to String cannot fail");
        }
        let unreferenced_key = crate::wal::object_wal_commit_path(
            std::path::Path::new(""),
            state.epoch,
            Sequence::new(2),
            &identity,
        );
        block_on_test_future(client.put(
            unreferenced_key.to_str().expect("WAL key utf8"),
            Arc::from(segment.as_slice()),
        ))
        .expect("write segment before head advances");
    }

    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("reopen object-store database");
    assert_eq!(
        db.get_sync(b"committed")
            .expect("read committed")
            .as_deref(),
        Some(b"yes".as_slice())
    );
    assert_eq!(db.get_sync(b"uncommitted").expect("read uncommitted"), None);
    assert_eq!(
        db.latest_read_version(),
        ReadVersion::from_sequence(Sequence::new(1))
    );
}

#[test]
fn object_store_recovery_rejects_truncated_confirmed_wal_segment() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    {
        let db = block_on_test_future(Db::open_object_store(
            Arc::clone(&client),
            DbOptions::object_store(),
        ))
        .expect("open object-store database");
        db.put_sync(b"a", b"one").expect("put a");
        db.put_sync(b"b", b"two").expect("put b");

        let state =
            block_on_test_future(ObjectWriterLease::read_current(Arc::clone(&client), "LOCK"))
                .expect("read WAL head")
                .expect("WAL head exists");
        assert_eq!(state.committed_sequence, Sequence::new(2));
        let segment_key = state.current_wal_key.expect("segment key");
        let mut segment = block_on_test_future(client.get(&segment_key))
            .expect("read WAL segment")
            .expect("segment exists")
            .to_vec();
        segment.pop();
        block_on_test_future(client.put(&segment_key, Arc::from(segment.as_slice())))
            .expect("truncate confirmed WAL segment");
    }

    let error = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect_err("truncated confirmed WAL segment must fail closed");
    assert!(
        matches!(error, Error::Corruption { ref message } if message.contains("content identity mismatch")),
        "expected WAL content-identity corruption, got {error:?}"
    );
}

#[test]
fn object_store_split_wal_tier_recovers_unflushed_commits() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let storage_client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let wal_client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());

    {
        let db = block_on_test_future(Db::open_object_store_with_wal(
            Arc::clone(&storage_client),
            Arc::clone(&wal_client),
            DbOptions::object_store(),
        ))
        .expect("open split-tier object-store database");
        db.put_sync(b"k", b"from-wal-tier")
            .expect("confirmed write");
        assert_eq!(
            db.get_sync(b"k").expect("in-process read").as_deref(),
            Some(b"from-wal-tier".as_slice())
        );
    }

    let storage_objects = block_on_test_future(storage_client.list("")).expect("list storage");
    assert!(
        storage_objects
            .iter()
            .all(|meta| !crate::is_wal_object_key(&meta.key) && meta.key != "LOCK"),
        "storage tier must not receive WAL tier objects"
    );
    let wal_objects = block_on_test_future(wal_client.list("")).expect("list WAL tier");
    assert!(
        wal_objects.iter().any(|meta| meta.key == "LOCK"),
        "WAL tier should hold the writer lease/head"
    );
    assert!(
        wal_objects
            .iter()
            .any(|meta| crate::is_wal_object_key(&meta.key)),
        "WAL tier should hold the remote WAL segment"
    );

    let db = block_on_test_future(Db::open_object_store_with_wal(
        storage_client,
        wal_client,
        DbOptions::object_store(),
    ))
    .expect("reopen split-tier object-store database");
    assert_eq!(
        db.get_sync(b"k").expect("recovered read").as_deref(),
        Some(b"from-wal-tier".as_slice())
    );
}

#[test]
fn object_store_split_wal_tier_refresh_reads_remote_wal() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let storage_client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let wal_client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let writer = block_on_test_future(Db::open_object_store_with_wal(
        Arc::clone(&storage_client),
        Arc::clone(&wal_client),
        DbOptions::object_store(),
    ))
    .expect("open split-tier writer");
    let reader = block_on_test_future(Db::open_object_store_with_wal(
        storage_client,
        wal_client,
        DbOptions::object_store().read_only(),
    ))
    .expect("open split-tier reader");

    writer
        .put_sync(b"k", b"visible-after-refresh")
        .expect("put");
    assert_eq!(reader.get_sync(b"k").expect("stale reader"), None);

    let refreshed = block_on_test_future(reader.refresh_object_store()).expect("refresh");
    assert_eq!(refreshed, ReadVersion::from_sequence(Sequence::new(1)));
    assert_eq!(
        reader.get_sync(b"k").expect("refreshed read").as_deref(),
        Some(b"visible-after-refresh".as_slice())
    );
}

#[test]
fn object_store_read_only_refresh_replays_remote_wal_segment() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let writer = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store(),
    ))
    .expect("open writer");
    let reader = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store().read_only(),
    ))
    .expect("open reader");

    writer.put_sync(b"k", b"wal").expect("writer put");
    assert_eq!(reader.get_sync(b"k").expect("stale reader"), None);

    let refreshed = block_on_test_future(reader.refresh_object_store()).expect("refresh");
    assert_eq!(refreshed, ReadVersion::from_sequence(Sequence::new(1)));
    assert_eq!(
        reader.get_sync(b"k").expect("refreshed reader").as_deref(),
        Some(b"wal".as_slice())
    );
}

#[test]
fn object_store_read_only_refresh_reloads_manifest_after_flush() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let writer = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store(),
    ))
    .expect("open writer");
    let reader = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store().read_only(),
    ))
    .expect("open reader");

    writer.put_sync(b"k", b"table").expect("writer put");
    block_on_test_future(writer.flush()).expect("writer flush");

    let refreshed = block_on_test_future(reader.refresh_object_store()).expect("refresh");
    assert_eq!(refreshed, ReadVersion::from_sequence(Sequence::new(1)));
    assert_eq!(
        block_on_test_future(reader.get(b"k"))
            .expect("refreshed async read")
            .as_deref(),
        Some(b"table".as_slice())
    );
}

#[test]
fn object_store_refresh_updates_existing_named_bucket_handles() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let writer = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store(),
    ))
    .expect("open writer");
    let writer_bucket = block_on_test_future(writer.bucket("docs")).expect("create docs");

    let reader = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store().read_only(),
    ))
    .expect("open reader");
    let retained = block_on_test_future(reader.bucket("docs")).expect("open retained handle");

    block_on_test_future(writer_bucket.put(b"title", b"fresh")).expect("write docs");
    block_on_test_future(writer.flush()).expect("flush docs");
    block_on_test_future(reader.refresh_object_store()).expect("refresh reader");

    assert_eq!(
        block_on_test_future(retained.get(b"title"))
            .expect("retained handle follows refreshed registry")
            .as_deref(),
        Some(b"fresh".as_slice())
    );
}

#[test]
fn object_store_refresh_rejects_handle_from_dropped_bucket_generation() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let writer = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store(),
    ))
    .expect("open writer");
    let first = block_on_test_future(writer.bucket("scratch")).expect("create first generation");
    first
        .put_sync(b"old", b"value")
        .expect("write first generation");
    block_on_test_future(writer.flush()).expect("flush first generation");

    let reader = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store().read_only(),
    ))
    .expect("open reader");
    let retained = block_on_test_future(reader.bucket("scratch")).expect("retain first generation");

    block_on_test_future(writer.drop_bucket("scratch")).expect("drop first generation");
    let second = block_on_test_future(writer.bucket("scratch")).expect("create second generation");
    second
        .put_sync(b"new", b"value")
        .expect("write second generation");
    block_on_test_future(writer.flush()).expect("flush second generation");
    block_on_test_future(reader.refresh_object_store()).expect("refresh reader");

    assert!(matches!(
        block_on_test_future(retained.get(b"new")),
        Err(Error::BucketStale { .. })
    ));
}

#[test]
fn object_store_refresh_requires_read_only_object_store_handle() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let writer = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("open writer");

    assert!(matches!(
        block_on_test_future(writer.refresh_object_store()),
        Err(Error::InvalidOptions { .. })
    ));
    let memory = Db::open_sync(DbOptions::memory()).expect("open memory");
    assert!(matches!(
        block_on_test_future(memory.refresh_object_store()),
        Err(Error::InvalidOptions { .. })
    ));
}

#[test]
fn object_store_orphans_are_retained_without_reader_retirement_proof() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let db = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store(),
    ))
    .expect("open object-store database");
    db.put_sync(b"k", b"v").expect("put");
    block_on_test_future(db.flush()).expect("flush (writes a referenced table object)");

    // Plant an orphan: a table object no manifest references, as a failed
    // flush would leave behind.
    let orphan_key =
        crate::table::table_path(std::path::Path::new(""), crate::table::TableId(999_999))
            .to_string_lossy()
            .into_owned();
    block_on_test_future(client.put(&orphan_key, Arc::from(b"junk".as_slice())))
        .expect("plant orphan");
    assert!(
        block_on_test_future(client.head(&orphan_key))
            .unwrap()
            .is_some()
    );

    let deleted = block_on_test_future(db.cleanup_object_store_orphans_async()).expect("gc");
    assert_eq!(deleted, 0, "unsafe remote reclamation remains disabled");
    assert!(
        block_on_test_future(client.head(&orphan_key))
            .unwrap()
            .is_some(),
        "orphan is retained while remote reader lifetime is unknowable"
    );

    // The referenced table object survives: reopen and read it back.
    drop(db);
    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("reopen");
    assert_eq!(
        db.get_sync(b"k").expect("get after gc").as_deref(),
        Some(b"v".as_slice())
    );
}

#[test]
fn drop_bucket_is_logical_and_retains_remote_objects_safely() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let db = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store(),
    ))
    .expect("open object-store database");

    // A "scratch" bucket with a flushed table object, plus a "keep" bucket.
    let scratch = block_on_test_future(db.bucket("scratch")).expect("scratch");
    scratch.put_sync(b"k", b"v").expect("put scratch");
    let keep = block_on_test_future(db.bucket("keep")).expect("keep");
    keep.put_sync(b"k", b"keep").expect("put keep");
    block_on_test_future(db.flush()).expect("flush");
    drop(scratch);
    drop(keep);

    block_on_test_future(db.drop_bucket("scratch")).expect("drop scratch");

    // Logical deletion is immediate, while physical immutable objects remain
    // until a durable reader-retirement protocol can prove deletion safe.
    assert_eq!(
        block_on_test_future(db.cleanup_object_store_orphans_async()).expect("gc"),
        0,
        "remote reclamation is intentionally conservative"
    );

    // scratch reopens empty; keep survives, including across a reopen.
    assert_eq!(
        block_on_test_future(db.bucket("scratch"))
            .expect("scratch")
            .get_sync(b"k")
            .expect("get"),
        None
    );
    drop(db);
    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("reopen");
    assert_eq!(
        block_on_test_future(db.bucket("scratch"))
            .expect("scratch")
            .get_sync(b"k")
            .expect("get"),
        None,
        "dropped bucket stays gone across reopen"
    );
    assert_eq!(
        block_on_test_future(db.bucket("keep"))
            .expect("keep")
            .get_sync(b"k")
            .expect("get")
            .as_deref(),
        Some(b"keep".as_slice()),
        "an untouched bucket survives the drop"
    );
}

#[test]
fn object_store_compaction_merges_tables_and_retains_old_objects_safely() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let db = block_on_test_future(Db::open_object_store(
        Arc::clone(&client),
        DbOptions::object_store(),
    ))
    .expect("open object-store database");

    // Three flushed tables, including an overwrite of `a`.
    db.put_sync(b"a", b"1").expect("put a");
    block_on_test_future(db.flush()).expect("flush 1");
    db.put_sync(b"b", b"2").expect("put b");
    block_on_test_future(db.flush()).expect("flush 2");
    db.put_sync(b"a", b"1b").expect("overwrite a");
    block_on_test_future(db.flush()).expect("flush 3");

    // Compact; obsolete immutable inputs remain safe for older readers.
    block_on_test_future(db.compact_range(KeyRange::all())).expect("compact");
    assert_eq!(
        db.get_sync(b"a").expect("get a").as_deref(),
        Some(b"1b".as_slice()),
        "newest value wins after compaction"
    );
    assert_eq!(
        db.get_sync(b"b").expect("get b").as_deref(),
        Some(b"2".as_slice())
    );
    block_on_test_future(db.run_maintenance_with_budget(MaintenanceBudget::unbounded()))
        .expect("maintenance completes with conservative retention");

    // Reopen: the compacted tables are durable and reads are still correct.
    drop(db);
    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("reopen");
    assert_eq!(
        db.get_sync(b"a").expect("get a after reopen").as_deref(),
        Some(b"1b".as_slice())
    );
    assert_eq!(
        db.get_sync(b"b").expect("get b after reopen").as_deref(),
        Some(b"2".as_slice())
    );
}

#[test]
fn object_store_flush_future_is_send() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    fn assert_send<T: Send>(_: T) {}

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("open object-store database");
    // Compile-time guarantee: the object-store flush future is Send, so it can
    // be spawned on a multi-threaded executor (the manifest CAS no longer holds
    // the std mutex across the await).
    assert_send(db.flush());
    assert_send(db.bucket_with_options("docs", BucketOptions::default()));
    assert_send(db.create_checkpoint("cp"));
    assert_send(db.delete_checkpoint("cp"));
    assert_send(db.compact_range(KeyRange::all()));
    assert_send(db.run_maintenance_with_budget(MaintenanceBudget::single_unit()));
}

#[test]
fn object_store_flush_is_rejected_synchronously() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("open object-store database");
    assert!(
        db.flush_sync().is_err(),
        "object-store flush must require the async API"
    );
}

#[test]
fn object_store_durable_manifest_install_failure_closes_handle() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let db = block_on_test_future(Db::open_object_store(client, DbOptions::object_store()))
        .expect("open object-store database");

    let error = block_on_test_future(async {
        let sequence = db.last_committed_sequence();
        let (mut object, _serialize) = db.checkout_object_manifest().await?;
        object
            .create_checkpoint("durable-only".to_owned(), sequence)
            .await?;

        let manifest = db
            .inner
            .manifest
            .as_ref()
            .expect("object-store database has manifest");
        std::thread::scope(|scope| {
            let handle = scope.spawn(|| {
                let _guard = manifest.lock().expect("manifest lock before poison");
                panic!("poison manifest store after durable publish");
            });
            assert!(handle.join().is_err());
        });

        db.install_object_manifest_after_durable_publish("checkpoint creation", object)
    })
    .expect_err("poisoned install after durable publish must fail");

    assert!(
        matches!(error, Error::Corruption { ref message }
            if message.contains("checkpoint creation published durable state")
                && message.contains("database handle closed")),
        "expected durable-publish corruption with close guidance, got {error:?}"
    );
    assert!(db.closed_after_durable_publish_error());
    assert!(
        matches!(db.put_sync(b"k", b"v"), Err(Error::Closed)),
        "closed handle must reject later writes"
    );
}

#[test]
fn object_store_maintenance_budget_reports_flush_exhaustion_before_compaction() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let mut options = DbOptions::object_store();
    options.background_worker_count = 0;
    options.write_buffer_bytes = 1;
    let db = block_on_test_future(Db::open_object_store(client, options))
        .expect("open object-store database");

    db.put_sync(b"a", b"one").expect("write first immutable");
    db.put_sync(b"b", b"two").expect("write second immutable");

    let outcome =
        block_on_test_future(db.run_maintenance_with_budget(MaintenanceBudget::single_unit()))
            .expect("run budgeted maintenance");

    assert_eq!(outcome.flushes, 1);
    assert_eq!(outcome.compactions, 0);
    assert!(outcome.budget_exhausted());
    assert!(!outcome.busy());
}

#[test]
fn object_store_compaction_busy_is_reported_as_runtime_busy() {
    use crate::object_store::{InMemoryObjectStore, ObjectClient};

    let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
    let mut options = DbOptions::object_store();
    options.background_worker_count = 0;
    let db = block_on_test_future(Db::open_object_store(client, options))
        .expect("open object-store database");

    db.put_sync(b"a", b"one").expect("write first table");
    block_on_test_future(db.flush()).expect("flush first table");
    db.put_sync(b"b", b"two").expect("write second table");
    block_on_test_future(db.flush()).expect("flush second table");

    let _guard = db
        .inner
        .maintenance
        .reserve_compactions(vec![CompactionReservation {
            bucket: DEFAULT_BUCKET_NAME.to_owned(),
            range: KeyRange::all(),
        }])
        .expect("test reserves compaction range");

    let error = block_on_test_future(db.compact_range(KeyRange::all()))
        .expect_err("overlapping object-store compaction must be busy");
    assert!(
        matches!(error, Error::RuntimeBusy { ref message }
            if message == "object-store compaction is already active"),
        "expected RuntimeBusy, got {error:?}"
    );
}
