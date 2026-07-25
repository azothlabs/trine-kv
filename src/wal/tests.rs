#[test]
fn classifies_wal_objects_by_final_segment() {
    use super::is_wal_object_key;
    // WAL objects under any database key prefix.
    assert!(is_wal_object_key("trine.wal"));
    assert!(is_wal_object_key("proj/db/trine.wal"));
    assert!(is_wal_object_key("proj/db/trine.wal.shard-0003"));
    assert!(is_wal_object_key("proj/db/trine.wal.tmp"));
    // Non-WAL objects: SSTables, manifest, lease.
    assert!(!is_wal_object_key("proj/db/000123.sst"));
    assert!(!is_wal_object_key("proj/db/MANIFEST"));
    assert!(!is_wal_object_key("proj/db/LEASE"));
}

use std::{
    fs,
    future::Future,
    path::Path,
    task::{Context, Poll, Waker},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    limits, options::DurabilityMode, storage::NativeFileBackend, types::Sequence,
    write_batch::BatchOperation,
};

use super::{
    DEFAULT_WAL_SHARD_COUNT, WAL_FILE_NAME, WAL_FORMAT_VERSION, WAL_FRONT_DOOR_QUEUE_CAPACITY,
    WAL_MAGIC, WalFrontDoor, append_batch_with_backend_async, checksum, decode_frames_after,
    decode_payload, discover_wal_paths_with_backend, discover_wal_paths_with_backend_async,
    merge_batch_streams_by_sequence, object_wal_sequence_from_path, read_all_batches,
    read_batches_after_with_backend, read_batches_after_with_backend_async,
    read_recovery_streams_after_with_backend_async, rewrite_batches_after_with_backend_async,
    wal_rewrite_tmp_path, wal_shard_path,
};

#[test]
fn object_wal_paths_require_content_identity() {
    let path =
        Path::new("trine.wal.epoch-00000000000000000001.commit-00000000000000000002.trinewal");
    assert!(matches!(
        object_wal_sequence_from_path(path),
        Err(crate::Error::Corruption { .. })
    ));
}

#[test]
fn wal_front_door_accepts_whole_commit_record() {
    let dir = temp_dir("front-door-accept");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let path = dir.join(WAL_FILE_NAME);
    let backend = NativeFileBackend::new();
    let front_door =
        WalFrontDoor::open_single_lane_with_backend(&backend, &path).expect("front door opens");
    let operations = vec![BatchOperation::Put {
        bucket: "default".to_owned(),
        key: b"a".to_vec(),
        value: b"a1".to_vec(),
    }];

    let accepted = front_door
        .accept_commit(Sequence::new(7), &operations, DurabilityMode::Flush)
        .expect("front door accepts commit");

    assert_eq!(accepted.sequence(), Sequence::new(7));
    let stats = front_door.stats();
    assert_eq!(stats.queue_capacity, WAL_FRONT_DOOR_QUEUE_CAPACITY);
    assert_eq!(stats.open_shards, 1);
    assert_eq!(stats.records_accepted, 1);
    let batches =
        read_batches_after_with_backend(&backend, &path, Sequence::ZERO).expect("WAL reads");
    assert_eq!(
        batches,
        vec![super::WalBatch {
            sequence: Sequence::new(7),
            operations,
        }]
    );
    cleanup_dir(&dir);
}

#[test]
fn wal_front_door_rewrite_reopens_append_lane() {
    let dir = temp_dir("front-door-rewrite");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let path = dir.join(WAL_FILE_NAME);
    let backend = NativeFileBackend::new();
    let front_door =
        WalFrontDoor::open_single_lane_with_backend(&backend, &path).expect("front door opens");

    front_door
        .accept_commit(Sequence::new(1), &[put("a", "old")], DurabilityMode::Flush)
        .expect("first commit accepts");
    front_door
        .accept_commit(Sequence::new(2), &[put("b", "kept")], DurabilityMode::Flush)
        .expect("second commit accepts");
    front_door
        .rewrite_after_replay_floor(Sequence::new(1))
        .expect("front door rewrites WAL");
    front_door
        .accept_commit(Sequence::new(3), &[put("c", "new")], DurabilityMode::Flush)
        .expect("append lane still accepts after rewrite");

    let sequences = read_batches_after_with_backend(&backend, &path, Sequence::ZERO)
        .expect("WAL reads")
        .into_iter()
        .map(|batch| batch.sequence)
        .collect::<Vec<_>>();
    assert_eq!(sequences, vec![Sequence::new(2), Sequence::new(3)]);
    cleanup_dir(&dir);
}

#[test]
fn wal_front_door_routes_commits_across_shards() {
    let dir = temp_dir("front-door-shards");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let backend = NativeFileBackend::new();
    let front_door =
        WalFrontDoor::open_sharded_with_backend(&backend, &dir, DEFAULT_WAL_SHARD_COUNT)
            .expect("front door opens");
    assert_eq!(front_door.stats().open_shards, 0);
    assert_eq!(
        front_door.stats().queue_capacity,
        WAL_FRONT_DOOR_QUEUE_CAPACITY
    );

    let accepted = (1..=DEFAULT_WAL_SHARD_COUNT)
        .map(|sequence| {
            front_door
                .accept_commit(
                    Sequence::new(sequence as u64),
                    &[put("k", "v")],
                    DurabilityMode::Flush,
                )
                .expect("commit accepts")
        })
        .collect::<Vec<_>>();

    assert_eq!(
        accepted
            .iter()
            .map(|accepted| accepted.shard_index())
            .collect::<Vec<_>>(),
        (0..DEFAULT_WAL_SHARD_COUNT).collect::<Vec<_>>()
    );
    assert_eq!(front_door.stats().open_shards, DEFAULT_WAL_SHARD_COUNT);
    let batches = read_all_batches(&dir).expect("all WAL batches read");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>(),
        (1..=DEFAULT_WAL_SHARD_COUNT)
            .map(|sequence| Sequence::new(sequence as u64))
            .collect::<Vec<_>>()
    );
    cleanup_dir(&dir);
}

#[test]
fn wal_front_door_persists_only_dirty_shards() {
    let dir = temp_dir("front-door-dirty-shards");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let backend = NativeFileBackend::new();
    let front_door =
        WalFrontDoor::open_sharded_with_backend(&backend, &dir, DEFAULT_WAL_SHARD_COUNT)
            .expect("front door opens");

    for sequence in 1..=DEFAULT_WAL_SHARD_COUNT {
        front_door
            .accept_commit(
                Sequence::new(sequence as u64),
                &[put("k", "v")],
                DurabilityMode::Buffered,
            )
            .expect("commit accepts");
        front_door
            .persist(DurabilityMode::SyncData)
            .expect("dirty lane persists");
    }

    let after_first_round = backend.stats().operations.persist.requests;
    let expected_first_round =
        u64::try_from(DEFAULT_WAL_SHARD_COUNT).expect("shard count fits u64");
    assert_eq!(after_first_round, expected_first_round);

    front_door
        .persist(DurabilityMode::SyncData)
        .expect("clean lanes skip redundant persist");
    assert_eq!(
        backend.stats().operations.persist.requests,
        after_first_round
    );

    front_door
        .accept_commit(
            Sequence::new(expected_first_round + 1),
            &[put("k", "v")],
            DurabilityMode::Buffered,
        )
        .expect("next commit accepts");
    front_door
        .persist(DurabilityMode::SyncData)
        .expect("newly dirty lane persists");
    assert_eq!(
        backend.stats().operations.persist.requests,
        after_first_round + 1
    );
    cleanup_dir(&dir);
}

#[test]
fn wal_front_door_flush_persists_dirty_lane() {
    let dir = temp_dir("front-door-flush-persists");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let path = dir.join(WAL_FILE_NAME);
    let backend = NativeFileBackend::new();
    let front_door =
        WalFrontDoor::open_single_lane_with_backend(&backend, &path).expect("front door opens");

    front_door
        .accept_commit(Sequence::new(1), &[put("k", "v")], DurabilityMode::Flush)
        .expect("flush commit accepts");

    let after_commit = backend.stats().operations.persist.requests;
    assert!(
        after_commit >= 1,
        "Flush commits must request WAL backend persistence"
    );

    front_door
        .persist(DurabilityMode::Flush)
        .expect("clean flush persist succeeds");
    assert_eq!(backend.stats().operations.persist.requests, after_commit);
    cleanup_dir(&dir);
}

#[test]
fn wal_discovery_orders_legacy_and_shard_files() {
    let dir = temp_dir("wal-discovery");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let backend = NativeFileBackend::new();
    fs::write(wal_shard_path(&dir, 2), b"").expect("shard 2 writes");
    fs::write(wal_shard_path(&dir, 0), b"").expect("legacy WAL writes");
    fs::write(wal_shard_path(&dir, 1), b"").expect("shard 1 writes");

    let paths = discover_wal_paths_with_backend(&backend, &dir).expect("WAL paths discover");

    assert_eq!(
        paths,
        vec![
            wal_shard_path(&dir, 0),
            wal_shard_path(&dir, 1),
            wal_shard_path(&dir, 2),
        ]
    );
    cleanup_dir(&dir);
}

#[test]
fn async_wal_discovery_orders_legacy_and_shard_files() {
    let dir = temp_dir("async-wal-discovery");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let backend = NativeFileBackend::new();
    fs::write(wal_shard_path(&dir, 2), b"").expect("shard 2 writes");
    fs::write(wal_shard_path(&dir, 0), b"").expect("legacy WAL writes");
    fs::write(wal_shard_path(&dir, 1), b"").expect("shard 1 writes");

    let paths = poll_ready(discover_wal_paths_with_backend_async(&backend, &dir))
        .expect("WAL paths discover through async helper");

    assert_eq!(
        paths,
        vec![
            wal_shard_path(&dir, 0),
            wal_shard_path(&dir, 1),
            wal_shard_path(&dir, 2),
        ]
    );
    cleanup_dir(&dir);
}

#[test]
fn async_wal_batch_read_honors_replay_floor() {
    let dir = temp_dir("async-wal-read-floor");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let path = dir.join(WAL_FILE_NAME);
    let backend = NativeFileBackend::new();
    let front_door =
        WalFrontDoor::open_single_lane_with_backend(&backend, &path).expect("front door opens");

    front_door
        .accept_commit(Sequence::new(1), &[put("a", "old")], DurabilityMode::Flush)
        .expect("first commit accepts");
    front_door
        .accept_commit(Sequence::new(2), &[put("b", "new")], DurabilityMode::Flush)
        .expect("second commit accepts");

    let batches = poll_ready(read_batches_after_with_backend_async(
        &backend,
        &path,
        Sequence::new(1),
    ))
    .expect("WAL reads through async helper");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>(),
        vec![Sequence::new(2)]
    );
    cleanup_dir(&dir);
}

#[test]
fn async_wal_append_helper_writes_batch() {
    let dir = temp_dir("async-wal-append");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let path = dir.join(WAL_FILE_NAME);
    let backend = NativeFileBackend::new();

    poll_ready(append_batch_with_backend_async(
        &backend,
        &path,
        Sequence::new(1),
        &[put("k", "v")],
        DurabilityMode::Flush,
    ))
    .expect("async WAL append helper writes");

    let batches = read_batches_after_with_backend(&backend, &path, Sequence::ZERO)
        .expect("WAL reads after append");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].sequence, Sequence::new(1));
    cleanup_dir(&dir);
}

#[test]
fn async_wal_rewrite_helper_keeps_batches_after_floor() {
    let dir = temp_dir("async-wal-rewrite");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let path = dir.join(WAL_FILE_NAME);
    let backend = NativeFileBackend::new();

    poll_ready(append_batch_with_backend_async(
        &backend,
        &path,
        Sequence::new(1),
        &[put("a", "old")],
        DurabilityMode::Flush,
    ))
    .expect("first async WAL append writes");
    poll_ready(append_batch_with_backend_async(
        &backend,
        &path,
        Sequence::new(2),
        &[put("b", "new")],
        DurabilityMode::Flush,
    ))
    .expect("second async WAL append writes");

    poll_ready(rewrite_batches_after_with_backend_async(
        &backend,
        &path,
        Sequence::new(1),
    ))
    .expect("async WAL rewrite helper rewrites");

    let batches = read_batches_after_with_backend(&backend, &path, Sequence::ZERO)
        .expect("WAL reads after rewrite");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>(),
        vec![Sequence::new(2)]
    );
    cleanup_dir(&dir);
}

#[test]
fn async_wal_recovery_streams_read_shards() {
    let dir = temp_dir("async-wal-streams");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let backend = NativeFileBackend::new();
    let front_door =
        WalFrontDoor::open_sharded_with_backend(&backend, &dir, DEFAULT_WAL_SHARD_COUNT)
            .expect("front door opens");

    for sequence in 1..=DEFAULT_WAL_SHARD_COUNT {
        front_door
            .accept_commit(
                Sequence::new(sequence as u64),
                &[put("k", "v")],
                DurabilityMode::Flush,
            )
            .expect("commit accepts");
    }

    let streams = poll_ready(read_recovery_streams_after_with_backend_async(
        &backend,
        &dir,
        Sequence::ZERO,
    ))
    .expect("WAL streams read through async helper");
    let batches = merge_batch_streams_by_sequence(streams).expect("streams merge");
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.sequence)
            .collect::<Vec<_>>(),
        (1..=DEFAULT_WAL_SHARD_COUNT)
            .map(|sequence| Sequence::new(sequence as u64))
            .collect::<Vec<_>>()
    );
    cleanup_dir(&dir);
}

#[test]
fn wal_rewrite_temp_paths_keep_shard_identity() {
    let dir = temp_dir("wal-rewrite-temp-paths");
    let legacy_path = wal_shard_path(&dir, 0);
    let shard_path = wal_shard_path(&dir, 1);

    assert_eq!(
        wal_rewrite_tmp_path(&legacy_path),
        dir.join("trine.wal.tmp")
    );
    assert_eq!(
        wal_rewrite_tmp_path(&shard_path),
        dir.join("trine.wal.shard-0001.tmp")
    );
}

#[test]
fn wal_discovery_rejects_malformed_shard_file_name() {
    let dir = temp_dir("wal-discovery-malformed");
    fs::create_dir_all(&dir).expect("create WAL test dir");
    let backend = NativeFileBackend::new();
    fs::write(dir.join("trine.wal.shard-bad"), b"bad").expect("bad shard writes");

    let error =
        discover_wal_paths_with_backend(&backend, &dir).expect_err("malformed shard name fails");

    assert!(
        error.to_string().contains("malformed WAL shard file name"),
        "unexpected error: {error}"
    );
    cleanup_dir(&dir);
}

fn poll_ready<T>(future: impl Future<Output = crate::Result<T>>) -> crate::Result<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("WAL storage future unexpectedly pending"),
    }
}

#[test]
fn wal_stream_merge_orders_batches_across_sources() {
    let first = vec![batch(Sequence::new(1)), batch(Sequence::new(4))];
    let second = vec![batch(Sequence::new(2)), batch(Sequence::new(3))];

    let sequences = merge_batch_streams_by_sequence([first, second])
        .expect("streams merge")
        .into_iter()
        .map(|batch| batch.sequence)
        .collect::<Vec<_>>();

    assert_eq!(
        sequences,
        vec![
            Sequence::new(1),
            Sequence::new(2),
            Sequence::new(3),
            Sequence::new(4)
        ]
    );
}

#[test]
fn wal_stream_merge_rejects_duplicate_sequence() {
    let error = merge_batch_streams_by_sequence([
        vec![batch(Sequence::new(1)), batch(Sequence::new(3))],
        vec![batch(Sequence::new(2)), batch(Sequence::new(3))],
    ])
    .expect_err("duplicate sequence fails");

    assert!(
        error
            .to_string()
            .contains("duplicate WAL sequence across streams"),
        "unexpected error: {error}"
    );
}

#[test]
fn wal_stream_merge_rejects_non_increasing_source() {
    let error =
        merge_batch_streams_by_sequence([vec![batch(Sequence::new(2)), batch(Sequence::new(1))]])
            .expect_err("non-increasing source fails");

    assert!(
        error
            .to_string()
            .contains("WAL stream sequence did not increase"),
        "unexpected error: {error}"
    );
}

#[test]
fn wal_decode_rejects_operation_count_before_large_allocation() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1_u64.to_le_bytes());
    payload.extend_from_slice(&u32::MAX.to_le_bytes());

    let error = decode_payload(&payload).expect_err("oversized operation count fails");
    assert!(
        error
            .to_string()
            .contains("operation count exceeds payload bytes"),
        "unexpected error: {error}"
    );
}

#[test]
fn wal_decode_rejects_frame_payload_len_before_large_allocation() {
    let payload_len = u32::try_from(limits::MAX_WAL_FRAME_PAYLOAD_BYTES + 1)
        .expect("test payload length fits u32");
    let payload_checksum = 0_u32;
    let header_checksum = super::header_checksum(payload_len, payload_checksum);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&header_checksum.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());

    let error = decode_frames_after(&bytes, Sequence::ZERO)
        .expect_err("oversized frame payload should fail before allocation");

    assert!(error.to_string().contains("WAL payload length"));
}

#[test]
fn wal_decode_after_floor_skips_old_operation_payloads() {
    let mut old_payload = Vec::new();
    old_payload.extend_from_slice(&1_u64.to_le_bytes());
    old_payload.extend_from_slice(&u32::MAX.to_le_bytes());

    let new_payload = super::encode_payload(
        Sequence::new(2),
        &[BatchOperation::Put {
            bucket: "default".to_owned(),
            key: b"a".to_vec(),
            value: b"a1".to_vec(),
        }],
    )
    .expect("new payload encodes");

    let mut bytes = frame_for_payload(&old_payload);
    bytes.extend_from_slice(&frame_for_payload(&new_payload));

    let batches = decode_frames_after(&bytes, Sequence::new(1)).expect("old payload is skipped");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].sequence, Sequence::new(2));

    let error = decode_frames_after(&bytes, Sequence::ZERO)
        .expect_err("old payload is decoded without a replay floor");
    assert!(
        error
            .to_string()
            .contains("operation count exceeds payload bytes"),
        "unexpected error: {error}"
    );
}

fn frame_for_payload(payload: &[u8]) -> Vec<u8> {
    let payload_len = u32::try_from(payload.len()).expect("test payload fits u32");
    let payload_checksum = checksum(payload);
    let header_checksum = super::header_checksum(payload_len, payload_checksum);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&WAL_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload_len.to_le_bytes());
    bytes.extend_from_slice(&header_checksum.to_le_bytes());
    bytes.extend_from_slice(&payload_checksum.to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn put(key: &str, value: &str) -> BatchOperation {
    BatchOperation::Put {
        bucket: "default".to_owned(),
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

fn batch(sequence: Sequence) -> super::WalBatch {
    super::WalBatch {
        sequence,
        operations: Vec::new(),
    }
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "trine-kv-wal-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn cleanup_dir(dir: &std::path::Path) {
    match fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to cleanup {}: {error}", dir.display()),
    }
}
