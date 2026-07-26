use super::*;

#[test]
fn content_id_portable_bytes_round_trip_and_reject_unknown_algorithms() {
    let id = ContentId::for_bytes(b"portable content identity");
    assert_eq!(
        ContentId::from_bytes(id.to_bytes()).expect("identity decodes"),
        id
    );

    let mut unknown = id.to_bytes();
    unknown[0] = u8::MAX;
    assert!(matches!(
        ContentId::from_bytes(unknown),
        Err(Error::UnsupportedFormat { .. })
    ));
}

#[test]
fn leased_content_open_clone_renew_and_expiry_fail_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options())
            .await
            .expect("upload begins");
        upload.write(b"leased bytes").await.expect("bytes write");
        let sealed = upload.seal().await.expect("upload seals");
        let owner = ContentLeaseOwnerId::from_bytes([9_u8; 16]);
        let handle = db
            .open_content_leased(
                test_scope().storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(owner, Duration::from_mins(1)),
            )
            .await
            .expect("leased content opens");
        let clone = handle.clone();
        let lease_id = handle.lease_id().expect("lease identity exists");
        assert_eq!(clone.lease_id(), Some(lease_id));
        assert_eq!(
            handle
                .read_range(0, u64::MAX)
                .await
                .expect("leased range reads")
                .as_ref(),
            b"leased bytes"
        );
        assert!(
            db.content_has_active_lease(test_scope().storage_domain_id(), sealed.content_id())
                .await
                .expect("active lease probes")
        );

        let before = handle.lease_expires_at_unix_ms().expect("deadline exists");
        let renewed = clone
            .renew_lease(Duration::from_mins(2))
            .await
            .expect("lease renews");
        assert!(renewed >= before);
        assert_eq!(handle.lease_expires_at_unix_ms(), Some(renewed));

        let lease = handle.lease().expect("private lease exists");
        let expired = ContentLeaseRecord {
            lease_id,
            owner_id: owner,
            storage_domain_id: handle.storage_domain_id(),
            content_id: handle.content_id(),
            expires_at_unix_ms: 0,
        };
        let mut expire = db.transaction(TransactionOptions::default());
        expire
            .put_internal_bucket(
                CONTENT_LEASE_BUCKET,
                content_lease_key(handle.storage_domain_id(), handle.content_id(), lease_id),
                expired.encode(),
            )
            .expect("expired record stages");
        expire.commit().await.expect("expired record commits");
        lease.publish_expiry(0);
        assert!(matches!(
            handle.read_range(0, 1).await,
            Err(Error::ContentLeaseExpired { .. })
        ));
        assert!(matches!(
            clone.renew_lease(Duration::from_mins(1)).await,
            Err(Error::ContentLeaseExpired { .. })
        ));
        assert!(
            !db.content_has_active_lease(test_scope().storage_domain_id(), sealed.content_id())
                .await
                .expect("expired lease probes inactive")
        );

        let unleased = db
            .open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("ordinary content opens");
        assert!(matches!(
            unleased.renew_lease(Duration::from_secs(1)).await,
            Err(Error::ContentLeaseNotFound { .. })
        ));
        assert!(matches!(
            db.open_content_leased(
                test_scope().storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(owner, Duration::ZERO),
            )
            .await,
            Err(Error::InvalidOptions { .. })
        ));
    });
}

#[test]
fn durable_content_lease_survives_reopen_and_malformed_state_fails_closed() {
    let path = temp_db_path("content-lease-native");
    let (domain, content_id, lease_id) = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options())
            .await
            .expect("upload begins");
        upload
            .write(b"persistent lease")
            .await
            .expect("bytes write");
        let sealed = upload.seal().await.expect("upload seals");
        let domain = test_scope().storage_domain_id();
        let handle = db
            .open_content_leased(
                domain,
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([10_u8; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("leased content opens");
        (
            domain,
            sealed.content_id(),
            handle.lease_id().expect("lease exists"),
        )
    });

    block_on(async {
        let db = Db::open(&path).await.expect("native db reopens");
        assert!(
            db.content_has_active_lease(domain, content_id)
                .await
                .expect("durable lease reopens")
        );
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_LEASE_BUCKET,
                content_lease_key(domain, content_id, lease_id),
                b"malformed".to_vec(),
            )
            .expect("damage stages");
        damage.commit().await.expect("damage commits");
        assert!(matches!(
            db.content_has_active_lease(domain, content_id).await,
            Err(Error::InvalidFormat { .. })
        ));
    });

    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn token_expiry_overflow_does_not_publish_content() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(ContentUploadOptions::new(
                test_scope(),
                Duration::from_millis(u64::MAX),
            ))
            .await
            .expect("upload begins before seal fixes the expiry epoch");
        upload.write(b"unpublished").await.expect("bytes write");
        let upload_id = upload.upload_id();
        let content_id = ContentId::for_bytes(b"unpublished");

        assert!(matches!(
            upload.seal().await,
            Err(Error::InvalidOptions { .. })
        ));
        assert!(matches!(
            db.open_content(test_scope().storage_domain_id(), content_id)
                .await,
            Err(Error::ContentNotFound { .. })
        ));
        assert!(
            db.resume_content_upload(upload_id)
                .await
                .expect("upload remains resumable")
                .into_open()
                .is_some()
        );
    });
}

#[test]
fn memory_content_ranges_stream_and_expectations_fail_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut bytes = vec![0_u8; TEST_CHUNK_BYTES * 2 + 37];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::try_from(index % 251).expect("pattern is within u8");
        }

        let mut upload = db
            .begin_content_upload(
                test_upload_options()
                    .with_chunk_bytes(TEST_CHUNK_BYTES)
                    .with_expected_length(bytes.len() as u64)
                    .with_expected_content_id(ContentId::for_bytes(&bytes)),
            )
            .await
            .expect("upload begins");
        for piece in bytes.chunks(7_919) {
            upload.write(piece).await.expect("piece writes");
            assert!(upload.buffered_bytes() <= TEST_CHUNK_BYTES);
        }
        let sealed = upload.seal().await.expect("upload seals");
        assert!(matches!(
            db.open_content(
                StorageDomainId::from_bytes([88_u8; 16]),
                sealed.content_id(),
            )
            .await,
            Err(Error::ContentNotFound { .. })
        ));
        let handle = db
            .open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("sealed content opens");

        let start = TEST_CHUNK_BYTES as u64 - 11;
        let start_index = usize::try_from(start).expect("test range start fits usize");
        assert_eq!(
            handle
                .read_range(start, 29)
                .await
                .expect("cross-chunk range reads")
                .as_ref(),
            &bytes[start_index..start_index + 29]
        );
        assert!(
            handle
                .read_range(handle.len(), 10)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(handle.read_range(0, 0).await.unwrap().is_empty());
        assert!(matches!(
            handle.read_range(handle.len() + 1, 1).await,
            Err(Error::ContentRangeOutOfBounds { .. })
        ));
        handle.verify().await.expect("complete digest verifies");

        let mut stream = handle.stream();
        let mut streamed = 0_usize;
        while let Some(piece) = stream.next().await.expect("stream chunk reads") {
            assert!(piece.len() <= TEST_CHUNK_BYTES);
            streamed += piece.len();
        }
        assert_eq!(streamed, bytes.len());

        let mut mismatch = db
            .begin_content_upload(
                test_upload_options()
                    .with_chunk_bytes(TEST_CHUNK_BYTES)
                    .with_expected_length(1),
            )
            .await
            .expect("mismatch upload begins");
        let mismatch_id = mismatch.upload_id();
        assert!(matches!(
            mismatch.write(b"two").await,
            Err(Error::ContentLengthMismatch {
                expected: 1,
                actual: 3
            })
        ));
        assert_eq!(mismatch.len(), 0);
        let resumed = db
            .resume_content_upload(mismatch_id)
            .await
            .expect("rejected write leaves resumable upload state")
            .into_open()
            .expect("rejected write leaves the upload open");
        assert_eq!(resumed.len(), 0);
        drop(resumed);
        mismatch
            .write(b"x")
            .await
            .expect("valid replacement byte writes");
        mismatch.seal().await.expect("exact-length upload seals");
    });
}

#[test]
fn native_upload_resumes_across_reopen_and_remembers_seal() {
    let path = temp_db_path("content-resume-native");
    let mut bytes = vec![0_u8; TEST_CHUNK_BYTES * 2 + 37];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::try_from(index % 239).expect("pattern is within u8");
    }
    let first_len = TEST_CHUNK_BYTES + 123;

    let upload_id = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload
            .write(&bytes[..first_len])
            .await
            .expect("durable prefix writes");
        assert_eq!(upload.len(), first_len as u64);
        assert_eq!(upload.buffered_bytes(), 123);
        let upload_id = upload.upload_id();
        drop(upload);
        drop(db);
        upload_id
    });

    let sealed = block_on(async {
        let db = Db::open(&path).await.expect("native db reopens");
        let mut resumed = match db
            .resume_content_upload(upload_id)
            .await
            .expect("upload resumes")
        {
            ContentUploadResume::Open(upload) => upload,
            ContentUploadResume::Sealed(_) => panic!("upload unexpectedly sealed"),
        };
        assert_eq!(resumed.len(), first_len as u64);
        assert_eq!(resumed.buffered_bytes(), 123);
        resumed
            .write(&bytes[first_len..])
            .await
            .expect("remaining bytes write");
        let sealed = resumed.seal().await.expect("resumed upload seals");
        assert_eq!(sealed.content_id(), ContentId::for_bytes(&bytes));
        drop(db);
        sealed
    });

    block_on(async {
        let db = Db::open(&path).await.expect("sealed db reopens");
        let remembered = db
            .resume_content_upload(upload_id)
            .await
            .expect("sealed upload state reopens")
            .sealed()
            .expect("seal result is remembered");
        assert_eq!(remembered, sealed);
        assert_eq!(
            db.seal_content_upload(upload_id)
                .await
                .expect("seal retry is idempotent"),
            sealed
        );
        let handle = db
            .open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("resumed content opens");
        handle.verify().await.expect("resumed content verifies");
        let mut transaction = db.transaction(TransactionOptions::default());
        let attached = transaction
            .consume_upload_token(
                sealed.upload_token(),
                test_scope(),
                ContentChangeId::from_bytes([21_u8; 16]),
            )
            .await
            .expect("reopened token stages consumption");
        assert_eq!(attached.content_id(), sealed.content_id());
        transaction.put(b"native:attached", b"yes");
        transaction
            .commit()
            .await
            .expect("reopened token and marker commit");
        drop(handle);
        drop(db);
    });

    std::fs::remove_dir_all(&path).expect("test database removes");
}

#[test]
fn stale_upload_writer_conflicts_with_newer_revision() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut first = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        let upload_id = first.upload_id();
        let mut stale = db
            .resume_content_upload(upload_id)
            .await
            .expect("second writer resumes")
            .into_open()
            .expect("upload is open");

        first.write(b"winner").await.expect("first writer advances");
        assert!(matches!(
            stale.write(b"stale").await,
            Err(Error::ContentUploadConflict {
                expected_revision: 0,
                actual_revision: 1,
                ..
            })
        ));
        first.abort().await.expect("winning session aborts");
    });
}

#[test]
fn upload_token_rejects_wrong_scope_expiry_and_invalid_bearer() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload.write(b"attach me").await.expect("content writes");
        let sealed = upload.seal().await.expect("content seals");
        let token = sealed.upload_token();
        let change_a = ContentChangeId::from_bytes([11_u8; 16]);
        let before_expiry = sealed.token_expires_at_unix_ms() - 1;

        let wrong_scope = ContentAttachmentScope::new(
            test_scope().storage_domain_id(),
            OwnerScopeId::from_bytes([99_u8; 16]),
        );
        let mut wrong = db.transaction(TransactionOptions::default());
        assert!(matches!(
            wrong
                .consume_upload_token_at(token, wrong_scope, change_a, before_expiry)
                .await,
            Err(Error::UploadTokenScopeMismatch)
        ));

        let mut expired = db.transaction(TransactionOptions::default());
        assert!(matches!(
            expired
                .consume_upload_token_at(
                    token,
                    test_scope(),
                    change_a,
                    sealed.token_expires_at_unix_ms(),
                )
                .await,
            Err(Error::UploadTokenExpired { .. })
        ));

        let mut invalid_bytes = token.to_bytes();
        invalid_bytes[32] ^= 0xff;
        let invalid_token = UploadToken::from_bytes(invalid_bytes).expect("version remains valid");
        let mut invalid = db.transaction(TransactionOptions::default());
        assert!(matches!(
            invalid
                .consume_upload_token_at(invalid_token, test_scope(), change_a, before_expiry,)
                .await,
            Err(Error::UploadTokenInvalid)
        ));
    });
}

#[test]
fn upload_token_consumption_is_idempotent_and_atomic_with_catalog_writes() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload.write(b"attach me").await.expect("content writes");
        let sealed = upload.seal().await.expect("content seals");
        let token = sealed.upload_token();
        let change_a = ContentChangeId::from_bytes([11_u8; 16]);
        let change_b = ContentChangeId::from_bytes([12_u8; 16]);
        let before_expiry = sealed.token_expires_at_unix_ms() - 1;

        let mut abandoned = db.transaction(TransactionOptions::default());
        let claims = abandoned
            .consume_upload_token_at(token, test_scope(), change_a, before_expiry)
            .await
            .expect("available token stages");
        assert_eq!(claims.content_id(), sealed.content_id());
        abandoned.put(b"catalog:abandoned", b"must not commit");
        drop(abandoned);
        assert_eq!(
            db.get(b"catalog:abandoned").await.expect("marker reads"),
            None
        );

        let mut committed = db.transaction(TransactionOptions::default());
        committed
            .consume_upload_token_at(token, test_scope(), change_a, before_expiry)
            .await
            .expect("dropped transaction left token available");
        committed.put(b"catalog:file", b"visible");
        committed.commit().await.expect("token and catalog commit");
        assert_eq!(
            db.get(b"catalog:file").await.expect("catalog reads"),
            Some(b"visible".to_vec())
        );

        let mut idempotent = db.transaction(TransactionOptions::default());
        let repeated = idempotent
            .consume_upload_token_at(
                token,
                test_scope(),
                change_a,
                sealed.token_expires_at_unix_ms() + 1,
            )
            .await
            .expect("same committed ChangeId remains idempotent after expiry");
        assert_eq!(repeated, claims);
        idempotent.commit().await.expect("idempotent retry commits");

        let mut sync_retry = db.transaction(TransactionOptions::default());
        assert_eq!(
            sync_retry
                .consume_upload_token_sync(token, test_scope(), change_a)
                .expect("sync API returns the same committed claims"),
            claims
        );
        sync_retry
            .commit_sync()
            .expect("sync idempotent retry commits");

        let mut reused = db.transaction(TransactionOptions::default());
        assert!(matches!(
            reused
                .consume_upload_token_at(token, test_scope(), change_b, before_expiry)
                .await,
            Err(Error::UploadTokenAlreadyConsumed)
        ));
    });
}

#[test]
fn competing_token_consumptions_commit_with_exactly_one_catalog_write() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let change_a = ContentChangeId::from_bytes([11_u8; 16]);
        let change_b = ContentChangeId::from_bytes([12_u8; 16]);
        let mut second_upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("second upload begins");
        second_upload
            .write(b"conflict content")
            .await
            .expect("second content writes");
        let second = second_upload.seal().await.expect("second content seals");
        let second_before_expiry = second.token_expires_at_unix_ms() - 1;
        let mut loser = db.transaction(TransactionOptions::default());
        loser
            .consume_upload_token_at(
                second.upload_token(),
                test_scope(),
                change_a,
                second_before_expiry,
            )
            .await
            .expect("loser stages token");
        loser.put(b"catalog:loser", b"must not commit");
        let mut winner = db.transaction(TransactionOptions::default());
        winner
            .consume_upload_token_at(
                second.upload_token(),
                test_scope(),
                change_b,
                second_before_expiry,
            )
            .await
            .expect("winner stages same available token");
        winner.put(b"catalog:winner", b"committed");
        winner.commit().await.expect("winner commits first");
        assert!(matches!(loser.commit().await, Err(Error::Conflict { .. })));
        assert_eq!(db.get(b"catalog:loser").await.expect("loser reads"), None);
        assert_eq!(
            db.get(b"catalog:winner").await.expect("winner reads"),
            Some(b"committed".to_vec())
        );
    });
}

#[test]
fn native_content_exceeds_ordinary_value_limit_and_reopens() {
    let path = temp_db_path("content-large-native");
    let content_id = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let one_mib = 1024 * 1024;
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(one_mib))
            .await
            .expect("large upload begins");
        let mut piece = vec![0_u8; one_mib];
        for part in 0_u8..65 {
            piece.fill(part);
            upload.write(&piece).await.expect("large piece writes");
            assert_eq!(upload.buffered_bytes(), 0);
        }
        assert_eq!(upload.len(), 65 * one_mib as u64);
        let sealed = upload.seal().await.expect("large upload seals");
        drop(db);
        sealed.content_id()
    });

    block_on(async {
        let reopened = Db::open(&path).await.expect("native db reopens");
        let handle = reopened
            .open_content(test_scope().storage_domain_id(), content_id)
            .await
            .expect("large content reopens");
        assert_eq!(handle.len(), 65 * 1024 * 1024);
        let boundary = handle
            .read_range(1024 * 1024 - 8, 16)
            .await
            .expect("native boundary range reads");
        assert_eq!(&boundary[..8], &[0_u8; 8]);
        assert_eq!(&boundary[8..], &[1_u8; 8]);
        handle.verify().await.expect("reopened content verifies");
        drop(handle);
        drop(reopened);
    });

    std::fs::remove_dir_all(&path).expect("test database removes");
}

#[test]
#[allow(clippy::too_many_lines)] // one end-to-end request-count and failure-ordering scenario
fn object_store_counts_requests_detects_tampering_and_hides_failed_seal() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let db =
            Db::open_object_store_at(client.clone(), "content-probe", DbOptions::object_store())
                .await
                .expect("object db opens");
        client.reset_counts();

        let bytes = vec![7_u8; TEST_CHUNK_BYTES * 2 + 17];
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("object upload begins");
        upload.write(&bytes).await.expect("object bytes write");
        let sealed = upload.seal().await.expect("object upload seals");
        let after_seal = client.counts();
        assert_eq!(
            after_seal.put, 0,
            "published upload objects are create-only"
        );
        assert_eq!(
            after_seal.put_if, 41,
            "database commits, object-mutation lease fences, chunks, and descriptors use conditional writes"
        );

        let handle = db
            .open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("object content opens");
        let after_open = client.counts();
        assert_eq!(
            after_open.head - after_seal.head,
            2,
            "open checks the domain access barrier before the descriptor"
        );
        assert_eq!(after_open.get_range - after_seal.get_range, 1);

        let range = handle
            .read_range(TEST_CHUNK_BYTES as u64 - 5, 10)
            .await
            .expect("object boundary range reads");
        assert_eq!(range.as_ref(), &[7_u8; 10]);
        let after_range = client.counts();
        assert_eq!(after_range.head - after_open.head, 2);
        assert_eq!(after_range.get_range - after_open.get_range, 2);
        assert_eq!(
            after_range.get - after_open.get,
            0,
            "content reads avoid whole-object GET"
        );

        let chunk = client
            .inner
            .list("content-probe/content-v1/chunks")
            .await
            .expect("chunks list")
            .into_iter()
            .next()
            .expect("at least one chunk");
        let mut corrupted = client
            .inner
            .get(&chunk.key)
            .await
            .expect("chunk reads")
            .expect("chunk exists")
            .to_vec();
        let last = corrupted.last_mut().expect("chunk has payload");
        *last ^= 0xff;
        client
            .inner
            .put(&chunk.key, Arc::from(corrupted))
            .await
            .expect("corrupt chunk replaces");
        assert!(matches!(
            handle.read_range(0, 1).await,
            Err(Error::ContentDigestMismatch { .. })
        ));

        drop(handle);
        drop(db);

        let failed_client = Arc::new(MeasuredClient::new());
        let failed_db = Db::open_object_store_at(
            failed_client.clone(),
            "failed-content-probe",
            DbOptions::object_store(),
        )
        .await
        .expect("failure db opens");
        failed_client
            .fail_descriptors
            .store(true, Ordering::Release);
        let expected = ContentId::for_bytes(b"not published");
        let mut failed_upload = failed_db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("failed upload begins");
        failed_upload
            .write(b"not published")
            .await
            .expect("failed upload chunk buffers");
        let failed_upload_id = failed_upload.upload_id();
        assert!(failed_upload.seal().await.is_err());
        assert!(matches!(
            failed_db
                .open_content(test_scope().storage_domain_id(), expected)
                .await,
            Err(Error::ContentNotFound { .. })
        ));
        assert!(matches!(
            failed_db.abort_content_upload(failed_upload_id).await,
            Err(Error::ContentUploadSealed { .. })
        ));
        failed_client
            .fail_descriptors
            .store(false, Ordering::Release);
        let repaired = failed_db
            .seal_content_upload(failed_upload_id)
            .await
            .expect("sealing marker publishes missing descriptor on retry");
        assert_eq!(repaired.content_id(), expected);
        failed_db
            .open_content(test_scope().storage_domain_id(), expected)
            .await
            .expect("retried descriptor opens");
        drop(failed_db);
    });
}

#[test]
fn sealing_state_failure_keeps_descriptor_hidden_and_retry_completes() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let db = Db::open_object_store_at(
            client.clone(),
            "seal-retry-probe",
            DbOptions::object_store(),
        )
        .await
        .expect("object db opens");
        let bytes = vec![11_u8; TEST_CHUNK_BYTES + 31];
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload.write(&bytes).await.expect("bytes write");
        let upload_id = upload.upload_id();
        let content_id = ContentId::for_bytes(&bytes);

        client
            .fail_sealing_upload_states
            .store(true, Ordering::Release);
        assert!(upload.seal().await.is_err());
        assert!(matches!(
            db.open_content(test_scope().storage_domain_id(), content_id)
                .await,
            Err(Error::ContentNotFound { .. })
        ));

        client
            .fail_sealing_upload_states
            .store(false, Ordering::Release);
        let repaired = db
            .seal_content_upload(upload_id)
            .await
            .expect("seal retry completes session state");
        assert_eq!(repaired.content_id(), content_id);
        assert_eq!(repaired.len(), bytes.len() as u64);
        assert_eq!(
            db.resume_content_upload(upload_id)
                .await
                .expect("repaired session resumes")
                .sealed(),
            Some(repaired)
        );
    });
}

#[test]
fn resume_repairs_token_issued_before_sealed_session_state() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let db = Db::open_object_store_at(
            client.clone(),
            "token-seal-retry-probe",
            DbOptions::object_store(),
        )
        .await
        .expect("object db opens");
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        upload
            .write(b"token crash window")
            .await
            .expect("bytes write");
        let upload_id = upload.upload_id();

        client
            .fail_sealed_upload_states
            .store(true, Ordering::Release);
        assert!(upload.seal().await.is_err());
        client
            .fail_sealed_upload_states
            .store(false, Ordering::Release);

        let sealed = db
            .resume_content_upload(upload_id)
            .await
            .expect("resume completes sealing state")
            .sealed()
            .expect("session is sealed after recovery");
        let mut transaction = db.transaction(TransactionOptions::default());
        let claims = transaction
            .consume_upload_token(
                sealed.upload_token(),
                test_scope(),
                ContentChangeId::from_bytes([31_u8; 16]),
            )
            .await
            .expect("recovered token stages consumption");
        assert_eq!(claims.content_id(), sealed.content_id());
        transaction.put(b"token:recovered", b"yes");
        transaction
            .commit()
            .await
            .expect("recovered token commits atomically");
    });
}

#[test]
fn resume_ignores_chunk_bytes_not_confirmed_by_session_revision() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let db = Db::open_object_store_at(
            client.clone(),
            "progress-retry-probe",
            DbOptions::object_store(),
        )
        .await
        .expect("object db opens");
        let bytes = vec![19_u8; TEST_CHUNK_BYTES + 29];
        let mut upload = db
            .begin_content_upload(test_upload_options().with_chunk_bytes(TEST_CHUNK_BYTES))
            .await
            .expect("upload begins");
        let upload_id = upload.upload_id();

        client
            .fail_open_upload_states
            .store(true, Ordering::Release);
        assert!(upload.write(&bytes).await.is_err());
        client
            .fail_open_upload_states
            .store(false, Ordering::Release);

        let mut resumed = db
            .resume_content_upload(upload_id)
            .await
            .expect("upload resumes from old revision")
            .into_open()
            .expect("session remains open");
        assert_eq!(resumed.len(), 0);
        assert_eq!(resumed.buffered_bytes(), 0);
        resumed
            .write(&bytes)
            .await
            .expect("identical immutable chunk retries are idempotent");
        let sealed = resumed.seal().await.expect("retried upload seals");
        assert_eq!(sealed.content_id(), ContentId::for_bytes(&bytes));
        db.open_content(test_scope().storage_domain_id(), sealed.content_id())
            .await
            .expect("retried content opens")
            .verify()
            .await
            .expect("retried content verifies");
    });
}
