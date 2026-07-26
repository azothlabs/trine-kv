use super::*;

#[test]
fn physical_content_reclamation_is_disabled_by_default() {
    assert_eq!(
        DbOptions::memory().content_reclamation,
        ContentReclamationMode::Disabled
    );
}

#[test]
fn wasi_storage_mode_does_not_alias_the_qualified_native_backend() {
    let options = DbOptions::wasi_persistent("wasi-content-reclaim-disabled")
        .with_content_reclamation(ContentReclamationMode::QualifiedNativeFilesystem);
    assert!(matches!(
        options.storage_mode,
        StorageMode::HostPersistent {
            backend: HostStorageBackend::Wasi { .. },
        }
    ));
    assert_eq!(
        options.content_reclamation,
        ContentReclamationMode::QualifiedNativeFilesystem
    );
}

#[test]
fn wasi_and_browser_reclamation_use_distinct_qualifications() {
    let wasi = DbOptions::wasi_persistent("wasi-content-reclaim-qualified")
        .with_content_reclamation(ContentReclamationMode::QualifiedWasiFilesystem);
    assert_eq!(
        wasi.content_reclamation,
        ContentReclamationMode::QualifiedWasiFilesystem
    );
    let browser = DbOptions::browser_persistent()
        .with_content_reclamation(ContentReclamationMode::QualifiedBrowserStorage);
    assert_eq!(
        browser.content_reclamation,
        ContentReclamationMode::QualifiedBrowserStorage
    );
}

#[test]
fn empty_content_upload_has_a_durable_zero_byte_reservation_and_seals() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let upload = db
            .begin_content_upload(test_upload_options().with_expected_length(0))
            .await
            .expect("empty upload begins");
        let sealed = upload.seal().await.expect("empty upload seals");
        assert_eq!(sealed.content_id(), ContentId::for_bytes(b""));
        assert_eq!(sealed.len(), 0);
        let content = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("empty content opens");
        assert_eq!(content.len(), 0);
        assert!(
            content
                .read_range(0, 1)
                .await
                .expect("empty read")
                .is_empty()
        );
    });
}

#[test]
fn object_store_reclamation_qualification_is_not_transferable_between_clients() {
    block_on(async {
        let qualified_client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let other_client = Arc::new(InMemoryObjectStore::new());
        let qualification = qualify_object_store_reclamation(
            Arc::clone(&qualified_client),
            "client-bound-reclamation",
            ObjectStoreReclamationAttestation::new(
                ObjectStoreReclamationEvidenceDigest::for_bytes(b"client-bound evidence"),
            ),
        )
        .await
        .expect("first client qualifies");
        assert!(matches!(
            Db::open_object_store_at(
                other_client,
                "client-bound-reclamation",
                DbOptions::object_store().with_content_reclamation(
                    ContentReclamationMode::QualifiedObjectStore(qualification),
                ),
            )
            .await,
            Err(Error::UnsupportedBackend { .. })
        ));
    });
}

#[test]
fn caller_supplied_upload_id_binds_options_and_recovers_exact_state() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let upload_id = UploadId::new().expect("upload identity");
        let options = test_upload_options().with_expected_length(6);

        let ContentUploadResume::Open(mut first) = db
            .begin_content_upload_with_id(upload_id, options)
            .await
            .expect("first begin")
        else {
            panic!("first begin must be open");
        };
        first.write(b"abc").await.expect("write prefix");
        drop(first);

        let different = options.with_expected_length(7);
        assert!(matches!(
            db.begin_content_upload_with_id(upload_id, different).await,
            Err(Error::InvalidOptions { .. })
        ));

        let ContentUploadResume::Open(mut retry) = db
            .begin_content_upload_with_id(upload_id, options)
            .await
            .expect("exact retry")
        else {
            panic!("retry before seal must be open");
        };
        assert_eq!(retry.len(), 3);
        retry.write(b"def").await.expect("write suffix");
        let sealed = retry.seal().await.expect("seal");

        let ContentUploadResume::Sealed(recovered) = db
            .begin_content_upload_with_id(upload_id, options)
            .await
            .expect("sealed retry")
        else {
            panic!("retry after seal must recover sealed result");
        };
        assert_eq!(recovered, sealed);
    });
}

#[test]
#[allow(clippy::too_many_lines)] // one end-to-end lifecycle assertion is clearer than split setup
fn upload_maintenance_reclaims_orphans_and_prunes_only_sealed_idempotency_state() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let domain = test_scope().storage_domain_id();
        let abandoned_id = UploadId::new().expect("abandoned upload identity");
        let ContentUploadResume::Open(mut abandoned) = db
            .begin_content_upload_with_id(
                abandoned_id,
                test_upload_options().with_expected_length(6),
            )
            .await
            .expect("abandoned upload begins")
        else {
            panic!("new upload is open");
        };
        abandoned
            .write(b"abc")
            .await
            .expect("partial bytes persist");
        drop(abandoned);

        let listed = db.list_content_uploads().await.expect("uploads list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].upload_id(), abandoned_id);
        assert_eq!(listed[0].state(), ContentUploadState::Open);
        assert_eq!(listed[0].len(), 3);
        assert!(listed[0].updated_at_unix_ms() > 0);
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("reserved quota reads")
                .upload_reserved_bytes(),
            6
        );

        let reaped = db
            .reap_inactive_content_uploads(u64::MAX)
            .await
            .expect("inactive upload is reaped");
        assert_eq!(reaped.scanned(), 1);
        assert_eq!(reaped.aborted(), 1);
        assert!(
            db.list_content_uploads()
                .await
                .expect("uploads relist")
                .is_empty()
        );
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("released quota reads")
                .upload_reserved_bytes(),
            0
        );
        assert!(matches!(
            db.resume_content_upload(abandoned_id).await,
            Err(Error::ContentUploadNotFound { .. })
        ));
        assert!(matches!(
            db.begin_content_upload_with_id(
                abandoned_id,
                test_upload_options().with_expected_length(1)
            )
            .await,
            Err(Error::ContentUploadNotFound { .. })
        ));

        let sealed_id = UploadId::new().expect("sealed upload identity");
        let ContentUploadResume::Open(mut upload) = db
            .begin_content_upload_with_id(sealed_id, test_upload_options().with_expected_length(3))
            .await
            .expect("sealed upload begins")
        else {
            panic!("new upload is open");
        };
        upload.write(b"xyz").await.expect("sealed bytes write");
        let sealed = upload.seal().await.expect("upload seals");
        let pruned = db
            .prune_sealed_content_uploads(u64::MAX)
            .await
            .expect("sealed state prunes");
        assert_eq!(pruned.pruned_sealed(), 1);
        assert!(matches!(
            db.resume_content_upload(sealed_id).await,
            Err(Error::ContentUploadSealed { .. })
        ));
        assert!(matches!(
            db.begin_content_upload_with_id(
                sealed_id,
                test_upload_options().with_expected_length(4)
            )
            .await,
            Err(Error::ContentUploadSealed { .. })
        ));
        let content = db
            .open_content(domain, sealed.content_id())
            .await
            .expect("content remains after idempotency state prune");
        assert_eq!(
            content
                .read_range(0, 3)
                .await
                .expect("content reads")
                .as_ref(),
            b"xyz"
        );
    });
}

#[test]
fn upload_maintenance_revisits_tombstones_to_remove_late_visible_chunks() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");

        let aborted_id = UploadId::new().expect("aborted upload identity");
        let ContentUploadResume::Open(upload) = db
            .begin_content_upload_with_id(aborted_id, test_upload_options())
            .await
            .expect("aborted upload begins")
        else {
            panic!("new upload is open");
        };
        upload.abort().await.expect("upload aborts");
        db.write_content_partial_chunk(aborted_id, 0, 99, Arc::from(b"late".as_slice()))
            .await
            .expect("plant late-visible aborted chunk");

        db.reap_inactive_content_uploads(u64::MAX)
            .await
            .expect("aborted tombstone cleanup");
        assert!(
            db.read_content_partial_chunk(aborted_id, 0, 99)
                .await
                .expect("read aborted orphan")
                .is_none()
        );

        let sealed_id = UploadId::new().expect("sealed upload identity");
        let domain = test_scope().storage_domain_id();
        let ContentUploadResume::Open(mut upload) = db
            .begin_content_upload_with_id(sealed_id, test_upload_options().with_expected_length(3))
            .await
            .expect("sealed upload begins")
        else {
            panic!("new upload is open");
        };
        upload.write(b"xyz").await.expect("sealed bytes write");
        let sealed = upload.seal().await.expect("upload seals");
        db.prune_sealed_content_uploads(u64::MAX)
            .await
            .expect("sealed state retires");
        db.write_content_partial_chunk(sealed_id, 0, 100, Arc::from(b"late".as_slice()))
            .await
            .expect("plant late-visible sealed partial");

        db.prune_sealed_content_uploads(u64::MAX)
            .await
            .expect("sealed tombstone cleanup");
        assert!(
            db.read_content_partial_chunk(sealed_id, 0, 100)
                .await
                .expect("read sealed orphan")
                .is_none()
        );
        let content = db
            .open_content(domain, sealed.content_id())
            .await
            .expect("canonical sealed content survives");
        assert_eq!(
            content
                .read_range(0, 3)
                .await
                .expect("content reads")
                .as_ref(),
            b"xyz"
        );
    });
}

#[test]
fn object_store_upload_state_rejects_a_stale_revision_transition() {
    block_on(async {
        let client = Arc::new(InMemoryObjectStore::new());
        let db = Db::open_object_store(client, DbOptions::object_store())
            .await
            .expect("open object store");
        let upload_id = UploadId::new().expect("upload identity");
        let ContentUploadResume::Open(mut upload) = db
            .begin_content_upload_with_id(upload_id, test_upload_options().with_expected_length(1))
            .await
            .expect("begin upload")
        else {
            panic!("new upload is open");
        };
        let stale: UploadSessionState = db
            .require_upload_state(upload_id)
            .await
            .expect("read initial state");
        upload.write(b"x").await.expect("advance durable revision");

        let stale_transition = stale.into_aborting().expect("build stale transition");
        assert!(matches!(
            db.write_upload_state(&stale_transition).await,
            Err(Error::ContentUploadConflict { .. })
        ));
        assert_eq!(
            db.require_upload_state(upload_id)
                .await
                .expect("current state remains")
                .revision(),
            1
        );
    });
}

#[test]
fn object_store_upload_can_durably_extend_the_same_partial_chunk() {
    block_on(async {
        let client = Arc::new(InMemoryObjectStore::new());
        let db = Db::open_object_store(client, DbOptions::object_store())
            .await
            .expect("open object store");
        let mut upload = db
            .begin_content_upload(test_upload_options().with_expected_length(2))
            .await
            .expect("begin upload");

        upload
            .write(b"a")
            .await
            .expect("write first partial prefix");
        upload
            .write(b"b")
            .await
            .expect("extend the same staging chunk");
        let sealed = upload.seal().await.expect("seal extended upload");
        let content = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("open sealed content");
        assert_eq!(
            content
                .read_range(0, 2)
                .await
                .expect("read content")
                .as_ref(),
            b"ab"
        );
    });
}

#[test]
fn physical_quota_reserves_streams_reconciles_and_releases() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let domain = test_scope().storage_domain_id();
        let configured = db
            .set_content_physical_quota(domain, Some(8))
            .await
            .expect("physical quota configures");
        assert_eq!(configured.accounted_bytes(), 0);
        assert_eq!(configured.remaining(), Some(8));

        let upload_id = UploadId::new().expect("known upload identity");
        let ContentUploadResume::Open(known) = db
            .begin_content_upload_with_id(upload_id, test_upload_options().with_expected_length(6))
            .await
            .expect("known upload reserves")
        else {
            panic!("new known upload is open");
        };
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("known reservation reads")
                .upload_reserved_bytes(),
            6
        );
        assert!(matches!(
            db.begin_content_upload(test_upload_options().with_expected_length(3))
                .await,
            Err(Error::ContentPhysicalQuotaExceeded {
                limit: 8,
                requested_bytes: 3,
                ..
            })
        ));
        known.abort().await.expect("known reservation aborts");
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("released reservation reads")
                .accounted_bytes(),
            0
        );

        let mut streamed = db
            .begin_content_upload(test_upload_options())
            .await
            .expect("unknown upload begins");
        streamed
            .write(b"abcde")
            .await
            .expect("stream reserves bytes");
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("stream reservation reads")
                .upload_reserved_bytes(),
            5
        );
        assert!(matches!(
            streamed.write(b"wxyz").await,
            Err(Error::ContentPhysicalQuotaExceeded {
                limit: 8,
                requested_bytes: 4,
                ..
            })
        ));
        let sealed = streamed.seal().await.expect("stream seals");
        assert_eq!(sealed.len(), 5);
        let reconciled = db
            .content_physical_quota(domain)
            .await
            .expect("sealed accounting reads");
        assert_eq!(reconciled.unique_content_bytes(), 5);
        assert_eq!(reconciled.upload_reserved_bytes(), 0);
        assert_eq!(reconciled.remaining(), Some(3));
        assert!(matches!(
            db.set_content_physical_quota(domain, Some(4)).await,
            Err(Error::ContentPhysicalQuotaExceeded {
                limit: 4,
                requested_bytes: 0,
                ..
            })
        ));
    });
}

#[test]
fn expected_upload_length_is_a_pre_write_physical_quota_boundary() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let domain = test_scope().storage_domain_id();
        db.set_content_physical_quota(domain, Some(1))
            .await
            .expect("physical quota configures");

        let mut upload = db
            .begin_content_upload(test_upload_options().with_expected_length(1))
            .await
            .expect("expected upload reserves");
        assert!(matches!(
            upload.write(b"too large").await,
            Err(Error::ContentLengthMismatch {
                expected: 1,
                actual: 9
            })
        ));
        assert_eq!(upload.len(), 0);
        assert_eq!(upload.buffered_bytes(), 0);
        let quota = db
            .content_physical_quota(domain)
            .await
            .expect("rejected write leaves accounting readable");
        assert_eq!(quota.upload_reserved_bytes(), 1);
        assert_eq!(quota.accounted_bytes(), 1);

        upload
            .write(b"x")
            .await
            .expect("writer remains usable at the declared boundary");
        let sealed = upload.seal().await.expect("bounded upload seals");
        assert_eq!(sealed.len(), 1);
        let quota = db
            .content_physical_quota(domain)
            .await
            .expect("sealed accounting reads");
        assert_eq!(quota.unique_content_bytes(), 1);
        assert_eq!(quota.upload_reserved_bytes(), 0);
    });
}

#[test]
fn physical_quota_concurrent_reservations_do_not_overcommit() {
    block_on(async {
        let db = Db::open(DbOptions::memory()).await.expect("open database");
        let domain = test_scope().storage_domain_id();
        db.set_content_physical_quota(domain, Some(10))
            .await
            .expect("physical quota configures");
        let options = test_upload_options().with_expected_length(6);
        let (first, second) = futures::join!(
            db.begin_content_upload(options),
            db.begin_content_upload(options)
        );
        let successes = usize::from(first.is_ok()) + usize::from(second.is_ok());
        let rejections = usize::from(matches!(
            &first,
            Err(Error::ContentPhysicalQuotaExceeded { .. } | Error::Conflict { .. })
        )) + usize::from(matches!(
            &second,
            Err(Error::ContentPhysicalQuotaExceeded { .. } | Error::Conflict { .. })
        ));
        assert_eq!((successes, rejections), (1, 1));
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("concurrent accounting reads")
                .upload_reserved_bytes(),
            6
        );
        if let Ok(upload) = first {
            upload.abort().await.expect("first winner aborts");
        }
        if let Ok(upload) = second {
            upload.abort().await.expect("second winner aborts");
        }
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("released concurrent accounting reads")
                .accounted_bytes(),
            0
        );
    });
}

#[test]
fn physical_quota_reservation_survives_reopen_and_resume() {
    let path = temp_db_path("content-physical-quota-reopen");
    let upload_id = UploadId::new().expect("upload identity generates");
    block_on(async {
        let db = Db::open(DbOptions::new(&path))
            .await
            .expect("database opens");
        let domain = test_scope().storage_domain_id();
        db.set_content_physical_quota(domain, Some(32))
            .await
            .expect("physical quota configures");
        let ContentUploadResume::Open(mut upload) = db
            .begin_content_upload_with_id(upload_id, test_upload_options())
            .await
            .expect("unknown upload begins")
        else {
            panic!("new upload is open");
        };
        upload.write(b"durable").await.expect("prefix writes");
        drop(upload);
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("reservation reads")
                .upload_reserved_bytes(),
            7
        );
        db.close().await.expect("database closes");
    });
    block_on(async {
        let db = Db::open(DbOptions::new(&path))
            .await
            .expect("database reopens");
        let domain = test_scope().storage_domain_id();
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("reopened reservation reads")
                .upload_reserved_bytes(),
            7
        );
        let ContentUploadResume::Open(upload) = db
            .resume_content_upload(upload_id)
            .await
            .expect("upload resumes")
        else {
            panic!("resumed upload is open");
        };
        assert_eq!(upload.len(), 7);
        upload.abort().await.expect("resumed upload aborts");
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("reopened release reads")
                .accounted_bytes(),
            0
        );
        db.close().await.expect("database closes after release");
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn aborting_upload_retains_quota_until_chunk_cleanup_recovers() {
    let path = temp_db_path("content-physical-quota-abort-recovery");
    let upload_id = UploadId::new().expect("upload identity generates");
    block_on(async {
        let db = Db::open(DbOptions::new(&path))
            .await
            .expect("database opens");
        let domain = test_scope().storage_domain_id();
        db.set_content_physical_quota(domain, Some(32))
            .await
            .expect("physical quota configures");
        let ContentUploadResume::Open(mut upload) = db
            .begin_content_upload_with_id(upload_id, test_upload_options())
            .await
            .expect("upload begins")
        else {
            panic!("new upload is open");
        };
        upload
            .write(b"durable")
            .await
            .expect("partial chunk writes");
        let delete_fault = StorageFaultGuard::install(
            &path,
            StorageFaultPoint::ObjectDelete,
            Some(StorageObjectKind::ContentChunk),
            1,
        );
        assert!(matches!(upload.abort().await, Err(Error::Io(_))));
        assert_eq!(
            db.content_physical_quota(domain)
                .await
                .expect("failed abort accounting reads")
                .upload_reserved_bytes(),
            7
        );
        drop(delete_fault);
        db.close().await.expect("database closes");
    });
    block_on(async {
        let db = Db::open(DbOptions::new(&path))
            .await
            .expect("database reopens");
        assert!(matches!(
            db.resume_content_upload(upload_id).await,
            Err(Error::ContentUploadNotFound { .. })
        ));
        assert_eq!(
            db.content_physical_quota(test_scope().storage_domain_id())
                .await
                .expect("recovered abort accounting reads")
                .accounted_bytes(),
            0
        );
        db.close().await.expect("database closes after recovery");
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}
