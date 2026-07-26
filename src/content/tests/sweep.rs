use super::*;

#[cfg(target_os = "wasi")]
#[test]
fn qualified_wasi_sweep_reclaims_after_process_reopen() {
    let path = PathBuf::from(format!(
        "wasi-content-reclaim-{}",
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let options = || {
        DbOptions::wasi_persistent(&path)
            .with_content_reclamation(ContentReclamationMode::QualifiedWasiFilesystem)
    };
    let sealed = block_on(async {
        let db = Db::open(options()).await.expect("WASI database opens");
        let sealed = seal_content_without_access_barrier(&db, b"WASI physical reclamation").await;
        let old = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("pre-barrier WASI handle opens");
        db.enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(130))
            .await
            .expect("WASI leased-only barrier commits");
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("pre-barrier WASI handle remains valid")
                .as_ref(),
            b"WASI physical reclamation"
        );
        drop(old);
        consume_reclaim_token(&db, sealed, 131).await;
        let barrier = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(130))
            .await
            .expect("WASI barrier coordinate reads");
        db.attest_content_reader_drain(
            barrier,
            test_reader_drain_attestation_id(131),
            test_reader_drain_options(ContentReaderDrainKind::NativeProcessSetRestarted, 132),
        )
        .await
        .expect("WASI process-set drain attests");
        let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 133).await;
        let mut grace_tx = db.transaction(TransactionOptions::default());
        grace_tx
            .stage_content_reclaim_grace(authorization, Duration::from_millis(1))
            .await
            .expect("WASI grace stages");
        grace_tx.commit().await.expect("WASI grace commits");
        let grace = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("WASI grace reads")
            .expect("WASI grace exists");
        let clock = ContentReclaimClockAttestation::new(
            grace,
            test_reclaim_clock_attestation_id(134),
            ContentReclaimClockCoordinatorId::from_bytes([135; 16]),
            ContentReclaimClockEvidenceDigest::for_bytes(b"WASI restart clock evidence"),
            grace.not_before_unix_ms(),
        )
        .expect("WASI clock binds grace");
        let mut prepare = db.transaction(TransactionOptions::default());
        let fresh = reclaim_authorization(&prepare, sealed, 136);
        assert_eq!(
            prepare
                .stage_content_reclaim_sweep(fresh, clock)
                .await
                .expect("WASI sweep stages"),
            ContentReclaimSweepStage::Staged
        );
        prepare.commit().await.expect("WASI Prepared commits");
        db.close().await.expect("WASI database closes at Prepared");
        sealed
    });

    block_on(async {
        let reopened = Db::open(options())
            .await
            .expect("WASI database reopens at Prepared");
        let sweep = reopened
            .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("WASI sweep resumes");
        assert!(sweep.reclaimed_at().is_some());
        assert!(matches!(
            reopened
                .open_content_leased(
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                    ContentLeaseOptions::new(
                        ContentLeaseOwnerId::from_bytes([137; 16]),
                        Duration::from_secs(60),
                    ),
                )
                .await,
            Err(Error::ContentNotFound { .. })
        ));
        reopened
            .close()
            .await
            .expect("reclaimed WASI database closes");
    });
    // wasm32-wasip1's std directory removal may report DirectoryNotEmpty for
    // nested host-preopened paths even after every database handle closes. The
    // test namespace is unique and the host harness owns final fixture cleanup.
    let _ = std::fs::remove_dir_all(path);
}

#[cfg(not(feature = "s3"))]
#[test]
fn qualified_object_store_sweep_binds_evidence_and_recovers_partial_delete() {
    block_on(qualified_object_store_sweep_impl());
}

#[cfg(feature = "s3")]
#[tokio::test(flavor = "multi_thread")]
async fn qualified_object_store_sweep_binds_evidence_and_recovers_partial_delete() {
    qualified_object_store_sweep_impl().await;
}

#[allow(clippy::too_many_lines)] // Provider evidence, reopen, fault, and retry form one safety proof.
async fn qualified_object_store_sweep_impl() {
    let prefix = "content-reclaim-sweep-object";
    let client = Arc::new(MeasuredClient::new());
    let probe_client: Arc<dyn ObjectClient> = client.clone();
    let evidence = ObjectStoreReclamationEvidenceDigest::for_bytes(
        b"test unversioned unlocked exclusive namespace revision 1",
    );
    let qualification = qualify_object_store_reclamation(
        Arc::clone(&probe_client),
        prefix,
        ObjectStoreReclamationAttestation::new(evidence),
    )
    .await
    .expect("object-store delete contract qualifies");
    let options = || {
        DbOptions::object_store().with_content_reclamation(
            ContentReclamationMode::QualifiedObjectStore(qualification.clone()),
        )
    };

    let db = Db::open_object_store_at(client.clone(), prefix, options())
        .await
        .expect("qualified object database opens");
    let sealed = seal_reclaim_content(&db, b"qualified object reclaim bytes").await;
    consume_reclaim_token(&db, sealed, 111).await;
    attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 112).await;
    let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 113).await;
    let mut grace_tx = db.transaction(TransactionOptions::default());
    grace_tx
        .stage_content_reclaim_grace(authorization, Duration::from_millis(1))
        .await
        .expect("object grace stages");
    grace_tx.commit().await.expect("object grace commits");
    let grace = db
        .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("object grace reads")
        .expect("object grace exists");
    let clock = ContentReclaimClockAttestation::new(
        grace,
        test_reclaim_clock_attestation_id(114),
        ContentReclaimClockCoordinatorId::from_bytes([115; 16]),
        ContentReclaimClockEvidenceDigest::for_bytes(b"object clock evidence"),
        grace.not_before_unix_ms(),
    )
    .expect("object clock binds grace");
    let mut prepare = db.transaction(TransactionOptions::default());
    let fresh = reclaim_authorization(&prepare, sealed, 116);
    assert_eq!(
        prepare
            .stage_content_reclaim_sweep(fresh, clock)
            .await
            .expect("object sweep stages"),
        ContentReclaimSweepStage::Staged
    );
    prepare.commit().await.expect("object Prepared commits");
    let prepared = db
        .content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("object Prepared reads")
        .expect("object Prepared exists");
    assert!(prepared.reclaimed_at().is_none());
    db.close().await.expect("object database closes");
    drop(db);

    let changed = qualify_object_store_reclamation(
        Arc::clone(&probe_client),
        prefix,
        ObjectStoreReclamationAttestation::new(ObjectStoreReclamationEvidenceDigest::for_bytes(
            b"test unversioned unlocked exclusive namespace revision 2",
        )),
    )
    .await
    .expect("changed evidence independently probes");
    let mismatched = Db::open_object_store_at(
        client.clone(),
        prefix,
        DbOptions::object_store()
            .with_content_reclamation(ContentReclamationMode::QualifiedObjectStore(changed)),
    )
    .await
    .expect("database opens with changed runtime evidence");
    assert!(matches!(
        mismatched
            .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await,
        Err(Error::UnsupportedBackend { .. })
    ));
    mismatched
        .close()
        .await
        .expect("mismatched database closes");
    drop(mismatched);

    let resumed = Db::open_object_store_at(client.clone(), prefix, options())
        .await
        .expect("database reopens with original evidence");
    client
        .report_provider_version
        .store(true, Ordering::Release);
    assert!(matches!(
        resumed
            .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await,
        Err(Error::UnsupportedBackend { .. })
    ));
    client
        .report_provider_version
        .store(false, Ordering::Release);
    client
        .fail_content_descriptor_delete_once
        .store(true, Ordering::Release);
    assert!(matches!(
        resumed
            .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await,
        Err(Error::Io(_))
    ));
    assert!(
        resumed
            .content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("failed object sweep remains readable")
            .is_some_and(|sweep| sweep.reclaimed_at().is_none())
    );
    let reclaimed = resumed
        .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("object sweep resumes after injected delete failure");
    assert!(reclaimed.reclaimed_at().is_some());
    resumed.close().await.expect("reclaimed database closes");
    drop(resumed);
    assert!(
        client
            .list(prefix)
            .await
            .expect("reclaimed object prefix lists")
            .iter()
            .all(|meta| {
                !meta.key.contains("/content-v1/chunks/") && !meta.key.contains("/descriptors/")
            })
    );
    drop(probe_client);
    drop(client);
}

#[cfg(feature = "s3")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires audited S3-compatible credentials and makes billable requests"]
#[allow(clippy::too_many_lines)] // Full provider qualification and reclaim lifecycle stays auditable.
async fn s3_live_qualified_content_reclamation() {
    use crate::s3::{ObjectStoreClient, S3ClientOptions};

    let Ok(bucket) = std::env::var("TRINE_S3_BUCKET") else {
        eprintln!("skipping live reclamation: TRINE_S3_BUCKET is not set");
        return;
    };
    let Ok(evidence) = std::env::var("TRINE_S3_RECLAMATION_EVIDENCE") else {
        eprintln!(
            "skipping live reclamation: set TRINE_S3_RECLAMATION_EVIDENCE only after auditing the exact provider namespace"
        );
        return;
    };
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".to_owned());
    let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
    let allow_http = std::env::var("TRINE_S3_ALLOW_HTTP").is_ok_and(|value| value == "1");
    let client: Arc<dyn ObjectClient> = Arc::new(
        ObjectStoreClient::s3_with_options(
            bucket,
            region,
            S3ClientOptions {
                endpoint,
                allow_http,
            },
        )
        .expect("live S3-compatible client builds"),
    );
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let prefix = format!("trine-kv-it/content-reclaim/{}-{nonce}", std::process::id());
    let qualification = qualify_object_store_reclamation(
        Arc::clone(&client),
        &prefix,
        ObjectStoreReclamationAttestation::new(ObjectStoreReclamationEvidenceDigest::for_bytes(
            evidence.as_bytes(),
        )),
    )
    .await
    .expect("live provider reclamation contract qualifies");
    let options = || {
        DbOptions::object_store().with_content_reclamation(
            ContentReclamationMode::QualifiedObjectStore(qualification.clone()),
        )
    };

    let db = Db::open_object_store_at(Arc::clone(&client), &prefix, options())
        .await
        .expect("live qualified database opens");
    let sealed = seal_reclaim_content(&db, b"live provider physical reclaim bytes").await;
    consume_reclaim_token(&db, sealed, 121).await;
    attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 122).await;
    let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 123).await;
    let mut grace_tx = db.transaction(TransactionOptions::default());
    grace_tx
        .stage_content_reclaim_grace(authorization, Duration::from_millis(1))
        .await
        .expect("live provider grace stages");
    grace_tx
        .commit()
        .await
        .expect("live provider grace commits");
    let grace = db
        .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("live provider grace reads")
        .expect("live provider grace exists");
    let clock = ContentReclaimClockAttestation::new(
        grace,
        test_reclaim_clock_attestation_id(124),
        ContentReclaimClockCoordinatorId::from_bytes([125; 16]),
        ContentReclaimClockEvidenceDigest::for_bytes(b"live provider monotonic clock evidence"),
        grace.not_before_unix_ms(),
    )
    .expect("live provider clock binds grace");
    let mut prepare = db.transaction(TransactionOptions::default());
    let fresh = reclaim_authorization(&prepare, sealed, 126);
    assert_eq!(
        prepare
            .stage_content_reclaim_sweep(fresh, clock)
            .await
            .expect("live provider sweep stages"),
        ContentReclaimSweepStage::Staged
    );
    prepare
        .commit()
        .await
        .expect("live provider Prepared commits");
    db.close().await.expect("live provider closes at Prepared");

    let reopened = Db::open_object_store_at(Arc::clone(&client), &prefix, options())
        .await
        .expect("live provider reopens at Prepared");
    let reclaimed = reopened
        .resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
        .await
        .expect("live provider sweep resumes");
    assert!(reclaimed.reclaimed_at().is_some());
    assert!(matches!(
        reopened
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([127; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
        Err(Error::ContentNotFound { .. })
    ));
    reopened
        .close()
        .await
        .expect("live provider database closes");

    let remaining = client.list(&prefix).await.expect("live prefix lists");
    assert!(
        remaining.iter().all(|meta| {
            !meta.key.contains("/content-v1/chunks/")
                && !meta.key.contains("/content-v1/descriptors/")
        }),
        "reclaimed content bytes remain in the provider namespace"
    );
    for meta in remaining {
        client
            .delete(&meta.key)
            .await
            .expect("live fixture object deletes");
    }
}

#[test]
fn refreshed_object_store_reader_observes_grace_without_content_delete() {
    block_on(async {
        let client = Arc::new(MeasuredClient::new());
        let prefix = "content-reclaim-grace-object";
        let writer = Db::open_object_store_at(
            client.clone(),
            prefix,
            DbOptions::object_store()
                .with_content_reclamation(ContentReclamationMode::QualifiedNativeFilesystem),
        )
        .await
        .expect("object-store writer opens");
        let reader = Db::open_object_store_at(
            client.clone(),
            prefix,
            DbOptions::object_store().read_only(),
        )
        .await
        .expect("object-store reader opens");
        let bytes = b"object-store reclaim grace";
        let sealed = seal_content_without_access_barrier(&writer, bytes).await;
        consume_reclaim_token(&writer, sealed, 96).await;
        reader
            .refresh_object_store()
            .await
            .expect("reader refreshes sealed state");
        let old = reader
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("pre-barrier object-store handle opens");
        writer
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(52))
            .await
            .expect("object-store barrier commits");
        attest_reclaim_reader_drain(&writer, sealed.storage_domain_id(), 52).await;
        let (authorization, _) = commit_reclaim_quarantine(&writer, sealed, 75).await;

        client.reset_counts();
        let mut grace = writer.transaction(TransactionOptions::default());
        grace
            .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
            .await
            .expect("object-store grace stages");
        grace.commit().await.expect("object-store grace commits");
        assert_eq!(client.counts().content_delete, 0);

        let grace_record = writer
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("object-store grace reads")
            .expect("object-store grace exists");
        let clock = ContentReclaimClockAttestation::new(
            grace_record,
            test_reclaim_clock_attestation_id(61),
            ContentReclaimClockCoordinatorId::from_bytes([62; 16]),
            ContentReclaimClockEvidenceDigest::for_bytes(b"object store must stay fail closed"),
            grace_record.not_before_unix_ms(),
        )
        .expect("clock claim is well formed");
        let mut unsupported = writer.transaction(TransactionOptions::default());
        let fresh = reclaim_authorization(&unsupported, sealed, 78);
        assert!(matches!(
            unsupported.stage_content_reclaim_sweep(fresh, clock).await,
            Err(Error::UnsupportedBackend { .. })
        ));
        assert_eq!(client.counts().content_delete, 0);

        reader
            .refresh_object_store()
            .await
            .expect("reader refreshes grace state");
        assert_eq!(
            reader
                .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("refreshed reader sees grace"),
            writer
                .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("writer sees grace")
        );
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("old object-store handle still reads")
                .as_ref(),
            bytes
        );
    });
}

#[test]
fn upload_activity_racing_staged_reclaim_grace_forces_conflict() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let bytes = b"reclaim grace upload race";
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 95).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 49).await;
        let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 74).await;
        let mut grace = db.transaction(TransactionOptions::default());
        grace
            .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
            .await
            .expect("grace stages before upload activity");

        let repeated = seal_content_without_access_barrier(&db, bytes).await;
        assert_eq!(repeated.content_id(), sealed.content_id());
        assert!(matches!(grace.commit().await, Err(Error::Conflict { .. })));
        assert!(
            db.content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("raced grace remains absent")
                .is_none()
        );
        assert!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("upload revival removes quarantine")
                .is_none()
        );
    });
}

#[test]
fn malformed_reclaim_grace_blocks_query_and_revival() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let bytes = b"malformed reclaim grace";
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 93).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 46).await;
        commit_reclaim_quarantine(&db, sealed, 73).await;

        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_reclaim_grace_key(sealed.storage_domain_id(), sealed.content_id()),
                b"damaged reclaim grace".to_vec(),
            )
            .expect("malformed grace stages");
        damage.commit().await.expect("malformed grace commits");
        assert!(matches!(
            db.content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        let mut unsafe_caller = db.transaction(TransactionOptions::default());
        assert!(matches!(
            unsafe_caller
                .stage_content_activity(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        unsafe_caller
            .commit()
            .await
            .expect("committing after validation error writes no lifecycle change");
        assert!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("failed revival retained quarantine")
                .is_some()
        );
        let mut upload = db
            .begin_content_upload(test_upload_options())
            .await
            .expect("revival upload begins");
        upload.write(bytes).await.expect("revival bytes write");
        assert!(matches!(
            upload.seal().await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("quarantine remains readable")
                .is_some()
        );
    });
}

#[test]
fn malformed_reclaim_sweep_blocks_query_and_authoritative_activity() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"malformed reclaim sweep").await;
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_reclaim_sweep_key(sealed.storage_domain_id(), sealed.content_id()),
                b"damaged reclaim sweep".to_vec(),
            )
            .expect("malformed sweep stages");
        damage.commit().await.expect("malformed sweep commits");

        assert!(matches!(
            db.content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        let mut activity = db.transaction(TransactionOptions::default());
        assert!(matches!(
            activity
                .stage_content_activity(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        activity
            .commit()
            .await
            .expect("failed validation stages no lifecycle writes");
    });
}
