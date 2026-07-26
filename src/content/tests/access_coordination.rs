use super::*;

#[test]
fn leased_only_barrier_fences_new_unleased_opens_but_not_old_handles() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"barrier bytes").await;
        assert_eq!(
            db.content_access_mode(sealed.storage_domain_id())
                .await
                .expect("compatible mode reads"),
            ContentAccessMode::CompatibleUnleased
        );
        let old = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("unleased content opens before barrier");

        let barrier = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(3))
            .await
            .expect("leased-only barrier commits");
        assert!(barrier.enforced_at().as_u64() > 0);
        assert_eq!(
            db.content_access_mode(sealed.storage_domain_id())
                .await
                .expect("leased-only mode reads"),
            ContentAccessMode::LeasedOnly {
                barrier_id: barrier.barrier_id(),
            }
        );
        let repeated = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(4))
            .await
            .expect("existing barrier is adopted");
        assert_eq!(repeated, barrier);
        assert!(matches!(
            db.open_content(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::ContentLeaseRequired { barrier_id })
                if barrier_id == barrier.barrier_id()
        ));
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("pre-barrier handle still reads")
                .as_ref(),
            b"barrier bytes"
        );
        let leased = db
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([5; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("leased content opens after barrier");
        assert!(leased.lease_id().is_some());
    });
}

#[test]
fn reader_drain_attestation_is_barrier_bound_idempotent_and_does_not_close_handles() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"attested drain bytes").await;
        let old = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("pre-barrier handle opens");
        let barrier = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(13))
            .await
            .expect("leased-only barrier commits");
        let attestation_id = test_reader_drain_attestation_id(14);
        let options =
            test_reader_drain_options(ContentReaderDrainKind::NativeProcessSetRestarted, 15);
        let attestation = db
            .attest_content_reader_drain(barrier, attestation_id, options)
            .await
            .expect("trusted coordinator claim records");
        assert_eq!(attestation.storage_domain_id(), sealed.storage_domain_id());
        assert_eq!(attestation.barrier_id(), barrier.barrier_id());
        assert_eq!(attestation.attestation_id(), attestation_id);
        assert_eq!(attestation.options(), options);
        assert_eq!(attestation.barrier_enforced_at(), barrier.enforced_at());
        assert!(attestation.attested_at().as_u64() > barrier.enforced_at().as_u64());
        assert_eq!(
            db.content_reader_drain_attestation(sealed.storage_domain_id())
                .await
                .expect("attestation reads"),
            Some(attestation)
        );
        assert_eq!(
            db.attest_content_reader_drain(barrier, attestation_id, options)
                .await
                .expect("lost-response retry returns original"),
            attestation
        );
        assert!(matches!(
            db.attest_content_reader_drain(barrier, test_reader_drain_attestation_id(16), options,)
                .await,
            Err(Error::InvalidOptions { .. })
        ));
        assert!(matches!(
            db.attest_content_reader_drain(
                barrier,
                attestation_id,
                test_reader_drain_options(ContentReaderDrainKind::DomainBootstrap, 15),
            )
            .await,
            Err(Error::InvalidOptions { .. })
        ));

        // This intentionally dishonest coordinator call demonstrates the trust
        // boundary: persistence cannot terminate or observe an old handle.
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("old handle remains readable")
                .as_ref(),
            b"attested drain bytes"
        );
    });
}
#[test]
fn reader_drain_attestation_requires_an_active_exact_barrier_and_fails_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let domain = test_scope().storage_domain_id();
        assert_eq!(
            db.content_reader_drain_attestation(domain)
                .await
                .expect("compatible domain has no attestation"),
            None
        );
        let forged =
            ContentAccessBarrier::new(domain, test_access_barrier_id(17), ReadVersion::from_u64(1));
        assert!(matches!(
            db.attest_content_reader_drain(
                forged,
                test_reader_drain_attestation_id(18),
                test_reader_drain_options(ContentReaderDrainKind::DomainBootstrap, 19),
            )
            .await,
            Err(Error::InvalidOptions { .. })
        ));

        let barrier = db
            .enforce_content_leased_only(domain, test_access_barrier_id(20))
            .await
            .expect("leased-only barrier commits");
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_reader_drain_attestation_key(domain),
                b"malformed".to_vec(),
            )
            .expect("malformed attestation stages");
        damage
            .commit()
            .await
            .expect("malformed attestation commits");
        assert!(matches!(
            db.content_reader_drain_attestation(domain).await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(matches!(
            db.attest_content_reader_drain(
                barrier,
                test_reader_drain_attestation_id(21),
                test_reader_drain_options(ContentReaderDrainKind::DomainBootstrap, 22),
            )
            .await,
            Err(Error::InvalidFormat { .. })
        ));
    });
}

#[test]
fn reclaim_intent_requires_coordinated_leased_only_barrier() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"access-gated reclaim").await;
        consume_reclaim_token(&db, sealed, 40).await;
        let mut compatible = db.transaction(TransactionOptions::default());
        assert!(matches!(
            compatible
                .stage_content_reclaim_intent(reclaim_authorization(&compatible, sealed, 30))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::UnleasedAccessAllowed,
            })
        ));

        let barrier_id = test_access_barrier_id(6);
        db.write_content_access_barrier_record(ContentAccessBarrierRecord {
            storage_domain_id: sealed.storage_domain_id(),
            barrier_id,
        })
        .await
        .expect("backend barrier publishes without coordinate");
        let mut interrupted = db.transaction(TransactionOptions::default());
        assert!(matches!(
            interrupted
                .stage_content_reclaim_intent(reclaim_authorization(&interrupted, sealed, 31))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::LeasedOnlyBarrierUncoordinated {
                    barrier_id: blocked,
                },
            }) if blocked == barrier_id
        ));

        let barrier = db
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(7))
            .await
            .expect("interrupted barrier coordinate resumes");
        assert_eq!(barrier.barrier_id(), barrier_id);
        let mut coordinated = db.transaction(TransactionOptions::default());
        coordinated
            .stage_content_reclaim_intent(reclaim_authorization(&coordinated, sealed, 32))
            .await
            .expect("coordinated leased-only barrier permits intent");
    });
}

#[test]
fn malformed_access_barrier_fails_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(&db, b"malformed access barrier").await;
        db.write_content_access_barrier_bytes_for_test(
            sealed.storage_domain_id(),
            Arc::<[u8]>::from(b"malformed".as_slice()),
        )
        .await
        .expect("malformed barrier publishes for test");
        assert!(matches!(
            db.content_access_mode(sealed.storage_domain_id()).await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(matches!(
            db.open_content(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
    });
}

#[test]
fn stale_native_read_only_handle_observes_leased_only_barrier_without_refresh() {
    let path = temp_db_path("content-access-barrier-native-reader");
    block_on(async {
        let writer = Db::open(&path).await.expect("native writer opens");
        let sealed = seal_content_without_access_barrier(&writer, b"native stale reader").await;
        let reader = Db::open(DbOptions::persistent_read_only(&path))
            .await
            .expect("native read-only handle opens before barrier");
        writer
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(8))
            .await
            .expect("writer publishes barrier");

        assert!(matches!(
            reader
                .open_content(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::ContentLeaseRequired { .. })
        ));
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn stale_object_store_reader_observes_leased_only_barrier_without_kv_refresh() {
    block_on(async {
        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let writer = Db::open_object_store_at(
            Arc::clone(&client),
            "content-access-barrier-object",
            DbOptions::object_store(),
        )
        .await
        .expect("object-store writer opens");
        let sealed = seal_content_without_access_barrier(&writer, b"object stale reader").await;
        let reader = Db::open_object_store_at(
            Arc::clone(&client),
            "content-access-barrier-object",
            DbOptions::object_store().read_only(),
        )
        .await
        .expect("stale object-store reader opens before barrier");
        writer
            .enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(9))
            .await
            .expect("object-store barrier publishes");

        assert!(matches!(
            reader
                .open_content(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::ContentLeaseRequired { .. })
        ));
    });
}

#[test]
fn reader_drain_attestation_survives_native_reopen() {
    let path = temp_db_path("content-reader-drain-native-reopen");
    let domain = test_scope().storage_domain_id();
    let recorded = block_on(async {
        let db = Db::open(&path).await.expect("native writer opens");
        let barrier = db
            .enforce_content_leased_only(domain, test_access_barrier_id(23))
            .await
            .expect("native barrier commits");
        db.attest_content_reader_drain(
            barrier,
            test_reader_drain_attestation_id(24),
            test_reader_drain_options(ContentReaderDrainKind::NativeProcessSetRestarted, 25),
        )
        .await
        .expect("native reader drain attests")
    });
    block_on(async {
        let reopened = Db::open(&path).await.expect("native writer reopens");
        assert_eq!(
            reopened
                .content_reader_drain_attestation(domain)
                .await
                .expect("native attestation reads after reopen"),
            Some(recorded)
        );
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn refreshed_object_store_reader_observes_remote_drain_attestation() {
    block_on(async {
        let client: Arc<dyn ObjectClient> = Arc::new(InMemoryObjectStore::new());
        let prefix = "content-reader-drain-object";
        let writer =
            Db::open_object_store_at(Arc::clone(&client), prefix, DbOptions::object_store())
                .await
                .expect("object-store writer opens");
        let reader = Db::open_object_store_at(
            Arc::clone(&client),
            prefix,
            DbOptions::object_store().read_only(),
        )
        .await
        .expect("object-store reader opens before attestation");
        let domain = test_scope().storage_domain_id();
        let barrier = writer
            .enforce_content_leased_only(domain, test_access_barrier_id(26))
            .await
            .expect("object-store barrier commits");
        let attestation = writer
            .attest_content_reader_drain(
                barrier,
                test_reader_drain_attestation_id(27),
                test_reader_drain_options(ContentReaderDrainKind::RemoteCredentialEpochRetired, 28),
            )
            .await
            .expect("remote reader drain attests");

        reader
            .refresh_object_store()
            .await
            .expect("reader refreshes protected KV state");
        assert_eq!(
            reader
                .content_reader_drain_attestation(domain)
                .await
                .expect("refreshed reader sees attestation"),
            Some(attestation)
        );
    });
}
