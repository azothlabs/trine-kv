use super::*;

#[test]
fn reclaim_intent_checks_token_and_is_durable_and_idempotent() {
    let path = temp_db_path("content-reclaim-intent");
    let authorization = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let sealed = seal_reclaim_content(&db, b"reclaim intent bytes").await;
        let mut blocked = db.transaction(TransactionOptions::default());
        let blocked_authorization = reclaim_authorization(&blocked, sealed, 31);
        assert!(matches!(
            blocked
                .stage_content_reclaim_intent(blocked_authorization)
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::UploadToken { .. },
            })
        ));

        consume_reclaim_token(&db, sealed, 41).await;
        let mut accepted = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&accepted, sealed, 32);
        assert_eq!(
            accepted
                .stage_content_reclaim_intent(authorization)
                .await
                .expect("reclaim intent stages"),
            ContentReclaimIntentStage::Staged
        );
        accepted.commit().await.expect("reclaim intent commits");

        let mut repeated = db.transaction(TransactionOptions::default());
        assert!(matches!(
            repeated
                .stage_content_reclaim_intent(authorization)
                .await
                .expect("reclaim intent repeats"),
            ContentReclaimIntentStage::Existing { .. }
        ));
        authorization
    });

    block_on(async {
        let db = Db::open(&path).await.expect("native db reopens");
        let mut repeated = db.transaction(TransactionOptions::default());
        assert!(matches!(
            repeated
                .stage_content_reclaim_intent(authorization)
                .await
                .expect("durable reclaim intent reads"),
            ContentReclaimIntentStage::Existing { .. }
        ));
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn quarantine_requires_drain_and_exact_intent() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"quarantine state bytes").await;
        consume_reclaim_token(&db, sealed, 71).await;

        let mut without_drain = db.transaction(TransactionOptions::default());
        let first_authorization = reclaim_authorization(&without_drain, sealed, 61);
        assert!(matches!(
            without_drain
                .stage_content_quarantine(first_authorization)
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReaderDrainNotAttested { .. },
            })
        ));

        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 31).await;
        let mut without_intent = db.transaction(TransactionOptions::default());
        assert!(matches!(
            without_intent
                .stage_content_quarantine(reclaim_authorization(&without_intent, sealed, 62))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReclaimIntentRequired,
            })
        ));
    });
}

#[test]
fn quarantine_blocks_reads_is_idempotent_and_can_revive() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"quarantine state bytes").await;
        consume_reclaim_token(&db, sealed, 71).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 31).await;
        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 63);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        intent.commit().await.expect("reclaim intent commits");

        let mut quarantine = db.transaction(TransactionOptions::default());
        assert_eq!(
            quarantine
                .stage_content_quarantine(authorization)
                .await
                .expect("quarantine stages"),
            ContentQuarantineStage::Staged
        );
        let commit = quarantine.commit().await.expect("quarantine commits");
        let record = db
            .content_quarantine(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("quarantine reads")
            .expect("quarantine is durable");
        assert_eq!(record.quarantined_at(), commit.read_version());
        assert_eq!(record.proof_token(), authorization.proof_token());
        assert_eq!(record.verified_at(), authorization.verified_at());
        assert!(matches!(
            db.open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([72; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::ContentQuarantined { quarantined_at })
                if quarantined_at == commit.read_version()
        ));

        let mut repeated = db.transaction(TransactionOptions::default());
        assert_eq!(
            repeated
                .stage_content_quarantine(authorization)
                .await
                .expect("quarantine retry reads durable state"),
            ContentQuarantineStage::Existing {
                quarantined_at: commit.read_version(),
            }
        );

        let hold = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(73),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Repair,
                    ContentPhysicalHoldOwnerId::from_bytes([73; 16]),
                ),
            )
            .await
            .expect("repair hold returns quarantined content to active");
        assert_eq!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("revived quarantine status reads"),
            None
        );
        db.open_content_leased(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentLeaseOptions::new(
                ContentLeaseOwnerId::from_bytes([74; 16]),
                Duration::from_mins(1),
            ),
        )
        .await
        .expect("leased read succeeds after revival");
        hold.release().await.expect("repair hold releases");
    });
}

#[test]
fn new_upload_authority_atomically_revives_quarantined_content() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let bytes = b"quarantine token revival";
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 75).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 35).await;

        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 65);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        intent.commit().await.expect("reclaim intent commits");
        let mut quarantine = db.transaction(TransactionOptions::default());
        assert_eq!(
            quarantine
                .stage_content_quarantine(authorization)
                .await
                .expect("quarantine stages"),
            ContentQuarantineStage::Staged
        );
        quarantine.commit().await.expect("quarantine commits");

        let repeated_content = seal_content_without_access_barrier(&db, bytes).await;
        assert_eq!(repeated_content.content_id(), sealed.content_id());
        assert_eq!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("revived quarantine status reads"),
            None
        );
        let mut stale_retry = db.transaction(TransactionOptions::default());
        assert!(matches!(
            stale_retry.stage_content_quarantine(authorization).await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReclaimIntentRequired,
            })
        ));
    });
}

#[test]
fn leased_open_racing_staged_quarantine_forces_transaction_conflict() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"quarantine lease race").await;
        consume_reclaim_token(&db, sealed, 75).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 34).await;
        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 64);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        intent.commit().await.expect("reclaim intent commits");

        let mut quarantine = db.transaction(TransactionOptions::default());
        quarantine
            .stage_content_quarantine(authorization)
            .await
            .expect("quarantine stages before lease");
        db.open_content_leased(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentLeaseOptions::new(
                ContentLeaseOwnerId::from_bytes([76; 16]),
                Duration::from_mins(1),
            ),
        )
        .await
        .expect("concurrent leased open commits first");
        assert!(matches!(
            quarantine.commit().await,
            Err(Error::Conflict { .. })
        ));
        assert_eq!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("failed quarantine remains absent"),
            None
        );
    });
}

#[test]
fn quarantine_survives_native_reopen_and_keeps_leased_reads_fenced() {
    let path = temp_db_path("content-quarantine-native-reopen");
    let (domain, content_id, quarantined_at) = block_on(async {
        let db = Db::open(&path).await.expect("native writer opens");
        let sealed = seal_reclaim_content(&db, b"durable quarantine bytes").await;
        consume_reclaim_token(&db, sealed, 77).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 37).await;
        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 65);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("native reclaim intent stages");
        intent
            .commit()
            .await
            .expect("native reclaim intent commits");
        let mut quarantine = db.transaction(TransactionOptions::default());
        quarantine
            .stage_content_quarantine(authorization)
            .await
            .expect("native quarantine stages");
        let commit = quarantine
            .commit()
            .await
            .expect("native quarantine commits");
        (
            sealed.storage_domain_id(),
            sealed.content_id(),
            commit.read_version(),
        )
    });

    block_on(async {
        let reopened = Db::open(&path).await.expect("native writer reopens");
        let quarantine = reopened
            .content_quarantine(domain, content_id)
            .await
            .expect("reopened quarantine reads")
            .expect("reopened quarantine exists");
        assert_eq!(quarantine.quarantined_at(), quarantined_at);
        assert!(matches!(
            reopened
                .open_content_leased(
                    domain,
                    content_id,
                    ContentLeaseOptions::new(
                        ContentLeaseOwnerId::from_bytes([78; 16]),
                        Duration::from_mins(1),
                    ),
                )
                .await,
            Err(Error::ContentQuarantined { quarantined_at: blocked })
                if blocked == quarantined_at
        ));
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn malformed_quarantine_fails_reads_and_revival_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"malformed quarantine bytes").await;
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_quarantine_key(sealed.storage_domain_id(), sealed.content_id()),
                b"malformed".to_vec(),
            )
            .expect("malformed quarantine stages");
        damage.commit().await.expect("malformed quarantine commits");

        assert!(matches!(
            db.content_quarantine(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(matches!(
            db.open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([79; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::InvalidFormat { .. })
        ));
        assert!(matches!(
            db.acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(80),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Administrative,
                    ContentPhysicalHoldOwnerId::from_bytes([80; 16]),
                ),
            )
            .await,
            Err(Error::InvalidFormat { .. })
        ));
    });
}

#[test]
fn reclaim_grace_requires_quarantine_is_idempotent_and_keeps_bytes() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let bytes = b"reclaim grace retains bytes";
        let sealed = seal_content_without_access_barrier(&db, bytes).await;
        let old = db
            .open_content(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("pre-barrier handle opens");
        db.enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(40))
            .await
            .expect("leased-only barrier commits");
        consume_reclaim_token(&db, sealed, 91).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 40).await;

        let mut intent = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&intent, sealed, 71);
        intent
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        intent.commit().await.expect("reclaim intent commits");
        let mut without_quarantine = db.transaction(TransactionOptions::default());
        assert!(matches!(
            without_quarantine
                .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::QuarantineRequired,
            })
        ));

        let mut quarantine = db.transaction(TransactionOptions::default());
        quarantine
            .stage_content_quarantine(authorization)
            .await
            .expect("quarantine stages");
        let quarantine_commit = quarantine.commit().await.expect("quarantine commits");
        let mut grace = db.transaction(TransactionOptions::default());
        assert_eq!(
            grace
                .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
                .await
                .expect("grace stages"),
            ContentReclaimGraceStage::Staged
        );
        let grace_commit = grace.commit().await.expect("grace commits");
        let record = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("grace reads")
            .expect("grace exists");
        assert_eq!(record.quarantined_at(), quarantine_commit.read_version());
        assert_eq!(record.started_at(), grace_commit.read_version());
        assert_eq!(record.requested_duration_ms(), 60_000);
        assert_eq!(
            record.not_before_unix_ms(),
            record.observed_at_unix_ms() + record.requested_duration_ms()
        );
        assert_eq!(
            old.read_range(0, u64::MAX)
                .await
                .expect("old handle still reads because grace deletes nothing")
                .as_ref(),
            bytes
        );

        let mut repeated = db.transaction(TransactionOptions::default());
        assert_eq!(
            repeated
                .stage_content_reclaim_grace(authorization, Duration::from_mins(1))
                .await
                .expect("grace retry reads existing record"),
            ContentReclaimGraceStage::Existing {
                started_at: grace_commit.read_version(),
            }
        );
        let mut different = db.transaction(TransactionOptions::default());
        assert!(matches!(
            different
                .stage_content_reclaim_grace(authorization, Duration::from_secs(61))
                .await,
            Err(Error::InvalidOptions { .. })
        ));
        assert_provider_hold_revives_grace(&db, sealed).await;
    });
}

#[test]
fn fresh_proof_recovers_quarantine_committed_before_grace() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_content_without_access_barrier(
            &db,
            b"fresh proof recovers continuously quarantined content",
        )
        .await;
        db.enforce_content_leased_only(sealed.storage_domain_id(), test_access_barrier_id(42))
            .await
            .expect("leased-only barrier commits");
        consume_reclaim_token(&db, sealed, 92).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 42).await;

        let (original, quarantined_at) = commit_reclaim_quarantine(&db, sealed, 78).await;
        let durable_quarantine = db
            .content_quarantine(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("quarantine reads")
            .expect("quarantine is durable");
        assert_eq!(durable_quarantine.quarantined_at(), quarantined_at);

        let mut future = db.transaction(TransactionOptions::default());
        let future_authorization = ContentReclaimAuthorization::new(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentReclaimProofToken::from_bytes([80; 49]),
            ReadVersion::from_u64(future.read_version().as_u64().saturating_add(1)),
            u64::MAX,
        );
        assert!(matches!(
            future
                .stage_content_reclaim_grace(future_authorization, Duration::from_mins(1),)
                .await,
            Err(Error::InvalidOptions { .. })
        ));

        let mut recovery = db.transaction(TransactionOptions::default());
        let fresh = reclaim_authorization(&recovery, sealed, 79);
        assert_ne!(fresh.proof_token(), original.proof_token());
        assert!(fresh.verified_at().as_u64() >= quarantined_at.as_u64());
        assert_eq!(
            recovery
                .stage_content_reclaim_grace(fresh, Duration::from_mins(1))
                .await
                .expect("fresh proof stages grace over continuous quarantine"),
            ContentReclaimGraceStage::Staged
        );
        let committed = recovery.commit().await.expect("recovered grace commits");
        let grace = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("recovered grace reads")
            .expect("recovered grace exists");
        assert_eq!(grace.quarantined_at(), quarantined_at);
        assert_eq!(grace.proof_token(), original.proof_token());
        assert_eq!(grace.started_at(), committed.read_version());
    });
}

#[test]
fn reclaim_grace_survives_reopen_and_upload_activity_revives_content() {
    let path = temp_db_path("content-reclaim-grace-reopen");
    let (domain, content_id, recorded) = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let bytes = b"durable reclaim grace";
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 92).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 43).await;
        let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 72).await;
        let mut grace = db.transaction(TransactionOptions::default());
        grace
            .stage_content_reclaim_grace(authorization, Duration::from_mins(2))
            .await
            .expect("native grace stages");
        grace.commit().await.expect("native grace commits");
        let record = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("native grace reads")
            .expect("native grace exists");
        (sealed.storage_domain_id(), sealed.content_id(), record)
    });

    block_on(async {
        let db = Db::open(&path).await.expect("native db reopens");
        assert_eq!(
            db.content_reclaim_grace(domain, content_id)
                .await
                .expect("reopened grace reads"),
            Some(recorded)
        );
        let repeated = seal_content_without_access_barrier(&db, b"durable reclaim grace").await;
        assert_eq!(repeated.content_id(), content_id);
        assert_eq!(
            db.content_reclaim_grace(domain, content_id)
                .await
                .expect("revived grace status reads"),
            None
        );
        assert_eq!(
            db.content_quarantine(domain, content_id)
                .await
                .expect("revived quarantine status reads"),
            None
        );
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
#[allow(clippy::too_many_lines)] // One reopen/fault/re-upload lifecycle must stay ordered.
fn qualified_filesystem_sweep_reclaims_after_reopen_and_allows_later_reupload() {
    let path = temp_db_path("content-reclaim-sweep-native-reopen");
    let options = || {
        DbOptions::new(&path)
            .with_content_reclamation(ContentReclamationMode::QualifiedNativeFilesystem)
    };
    let bytes = b"durable physical reclaim bytes";
    let (sealed, prepared_at) = block_on(async {
        let db = Db::open(options()).await.expect("native db opens");
        let sealed = seal_reclaim_content(&db, bytes).await;
        consume_reclaim_token(&db, sealed, 97).await;
        attest_reclaim_reader_drain(&db, sealed.storage_domain_id(), 55).await;
        let (authorization, _) = commit_reclaim_quarantine(&db, sealed, 76).await;
        let mut grace_tx = db.transaction(TransactionOptions::default());
        grace_tx
            .stage_content_reclaim_grace(authorization, Duration::from_millis(1))
            .await
            .expect("grace stages");
        grace_tx.commit().await.expect("grace commits");
        let grace = db
            .content_reclaim_grace(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("grace reads")
            .expect("grace is durable");
        assert!(matches!(
            ContentReclaimClockAttestation::new(
                grace,
                test_reclaim_clock_attestation_id(54),
                ContentReclaimClockCoordinatorId::from_bytes([55; 16]),
                ContentReclaimClockEvidenceDigest::for_bytes(b"premature clock claim"),
                grace.not_before_unix_ms() - 1,
            ),
            Err(Error::InvalidOptions { .. })
        ));
        let clock = ContentReclaimClockAttestation::new(
            grace,
            test_reclaim_clock_attestation_id(56),
            ContentReclaimClockCoordinatorId::from_bytes([57; 16]),
            ContentReclaimClockEvidenceDigest::for_bytes(b"trusted restart and monotonic evidence"),
            grace.not_before_unix_ms(),
        )
        .expect("trusted clock claim binds grace");
        let mut prepare = db.transaction(TransactionOptions::default());
        let expired_at_trusted_observation = ContentReclaimAuthorization::new(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentReclaimProofToken::from_bytes([78; 49]),
            prepare.read_version(),
            clock.observed_at_unix_ms(),
        );
        assert!(matches!(
            prepare
                .stage_content_reclaim_sweep(expired_at_trusted_observation, clock)
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ProofExpired { .. },
            })
        ));
        let fresh = reclaim_authorization(&prepare, sealed, 77);
        assert_eq!(
            prepare
                .stage_content_reclaim_sweep(fresh, clock)
                .await
                .expect("final sweep stages"),
            ContentReclaimSweepStage::Staged
        );
        let commit = prepare.commit().await.expect("Prepared sweep commits");
        let sweep = db
            .content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("Prepared sweep reads")
            .expect("Prepared sweep exists");
        assert_eq!(sweep.prepared_at(), commit.read_version());
        assert_eq!(sweep.reclaimed_at(), None);
        assert!(matches!(
            db.open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([58; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::SweepPrepared { .. },
            })
        ));
        assert!(matches!(
            db.acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(61),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Backup,
                    ContentPhysicalHoldOwnerId::from_bytes([61; 16]),
                ),
            )
            .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::SweepPrepared { .. },
            })
        ));
        db.close().await.expect("native db closes");
        (sealed, commit.read_version())
    });

    block_on(async {
        let db = Db::open(options()).await.expect("native db reopens");
        let prepared = db
            .content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
            .await
            .expect("Prepared sweep survives reopen")
            .expect("Prepared sweep remains");
        assert_eq!(prepared.prepared_at(), prepared_at);
        let descriptor_fault = StorageFaultGuard::install(
            &path,
            StorageFaultPoint::ObjectDelete,
            Some(StorageObjectKind::ContentDescriptor),
            1,
        );
        assert!(matches!(
            db.resume_content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
                .await,
            Err(Error::Io(_))
        ));
        assert!(
            db.content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("failed delete keeps sweep readable")
                .is_some_and(|sweep| sweep.reclaimed_at().is_none())
        );
        drop(descriptor_fault);
        db.close().await.expect("failed sweep db closes");
    });

    block_on(async {
        let db = Db::open(options())
            .await
            .expect("native db reopens after delete fault");
        let (first, second) =
            thread::scope(|scope| {
                let first = scope.spawn(|| {
                    block_on(db.resume_content_reclaim_sweep(
                        sealed.storage_domain_id(),
                        sealed.content_id(),
                    ))
                });
                let second = scope.spawn(|| {
                    block_on(db.resume_content_reclaim_sweep(
                        sealed.storage_domain_id(),
                        sealed.content_id(),
                    ))
                });
                (
                    first.join().expect("first sweep thread joins"),
                    second.join().expect("second sweep thread joins"),
                )
            });
        let first = first.expect("first concurrent sweep completes");
        let second = second.expect("second concurrent sweep is idempotent");
        assert!(first.reclaimed_at().is_some());
        assert_eq!(first, second);
        assert!(matches!(
            db.open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([59; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::ContentNotFound { .. })
        ));
        assert_eq!(
            db.content_physical_quota(sealed.storage_domain_id())
                .await
                .expect("reclaimed physical accounting reads")
                .unique_content_bytes(),
            0
        );
        let replacement = seal_content_without_access_barrier(&db, bytes).await;
        assert_eq!(replacement.content_id(), sealed.content_id());
        assert_eq!(
            db.content_physical_quota(sealed.storage_domain_id())
                .await
                .expect("replacement physical accounting reads")
                .unique_content_bytes(),
            bytes.len() as u64
        );
        assert_eq!(
            db.content_reclaim_sweep(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("replacement clears tombstone"),
            None
        );
        let handle = db
            .open_content_leased(
                replacement.storage_domain_id(),
                replacement.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([60; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("replacement opens under a lease");
        assert_eq!(
            handle
                .read_range(0, u64::MAX)
                .await
                .expect("replacement bytes read")
                .as_ref(),
            bytes
        );
        db.close().await.expect("replacement db closes");
    });
    std::fs::remove_dir_all(path).expect("test database removes");
}
