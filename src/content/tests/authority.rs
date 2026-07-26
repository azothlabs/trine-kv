use super::*;

#[test]
fn reclaim_intent_is_blocked_by_lease_and_later_lease_supersedes_it() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"leased reclaim bytes").await;
        consume_reclaim_token(&db, sealed, 42).await;
        let handle = db
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([12; 16]),
                    Duration::from_millis(2),
                ),
            )
            .await
            .expect("leased content opens");
        let mut blocked = db.transaction(TransactionOptions::default());
        assert!(matches!(
            blocked
                .stage_content_reclaim_intent(reclaim_authorization(&blocked, sealed, 33))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ReadLease { .. },
            })
        ));
        thread::sleep(Duration::from_millis(10));

        let mut accepted = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&accepted, sealed, 34);
        accepted
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("expired lease permits intent");
        accepted.commit().await.expect("reclaim intent commits");

        let newer = db
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([13; 16]),
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("new lease cancels intent");
        assert!(newer.lease_id().is_some());
        assert!(handle.lease_id().is_some());
        let mut stale = db.transaction(TransactionOptions::default());
        assert!(matches!(
            stale.stage_content_reclaim_intent(authorization).await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded { .. },
            })
        ));
    });
}

#[test]
fn concurrent_lease_conflicts_with_staged_reclaim_intent() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"concurrent reclaim bytes").await;
        consume_reclaim_token(&db, sealed, 43).await;
        let mut reclaim = db.transaction(TransactionOptions::default());
        reclaim
            .stage_content_reclaim_intent(reclaim_authorization(&reclaim, sealed, 35))
            .await
            .expect("reclaim intent stages");

        db.open_content_leased(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentLeaseOptions::new(
                ContentLeaseOwnerId::from_bytes([14; 16]),
                Duration::from_mins(1),
            ),
        )
        .await
        .expect("concurrent lease commits");
        assert!(matches!(
            reclaim.commit().await,
            Err(Error::Conflict { .. })
        ));
    });
}

#[test]
fn new_upload_authority_supersedes_reclaim_intent() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"repeated content bytes").await;
        consume_reclaim_token(&db, sealed, 45).await;
        let mut accepted = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&accepted, sealed, 38);
        accepted
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("reclaim intent stages");
        accepted.commit().await.expect("reclaim intent commits");

        let newer = seal_reclaim_content(&db, b"repeated content bytes").await;
        assert_eq!(newer.content_id(), sealed.content_id());
        let mut stale = db.transaction(TransactionOptions::default());
        assert!(matches!(
            stale.stage_content_reclaim_intent(authorization).await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded { .. },
            })
        ));
    });
}

#[test]
fn malformed_control_and_expired_proof_fail_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"malformed reclaim bytes").await;
        consume_reclaim_token(&db, sealed, 44).await;
        let mut expired = db.transaction(TransactionOptions::default());
        let expired_authorization = ContentReclaimAuthorization::new(
            sealed.storage_domain_id(),
            sealed.content_id(),
            ContentReclaimProofToken::from_bytes([36; 49]),
            expired.read_version(),
            1,
        );
        assert!(matches!(
            expired
                .stage_content_reclaim_intent(expired_authorization)
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::ProofExpired { .. },
            })
        ));

        let token_damaged = seal_reclaim_content(&db, b"malformed token index bytes").await;
        let mut damage_token = db.transaction(TransactionOptions::default());
        damage_token
            .put_internal_bucket(
                CONTENT_TOKEN_INDEX_BUCKET,
                content_token_index_key(
                    token_damaged.storage_domain_id(),
                    token_damaged.content_id(),
                    token_damaged.upload_token(),
                ),
                b"malformed".to_vec(),
            )
            .expect("token-index damage stages");
        damage_token
            .commit()
            .await
            .expect("token-index damage commits");
        let mut token_reclaim = db.transaction(TransactionOptions::default());
        assert!(matches!(
            token_reclaim
                .stage_content_reclaim_intent(reclaim_authorization(
                    &token_reclaim,
                    token_damaged,
                    39,
                ))
                .await,
            Err(Error::InvalidFormat { .. })
        ));

        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_CONTROL_BUCKET,
                content_control_key(sealed.storage_domain_id(), sealed.content_id()),
                b"malformed".to_vec(),
            )
            .expect("damage stages");
        damage.commit().await.expect("damage commits");
        let mut reclaim = db.transaction(TransactionOptions::default());
        assert!(matches!(
            reclaim
                .stage_content_reclaim_intent(reclaim_authorization(&reclaim, sealed, 37))
                .await,
            Err(Error::InvalidFormat { .. })
        ));
    });
}

#[test]
fn physical_hold_blocks_intent_release_allows_it_and_later_hold_supersedes() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"physical hold reclaim bytes").await;
        consume_reclaim_token(&db, sealed, 46).await;
        let owner = ContentPhysicalHoldOwnerId::from_bytes([17; 16]);
        let hold_id = test_hold_id(22);
        let hold = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                ContentPhysicalHoldOptions::until_released(ContentPhysicalHoldKind::Backup, owner),
            )
            .await
            .expect("backup hold acquires");
        let repeated = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                ContentPhysicalHoldOptions::until_released(ContentPhysicalHoldKind::Backup, owner),
            )
            .await
            .expect("lost-response acquisition retries exactly");
        assert_eq!(repeated.id(), hold.id());
        assert_eq!(repeated.expires_at_unix_ms(), None);
        let clone = hold.clone();
        let mut blocked = db.transaction(TransactionOptions::default());
        assert!(matches!(
            blocked
                .stage_content_reclaim_intent(reclaim_authorization(&blocked, sealed, 40))
                .await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::PhysicalHold {
                    kind: ContentPhysicalHoldKind::Backup,
                    expires_at_unix_ms: None,
                    ..
                },
            })
        ));

        hold.release().await.expect("backup hold releases");
        assert!(clone.is_released());
        clone.release().await.expect("release retry is idempotent");
        assert!(matches!(
            db.resume_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                owner,
            )
            .await,
            Err(Error::ContentPhysicalHoldNotFound { .. })
        ));
        assert!(matches!(
            db.acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                ContentPhysicalHoldOptions::until_released(ContentPhysicalHoldKind::Backup, owner,),
            )
            .await,
            Err(Error::ContentPhysicalHoldNotFound { .. })
        ));
        let mut accepted = db.transaction(TransactionOptions::default());
        let authorization = reclaim_authorization(&accepted, sealed, 41);
        accepted
            .stage_content_reclaim_intent(authorization)
            .await
            .expect("released hold permits intent");
        accepted.commit().await.expect("reclaim intent commits");

        let migration = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(23),
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Migration,
                    owner,
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("later migration hold acquires");
        assert!(migration.expires_at_unix_ms().is_some());
        let mut stale = db.transaction(TransactionOptions::default());
        assert!(matches!(
            stale.stage_content_reclaim_intent(authorization).await,
            Err(Error::ContentReclaimBlocked {
                blocker: ContentReclaimBlocker::Superseded { .. },
            })
        ));
    });
}

#[test]
fn every_physical_hold_kind_enters_the_same_reclaim_fence() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        for (index, kind) in [
            ContentPhysicalHoldKind::Migration,
            ContentPhysicalHoldKind::Backup,
            ContentPhysicalHoldKind::Repair,
            ContentPhysicalHoldKind::Provider,
            ContentPhysicalHoldKind::Administrative,
            ContentPhysicalHoldKind::Processing,
            ContentPhysicalHoldKind::Offline,
        ]
        .into_iter()
        .enumerate()
        {
            let label = format!("physical hold class {kind}");
            let sealed = seal_reclaim_content(&db, label.as_bytes()).await;
            let change = u8::try_from(60 + index).expect("test change fits");
            consume_reclaim_token(&db, sealed, change).await;
            let hold = db
                .acquire_content_physical_hold(
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                    test_hold_id(change),
                    ContentPhysicalHoldOptions::until_released(
                        kind,
                        ContentPhysicalHoldOwnerId::from_bytes([change; 16]),
                    ),
                )
                .await
                .expect("physical hold class acquires");
            let mut blocked = db.transaction(TransactionOptions::default());
            let result = blocked
                .stage_content_reclaim_intent(reclaim_authorization(
                    &blocked,
                    sealed,
                    change.saturating_add(10),
                ))
                .await;
            assert!(matches!(
                result,
                Err(Error::ContentReclaimBlocked {
                    blocker: ContentReclaimBlocker::PhysicalHold {
                        kind: blocked_kind,
                        ..
                    },
                }) if blocked_kind == kind
            ));
            hold.release().await.expect("physical hold class releases");
        }
    });
}

#[test]
fn concurrent_physical_hold_conflicts_with_staged_reclaim_intent() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"concurrent physical hold bytes").await;
        consume_reclaim_token(&db, sealed, 47).await;
        let mut reclaim = db.transaction(TransactionOptions::default());
        reclaim
            .stage_content_reclaim_intent(reclaim_authorization(&reclaim, sealed, 42))
            .await
            .expect("reclaim intent stages");

        db.acquire_content_physical_hold(
            sealed.storage_domain_id(),
            sealed.content_id(),
            test_hold_id(24),
            ContentPhysicalHoldOptions::expiring(
                ContentPhysicalHoldKind::Repair,
                ContentPhysicalHoldOwnerId::from_bytes([18; 16]),
                Duration::from_mins(1),
            ),
        )
        .await
        .expect("concurrent repair hold commits");
        assert!(matches!(
            reclaim.commit().await,
            Err(Error::Conflict { .. })
        ));
    });
}

#[test]
fn physical_hold_renewal_expiry_and_owner_checks_fail_closed() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let sealed = seal_reclaim_content(&db, b"expiring physical hold bytes").await;
        consume_reclaim_token(&db, sealed, 48).await;
        let owner = ContentPhysicalHoldOwnerId::from_bytes([19; 16]);
        let hold_id = test_hold_id(25);
        let hold = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Provider,
                    owner,
                    Duration::from_mins(1),
                ),
            )
            .await
            .expect("provider hold acquires");
        let before = hold.expires_at_unix_ms().expect("expiry exists");
        let renewed = hold
            .renew(Duration::from_mins(2))
            .await
            .expect("provider hold renews");
        assert!(renewed >= before);
        assert_eq!(hold.expires_at_unix_ms(), Some(renewed));
        assert!(matches!(
            db.resume_content_physical_hold(
                hold.storage_domain_id(),
                hold.content_id(),
                hold.id(),
                ContentPhysicalHoldOwnerId::from_bytes([20; 16]),
            )
            .await,
            Err(Error::ContentPhysicalHoldOwnerMismatch)
        ));

        let short = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(26),
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Provider,
                    owner,
                    Duration::from_millis(2),
                ),
            )
            .await
            .expect("short provider hold acquires");
        thread::sleep(Duration::from_millis(10));
        assert!(matches!(
            short.renew(Duration::from_mins(1)).await,
            Err(Error::ContentPhysicalHoldExpired { .. })
        ));
        assert!(matches!(
            db.acquire_content_physical_hold(
                short.storage_domain_id(),
                short.content_id(),
                short.id(),
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Provider,
                    owner,
                    Duration::from_mins(1),
                ),
            )
            .await,
            Err(Error::ContentPhysicalHoldExpired { .. })
        ));
        assert!(matches!(
            db.resume_content_physical_hold(
                short.storage_domain_id(),
                short.content_id(),
                short.id(),
                owner,
            )
            .await,
            Err(Error::ContentPhysicalHoldExpired { .. })
        ));
    });
}

#[test]
fn physical_hold_survives_native_reopen_and_malformed_state_blocks_intent() {
    let path = temp_db_path("content-physical-hold-native");
    let (sealed, hold_id, owner) = block_on(async {
        let db = Db::open(&path).await.expect("native db opens");
        let sealed = seal_reclaim_content(&db, b"durable physical hold bytes").await;
        consume_reclaim_token(&db, sealed, 49).await;
        let owner = ContentPhysicalHoldOwnerId::from_bytes([21; 16]);
        let hold = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(27),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Administrative,
                    owner,
                ),
            )
            .await
            .expect("administrative hold acquires");
        (sealed, hold.id(), owner)
    });

    block_on(async {
        let db = Db::open(DbOptions::persistent_read_only(&path))
            .await
            .expect("native read-only db reopens");
        let hold = db
            .resume_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                hold_id,
                owner,
            )
            .await
            .expect("administrative hold resumes");
        assert_eq!(hold.kind(), ContentPhysicalHoldKind::Administrative);
        assert_eq!(hold.expires_at_unix_ms(), None);
    });

    block_on(async {
        let db = Db::open(&path).await.expect("native writable db reopens");
        let mut damage = db.transaction(TransactionOptions::default());
        damage
            .put_internal_bucket(
                CONTENT_PHYSICAL_HOLD_BUCKET,
                content_physical_hold_key(
                    sealed.storage_domain_id(),
                    sealed.content_id(),
                    ContentPhysicalHoldId::from_bytes([1; 16]).expect("test hold id decodes"),
                ),
                b"malformed".to_vec(),
            )
            .expect("hold damage stages");
        damage.commit().await.expect("hold damage commits");
        let mut reclaim = db.transaction(TransactionOptions::default());
        assert!(matches!(
            reclaim
                .stage_content_reclaim_intent(reclaim_authorization(&reclaim, sealed, 43))
                .await,
            Err(Error::InvalidFormat { .. })
        ));
    });

    std::fs::remove_dir_all(path).expect("test database removes");
}

#[test]
fn lifecycle_vacuum_removes_expired_authority_and_preserves_active_indefinite_holds() {
    block_on(async {
        let db = Db::open(DbOptions::memory())
            .await
            .expect("memory db opens");
        let mut upload = db
            .begin_content_upload(ContentUploadOptions::new(
                test_scope(),
                Duration::from_millis(2),
            ))
            .await
            .expect("short-token upload begins");
        upload.write(b"vacuum").await.expect("content writes");
        let sealed = upload.seal().await.expect("content seals");
        let lease = db
            .open_content_leased(
                sealed.storage_domain_id(),
                sealed.content_id(),
                ContentLeaseOptions::new(
                    ContentLeaseOwnerId::from_bytes([71; 16]),
                    Duration::from_millis(2),
                ),
            )
            .await
            .expect("short lease opens");
        let expiring = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(71),
                ContentPhysicalHoldOptions::expiring(
                    ContentPhysicalHoldKind::Repair,
                    ContentPhysicalHoldOwnerId::from_bytes([72; 16]),
                    Duration::from_millis(2),
                ),
            )
            .await
            .expect("short hold acquires");
        let indefinite_owner = ContentPhysicalHoldOwnerId::from_bytes([73; 16]);
        let indefinite = db
            .acquire_content_physical_hold(
                sealed.storage_domain_id(),
                sealed.content_id(),
                test_hold_id(72),
                ContentPhysicalHoldOptions::until_released(
                    ContentPhysicalHoldKind::Administrative,
                    indefinite_owner,
                ),
            )
            .await
            .expect("indefinite hold acquires");

        thread::sleep(Duration::from_millis(10));
        let cutoff = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_millis(),
        )
        .expect("current time fits u64");
        let report = db
            .vacuum_content_lifecycle(cutoff)
            .await
            .expect("lifecycle vacuum commits");
        assert_eq!(report.scanned(), 4);
        assert_eq!(report.expired_tokens_removed(), 1);
        assert_eq!(report.expired_leases_removed(), 1);
        assert_eq!(report.inactive_holds_removed(), 1);
        assert!(
            !db.content_has_active_lease(sealed.storage_domain_id(), sealed.content_id())
                .await
                .expect("lease index reads")
        );
        assert!(matches!(
            db.resume_content_physical_hold(
                expiring.storage_domain_id(),
                expiring.content_id(),
                expiring.id(),
                expiring.owner_id(),
            )
            .await,
            Err(Error::ContentPhysicalHoldNotFound { .. })
        ));
        db.resume_content_physical_hold(
            indefinite.storage_domain_id(),
            indefinite.content_id(),
            indefinite.id(),
            indefinite_owner,
        )
        .await
        .expect("active indefinite hold remains");
        drop(lease);
    });
}
