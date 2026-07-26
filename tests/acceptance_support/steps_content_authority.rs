use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cucumber::{given, then, when};
use trine_kv::{
    ContentAccessBarrierId, ContentLeaseOptions, ContentLeaseOwnerId, ContentPhysicalHoldId,
    ContentPhysicalHoldKind, ContentPhysicalHoldOptions, ContentPhysicalHoldOwnerId, Error,
};

use super::{content_fixture, world::TrineWorld};

const LEASE_OWNER: ContentLeaseOwnerId = ContentLeaseOwnerId::from_bytes([31; 16]);
const HOLD_OWNER: ContentPhysicalHoldOwnerId = ContentPhysicalHoldOwnerId::from_bytes([41; 16]);
const OTHER_HOLD_OWNER: ContentPhysicalHoldOwnerId =
    ContentPhysicalHoldOwnerId::from_bytes([42; 16]);

fn current_epoch_millis() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock follows Unix epoch")
            .as_millis(),
    )
    .expect("current Unix milliseconds fit u64")
}

fn hold_kind(name: &str) -> ContentPhysicalHoldKind {
    match name {
        "migration" => ContentPhysicalHoldKind::Migration,
        "backup" => ContentPhysicalHoldKind::Backup,
        "repair" => ContentPhysicalHoldKind::Repair,
        "provider" => ContentPhysicalHoldKind::Provider,
        "administrative" => ContentPhysicalHoldKind::Administrative,
        "processing" => ContentPhysicalHoldKind::Processing,
        "offline" => ContentPhysicalHoldKind::Offline,
        other => panic!("unknown public physical-hold class {other:?}"),
    }
}

#[given(expr = "sealed content contains {string}")]
async fn sealed_content_contains(world: &mut TrineWorld, value: String) {
    let sealed = content_fixture::seal_bytes(world.db(), value.as_bytes())
        .await
        .expect("content fixture seals");
    world.first_content_id = Some(sealed.content_id());
    world.content_domain = Some(sealed.storage_domain_id());
    world.sealed_content_bytes = Some(value.into_bytes());
}

#[given("I retain an ordinary handle to the sealed content")]
async fn retain_ordinary_content_handle(world: &mut TrineWorld) {
    world.retained_content_handle = Some(
        world
            .db()
            .open_content(
                world.content_domain.expect("content domain exists"),
                world
                    .first_content_id
                    .expect("sealed content identity exists"),
            )
            .await
            .expect("ordinary content handle opens before the barrier"),
    );
}

#[when("I enforce leased-only access for the content domain")]
async fn enforce_leased_only(world: &mut TrineWorld) {
    let barrier = world
        .db()
        .enforce_content_leased_only(
            world.content_domain.expect("content domain exists"),
            ContentAccessBarrierId::generate().expect("barrier identity generates"),
        )
        .await
        .expect("leased-only access is enforced");
    if world.first_barrier_id.is_none() {
        world.first_barrier_id = Some(barrier.barrier_id());
    } else {
        world.second_barrier_id = Some(barrier.barrier_id());
    }
}

#[when("I repeat leased-only enforcement for the content domain")]
async fn repeat_leased_only_enforcement(world: &mut TrineWorld) {
    enforce_leased_only(world).await;
}

#[then("both enforcement calls report the same barrier")]
fn enforcement_is_idempotent(world: &mut TrineWorld) {
    assert_eq!(world.first_barrier_id, world.second_barrier_id);
}

#[when("I try to open the sealed content without a lease")]
async fn try_open_without_lease(world: &mut TrineWorld) {
    let result = world
        .db()
        .open_content(
            world.content_domain.expect("content domain exists"),
            world
                .first_content_id
                .expect("sealed content identity exists"),
        )
        .await;
    world.record_error(result);
}

#[then("the operation is rejected because a content lease is required")]
fn content_lease_is_required(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentLeaseRequired { .. })
    ));
}

#[then(expr = "the retained pre-barrier content handle reads {string}")]
async fn retained_pre_barrier_handle_reads(world: &mut TrineWorld, expected: String) {
    let handle = world
        .retained_content_handle
        .as_ref()
        .expect("pre-barrier content handle is retained");
    assert_eq!(
        handle
            .read_range(0, u64::MAX)
            .await
            .expect("pre-barrier handle remains readable")
            .as_ref(),
        expected.as_bytes()
    );
}

#[when(expr = "I open the sealed content with a {int} second lease")]
async fn open_with_second_lease(world: &mut TrineWorld, seconds: u64) {
    world.leased_content_handle = Some(
        world
            .db()
            .open_content_leased(
                world.content_domain.expect("content domain exists"),
                world
                    .first_content_id
                    .expect("sealed content identity exists"),
                ContentLeaseOptions::new(LEASE_OWNER, Duration::from_secs(seconds)),
            )
            .await
            .expect("leased content handle opens"),
    );
}

#[when(expr = "I open the sealed content with a {int} millisecond lease")]
async fn open_with_millisecond_lease(world: &mut TrineWorld, milliseconds: u64) {
    world.leased_content_handle = Some(
        world
            .db()
            .open_content_leased(
                world.content_domain.expect("content domain exists"),
                world
                    .first_content_id
                    .expect("sealed content identity exists"),
                ContentLeaseOptions::new(LEASE_OWNER, Duration::from_millis(milliseconds)),
            )
            .await
            .expect("short leased content handle opens"),
    );
}

#[when("I try to open the sealed content with a lease")]
async fn try_open_with_lease(world: &mut TrineWorld) {
    let result = world
        .db()
        .open_content_leased(
            world.content_domain.expect("content domain exists"),
            world
                .first_content_id
                .expect("sealed content identity exists"),
            ContentLeaseOptions::new(LEASE_OWNER, Duration::from_mins(1)),
        )
        .await;
    match result {
        Ok(handle) => {
            world.leased_content_handle = Some(handle);
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error),
    }
}

#[then(expr = "the leased content handle reads {string}")]
async fn leased_content_handle_reads(world: &mut TrineWorld, expected: String) {
    assert_eq!(
        world
            .leased_content_handle
            .as_ref()
            .expect("leased content handle exists")
            .read_range(0, u64::MAX)
            .await
            .expect("leased content reads")
            .as_ref(),
        expected.as_bytes()
    );
}

#[when("I clone the leased content handle")]
fn clone_leased_content_handle(world: &mut TrineWorld) {
    world.cloned_leased_content_handle = Some(
        world
            .leased_content_handle
            .as_ref()
            .expect("leased content handle exists")
            .clone(),
    );
}

#[when("I remember the leased content deadline")]
fn remember_lease_deadline(world: &mut TrineWorld) {
    world.remembered_lease_deadline = world
        .leased_content_handle
        .as_ref()
        .expect("leased content handle exists")
        .lease_expires_at_unix_ms();
}

#[when(expr = "I renew the cloned content lease for {int} seconds")]
async fn renew_cloned_lease(world: &mut TrineWorld, seconds: u64) {
    world
        .cloned_leased_content_handle
        .as_ref()
        .expect("cloned leased content handle exists")
        .renew_lease(Duration::from_secs(seconds))
        .await
        .expect("cloned content lease renews");
}

#[then("both leased handles report a later common deadline")]
fn leased_handles_share_later_deadline(world: &mut TrineWorld) {
    let first = world
        .leased_content_handle
        .as_ref()
        .expect("leased content handle exists")
        .lease_expires_at_unix_ms()
        .expect("handle has a lease");
    let second = world
        .cloned_leased_content_handle
        .as_ref()
        .expect("cloned leased content handle exists")
        .lease_expires_at_unix_ms()
        .expect("clone has a lease");
    assert_eq!(first, second);
    assert!(
        first
            > world
                .remembered_lease_deadline
                .expect("old lease deadline was remembered")
    );
}

#[when("I wait until the lease deadline has passed")]
fn wait_until_lease_deadline(world: &mut TrineWorld) {
    let deadline = world
        .leased_content_handle
        .as_ref()
        .expect("leased content handle exists")
        .lease_expires_at_unix_ms()
        .expect("handle has a lease");
    while current_epoch_millis() <= deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[when("I try to read through the leased content handle")]
async fn try_read_through_lease(world: &mut TrineWorld) {
    let result = world
        .leased_content_handle
        .as_ref()
        .expect("leased content handle exists")
        .read_range(0, u64::MAX)
        .await;
    world.record_error(result);
}

#[when("I try to renew the leased content handle")]
async fn try_renew_leased_handle(world: &mut TrineWorld) {
    let result = world
        .leased_content_handle
        .as_ref()
        .expect("leased content handle exists")
        .renew_lease(Duration::from_mins(1))
        .await;
    world.record_error(result);
}

#[then("the operation is rejected because the content lease expired")]
fn content_lease_expired(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentLeaseExpired { .. })
    ));
}

#[when("I acquire a remembered until-released backup hold")]
async fn acquire_backup_hold(world: &mut TrineWorld) {
    acquire_hold(world, ContentPhysicalHoldKind::Backup).await;
}

#[when(expr = "I acquire a remembered until-released {string} hold")]
async fn acquire_named_hold(world: &mut TrineWorld, kind: String) {
    acquire_hold(world, hold_kind(&kind)).await;
}

async fn acquire_hold(world: &mut TrineWorld, kind: ContentPhysicalHoldKind) {
    let hold_id = ContentPhysicalHoldId::generate().expect("physical hold identity generates");
    let hold = world
        .db()
        .acquire_content_physical_hold(
            world.content_domain.expect("content domain exists"),
            world
                .first_content_id
                .expect("sealed content identity exists"),
            hold_id,
            ContentPhysicalHoldOptions::until_released(kind, HOLD_OWNER),
        )
        .await
        .expect("until-released physical hold publishes");
    world.remembered_hold_id = Some(hold_id);
    world.remembered_hold_owner = Some(HOLD_OWNER);
    world.remembered_hold_kind = Some(kind);
    world.physical_hold = Some(hold);
}

#[when("I resume the remembered physical hold")]
async fn resume_remembered_hold(world: &mut TrineWorld) {
    world.physical_hold = Some(
        world
            .db()
            .resume_content_physical_hold(
                world.content_domain.expect("content domain exists"),
                world
                    .first_content_id
                    .expect("sealed content identity exists"),
                world.remembered_hold_id.expect("physical hold id exists"),
                world
                    .remembered_hold_owner
                    .expect("physical hold owner exists"),
            )
            .await
            .expect("physical hold resumes"),
    );
}

#[when("I release the remembered physical hold twice")]
async fn release_remembered_hold_twice(world: &mut TrineWorld) {
    let hold = world
        .physical_hold
        .as_ref()
        .expect("physical hold is active");
    hold.release().await.expect("physical hold releases");
    hold.release()
        .await
        .expect("physical hold release is idempotent");
}

#[when("I try to resume the remembered physical hold")]
async fn try_resume_remembered_hold(world: &mut TrineWorld) {
    let result = world
        .db()
        .resume_content_physical_hold(
            world.content_domain.expect("content domain exists"),
            world
                .first_content_id
                .expect("sealed content identity exists"),
            world.remembered_hold_id.expect("physical hold id exists"),
            world
                .remembered_hold_owner
                .expect("physical hold owner exists"),
        )
        .await;
    world.record_error(result);
}

#[when("I try to resume the remembered physical hold as another owner")]
async fn try_resume_hold_as_another_owner(world: &mut TrineWorld) {
    let result = world
        .db()
        .resume_content_physical_hold(
            world.content_domain.expect("content domain exists"),
            world
                .first_content_id
                .expect("sealed content identity exists"),
            world.remembered_hold_id.expect("physical hold id exists"),
            OTHER_HOLD_OWNER,
        )
        .await;
    world.record_error(result);
}

#[then("the operation is rejected because the physical hold is absent")]
fn physical_hold_is_absent(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentPhysicalHoldNotFound { .. })
    ));
}

#[then("the operation is rejected because the physical hold owner differs")]
fn physical_hold_owner_differs(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentPhysicalHoldOwnerMismatch)
    ));
}

#[then(expr = "the resumed physical hold class is {string}")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber expression captures require an owned String parameter"
)]
fn resumed_hold_has_class(world: &mut TrineWorld, expected: String) {
    let hold = world.physical_hold.as_ref().expect("physical hold resumed");
    assert_eq!(hold.kind(), hold_kind(&expected));
    assert_eq!(Some(hold.kind()), world.remembered_hold_kind);
}

#[when(expr = "I acquire a remembered expiring provider hold for {int} milliseconds")]
async fn acquire_remembered_expiring_hold(world: &mut TrineWorld, milliseconds: u64) {
    acquire_expiring_hold(world, Duration::from_millis(milliseconds)).await;
}

#[when(expr = "I acquire a remembered expiring provider hold for {int} seconds")]
async fn acquire_remembered_expiring_hold_seconds(world: &mut TrineWorld, seconds: u64) {
    acquire_expiring_hold(world, Duration::from_secs(seconds)).await;
}

async fn acquire_expiring_hold(world: &mut TrineWorld, lifetime: Duration) {
    let hold_id = ContentPhysicalHoldId::generate().expect("physical hold identity generates");
    let hold = world
        .db()
        .acquire_content_physical_hold(
            world.content_domain.expect("content domain exists"),
            world
                .first_content_id
                .expect("sealed content identity exists"),
            hold_id,
            ContentPhysicalHoldOptions::expiring(
                ContentPhysicalHoldKind::Provider,
                HOLD_OWNER,
                lifetime,
            ),
        )
        .await
        .expect("expiring physical hold publishes");
    world.remembered_hold_id = Some(hold_id);
    world.remembered_hold_owner = Some(HOLD_OWNER);
    world.remembered_hold_kind = Some(ContentPhysicalHoldKind::Provider);
    world.physical_hold = Some(hold);
}

#[when(expr = "I acquire a second expiring provider hold for {int} milliseconds")]
async fn acquire_second_expiring_hold(world: &mut TrineWorld, milliseconds: u64) {
    acquire_expiring_hold(world, Duration::from_millis(milliseconds)).await;
}

#[when("I remember the physical hold deadline")]
fn remember_physical_hold_deadline(world: &mut TrineWorld) {
    world.remembered_hold_deadline = world
        .physical_hold
        .as_ref()
        .expect("physical hold is active")
        .expires_at_unix_ms();
}

#[when(expr = "I renew the physical hold for {int} millisecond")]
async fn renew_physical_hold_millisecond(world: &mut TrineWorld, milliseconds: u64) {
    world
        .physical_hold
        .as_ref()
        .expect("physical hold is active")
        .renew(Duration::from_millis(milliseconds))
        .await
        .expect("physical hold renewal succeeds");
}

#[when(expr = "I renew the physical hold for {int} seconds")]
async fn renew_physical_hold_seconds(world: &mut TrineWorld, seconds: u64) {
    world
        .physical_hold
        .as_ref()
        .expect("physical hold is active")
        .renew(Duration::from_secs(seconds))
        .await
        .expect("physical hold renewal succeeds");
}

#[then("the physical hold deadline is unchanged")]
fn physical_hold_deadline_is_unchanged(world: &mut TrineWorld) {
    assert_eq!(
        world
            .physical_hold
            .as_ref()
            .expect("physical hold is active")
            .expires_at_unix_ms(),
        world.remembered_hold_deadline
    );
}

#[then("the physical hold deadline is later")]
fn physical_hold_deadline_is_later(world: &mut TrineWorld) {
    assert!(
        world
            .physical_hold
            .as_ref()
            .expect("physical hold is active")
            .expires_at_unix_ms()
            .expect("expiring physical hold has a deadline")
            > world
                .remembered_hold_deadline
                .expect("old physical hold deadline was remembered")
    );
}

#[when("I wait until the physical hold deadline has passed")]
fn wait_until_physical_hold_deadline(world: &mut TrineWorld) {
    let deadline = world
        .physical_hold
        .as_ref()
        .expect("physical hold is active")
        .expires_at_unix_ms()
        .expect("expiring physical hold has a deadline");
    while current_epoch_millis() <= deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[when(expr = "I try to renew the physical hold for {int} seconds")]
async fn try_renew_physical_hold(world: &mut TrineWorld, seconds: u64) {
    let result = world
        .physical_hold
        .as_ref()
        .expect("physical hold is active")
        .renew(Duration::from_secs(seconds))
        .await;
    world.record_error(result);
}

#[then("the operation is rejected because the physical hold expired")]
fn physical_hold_expired(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentPhysicalHoldExpired { .. })
    ));
}
