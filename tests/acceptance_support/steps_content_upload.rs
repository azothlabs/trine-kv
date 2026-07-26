use std::time::Duration;

use cucumber::{given, then, when};
use trine_kv::{ContentId, ContentUploadOptions, ContentUploadResume, Error};

use super::{content_fixture::content_scope, world::TrineWorld};

fn upload_options() -> ContentUploadOptions {
    ContentUploadOptions::new(content_scope(), Duration::from_hours(1))
}

#[when(expr = "I begin a remembered content upload with bytes {string}")]
async fn begin_remembered_upload_with_bytes(world: &mut TrineWorld, value: String) {
    let mut upload = world
        .db()
        .begin_content_upload(upload_options())
        .await
        .expect("remembered upload begins");
    upload
        .write(value.as_bytes())
        .await
        .expect("remembered upload bytes persist");
    world.remembered_upload_id = Some(upload.upload_id());
    world.upload_bytes = value.into_bytes();
    world.pending_upload = Some(upload);
}

#[when(expr = "I resume the remembered upload and append {string}")]
async fn resume_remembered_upload_and_append(world: &mut TrineWorld, suffix: String) {
    let upload_id = world
        .remembered_upload_id
        .expect("remembered upload identity exists");
    let mut upload = match world
        .db()
        .resume_content_upload(upload_id)
        .await
        .expect("remembered upload resumes")
    {
        ContentUploadResume::Open(upload) => upload,
        ContentUploadResume::Sealed(_) => panic!("open upload unexpectedly resumed as sealed"),
    };
    assert_eq!(
        upload.len(),
        u64::try_from(world.upload_bytes.len()).expect("fixture length fits u64"),
        "resume starts at the exact confirmed byte boundary"
    );
    upload
        .write(suffix.as_bytes())
        .await
        .expect("resumed suffix persists");
    world.upload_bytes.extend_from_slice(suffix.as_bytes());
    world.pending_upload = Some(upload);
}

#[when("I seal the remembered upload")]
async fn seal_remembered_upload(world: &mut TrineWorld) {
    let sealed = world
        .pending_upload
        .take()
        .expect("remembered upload is open")
        .seal()
        .await
        .expect("remembered upload seals");
    assert_eq!(sealed.content_id(), world.expected_content_for_upload());
    world.first_content_id = Some(sealed.content_id());
    world.content_domain = Some(sealed.storage_domain_id());
    world.sealed_content_bytes = Some(world.upload_bytes.clone());
}

#[when("I remember the sealed content identity")]
fn remember_sealed_content_identity(world: &mut TrineWorld) {
    world.remembered_content_id = world.first_content_id;
}

#[when("I resume the remembered sealed upload")]
async fn resume_remembered_sealed_upload(world: &mut TrineWorld) {
    let upload_id = world
        .remembered_upload_id
        .expect("remembered upload identity exists");
    let resumed = world
        .db()
        .resume_content_upload(upload_id)
        .await
        .expect("sealed upload resumes");
    let sealed = resumed
        .sealed()
        .expect("sealed upload cannot become writable again");
    world.second_content_id = Some(sealed.content_id());
}

#[then("the resumed seal has the remembered content identity")]
fn resumed_seal_has_remembered_identity(world: &mut TrineWorld) {
    assert_eq!(world.second_content_id, world.remembered_content_id);
}

#[when("I abort the remembered upload")]
async fn abort_remembered_upload(world: &mut TrineWorld) {
    world
        .pending_upload
        .take()
        .expect("remembered upload is open")
        .abort()
        .await
        .expect("remembered upload aborts");
}

#[when("I try to resume the remembered upload")]
async fn try_resume_remembered_upload(world: &mut TrineWorld) {
    let result = world
        .db()
        .resume_content_upload(
            world
                .remembered_upload_id
                .expect("remembered upload identity exists"),
        )
        .await;
    world.record_error(result);
}

#[then("the operation is rejected because the content upload is absent")]
fn content_upload_is_absent(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentUploadNotFound { .. })
    ));
}

#[when("I try to open the remembered upload bytes by content identity")]
async fn try_open_remembered_upload_bytes(world: &mut TrineWorld) {
    try_open_bytes_by_identity(world, world.upload_bytes.clone()).await;
}

#[when(expr = "I begin a content upload expecting {int} bytes")]
async fn begin_upload_expecting(world: &mut TrineWorld, expected: u64) {
    world.pending_upload = Some(
        world
            .db()
            .begin_content_upload(upload_options().with_expected_length(expected))
            .await
            .expect("length-bound upload begins"),
    );
}

#[when(expr = "I begin a remembered content upload expecting {int} bytes")]
async fn begin_remembered_upload_expecting(world: &mut TrineWorld, expected: u64) {
    begin_upload_expecting(world, expected).await;
    world.remembered_upload_id = Some(
        world
            .pending_upload
            .as_ref()
            .expect("length-bound upload is open")
            .upload_id(),
    );
    world.upload_bytes.clear();
}

#[when(expr = "I try to write {string} to the upload")]
async fn try_write_to_upload(world: &mut TrineWorld, value: String) {
    let mut upload = world.pending_upload.take().expect("content upload is open");
    let result = upload.write(value.as_bytes()).await;
    if result.is_ok() {
        world.upload_bytes.extend_from_slice(value.as_bytes());
    }
    world.pending_upload = Some(upload);
    world.record_error(result);
}

#[when(expr = "I write {string} to the remembered upload")]
async fn write_to_remembered_upload(world: &mut TrineWorld, value: String) {
    let upload = world
        .pending_upload
        .as_mut()
        .expect("remembered content upload is open");
    upload
        .write(value.as_bytes())
        .await
        .expect("remembered upload write persists");
    world.upload_bytes.extend_from_slice(value.as_bytes());
}

#[when(expr = "I write {string} to the upload")]
async fn write_to_upload(world: &mut TrineWorld, value: String) {
    write_to_remembered_upload(world, value).await;
}

#[when(expr = "I try to open bytes {string} by content identity")]
async fn try_open_literal_bytes(world: &mut TrineWorld, value: String) {
    try_open_bytes_by_identity(world, value.into_bytes()).await;
}

async fn try_open_bytes_by_identity(world: &mut TrineWorld, value: Vec<u8>) {
    let result = world
        .db()
        .open_content(
            content_scope().storage_domain_id(),
            ContentId::for_bytes(&value),
        )
        .await;
    world.record_error(result);
}

#[then("the operation is rejected because content length differs")]
fn content_length_differs(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentLengthMismatch { .. })
    ));
}

#[given(expr = "the content domain has a physical quota of {int} bytes")]
async fn content_domain_has_quota(world: &mut TrineWorld, limit: u64) {
    let quota = world
        .db()
        .set_content_physical_quota(content_scope().storage_domain_id(), Some(limit))
        .await
        .expect("physical content quota publishes");
    assert_eq!(quota.limit(), Some(limit));
}

#[when(expr = "I try to begin another content upload expecting {int} byte")]
async fn try_begin_another_upload(world: &mut TrineWorld, expected: u64) {
    let result = world
        .db()
        .begin_content_upload(upload_options().with_expected_length(expected))
        .await;
    world.record_error(result);
}

#[when(expr = "I begin another content upload expecting {int} byte")]
async fn begin_another_upload(world: &mut TrineWorld, expected: u64) {
    begin_upload_expecting(world, expected).await;
    world.upload_bytes.clear();
}

#[then("the operation is rejected because the physical content quota is exhausted")]
fn physical_content_quota_is_exhausted(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentPhysicalQuotaExceeded { .. })
    ));
}

#[when("I seal the upload")]
async fn seal_upload(world: &mut TrineWorld) {
    let sealed = world
        .pending_upload
        .take()
        .expect("content upload is open")
        .seal()
        .await
        .expect("content upload seals");
    world.first_content_id = Some(sealed.content_id());
    world.content_domain = Some(sealed.storage_domain_id());
}

#[then(expr = "the content domain accounts for {int} unique byte and {int} reserved bytes")]
async fn content_domain_accounts_for(world: &mut TrineWorld, unique: u64, reserved: u64) {
    let quota = world
        .db()
        .content_physical_quota(content_scope().storage_domain_id())
        .await
        .expect("physical content quota reads");
    assert_eq!(quota.unique_content_bytes(), unique);
    assert_eq!(quota.upload_reserved_bytes(), reserved);
}

#[when("I remember the upload maintenance timestamp")]
async fn remember_upload_maintenance_timestamp(world: &mut TrineWorld) {
    let upload_id = world
        .remembered_upload_id
        .expect("remembered upload identity exists");
    let uploads = world
        .db()
        .list_content_uploads()
        .await
        .expect("durable upload states list");
    let upload = uploads
        .into_iter()
        .find(|upload| upload.upload_id() == upload_id)
        .expect("remembered upload is present in the maintenance index");
    world.remembered_upload_updated_at = Some(upload.updated_at_unix_ms());
}

#[when("I abandon the live upload handle without aborting")]
fn abandon_live_upload_handle(world: &mut TrineWorld) {
    world
        .pending_upload
        .take()
        .expect("an open upload handle exists");
}

#[when("I reap uploads at the exact remembered timestamp")]
async fn reap_uploads_at_exact_timestamp(world: &mut TrineWorld) {
    world.upload_maintenance_report = Some(
        world
            .db()
            .reap_inactive_content_uploads(
                world
                    .remembered_upload_updated_at
                    .expect("upload maintenance timestamp exists"),
            )
            .await
            .expect("upload maintenance at the exclusive boundary succeeds"),
    );
}

#[when("I reap uploads after the remembered timestamp")]
async fn reap_uploads_after_timestamp(world: &mut TrineWorld) {
    let cutoff = world
        .remembered_upload_updated_at
        .expect("upload maintenance timestamp exists")
        .checked_add(1)
        .expect("fixture timestamp can advance");
    world.upload_maintenance_report = Some(
        world
            .db()
            .reap_inactive_content_uploads(cutoff)
            .await
            .expect("inactive upload cleanup succeeds"),
    );
}

#[then(expr = "maintenance scanned {int} upload and aborted {int} uploads")]
fn upload_reap_report(world: &mut TrineWorld, scanned: u64, aborted: u64) {
    let report = world
        .upload_maintenance_report
        .expect("upload maintenance report exists");
    assert_eq!(report.scanned(), scanned);
    assert_eq!(report.aborted(), aborted);
    assert_eq!(report.pruned_sealed(), 0);
}

#[then(expr = "maintenance scanned {int} upload and aborted {int} upload")]
fn upload_reap_report_singular(world: &mut TrineWorld, scanned: u64, aborted: u64) {
    upload_reap_report(world, scanned, aborted);
}

#[when("I resume the remembered upload without appending")]
async fn resume_remembered_upload_without_appending(world: &mut TrineWorld) {
    let resumed = world
        .db()
        .resume_content_upload(
            world
                .remembered_upload_id
                .expect("remembered upload identity exists"),
        )
        .await
        .expect("current upload remains resumable");
    world.pending_upload = Some(
        resumed
            .into_open()
            .expect("current upload remains in the open lifecycle"),
    );
}

#[then(expr = "the resumed upload length is {int} bytes")]
fn resumed_upload_has_length(world: &mut TrineWorld, expected: u64) {
    assert_eq!(
        world
            .pending_upload
            .as_ref()
            .expect("resumed upload is open")
            .len(),
        expected
    );
}

#[when("I prune sealed uploads at the exact remembered timestamp")]
async fn prune_sealed_uploads_at_exact_timestamp(world: &mut TrineWorld) {
    world.upload_maintenance_report = Some(
        world
            .db()
            .prune_sealed_content_uploads(
                world
                    .remembered_upload_updated_at
                    .expect("upload maintenance timestamp exists"),
            )
            .await
            .expect("sealed upload maintenance at the exclusive boundary succeeds"),
    );
}

#[when("I prune sealed uploads after the remembered timestamp")]
async fn prune_sealed_uploads_after_timestamp(world: &mut TrineWorld) {
    let cutoff = world
        .remembered_upload_updated_at
        .expect("upload maintenance timestamp exists")
        .checked_add(1)
        .expect("fixture timestamp can advance");
    world.upload_maintenance_report = Some(
        world
            .db()
            .prune_sealed_content_uploads(cutoff)
            .await
            .expect("old sealed upload state is pruned"),
    );
}

#[then(expr = "maintenance scanned {int} upload and pruned {int} sealed uploads")]
fn upload_prune_report(world: &mut TrineWorld, scanned: u64, pruned: u64) {
    let report = world
        .upload_maintenance_report
        .expect("upload maintenance report exists");
    assert_eq!(report.scanned(), scanned);
    assert_eq!(report.aborted(), 0);
    assert_eq!(report.pruned_sealed(), pruned);
}

#[then(expr = "maintenance scanned {int} upload and pruned {int} sealed upload")]
fn upload_prune_report_singular(world: &mut TrineWorld, scanned: u64, pruned: u64) {
    upload_prune_report(world, scanned, pruned);
}

#[then("the operation is rejected because the content upload is sealed")]
fn content_upload_is_sealed(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentUploadSealed { .. })
    ));
}
