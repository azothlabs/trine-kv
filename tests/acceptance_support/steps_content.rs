use std::time::Duration;

use cucumber::{then, when};
use trine_kv::{ContentId, ContentUploadOptions, Error};

use super::{content_fixture::content_scope, world::TrineWorld};

#[when(expr = "I stage immutable content {string} without sealing it")]
async fn stage_unsealed_content(world: &mut TrineWorld, value: String) {
    let scope = content_scope();
    let mut upload = world
        .db()
        .begin_content_upload(ContentUploadOptions::new(scope, Duration::from_hours(1)))
        .await
        .expect("content upload begins");
    upload
        .write(value.as_bytes())
        .await
        .expect("content upload stages bytes");
    world.expected_content_id = Some(ContentId::for_bytes(value.as_bytes()));
    world.content_domain = Some(scope.storage_domain_id());
}

#[when("I try to open the staged content by its expected identity")]
async fn try_open_staged_content(world: &mut TrineWorld) {
    let result = world
        .db()
        .open_content(
            world
                .content_domain
                .expect("content storage domain is retained"),
            world
                .expected_content_id
                .expect("expected content identity is retained"),
        )
        .await;
    world.record_error(result);
}

#[when(expr = "I upload and seal immutable content {string}")]
async fn upload_and_seal_content(world: &mut TrineWorld, value: String) {
    world.first_content_id = Some(
        super::content_fixture::seal_bytes(world.db(), value.as_bytes())
            .await
            .expect("content upload seals")
            .content_id(),
    );
    world.sealed_content_bytes = Some(value.into_bytes());
    world.content_domain = Some(content_scope().storage_domain_id());
}

#[when("I upload and seal the same immutable content again")]
async fn upload_and_seal_same_content(world: &mut TrineWorld) {
    let expected = world
        .first_content_id
        .expect("first immutable content identity exists");
    let value = world
        .sealed_content_bytes
        .as_deref()
        .expect("first immutable content bytes are retained");
    assert_eq!(expected, ContentId::for_bytes(value));
    world.second_content_id = Some(
        super::content_fixture::seal_bytes(world.db(), value)
            .await
            .expect("identical content upload seals")
            .content_id(),
    );
}

#[when("I read the sealed content")]
async fn read_sealed_content(world: &mut TrineWorld) {
    let content = world
        .db()
        .open_content(
            world
                .content_domain
                .expect("content storage domain is retained"),
            world
                .first_content_id
                .expect("sealed content identity is retained"),
        )
        .await
        .expect("sealed content opens");
    world.last_value = Some(
        content
            .read_range(0, content.len())
            .await
            .expect("sealed content reads")
            .to_vec(),
    );
}

#[then("the operation is rejected because content is not published")]
fn content_is_not_published(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ContentNotFound { .. })
    ));
}

#[then("both seals return the same content identity")]
fn seals_have_same_identity(world: &mut TrineWorld) {
    assert_eq!(world.first_content_id, world.second_content_id);
}
