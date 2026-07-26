use cucumber::{given, then, when};
use trine_kv::{Error, KeyRange, ReadVersion, WriteBatch, WriteOptions};

use super::world::TrineWorld;

#[given("I retain a snapshot")]
fn retain_snapshot(world: &mut TrineWorld) {
    world.snapshot = Some(world.db().snapshot());
}

#[when(expr = "I read key {string} from the snapshot")]
async fn read_key_from_snapshot(world: &mut TrineWorld, key: String) {
    let bucket = world
        .db()
        .default_bucket()
        .await
        .expect("default bucket opens");
    world.last_value = world
        .snapshot
        .as_ref()
        .expect("snapshot was retained")
        .get(&bucket, key.as_bytes())
        .await
        .expect("snapshot read succeeds");
}

#[when(expr = "I delete keys from {string} up to {string}")]
async fn delete_range(world: &mut TrineWorld, start: String, end: String) {
    let mut batch = WriteBatch::new();
    batch.delete_range(KeyRange::half_open(start.into_bytes(), end.into_bytes()));
    world
        .db()
        .write(batch, WriteOptions::default())
        .await
        .expect("range deletion commits");
}

#[when("I try to open a read version newer than the latest")]
fn open_future_read_version(world: &mut TrineWorld) {
    let future = world
        .db()
        .latest_read_version()
        .as_u64()
        .checked_add(1)
        .map(ReadVersion::from_u64)
        .expect("acceptance read version does not overflow");
    world.record_error(world.db().snapshot_at(future));
}

#[when("I try to open the remembered read version")]
fn open_remembered_read_version(world: &mut TrineWorld) {
    let version = world
        .remembered_version
        .expect("read version was remembered");
    world.record_error(world.db().snapshot_at(version));
}

#[given(expr = "I create checkpoint {string}")]
async fn create_checkpoint(world: &mut TrineWorld, name: String) {
    world.checkpoint_version = Some(
        world
            .db()
            .create_checkpoint(&name)
            .await
            .expect("checkpoint is created"),
    );
}

#[when(expr = "I try to create checkpoint {string}")]
async fn try_create_checkpoint(world: &mut TrineWorld, name: String) {
    let result = world.db().create_checkpoint(&name).await;
    world.record_error(result);
}

#[when(expr = "I delete checkpoint {string}")]
async fn delete_checkpoint(world: &mut TrineWorld, name: String) {
    world
        .db()
        .delete_checkpoint(&name)
        .await
        .expect("checkpoint is deleted");
}

#[when(expr = "I read key {string} at the checkpoint")]
async fn read_at_checkpoint(world: &mut TrineWorld, key: String) {
    let snapshot = world
        .db()
        .snapshot_at(
            world
                .checkpoint_version
                .expect("checkpoint version was retained"),
        )
        .expect("checkpoint version remains readable");
    let bucket = world
        .db()
        .default_bucket()
        .await
        .expect("default bucket opens");
    world.last_value = snapshot
        .get(&bucket, key.as_bytes())
        .await
        .expect("checkpoint read succeeds");
}

#[when("I try to open the checkpoint read version")]
fn open_checkpoint_read_version(world: &mut TrineWorld) {
    let version = world
        .checkpoint_version
        .expect("checkpoint version was retained");
    world.record_error(world.db().snapshot_at(version));
}

#[then("the operation is rejected because the read version is too new")]
fn read_version_is_too_new(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ReadVersionTooNew { .. })
    ));
}

#[then("the operation is rejected because the read version expired")]
fn read_version_expired(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::ReadVersionExpired { .. })
    ));
}

#[then("the operation is rejected because the checkpoint already exists")]
fn checkpoint_already_exists(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::CheckpointAlreadyExists { .. })
    ));
}
