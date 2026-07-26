use cucumber::{given, then, when};
use trine_kv::{DurabilityMode, Error, WriteBatch, WriteOptions};

use super::world::TrineWorld;

#[given("a new durable database")]
async fn new_durable_database(world: &mut TrineWorld) {
    world.open_new(1).await.expect("durable database opens");
}

#[given("a new durable database retaining only the latest read version")]
async fn new_latest_only_database(world: &mut TrineWorld) {
    world
        .open_new(1)
        .await
        .expect("latest-only durable database opens");
}

#[given(expr = "key {string} contains {string}")]
async fn key_contains(world: &mut TrineWorld, key: String, value: String) {
    world
        .db()
        .put(key.into_bytes(), value.into_bytes())
        .await
        .expect("acceptance write commits");
}

#[when(expr = "I write key {string} with value {string}")]
async fn write_key(world: &mut TrineWorld, key: String, value: String) {
    key_contains(world, key, value).await;
}

#[when(expr = "I try to write key {string} with value {string}")]
async fn try_write_key(world: &mut TrineWorld, key: String, value: String) {
    let result = world.db().put(key.into_bytes(), value.into_bytes()).await;
    world.record_error(result);
}

#[when(expr = "I read key {string}")]
async fn read_key(world: &mut TrineWorld, key: String) {
    world.last_value = world
        .db()
        .get(key.as_bytes())
        .await
        .expect("acceptance point read succeeds");
}

#[when(expr = "I try to read key {string}")]
async fn try_read_key(world: &mut TrineWorld, key: String) {
    let result = world.db().get(key.as_bytes()).await;
    match result {
        Ok(value) => {
            world.last_value = value;
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error),
    }
}

#[when(expr = "I delete key {string}")]
async fn delete_key(world: &mut TrineWorld, key: String) {
    world
        .db()
        .delete(key.into_bytes())
        .await
        .expect("point deletion commits");
}

#[when("I reopen the database")]
async fn reopen_database(world: &mut TrineWorld) {
    world.reopen(false).await.expect("durable database reopens");
}

#[when("I reopen the database read-only")]
async fn reopen_database_read_only(world: &mut TrineWorld) {
    world
        .reopen(true)
        .await
        .expect("durable database reopens read-only");
}

#[when("I flush the database")]
async fn flush_database(world: &mut TrineWorld) {
    world.db().flush().await.expect("database flush succeeds");
}

#[when("I compact the database")]
async fn compact_database(world: &mut TrineWorld) {
    world
        .db()
        .compact_range(trine_kv::KeyRange::all())
        .await
        .expect("database compaction succeeds");
}

#[when("I close the database")]
async fn close_database(world: &mut TrineWorld) {
    world.db().close().await.expect("database closes");
}

#[given("I remember the latest read version")]
fn remember_latest_read_version(world: &mut TrineWorld) {
    world.remembered_version = Some(world.db().latest_read_version());
}

#[when("I commit an empty batch")]
async fn commit_empty_batch(world: &mut TrineWorld) {
    world
        .db()
        .write(WriteBatch::new(), WriteOptions::default())
        .await
        .expect("empty batch is accepted");
}

#[when("another writer tries to open the same database")]
async fn second_writer_tries_to_open(world: &mut TrineWorld) {
    match world.try_open_second_writer().await {
        Ok(second) => {
            second
                .close()
                .await
                .expect("unexpected second writer still closes cleanly");
            world.last_error = None;
        }
        Err(error) => world.last_error = Some(error),
    }
}

#[given("the current writer confirms its durable ownership")]
async fn current_writer_confirms_ownership(world: &mut TrineWorld) {
    world
        .db()
        .persist(DurabilityMode::Flush)
        .await
        .expect("current writer renews and confirms its durable ownership");
}

#[then(expr = "the value is {string}")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "cucumber expression captures require an owned String parameter"
)]
fn value_is(world: &mut TrineWorld, expected: String) {
    assert_eq!(world.last_value.as_deref(), Some(expected.as_bytes()));
}

#[then(expr = "key {string} is absent")]
async fn key_is_absent(world: &mut TrineWorld, key: String) {
    assert_eq!(
        world
            .db()
            .get(key.as_bytes())
            .await
            .expect("absence check succeeds"),
        None
    );
}

#[then(expr = "key {string} contains {string}")]
async fn key_still_contains(world: &mut TrineWorld, key: String, expected: String) {
    assert_eq!(
        world
            .db()
            .get(key.as_bytes())
            .await
            .expect("point check succeeds")
            .as_deref(),
        Some(expected.as_bytes())
    );
}

#[then("the latest read version is unchanged")]
fn latest_read_version_is_unchanged(world: &mut TrineWorld) {
    assert_eq!(
        world.db().latest_read_version(),
        world
            .remembered_version
            .expect("latest read version was remembered")
    );
}

#[then("the operation is rejected because the database is closed")]
fn operation_is_closed(world: &mut TrineWorld) {
    assert!(matches!(world.last_error, Some(Error::Closed)));
}

#[then("the operation is rejected because the database is read-only")]
fn operation_is_read_only(world: &mut TrineWorld) {
    assert!(matches!(world.last_error, Some(Error::ReadOnly)));
}

#[then("the operation is rejected because the writer lease is unavailable")]
fn writer_lease_is_unavailable(world: &mut TrineWorld) {
    assert!(
        matches!(world.last_error, Some(Error::LeaseUnavailable { .. })),
        "expected LeaseUnavailable, observed {:?}",
        world.last_error
    );
}

#[then("the operation is rejected as invalid options")]
fn operation_is_invalid_options(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::InvalidOptions { .. })
    ));
}

#[then("the operation is rejected because the bucket is missing")]
fn operation_is_bucket_missing(world: &mut TrineWorld) {
    assert!(matches!(
        world.last_error,
        Some(Error::BucketMissing { .. })
    ));
}
