use cucumber::{given, then, when};
use trine_kv::{Error, WriteBatch, WriteOptions};

use super::world::TrineWorld;

#[given(expr = "named bucket {string} exists")]
async fn named_bucket_exists(world: &mut TrineWorld, name: String) {
    world
        .db()
        .bucket(name)
        .await
        .expect("named bucket is created");
}

#[given(expr = "named bucket {string} contains key {string} with value {string}")]
async fn named_bucket_contains(world: &mut TrineWorld, name: String, key: String, value: String) {
    world
        .db()
        .bucket(name)
        .await
        .expect("named bucket opens")
        .put(key.into_bytes(), value.into_bytes())
        .await
        .expect("named bucket write commits");
}

#[when(expr = "I read key {string} from named bucket {string}")]
async fn read_named_bucket(world: &mut TrineWorld, key: String, name: String) {
    world.last_value = world
        .db()
        .bucket(name)
        .await
        .expect("named bucket opens")
        .get(key.as_bytes())
        .await
        .expect("named bucket read succeeds");
}

#[given(expr = "I retain a handle to named bucket {string}")]
async fn retain_named_bucket(world: &mut TrineWorld, name: String) {
    world.retained_bucket = Some(world.db().bucket(name).await.expect("named bucket opens"));
}

#[when(expr = "I drop and recreate named bucket {string}")]
async fn drop_and_recreate_bucket(world: &mut TrineWorld, name: String) {
    world
        .db()
        .drop_bucket(name.as_str())
        .await
        .expect("named bucket drops");
    world
        .db()
        .bucket(name)
        .await
        .expect("named bucket recreates");
}

#[when(expr = "the retained bucket handle reads key {string}")]
async fn read_from_retained_bucket(world: &mut TrineWorld, key: String) {
    let result = world
        .retained_bucket
        .as_ref()
        .expect("bucket handle was retained")
        .get(key.as_bytes())
        .await;
    world.record_error(result);
}

#[when(expr = "I try to open named bucket {string}")]
async fn try_open_named_bucket(world: &mut TrineWorld, name: String) {
    let result = world.db().bucket(name).await;
    world.record_error(result);
}

#[when(
    expr = "I atomically write key {string} as {string} and named bucket {string} key {string} as {string}"
)]
async fn atomic_cross_bucket_write(
    world: &mut TrineWorld,
    key: String,
    value: String,
    bucket: String,
    bucket_key: String,
    bucket_value: String,
) {
    let mut batch = WriteBatch::new();
    batch.put(key.into_bytes(), value.into_bytes());
    batch
        .put_bucket(bucket, bucket_key.into_bytes(), bucket_value.into_bytes())
        .expect("scenario names a valid named bucket");
    world
        .db()
        .write(batch, WriteOptions::default())
        .await
        .expect("cross-bucket batch commits");
}

#[when(
    expr = "I try to atomically write key {string} as {string} and missing bucket {string} key {string} as {string}"
)]
async fn rejected_cross_bucket_write(
    world: &mut TrineWorld,
    key: String,
    value: String,
    bucket: String,
    bucket_key: String,
    bucket_value: String,
) {
    let mut batch = WriteBatch::new();
    batch.put(key.into_bytes(), value.into_bytes());
    batch
        .put_bucket(bucket, bucket_key.into_bytes(), bucket_value.into_bytes())
        .expect("scenario names a syntactically valid missing bucket");
    let result = world.db().write(batch, WriteOptions::default()).await;
    world.record_error(result);
}

#[then(expr = "key {string} is absent from named bucket {string}")]
async fn named_bucket_key_is_absent(world: &mut TrineWorld, key: String, name: String) {
    assert_eq!(
        world
            .db()
            .bucket(name)
            .await
            .expect("named bucket opens")
            .get(key.as_bytes())
            .await
            .expect("named bucket absence check succeeds"),
        None
    );
}

#[then(expr = "named bucket {string} key {string} contains {string}")]
async fn named_bucket_key_contains(
    world: &mut TrineWorld,
    name: String,
    key: String,
    expected: String,
) {
    assert_eq!(
        world
            .db()
            .bucket(name)
            .await
            .expect("named bucket opens")
            .get(key.as_bytes())
            .await
            .expect("named bucket point check succeeds")
            .as_deref(),
        Some(expected.as_bytes())
    );
}

#[then("the retained bucket handle is rejected as stale")]
fn retained_bucket_is_stale(world: &mut TrineWorld) {
    assert!(matches!(world.last_error, Some(Error::BucketStale { .. })));
}
