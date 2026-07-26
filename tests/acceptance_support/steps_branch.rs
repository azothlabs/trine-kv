#![expect(
    clippy::needless_pass_by_value,
    reason = "cucumber expression captures require owned String parameters"
)]

use cucumber::{given, then, when};
use trine_kv::{Error, KeyRange};

use super::{fixtures::parse_rows, world::TrineWorld};

#[given("a new durable native database retaining only the latest read version")]
async fn new_durable_native_database(world: &mut TrineWorld) {
    world
        .open_new_native(1)
        .await
        .expect("durable native database opens");
}

#[given(expr = "named bucket {string} contains keys {string}")]
async fn named_bucket_contains_keys(
    world: &mut TrineWorld,
    bucket_name: String,
    specification: String,
) {
    let bucket = world
        .db()
        .bucket(bucket_name)
        .await
        .expect("named bucket opens");
    for (key, value) in parse_rows(&specification) {
        bucket
            .put(key, value)
            .await
            .expect("named bucket fixture write commits");
    }
}

#[given(expr = "I create durable branch {string}")]
fn create_durable_branch(world: &mut TrineWorld, name: String) {
    create_branch(world, &name);
}

#[when(expr = "I create durable branch {string}")]
fn create_durable_branch_after_action(world: &mut TrineWorld, name: String) {
    create_branch(world, &name);
}

fn create_branch(world: &TrineWorld, name: &str) {
    world
        .db()
        .create_branch(name, world.db().latest_read_version())
        .expect("durable branch creates");
}

#[given(expr = "I create durable branch {string} from branch {string}")]
fn create_durable_child_branch(world: &mut TrineWorld, name: String, parent: String) {
    create_child_branch(world, &name, &parent);
}

#[when(expr = "I create durable branch {string} from branch {string}")]
fn create_durable_child_branch_after_action(world: &mut TrineWorld, name: String, parent: String) {
    create_child_branch(world, &name, &parent);
}

fn create_child_branch(world: &TrineWorld, name: &str, parent: &str) {
    world
        .db()
        .create_branch_from(name, parent)
        .expect("durable child branch creates");
}

#[when(expr = "named bucket {string} writes key {string} with value {string}")]
async fn named_bucket_writes(
    world: &mut TrineWorld,
    bucket_name: String,
    key: String,
    value: String,
) {
    world
        .db()
        .bucket(bucket_name)
        .await
        .expect("named bucket opens")
        .put(key.into_bytes(), value.into_bytes())
        .await
        .expect("root write commits");
}

#[when(expr = "branch {string} writes named bucket {string} key {string} as {string}")]
fn branch_writes(
    world: &mut TrineWorld,
    branch_name: String,
    bucket_name: String,
    key: String,
    value: String,
) {
    world
        .db()
        .open_branch(&branch_name)
        .expect("durable branch opens")
        .put(bucket_name, key.into_bytes(), value.into_bytes())
        .expect("durable branch write commits");
}

#[when(expr = "branch {string} deletes named bucket {string} key {string}")]
fn branch_deletes(world: &mut TrineWorld, branch_name: String, bucket_name: String, key: String) {
    world
        .db()
        .open_branch(&branch_name)
        .expect("durable branch opens")
        .delete(bucket_name, key.into_bytes())
        .expect("durable branch deletion commits");
}

#[when(expr = "I scan named bucket {string} on branch {string}")]
fn scan_branch(world: &mut TrineWorld, bucket_name: String, branch_name: String) {
    world.branch_rows = world
        .db()
        .open_branch(&branch_name)
        .expect("durable branch opens")
        .range(bucket_name, &KeyRange::all())
        .expect("branch range opens")
        .map(|row| {
            let row = row.expect("branch row reads");
            (row.key, row.value)
        })
        .collect();
}

#[when(expr = "I delete durable branch {string}")]
fn delete_durable_branch(world: &mut TrineWorld, name: String) {
    world
        .db()
        .delete_branch(&name)
        .expect("durable branch deletes");
}

#[when(expr = "I try to delete durable branch {string}")]
fn try_delete_durable_branch(world: &mut TrineWorld, name: String) {
    let result = world.db().delete_branch(&name);
    world.record_error(result);
}

#[when("I reopen the native database")]
async fn reopen_native_database(world: &mut TrineWorld) {
    world
        .reopen(false)
        .await
        .expect("durable native database reopens");
}

#[when(expr = "the root advances key {string} through {int} later values in named bucket {string}")]
async fn advance_root_values(
    world: &mut TrineWorld,
    key: String,
    count: usize,
    bucket_name: String,
) {
    let bucket = world
        .db()
        .bucket(bucket_name)
        .await
        .expect("named bucket opens");
    for index in 0..count {
        bucket
            .put(
                key.as_bytes().to_vec(),
                format!("root-{index}").into_bytes(),
            )
            .await
            .expect("root history advances");
    }
}

#[then(expr = "branch {string} key {string} in named bucket {string} contains {string}")]
fn branch_key_contains(
    world: &mut TrineWorld,
    branch_name: String,
    key: String,
    bucket_name: String,
    expected: String,
) {
    assert_eq!(
        world
            .db()
            .open_branch(&branch_name)
            .expect("durable branch opens")
            .get(bucket_name, key.as_bytes())
            .expect("branch key reads")
            .as_deref(),
        Some(expected.as_bytes())
    );
}

#[then(expr = "branch {string} key {string} in named bucket {string} is absent")]
fn branch_key_is_absent(
    world: &mut TrineWorld,
    branch_name: String,
    key: String,
    bucket_name: String,
) {
    assert_eq!(
        world
            .db()
            .open_branch(&branch_name)
            .expect("durable branch opens")
            .get(bucket_name, key.as_bytes())
            .expect("branch key reads"),
        None
    );
}

#[then(expr = "the branch rows are {string}")]
fn branch_rows_are(world: &mut TrineWorld, expected: String) {
    assert_eq!(world.branch_rows, parse_rows(&expected));
}

#[then(expr = "branch {string} reports parent {string}")]
fn branch_reports_parent(world: &mut TrineWorld, branch_name: String, expected: String) {
    let info = world
        .db()
        .branch_info(&branch_name)
        .expect("branch lineage reads")
        .expect("branch is active");
    assert_eq!(info.parent(), Some(expected.as_str()));
}

#[then("the operation is rejected while the branch has a child")]
fn branch_with_child_is_not_deleted(world: &mut TrineWorld) {
    assert!(
        matches!(world.last_error, Some(Error::InvalidOptions { .. })),
        "expected a typed invalid branch-lifecycle operation, observed {:?}",
        world.last_error
    );
}

#[then("no durable branches are listed")]
fn no_durable_branches_are_listed(world: &mut TrineWorld) {
    assert!(
        world
            .db()
            .list_branches()
            .expect("durable branch list reads")
            .is_empty()
    );
}
