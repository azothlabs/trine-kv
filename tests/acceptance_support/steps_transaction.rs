use cucumber::{then, when};
use trine_kv::{Error, KeyRange, TransactionOptions};

use super::world::TrineWorld;

#[when(
    expr = "a transaction reads {string}, stages key {string} as {string}, and another writer changes {string}"
)]
async fn point_dependency_changes(
    world: &mut TrineWorld,
    read_key: String,
    staged_key: String,
    staged_value: String,
    changed_key: String,
) {
    let mut transaction = world.db().transaction(TransactionOptions::default());
    transaction
        .get(read_key.as_bytes())
        .await
        .expect("transaction reads dependency");
    transaction.put(staged_key.into_bytes(), staged_value.into_bytes());
    world
        .db()
        .put(changed_key.into_bytes(), b"changed".to_vec())
        .await
        .expect("competing point write commits");
    world.record_error(transaction.commit().await);
}

#[when(
    expr = "a transaction reads keys from {string} up to {string}, stages key {string} as {string}, and another writer inserts {string}"
)]
async fn range_dependency_changes(
    world: &mut TrineWorld,
    start: String,
    end: String,
    staged_key: String,
    staged_value: String,
    inserted_key: String,
) {
    let mut transaction = world.db().transaction(TransactionOptions::default());
    transaction
        .range(KeyRange::half_open(start.into_bytes(), end.into_bytes()))
        .await
        .expect("transaction reads dependency range");
    transaction.put(staged_key.into_bytes(), staged_value.into_bytes());
    world
        .db()
        .put(inserted_key.into_bytes(), b"inserted".to_vec())
        .await
        .expect("competing range write commits");
    world.record_error(transaction.commit().await);
}

#[when(expr = "a transaction stages {string} and commits")]
async fn transaction_commits(world: &mut TrineWorld, rows: String) {
    let mut transaction = world.db().transaction(TransactionOptions::default());
    for entry in rows.split(',') {
        let (key, value) = entry
            .split_once('=')
            .expect("transaction fixture uses key=value syntax");
        transaction.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
    }
    transaction
        .commit()
        .await
        .expect("uncontended transaction commits");
}

#[then("the transaction is rejected as a conflict")]
fn transaction_is_rejected(world: &mut TrineWorld) {
    assert!(matches!(world.last_error, Some(Error::Conflict { .. })));
}
