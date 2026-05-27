use trine_kv::{
    Db, DbOptions, DurabilityMode, KeyRange, TransactionOptions, WriteBatch, WriteOptions,
};

fn main() -> trine_kv::Result<()> {
    let path = std::env::temp_dir().join(format!("trine-kv-quickstart-{}", std::process::id()));
    if path.exists() {
        std::fs::remove_dir_all(&path)?;
    }

    let db = Db::open(DbOptions::persistent(&path).with_durability(DurabilityMode::Flush))?;
    let users = db.bucket("users")?;

    users.put_with_options(b"user:001", b"Ada", WriteOptions::sync_all())?;

    let mut batch = WriteBatch::new();
    batch.put_bucket("users", b"user:002", b"Lin")?;
    batch.put_bucket("users", b"team:core", b"database")?;
    db.write(batch, WriteOptions::sync_all())?;

    assert_eq!(users.get(b"user:001")?, Some(b"Ada".to_vec()));

    let snapshot = db.snapshot();
    users.put(b"user:003", b"Grace")?;
    assert_eq!(snapshot.get(&users, b"user:003")?, None);
    assert_eq!(users.get(b"user:003")?, Some(b"Grace".to_vec()));

    let user_prefix_keys = users
        .prefix(b"user:")?
        .map(|item| item.map(|key_value| display_key(&key_value.key)))
        .collect::<trine_kv::Result<Vec<_>>>()?;
    assert_eq!(user_prefix_keys, ["user:001", "user:002", "user:003"]);

    let range = KeyRange::half_open(b"user:001", b"user:004");
    let range_values = users
        .range(&range)?
        .map(|item| item.map(|key_value| display_value(&key_value.value)))
        .collect::<trine_kv::Result<Vec<_>>>()?;
    assert_eq!(range_values, ["Ada", "Lin", "Grace"]);

    let mut txn = db.transaction(TransactionOptions::default());
    assert_eq!(txn.get_bucket("users", b"user:001")?, Some(b"Ada".to_vec()));
    txn.put_bucket("users", b"user:004", b"Barbara")?;
    txn.commit()?;

    db.flush()?;
    db.persist(DurabilityMode::SyncAll)?;
    drop(users);
    drop(snapshot);
    drop(db);

    let reopened = Db::open(DbOptions::persistent(&path))?;
    let users = reopened.bucket("users")?;
    assert_eq!(users.get(b"user:004")?, Some(b"Barbara".to_vec()));

    let stats = reopened.stats();
    assert_eq!(stats.live_buckets, 2);
    assert!(stats.total_tables > 0);

    drop(users);
    drop(reopened);
    std::fs::remove_dir_all(path)?;
    Ok(())
}

fn display_key(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn display_value(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
