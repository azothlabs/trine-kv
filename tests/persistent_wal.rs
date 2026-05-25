use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use trine_kv::{
    Db, DbOptions, DurabilityMode, KeyRange, KeyspaceOptions, WriteBatch, WriteOptions,
};

fn temp_db_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("trine-kv-{name}-{}-{nonce}", std::process::id()))
}

#[test]
fn persistent_wal_replays_point_and_range_batches() {
    let path = temp_db_path("wal-replay");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace opens");

        keyspace.insert(b"a", b"a1").expect("write a");
        keyspace.insert(b"b", b"b1").expect("write b");
        keyspace.insert(b"c", b"c1").expect("write c");
        keyspace.remove(b"b").expect("delete b");
        keyspace
            .remove_range(KeyRange::half_open(b"c", b"d"))
            .expect("range delete c");
        db.persist(DurabilityMode::Flush).expect("flush WAL");
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let keyspace = db
            .keyspace("default", KeyspaceOptions::default())
            .expect("keyspace reopens");

        assert_eq!(db.stats().live_keyspaces, 1);
        assert_eq!(keyspace.get(b"a").expect("a replays"), Some(b"a1".to_vec()));
        assert_eq!(keyspace.get(b"b").expect("b delete replays"), None);
        assert_eq!(keyspace.get(b"c").expect("range delete replays"), None);

        let mut batch = WriteBatch::new();
        batch.insert("default", b"d", b"d1");
        let info = db
            .write(
                batch,
                WriteOptions {
                    durability: DurabilityMode::Flush,
                },
            )
            .expect("post-replay write commits");
        assert_eq!(info.sequence().get(), 6);
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}

#[test]
fn persistent_wal_replays_cross_keyspace_batch() {
    let path = temp_db_path("cross-keyspace");
    let options = DbOptions::persistent(&path);

    {
        let db = Db::open(options.clone()).expect("persistent db opens");
        db.keyspace("users", KeyspaceOptions::default())
            .expect("users keyspace opens");
        db.keyspace("posts", KeyspaceOptions::default())
            .expect("posts keyspace opens");

        let mut batch = WriteBatch::new();
        batch.insert("users", b"1", b"ada");
        batch.insert("posts", b"1", b"hello");
        db.write(
            batch,
            WriteOptions {
                durability: DurabilityMode::Flush,
            },
        )
        .expect("cross-keyspace batch commits");
    }

    {
        let db = Db::open(options).expect("persistent db reopens");
        let users = db
            .keyspace("users", KeyspaceOptions::default())
            .expect("users keyspace reopens");
        let posts = db
            .keyspace("posts", KeyspaceOptions::default())
            .expect("posts keyspace reopens");

        assert_eq!(
            users.get(b"1").expect("users replay"),
            Some(b"ada".to_vec())
        );
        assert_eq!(
            posts.get(b"1").expect("posts replay"),
            Some(b"hello".to_vec())
        );
    }

    fs::remove_dir_all(path).expect("cleanup test db");
}
