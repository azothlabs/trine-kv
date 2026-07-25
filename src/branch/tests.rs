use super::{BranchLifecycle, RegistryEntry, data_bucket, fork_checkpoint, registry_bucket};
use crate::storage::{
    StorageObjectKind,
    fault_injection::{StorageFaultGuard, StorageFaultPoint},
};
use crate::{Db, DbOptions, KeyRange, ReadVersion};

fn memory_db() -> Db {
    Db::open_sync(DbOptions::memory()).expect("open in-memory db")
}

#[test]
fn branch_reads_parent_then_shadows_with_local_writes() {
    let db = memory_db();
    let bucket = db.bucket_sync("data").expect("bucket");
    bucket.put_sync(b"k1".to_vec(), b"v1".to_vec()).expect("p1");
    bucket.put_sync(b"k2".to_vec(), b"v2".to_vec()).expect("p2");

    let mut branch = db.branch_from_latest().expect("branch");
    assert_eq!(
        branch.get("data", b"k1").expect("get"),
        Some(b"v1".to_vec())
    );

    branch
        .put("data", b"k1", b"v1-branch".to_vec())
        .expect("put");
    branch.delete("data", b"k2").expect("delete");
    assert_eq!(
        branch.get("data", b"k1").expect("get"),
        Some(b"v1-branch".to_vec())
    );
    assert_eq!(branch.get("data", b"k2").expect("get"), None);

    // The parent is untouched.
    assert_eq!(bucket.get_sync(b"k1").expect("get"), Some(b"v1".to_vec()));
    assert_eq!(bucket.get_sync(b"k2").expect("get"), Some(b"v2".to_vec()));
}

#[test]
fn branch_pins_its_fork_while_the_parent_diverges() {
    let db = memory_db();
    let bucket = db.bucket_sync("data").expect("bucket");
    bucket.put_sync(b"k".to_vec(), b"v1".to_vec()).expect("p1");

    let branch = db.branch_from_latest().expect("branch");
    bucket.put_sync(b"k".to_vec(), b"v2".to_vec()).expect("p2");

    assert_eq!(
        branch.get("data", b"k").expect("get"),
        Some(b"v1".to_vec()),
        "the branch stays frozen at its fork while the parent diverges"
    );
    assert_eq!(bucket.get_sync(b"k").expect("get"), Some(b"v2".to_vec()));
}

#[test]
fn branch_at_a_retained_past_version_time_travels() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(8))
        .expect("open with retention");
    let bucket = db.bucket_sync("data").expect("bucket");
    bucket.put_sync(b"k".to_vec(), b"v1".to_vec()).expect("p1");
    let v1 = db.latest_read_version();
    bucket.put_sync(b"k".to_vec(), b"v2".to_vec()).expect("p2");

    let old = db.branch_at(v1).expect("branch at v1");
    assert_eq!(old.get("data", b"k").expect("get"), Some(b"v1".to_vec()));
    let now = db.branch_from_latest().expect("branch now");
    assert_eq!(now.get("data", b"k").expect("get"), Some(b"v2".to_vec()));
}

#[test]
fn ephemeral_branch_range_merges_overlay_over_parent() {
    let db = memory_db();
    let bucket = db.bucket_sync("data").expect("bucket");
    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
        bucket.put_sync(k.to_vec(), v.to_vec()).expect("seed");
    }

    let mut branch = db.branch_from_latest().expect("branch");
    branch
        .put("data", b"b", b"2-branch".to_vec())
        .expect("override b");
    branch.delete("data", b"c").expect("delete c");
    branch.put("data", b"d", b"4".to_vec()).expect("add d");

    let rows = branch.range("data", &KeyRange::all()).expect("range");
    let got: Vec<(Vec<u8>, Vec<u8>)> = rows
        .map(|kv| {
            let kv = kv.expect("row");
            (kv.key, kv.value)
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2-branch".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ]
    );
}

#[test]
fn durable_branch_persists_writes_and_shadows_parent() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(64)).expect("open");
    let bucket = db.bucket_sync("data").expect("bucket");
    bucket
        .put_sync(b"k1".to_vec(), b"parent".to_vec())
        .expect("p1");
    bucket
        .put_sync(b"k2".to_vec(), b"parent".to_vec())
        .expect("p2");

    db.create_branch("dev", db.latest_read_version())
        .expect("create");
    {
        let mut dev = db.open_branch("dev").expect("open");
        dev.put("data", b"k1", b"dev".to_vec()).expect("put");
        dev.delete("data", b"k2").expect("delete");
    }

    // A freshly opened handle sees the persisted branch writes; the parent is
    // untouched.
    let dev = db.open_branch("dev").expect("reopen");
    assert_eq!(dev.get("data", b"k1").expect("get"), Some(b"dev".to_vec()));
    assert_eq!(
        dev.get("data", b"k2").expect("get"),
        None,
        "branch tombstone hides parent"
    );
    assert_eq!(dev.get("data", b"k3").expect("get"), None);
    assert_eq!(
        bucket.get_sync(b"k1").expect("get"),
        Some(b"parent".to_vec())
    );
    assert_eq!(
        bucket.get_sync(b"k2").expect("get"),
        Some(b"parent".to_vec())
    );

    assert_eq!(db.list_branches().expect("list"), vec!["dev".to_string()]);
    assert!(dev.is_durable());
}

#[test]
fn durable_branch_write_commits_data_and_registry_together() {
    let dir =
        std::env::temp_dir().join(format!("trine-branch-atomic-write-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = Db::open_sync(&dir).expect("open");
    db.bucket_sync("data").expect("bucket");
    db.create_branch("dev", db.latest_read_version())
        .expect("create");
    let mut dev = db.open_branch("dev").expect("open");

    let fault = StorageFaultGuard::install(
        &dir,
        StorageFaultPoint::WalAppend,
        Some(StorageObjectKind::Wal),
        1,
    );
    assert!(dev.put("data", b"k", b"v".to_vec()).is_err());
    assert_eq!(fault.calls(), 1);
    drop(fault);

    let registry = db.read_registry("dev").expect("registry").expect("entry");
    assert!(registry.written_buckets.is_empty());
    assert_eq!(
        db.bucket_sync(data_bucket("dev", "data"))
            .expect("data bucket")
            .get_sync(b"k")
            .expect("data read"),
        None
    );

    dev.put("data", b"k", b"v".to_vec())
        .expect("same handle retries");
    drop(dev);
    let reopened = db.open_branch("dev").expect("branch reopens");
    assert_eq!(
        reopened.get("data", b"k").expect("read"),
        Some(b"v".to_vec())
    );
    drop(reopened);
    drop(db);
    std::fs::remove_dir_all(dir).expect("test database removes");
}

#[test]
fn deleting_marker_hides_branch_and_delete_resumes() {
    let db = memory_db();
    db.bucket_sync("data").expect("bucket");
    db.create_branch("dev", db.latest_read_version())
        .expect("create");
    let mut stale = db.open_branch("dev").expect("open");
    stale.put("data", b"k", b"v".to_vec()).expect("branch data");

    let mut entry = db.read_registry("dev").expect("registry").expect("entry");
    entry.lifecycle = BranchLifecycle::Deleting;
    db.bucket_sync(registry_bucket())
        .expect("registry bucket")
        .put_sync(b"dev".to_vec(), entry.encode())
        .expect("persist interrupted delete marker");

    assert!(db.list_branches().expect("list").is_empty());
    assert!(db.branch_info("dev").expect("info").is_none());
    assert!(db.open_branch("dev").is_err());
    assert!(stale.get("data", b"k").is_err());
    assert!(stale.put("data", b"k2", b"v2".to_vec()).is_err());

    db.delete_branch("dev").expect("delete resumes");
    db.delete_branch("dev")
        .expect("completed delete is idempotent");
    assert!(db.read_registry("dev").expect("registry").is_none());
}

#[test]
fn stale_handle_cannot_write_recreated_branch_generation() {
    let db = memory_db();
    db.bucket_sync("data").expect("bucket");
    let fork = db.latest_read_version();
    db.create_branch("dev", fork).expect("create");
    let mut stale = db.open_branch("dev").expect("open old generation");
    db.delete_branch("dev").expect("delete old generation");
    db.create_branch("dev", fork).expect("recreate");

    assert!(
        stale.put("data", b"stale", b"value".to_vec()).is_err(),
        "an old handle must not mutate a replacement with the same name and fork"
    );
    assert_eq!(
        db.open_branch("dev")
            .expect("open replacement")
            .get("data", b"stale")
            .expect("read replacement"),
        None
    );
}

#[test]
fn legacy_registry_entry_decodes_as_active_generation_zero() {
    let mut legacy = Vec::new();
    legacy.extend_from_slice(&7_u64.to_le_bytes());
    legacy.extend_from_slice(&0_u32.to_le_bytes());
    legacy.push(0);
    let decoded = RegistryEntry::decode(&legacy).expect("legacy entry decodes");
    assert_eq!(decoded.fork, ReadVersion::from_u64(7));
    assert_eq!(decoded.lifecycle, BranchLifecycle::Active);
    assert_eq!(decoded.generation, [0; 16]);
}

#[test]
fn durable_branch_survives_reopen_with_default_retention() {
    let dir = std::env::temp_dir().join(format!("trine-branch-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    // Default retention keeps only the latest version, yet the branch's fork
    // checkpoint pins its fork history durably — so it reopens with no manual
    // retention configuration (slice 3).
    {
        let db = Db::open_sync(&dir).expect("open");
        let bucket = db.bucket_sync("data").expect("bucket");
        bucket
            .put_sync(b"k".to_vec(), b"parent".to_vec())
            .expect("seed");
        db.create_branch("dev", db.latest_read_version())
            .expect("create");
        let mut dev = db.open_branch("dev").expect("open");
        dev.put("data", b"k", b"dev".to_vec()).expect("put");
        db.flush_sync().expect("flush");
    }
    // Reopen: the durable branch, its fork, and its writes all survive.
    let db = Db::open_sync(&dir).expect("reopen");
    assert_eq!(db.list_branches().expect("list"), vec!["dev".to_string()]);
    let dev = db.open_branch("dev").expect("open after reopen");
    assert_eq!(dev.get("data", b"k").expect("get"), Some(b"dev".to_vec()));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn durable_branch_fork_is_pinned_against_aggressive_gc() {
    // keep_last_read_versions(1) retains only the latest version, so without a
    // pin the fork would expire after the next parent write.
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(1)).expect("open");
    let bucket = db.bucket_sync("data").expect("bucket");
    bucket
        .put_sync(b"k".to_vec(), b"forked".to_vec())
        .expect("seed");
    db.create_branch("dev", db.latest_read_version())
        .expect("create");

    // Hammer the parent well past the fork; the fork checkpoint keeps that
    // history retained.
    for i in 0..50 {
        bucket
            .put_sync(b"k".to_vec(), format!("v{i}").into_bytes())
            .expect("churn");
    }

    let dev = db.open_branch("dev").expect("fork still openable");
    assert_eq!(
        dev.get("data", b"k").expect("get"),
        Some(b"forked".to_vec()),
        "the branch still reads its fork value despite aggressive parent GC"
    );
}

#[test]
fn delete_branch_releases_the_fork_pin() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(1)).expect("open");
    let bucket = db.bucket_sync("data").expect("bucket");
    bucket
        .put_sync(b"k".to_vec(), b"forked".to_vec())
        .expect("seed");
    let fork = db.latest_read_version();
    db.create_branch("dev", fork).expect("create");

    // While the branch lives, the fork stays pinned even past parent writes.
    bucket
        .put_sync(b"k".to_vec(), b"after".to_vec())
        .expect("write");
    assert!(
        db.branch_at(fork).is_ok(),
        "fork pinned while branch exists"
    );

    db.delete_branch("dev").expect("delete");
    assert!(
        db.open_branch("dev").is_err(),
        "deleted branch cannot be opened"
    );
    // The pin is released: with only the latest retained, a further write
    // pushes the floor past the fork, which is now expired.
    bucket
        .put_sync(b"k".to_vec(), b"later".to_vec())
        .expect("write");
    assert!(
        db.branch_at(fork).is_err(),
        "the fork is no longer pinned after the branch is deleted"
    );
}

#[test]
fn durable_branch_range_merges_over_parent() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(64)).expect("open");
    let bucket = db.bucket_sync("data").expect("bucket");
    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
        bucket.put_sync(k.to_vec(), v.to_vec()).expect("seed");
    }
    db.create_branch("dev", db.latest_read_version())
        .expect("create");
    let mut dev = db.open_branch("dev").expect("open");
    dev.put("data", b"b", b"2-dev".to_vec()).expect("override");
    dev.delete("data", b"c").expect("delete");
    dev.put("data", b"d", b"4".to_vec()).expect("add");

    let rows = dev.range("data", &KeyRange::all()).expect("range");
    let got: Vec<(Vec<u8>, Vec<u8>)> = rows
        .map(|kv| {
            let kv = kv.expect("row");
            (kv.key, kv.value)
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2-dev".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ]
    );
}

#[test]
fn branch_of_branch_reads_through_the_whole_chain() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(64)).expect("open");
    let bucket = db.bucket_sync("data").expect("bucket");
    bucket
        .put_sync(b"base".to_vec(), b"root".to_vec())
        .expect("seed");
    bucket
        .put_sync(b"shared".to_vec(), b"root".to_vec())
        .expect("seed");

    // a forks root and overrides `shared`, adds `a-only`.
    db.create_branch("a", db.latest_read_version())
        .expect("create a");
    {
        let mut a = db.open_branch("a").expect("open a");
        a.put("data", b"shared", b"a".to_vec()).expect("a override");
        a.put("data", b"a-only", b"a".to_vec()).expect("a add");
    }

    // b forks a, overrides `shared` again, adds `b-only`, deletes `a-only`.
    db.create_branch_from("b", "a").expect("create b from a");
    let mut b = db.open_branch("b").expect("open b");
    b.put("data", b"shared", b"b".to_vec()).expect("b override");
    b.put("data", b"b-only", b"b".to_vec()).expect("b add");
    b.delete("data", b"a-only").expect("b delete a-only");

    // b sees: its own writes, then a's, then root's, in that precedence.
    assert_eq!(b.get("data", b"shared").expect("get"), Some(b"b".to_vec()));
    assert_eq!(b.get("data", b"b-only").expect("get"), Some(b"b".to_vec()));
    assert_eq!(
        b.get("data", b"a-only").expect("get"),
        None,
        "b deleted a's key"
    );
    assert_eq!(
        b.get("data", b"base").expect("get"),
        Some(b"root".to_vec()),
        "falls through a (untouched) to the root"
    );

    // The range view merges the whole chain.
    let rows = b.range("data", &KeyRange::all()).expect("range");
    let got: Vec<(Vec<u8>, Vec<u8>)> = rows
        .map(|kv| {
            let kv = kv.expect("row");
            (kv.key, kv.value)
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (b"b-only".to_vec(), b"b".to_vec()),
            (b"base".to_vec(), b"root".to_vec()),
            (b"shared".to_vec(), b"b".to_vec()),
        ]
    );

    // a is unaffected by b.
    let a = db.open_branch("a").expect("reopen a");
    assert_eq!(a.get("data", b"shared").expect("get"), Some(b"a".to_vec()));
    assert_eq!(a.get("data", b"a-only").expect("get"), Some(b"a".to_vec()));
    assert_eq!(a.get("data", b"b-only").expect("get"), None);
}

#[test]
fn branch_of_branch_is_frozen_when_its_parent_advances() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(64)).expect("open");
    db.bucket_sync("data").expect("bucket");
    db.create_branch("a", db.latest_read_version())
        .expect("create a");
    {
        let mut a = db.open_branch("a").expect("open a");
        a.put("data", b"k", b"a1".to_vec()).expect("a write");
    }
    // b forks a at this point.
    db.create_branch_from("b", "a").expect("create b");
    // a keeps writing after the fork.
    {
        let mut a = db.open_branch("a").expect("reopen a");
        a.put("data", b"k", b"a2".to_vec()).expect("a write later");
    }
    // b sees a's value as of the fork, not a's later write.
    let b = db.open_branch("b").expect("open b");
    assert_eq!(b.get("data", b"k").expect("get"), Some(b"a1".to_vec()));
}

#[test]
fn cannot_delete_branch_with_children() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(64)).expect("open");
    db.bucket_sync("data").expect("bucket");
    db.create_branch("a", db.latest_read_version())
        .expect("create a");
    db.create_branch_from("b", "a").expect("create b");

    assert!(
        db.delete_branch("a").is_err(),
        "a still has child b, so it cannot be deleted"
    );
    // Delete the child first, then the parent.
    db.delete_branch("b").expect("delete child");
    db.delete_branch("a")
        .expect("delete parent after child gone");
    assert!(db.list_branches().expect("list").is_empty());
}

#[test]
fn recreated_branch_does_not_inherit_deleted_branch_data() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(64)).expect("open");
    let bucket = db.bucket_sync("data").expect("bucket");
    bucket
        .put_sync(b"k".to_vec(), b"parent".to_vec())
        .expect("seed");

    db.create_branch("dev", db.latest_read_version())
        .expect("create");
    {
        let mut dev = db.open_branch("dev").expect("open");
        dev.put("data", b"k", b"old".to_vec()).expect("write");
        dev.put("data", b"only-old", b"x".to_vec()).expect("write2");
    }
    db.delete_branch("dev")
        .expect("delete (clears the data bucket)");

    // Recreate the same name and write to the same bucket. The branch must
    // start from the parent, not inherit the deleted branch's rows.
    db.create_branch("dev", db.latest_read_version())
        .expect("recreate");
    let mut dev = db.open_branch("dev").expect("reopen");
    dev.put("data", b"k", b"new".to_vec()).expect("write");
    assert_eq!(dev.get("data", b"k").expect("get"), Some(b"new".to_vec()));
    assert_eq!(
        dev.get("data", b"only-old").expect("get"),
        None,
        "the deleted branch's data was cleared, not inherited"
    );
}

#[test]
fn branch_info_exposes_fork_and_parent_without_opening_data() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(64)).expect("open");
    db.bucket_sync("data").expect("bucket");
    assert!(
        db.branch_info("missing").expect("info").is_none(),
        "an unknown branch has no lineage"
    );

    let fork = db.latest_read_version();
    db.create_branch("a", fork).expect("create a");
    let a = db.branch_info("a").expect("info").expect("a present");
    assert_eq!(a.fork(), fork, "exposes the fork version for fall-through");
    assert_eq!(a.parent(), None, "a forked the root lineage");

    db.create_branch_from("b", "a").expect("create b");
    let b = db.branch_info("b").expect("info").expect("b present");
    assert_eq!(b.parent(), Some("a"), "exposes the parent for nesting");
}

#[test]
fn orphan_fork_checkpoint_cannot_be_rebound_to_another_version() {
    let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(64)).expect("open");
    let old_fork = db.latest_read_version();
    db.create_checkpoint_at_sync(&fork_checkpoint("dev"), old_fork)
        .expect("orphan checkpoint creates");
    db.bucket_sync("data")
        .expect("bucket")
        .put_sync(b"advance".to_vec(), b"1".to_vec())
        .expect("version advances");
    let new_fork = db.latest_read_version();
    assert_ne!(old_fork, new_fork);

    let error = db
        .create_branch("dev", new_fork)
        .expect_err("a stale orphan checkpoint cannot pin the new fork");
    assert!(error.to_string().contains("different version"));
    assert!(
        db.branch_info("dev").expect("registry reads").is_none(),
        "checkpoint mismatch must not publish a branch registry entry"
    );

    db.create_branch("dev", old_fork)
        .expect("retrying the matching interrupted create succeeds");
    assert_eq!(
        db.branch_info("dev")
            .expect("registry reads")
            .expect("branch publishes")
            .fork(),
        old_fork
    );
}

#[test]
fn drop_bucket_removes_it_in_memory() {
    let db = memory_db();
    let bucket = db.bucket_sync("scratch").expect("bucket");
    bucket.put_sync(b"k".to_vec(), b"v".to_vec()).expect("put");

    db.drop_bucket_sync("scratch").expect("drop");
    assert!(
        db.drop_bucket_sync("scratch").is_err(),
        "dropping a gone bucket errors"
    );
    assert!(
        db.drop_bucket_sync("default").is_err(),
        "the default bucket cannot be dropped"
    );

    // Recreating the name yields a fresh, empty bucket.
    let fresh = db.bucket_sync("scratch").expect("recreate");
    assert_eq!(fresh.get_sync(b"k").expect("get"), None);
}

#[test]
fn drop_bucket_persists_across_reopen() {
    let dir = std::env::temp_dir().join(format!("trine-drop-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    {
        let db = Db::open_sync(&dir).expect("open");
        db.bucket_sync("scratch")
            .expect("scratch")
            .put_sync(b"k".to_vec(), b"v".to_vec())
            .expect("put");
        db.bucket_sync("keep")
            .expect("keep")
            .put_sync(b"k".to_vec(), b"keep".to_vec())
            .expect("put");
        db.drop_bucket_sync("scratch").expect("drop");
    }
    // Reopen: the dropped bucket is gone (recreates empty); the other survives.
    let db = Db::open_sync(&dir).expect("reopen");
    assert_eq!(
        db.bucket_sync("scratch")
            .expect("scratch")
            .get_sync(b"k")
            .expect("get"),
        None,
        "dropped bucket did not come back with its data"
    );
    assert_eq!(
        db.bucket_sync("keep")
            .expect("keep")
            .get_sync(b"k")
            .expect("get"),
        Some(b"keep".to_vec()),
        "an untouched bucket survives the drop"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
