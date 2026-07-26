use std::{
    env,
    fs::OpenOptions as StdOpenOptions,
    io::Write as _,
    sync::{Arc, Mutex, OnceLock},
};

use super::*;
use crate::storage::{
    BlockingStorageObjectDeleteBackend, BlockingStorageWalRewriteBackend, NativeFileBackend,
    StorageObjectId, StorageObjectKind,
    fault_injection::{StorageFaultGuard, StorageFaultPoint},
};

static REPORT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn destructive_options(path: &std::path::Path) -> DbOptions {
    let mut options = DbOptions::persistent(path)
        .with_durability(DurabilityMode::SyncAll)
        .with_wal_shard_count(1);
    options.background_worker_count = 0;
    options
}

fn assert_injected_io(error: &Error, point: StorageFaultPoint) {
    assert!(
        matches!(
            error,
            Error::Io(source) | Error::ManifestPublishedDurabilityUnknown { source }
                if source.to_string().contains(&format!("{point:?}"))
        ),
        "expected injected {point:?} I/O error, got {error:?}"
    );
}

fn assert_unknown_wal_failure(error: &Error, point: StorageFaultPoint) {
    assert!(
        matches!(error, Error::Corruption { message } if
            message.contains(&format!("{point:?}"))
                && message.contains("durable outcome is unknown")
                && message.contains("database handle closed")),
        "expected stage-aware {point:?} WAL failure, got {error:?}"
    );
}

fn record_destructive_result(scenario: &str, outcome: &str) {
    let Some(path) = env::var_os("TRINE_DESTRUCTIVE_REPORT") else {
        return;
    };
    let _guard = REPORT_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("destructive report lock");
    let mut report = StdOpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open destructive report");
    writeln!(
        report,
        "{{\"scenario\":\"{scenario}\",\"outcome\":\"{outcome}\",\"os\":\"{}\",\"arch\":\"{}\"}}",
        env::consts::OS,
        env::consts::ARCH
    )
    .expect("append destructive report");
}

#[test]
fn destructive_wal_append_failure_does_not_publish_the_write() {
    let path = temp_db_path("destructive-wal-append");
    let options = destructive_options(&path);
    let db = Db::open_sync(options.clone()).expect("database opens");
    db.put_sync(b"baseline", b"safe")
        .expect("baseline commits");

    let fault = StorageFaultGuard::install(
        &path,
        StorageFaultPoint::WalAppend,
        Some(StorageObjectKind::Wal),
        1,
    );
    let error = db
        .put_sync(b"rejected", b"must-not-appear")
        .expect_err("injected WAL append failure reaches caller");
    assert_unknown_wal_failure(&error, StorageFaultPoint::WalAppend);
    assert_eq!(fault.calls(), 1);
    drop(fault);

    assert!(
        db.put_sync(b"after", b"visible").is_err(),
        "a possibly partial WAL append permanently closes the lane"
    );
    db.close_sync();
    drop(db);

    let reopened = Db::open_sync(options.clone()).expect("database reopens");
    assert_eq!(reopened.get_sync(b"baseline").expect("baseline reads"), Some(b"safe".to_vec()));
    assert_eq!(reopened.get_sync(b"rejected").expect("rejected key reads"), None);
    reopened
        .put_sync(b"after", b"visible")
        .expect("writes resume only after recovery");
    assert_eq!(reopened.get_sync(b"after").expect("later key reads"), Some(b"visible".to_vec()));
    reopened.close_sync();
    fs::remove_dir_all(path).expect("cleanup destructive database");
    record_destructive_result("wal_append_before_write", "rejected_and_recovered");
}

#[test]
fn destructive_wal_persist_failure_has_an_explicit_unknown_commit_result() {
    let path = temp_db_path("destructive-wal-persist");
    let options = destructive_options(&path);
    let db = Db::open_sync(options.clone()).expect("database opens");
    db.put_sync(b"baseline", b"safe")
        .expect("baseline commits");

    let fault = StorageFaultGuard::install(
        &path,
        StorageFaultPoint::WalPersist,
        Some(StorageObjectKind::Wal),
        1,
    );
    let error = db
        .put_sync(b"unknown", b"complete-record")
        .expect_err("injected WAL persistence failure reaches caller");
    assert_unknown_wal_failure(&error, StorageFaultPoint::WalPersist);
    assert_eq!(fault.calls(), 1);
    drop(fault);
    db.close_sync();
    drop(db);

    let reopened = Db::open_sync(options).expect("database reopens after sync failure");
    assert_eq!(reopened.get_sync(b"baseline").expect("baseline reads"), Some(b"safe".to_vec()));
    let uncertain = reopened.get_sync(b"unknown").expect("uncertain key reads");
    assert!(
        uncertain.is_none() || uncertain == Some(b"complete-record".to_vec()),
        "a failed durability call may have an unknown commit result, but never arbitrary bytes"
    );
    reopened
        .put_sync(b"after", b"usable")
        .expect("database remains writable after reopen");
    reopened.close_sync();
    fs::remove_dir_all(path).expect("cleanup destructive database");
    record_destructive_result("wal_persist_after_append", "unknown_result_reopened_cleanly");
}

#[test]
fn destructive_table_publish_failure_fails_closed_then_repairs_safe_temp() {
    let path = temp_db_path("destructive-table-publish");
    let options = destructive_options(&path);
    let db = Db::open_sync(options.clone()).expect("database opens");
    db.put_sync(b"confirmed", b"from-wal")
        .expect("confirmed write commits");

    let fault = StorageFaultGuard::install(
        &path,
        StorageFaultPoint::ObjectPublish,
        Some(StorageObjectKind::Table),
        1,
    );
    let error = db
        .flush_sync()
        .expect_err("table publish failure reaches caller");
    assert_injected_io(&error, StorageFaultPoint::ObjectPublish);
    assert_eq!(fault.calls(), 1);
    drop(fault);
    db.close_sync();
    drop(db);

    let error = Db::open_sync(options.clone()).expect_err("safe temp fails closed by default");
    assert!(matches!(error, Error::Corruption { .. }));

    let mut repair = options.clone();
    repair.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;
    let repaired = Db::open_sync(repair).expect("explicit safe-temp repair reopens");
    assert_eq!(repaired.get_sync(b"confirmed").expect("WAL value reads"), Some(b"from-wal".to_vec()));
    repaired.flush_sync().expect("flush retries after repair");
    repaired.close_sync();
    drop(repaired);

    let reopened = Db::open_sync(options).expect("normal reopen succeeds after repaired flush");
    assert_eq!(reopened.get_sync(b"confirmed").expect("confirmed value reads"), Some(b"from-wal".to_vec()));
    reopened.close_sync();
    fs::remove_dir_all(path).expect("cleanup destructive database");
    record_destructive_result("table_publish_before_rename", "failed_closed_and_repaired");
}

#[test]
fn destructive_manifest_publish_failure_requires_explicit_safe_temp_repair() {
    let path = temp_db_path("destructive-manifest-publish");
    let options = destructive_options(&path);
    let fault = StorageFaultGuard::install(
        &path,
        StorageFaultPoint::ManifestPublish,
        Some(StorageObjectKind::Manifest),
        1,
    );
    let error = Db::open_sync(options.clone()).expect_err("manifest publish failure reaches caller");
    assert_injected_io(&error, StorageFaultPoint::ManifestPublish);
    assert_eq!(fault.calls(), 1);
    drop(fault);

    let error = Db::open_sync(options.clone()).expect_err("manifest temp fails closed by default");
    assert!(matches!(error, Error::Corruption { .. }));
    let mut repair = options;
    repair.fail_on_corruption = FailOnCorruptionPolicy::RepairSafeTemporaryFiles;
    let db = Db::open_sync(repair).expect("explicit safe-temp repair creates database");
    db.put_sync(b"after", b"usable").expect("database is usable");
    db.close_sync();
    fs::remove_dir_all(path).expect("cleanup destructive database");
    record_destructive_result("manifest_publish_before_rename", "failed_closed_and_repaired");
}

#[test]
fn destructive_directory_sync_failure_keeps_renamed_manifest_recoverable() {
    let path = temp_db_path("destructive-directory-sync");
    let options = destructive_options(&path);
    let fault = StorageFaultGuard::install(
        &path,
        StorageFaultPoint::DirectorySync,
        None,
        1,
    );
    let error = Db::open_sync(options.clone()).expect_err("directory sync failure reaches caller");
    assert_injected_io(&error, StorageFaultPoint::DirectorySync);
    assert_eq!(fault.calls(), 1);
    drop(fault);

    let db = Db::open_sync(options).expect("renamed manifest supports retry");
    db.put_sync(b"after", b"usable").expect("database is usable");
    db.close_sync();
    fs::remove_dir_all(path).expect("cleanup destructive database");
    record_destructive_result("directory_sync_after_rename", "retry_reopened_cleanly");
}

#[test]
fn destructive_wal_rewrite_and_delete_faults_preserve_existing_files() {
    let path = temp_db_path("destructive-storage-primitives");
    fs::create_dir_all(&path).expect("create destructive root");
    let wal_path = path.join("trine.wal");
    let wal_temp = path.join("trine.wal.tmp");
    fs::write(&wal_path, b"old").expect("write original WAL");
    let wal = StorageObjectId::native_file(StorageObjectKind::Wal, &wal_path);
    let temporary = StorageObjectId::native_file(StorageObjectKind::Wal, &wal_temp);
    let backend = NativeFileBackend::new();

    let rewrite_fault = StorageFaultGuard::install(
        &path,
        StorageFaultPoint::WalRewritePublish,
        Some(StorageObjectKind::Wal),
        1,
    );
    let error = backend
        .rewrite_wal_blocking(
            wal.clone(),
            temporary.clone(),
            Arc::from(b"new".as_slice()),
            DurabilityMode::SyncAll,
        )
        .expect_err("WAL rewrite publish fault reaches caller");
    assert_injected_io(&error, StorageFaultPoint::WalRewritePublish);
    assert_eq!(fs::read(&wal_path).expect("original WAL reads"), b"old");
    assert_eq!(fs::read(&wal_temp).expect("complete WAL temp reads"), b"new");
    drop(rewrite_fault);
    backend
        .rewrite_wal_blocking(
            wal.clone(),
            temporary,
            Arc::from(b"new".as_slice()),
            DurabilityMode::SyncAll,
        )
        .expect("WAL rewrite retry succeeds");
    assert_eq!(fs::read(&wal_path).expect("rewritten WAL reads"), b"new");

    let delete_fault = StorageFaultGuard::install(
        &path,
        StorageFaultPoint::ObjectDelete,
        Some(StorageObjectKind::Wal),
        1,
    );
    let error = backend
        .delete_object_blocking(wal.clone())
        .expect_err("delete fault reaches caller");
    assert_injected_io(&error, StorageFaultPoint::ObjectDelete);
    assert!(wal_path.exists(), "failed delete preserves the file");
    drop(delete_fault);
    backend
        .delete_object_blocking(wal)
        .expect("delete retry succeeds");
    assert!(!wal_path.exists());

    fs::remove_dir_all(path).expect("cleanup destructive root");
    record_destructive_result("wal_rewrite_and_delete", "atomic_retry_succeeded");
}
