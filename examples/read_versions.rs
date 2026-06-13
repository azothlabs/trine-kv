use std::path::{Path, PathBuf};

use trine_kv::{Db, DbOptions, Error, Result};

fn main() -> Result<()> {
    let path = temp_path("trine-kv-read-versions");
    reset_dir(&path)?;
    run(&path)?;
    reset_dir(&path)?;
    Ok(())
}

fn run(path: &Path) -> Result<()> {
    let db = open_db(path)?;
    let profiles = db.bucket_sync("profiles")?;

    profiles.put_sync(b"profile:ada", b"v1")?;
    let checkpoint = db.create_checkpoint_sync("before-profile-refresh")?;

    profiles.put_sync(b"profile:ada", b"v2")?;
    db.flush_sync()?;

    drop(profiles);
    drop(db);

    let db = open_db(path)?;
    let profiles = db.bucket_sync("profiles")?;

    let restored = db.checkpoint_read_version_sync("before-profile-refresh")?;
    assert_eq!(restored, checkpoint);

    let old_snapshot = db.snapshot_at(restored)?;
    assert_eq!(
        old_snapshot.get_sync(&profiles, b"profile:ada")?,
        Some(b"v1".to_vec())
    );
    assert_eq!(profiles.get_sync(b"profile:ada")?, Some(b"v2".to_vec()));

    profiles.put_sync(b"profile:ada", b"v3")?;
    profiles.put_sync(b"profile:ada", b"v4")?;
    assert_eq!(
        old_snapshot.get_sync(&profiles, b"profile:ada")?,
        Some(b"v1".to_vec())
    );

    drop(old_snapshot);
    db.delete_checkpoint_sync("before-profile-refresh")?;
    profiles.put_sync(b"profile:ada", b"v5")?;

    match db.snapshot_at(restored) {
        Err(Error::ReadVersionExpired {
            requested,
            oldest_retained,
        }) => {
            assert_eq!(requested, restored);
            assert!(oldest_retained.as_u64() > restored.as_u64());
        }
        Ok(_) => panic!("deleted checkpoint should no longer retain the old read version"),
        Err(error) => return Err(error),
    }

    assert_eq!(db.latest_read_version().as_u64(), 5);
    assert_eq!(profiles.get_sync(b"profile:ada")?, Some(b"v5".to_vec()));

    drop(profiles);
    drop(db);
    Ok(())
}

fn open_db(path: &Path) -> Result<Db> {
    let mut options = DbOptions::new(path).with_keep_last_read_versions(2);
    options.background_worker_count = 0;
    Db::open_sync(options)
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{name}-{}", std::process::id()))
}

fn reset_dir(path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(Error::Io(error)),
    }
    Ok(())
}
