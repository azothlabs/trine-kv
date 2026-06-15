//! Durability tiers: fast non-strict writes by default, strict `F_FULLFSYNC`
//! when a commit must survive sudden power loss.
//!
//! Run with: `cargo run --example durability`

use trine_kv::{Db, DbOptions, DurabilityMode, WriteOptions};

fn main() -> trine_kv::Result<()> {
    let path = std::env::temp_dir().join(format!("trine-kv-durability-{}", std::process::id()));
    if path.exists() {
        std::fs::remove_dir_all(&path)?;
    }

    // Persistent databases default to `SyncAll` (non-strict): on macOS that is a
    // plain `fsync` - fast, and durable across a crash or kernel panic, but not
    // guaranteed across sudden power loss.
    let db = Db::open_sync(&path)?;
    db.put_sync(b"k1", b"non-strict default")?;

    // Ask a single write for strict, power-loss durability (macOS `F_FULLFSYNC`).
    db.put_with_options_sync(
        b"k2",
        b"power-loss durable",
        WriteOptions::sync_all_strict(),
    )?;

    assert_eq!(db.get_sync(b"k1")?, Some(b"non-strict default".to_vec()));
    assert_eq!(db.get_sync(b"k2")?, Some(b"power-loss durable".to_vec()));
    drop(db);

    // Or make strict the database-wide floor: every write is then power-loss
    // durable without per-write options. The floor cannot be quietly weakened by
    // a per-write request.
    let strict_path = path.join("strict");
    let strict_db = Db::open_sync(
        DbOptions::persistent(&strict_path).with_durability(DurabilityMode::SyncAllStrict),
    )?;
    strict_db.put_sync(b"k", b"v")?;
    assert_eq!(strict_db.get_sync(b"k")?, Some(b"v".to_vec()));
    drop(strict_db);

    std::fs::remove_dir_all(path)?;
    Ok(())
}
