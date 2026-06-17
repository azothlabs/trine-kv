//! Copy-on-write branches and time travel, built over the existing MVCC read
//! API and named buckets so the LSM read/write hot path is untouched — a
//! database that never branches pays nothing (see `docs/branching.md`).
//!
//! A [`Branch`] forks from a parent [`ReadVersion`] (a pinned [`crate::Snapshot`]
//! that also keeps the fork's history retained while the branch lives). It shares
//! all parent history at or below the fork — O(1) to create, no data copied — and
//! keeps its own divergent writes separate; reads consult the branch's writes
//! first and fall through to the pinned parent snapshot. The parent is never
//! affected.
//!
//! Two flavors share one API:
//!
//! * **Ephemeral clone** ([`Db::branch_from_latest`], [`Db::branch_at`]): writes
//!   live in an in-memory overlay and vanish with the handle — a scratch
//!   "what-if" clone or a point-in-time (`AS OF`) read view.
//! * **Durable named branch** ([`Db::create_branch`] + [`Db::open_branch`]): writes
//!   persist in the branch's own buckets, so they survive reopen and are
//!   compacted and recovered like any data — a git-style named branch. Because a
//!   branch's writes live in their **own** buckets (their own layer-set), they
//!   never enter the parent's trees, so branch activity cannot perturb the
//!   parent's compaction or read amplification.
//!
//! A durable branch pins its fork with a durable checkpoint, so the parent keeps
//! the branch's fork history — and the branch stays openable — across restarts
//! and aggressive retention, with no manual retention configuration, until
//! [`Db::delete_branch`] releases the pin.
//!
//! Scope: branches are single-level (forked from the database's own lineage; a
//! branch of a branch is a later slice). [`Db::delete_branch`] releases a
//! branch's fork pin and forgets it, but its data buckets are reclaimed only
//! once a bucket-drop primitive exists (a later slice).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;

use crate::bucket::BucketName;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::snapshot::Snapshot;
use crate::types::{KeyRange, KeyValue, ReadVersion, Value};

/// Prefix reserving the buckets branching keeps its own state in. Branch names
/// must not contain the `\u{1}` separator (they are simple identifiers).
const RESERVED: &str = "\u{1}trine-branch\u{1}";
const SEP: char = '\u{1}';

/// The bucket holding the branch registry: branch name → [`RegistryEntry`].
fn registry_bucket() -> String {
    format!("{RESERVED}registry")
}

/// The bucket holding a durable branch's divergent writes for one user bucket.
fn data_bucket(branch: &str, user_bucket: &str) -> String {
    format!("{RESERVED}{branch}{SEP}{user_bucket}")
}

/// A durable branch's persisted metadata: where it forked and which user buckets
/// it has written (so a read need not touch — or create — a data bucket the
/// branch has never written, and so the parent is consulted directly there).
struct RegistryEntry {
    fork: ReadVersion,
    written_buckets: BTreeSet<String>,
}

impl RegistryEntry {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.fork.as_u64().to_le_bytes());
        let count = u32::try_from(self.written_buckets.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&count.to_le_bytes());
        for bucket in &self.written_buckets {
            let len = u32::try_from(bucket.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bucket.as_bytes());
        }
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let corrupt = || Error::Corruption {
            message: "malformed branch registry entry".to_owned(),
        };
        let fork_bytes: [u8; 8] = bytes.get(0..8).ok_or_else(corrupt)?.try_into().expect("8");
        let fork = ReadVersion::from_u64(u64::from_le_bytes(fork_bytes));
        let mut pos = 8;
        let count_bytes: [u8; 4] = bytes
            .get(pos..pos + 4)
            .ok_or_else(corrupt)?
            .try_into()
            .expect("4");
        let count = u32::from_le_bytes(count_bytes);
        pos += 4;
        let mut written_buckets = BTreeSet::new();
        for _ in 0..count {
            let len_bytes: [u8; 4] = bytes
                .get(pos..pos + 4)
                .ok_or_else(corrupt)?
                .try_into()
                .expect("4");
            let len = u32::from_le_bytes(len_bytes) as usize;
            pos += 4;
            let name = bytes.get(pos..pos + len).ok_or_else(corrupt)?;
            pos += len;
            written_buckets.insert(String::from_utf8(name.to_vec()).map_err(|_| corrupt())?);
        }
        Ok(Self {
            fork,
            written_buckets,
        })
    }
}

/// Value tag in a durable branch's data bucket: a present value or a tombstone
/// (the branch deleted a key the parent still has). Distinguishes "the branch
/// wrote nothing here, fall through to the parent" (key absent) from "the branch
/// deleted it" (tombstone).
const TAG_PRESENT: u8 = 0;
const TAG_TOMBSTONE: u8 = 1;

fn encode_present(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 1);
    out.push(TAG_PRESENT);
    out.extend_from_slice(value);
    out
}

/// One ephemeral branch-local write held in the in-memory overlay.
enum OverlayWrite {
    Put(Value),
    Delete,
}

/// How a branch stores its divergent writes.
enum Backing {
    /// In-memory, lost with the handle (ephemeral clone / `AS OF` view).
    Ephemeral(HashMap<(BucketName, Vec<u8>), OverlayWrite>),
    /// Persisted in the branch's own buckets (durable named branch).
    Durable {
        name: String,
        written_buckets: BTreeSet<String>,
    },
}

/// A copy-on-write branch forked from a parent database at a fixed
/// [`ReadVersion`]. Reads see the parent's state as of the fork with the
/// branch's own writes layered on top; the parent is unaffected.
pub struct Branch<'db> {
    db: &'db Db,
    fork: Snapshot,
    backing: Backing,
}

impl<'db> Branch<'db> {
    fn ephemeral(db: &'db Db, fork: Snapshot) -> Self {
        Self {
            db,
            fork,
            backing: Backing::Ephemeral(HashMap::new()),
        }
    }

    fn durable(
        db: &'db Db,
        fork: Snapshot,
        name: String,
        written_buckets: BTreeSet<String>,
    ) -> Self {
        Self {
            db,
            fork,
            backing: Backing::Durable {
                name,
                written_buckets,
            },
        }
    }

    /// The parent version this branch forked from. Reads that fall through to the
    /// parent see its state as of exactly this version.
    #[must_use]
    pub const fn fork_version(&self) -> ReadVersion {
        self.fork.read_version()
    }

    /// Whether this branch's writes are persisted (durable named branch) or live
    /// only in memory (ephemeral clone).
    #[must_use]
    pub const fn is_durable(&self) -> bool {
        matches!(self.backing, Backing::Durable { .. })
    }

    /// Reads a key on the branch: the branch's own write if it has one, otherwise
    /// the parent's value as of the fork version.
    ///
    /// # Errors
    ///
    /// Returns an error if a bucket cannot be opened or a read fails.
    pub fn get(&self, bucket: impl Into<BucketName>, key: &[u8]) -> Result<Option<Value>> {
        let bucket = bucket.into();
        match &self.backing {
            Backing::Ephemeral(overlay) => match overlay.get(&(bucket.clone(), key.to_vec())) {
                Some(OverlayWrite::Put(value)) => return Ok(Some(value.clone())),
                Some(OverlayWrite::Delete) => return Ok(None),
                None => {}
            },
            Backing::Durable {
                name,
                written_buckets,
            } => {
                if written_buckets.contains(bucket.as_str()) {
                    let data = self.db.bucket_sync(data_bucket(name, bucket.as_str()))?;
                    if let Some(raw) = data.get_sync(key)? {
                        return Ok(decode_branch_value(&raw));
                    }
                }
            }
        }
        self.parent_get(&bucket, key)
    }

    fn parent_get(&self, bucket: &BucketName, key: &[u8]) -> Result<Option<Value>> {
        self.db
            .bucket_sync(bucket.clone())?
            .get_at_sync(&self.fork, key)
    }

    /// Writes a key on the branch. The write is visible to this branch's reads and
    /// never touches the parent. For a durable branch the write is persisted.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting a durable write fails (ephemeral never
    /// fails).
    pub fn put(
        &mut self,
        bucket: impl Into<BucketName>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) -> Result<()> {
        self.write(bucket.into(), key.into(), OverlayWrite::Put(value.into()))
    }

    /// Deletes a key on the branch (hiding any parent value, via a tombstone for a
    /// durable branch). The parent is unaffected.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting a durable tombstone fails.
    pub fn delete(&mut self, bucket: impl Into<BucketName>, key: impl Into<Vec<u8>>) -> Result<()> {
        self.write(bucket.into(), key.into(), OverlayWrite::Delete)
    }

    fn write(&mut self, bucket: BucketName, key: Vec<u8>, write: OverlayWrite) -> Result<()> {
        match &mut self.backing {
            Backing::Ephemeral(overlay) => {
                overlay.insert((bucket, key), write);
                Ok(())
            }
            Backing::Durable {
                name,
                written_buckets,
            } => {
                let data = self.db.bucket_sync(data_bucket(name, bucket.as_str()))?;
                match write {
                    OverlayWrite::Put(value) => data.put_sync(key, encode_present(&value))?,
                    OverlayWrite::Delete => data.put_sync(key, vec![TAG_TOMBSTONE])?,
                }
                // Record the first write to a user bucket so reads consult it (and
                // so the parent is consulted directly for never-written buckets).
                if written_buckets.insert(bucket.as_str().to_owned()) {
                    persist_registry(
                        self.db,
                        name,
                        &RegistryEntry {
                            fork: self.fork.read_version(),
                            written_buckets: written_buckets.clone(),
                        },
                    )?;
                }
                Ok(())
            }
        }
    }

    /// Scans a key range on the branch, merging its writes over the parent's state
    /// as of the fork: branch puts replace and branch deletes remove the parent's
    /// rows. Returns the merged rows in key order (eager; a lazy iterator is a
    /// later refinement).
    ///
    /// # Errors
    ///
    /// Returns an error if a bucket cannot be opened or a scan fails.
    pub fn range(&self, bucket: impl Into<BucketName>, range: &KeyRange) -> Result<Vec<KeyValue>> {
        let bucket = bucket.into();
        let mut merged: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
        for row in self
            .db
            .bucket_sync(bucket.clone())?
            .range_at_sync(&self.fork, range)?
        {
            let row = row?;
            merged.insert(row.key, row.value);
        }
        self.apply_branch_writes(&bucket, range, &mut merged)?;
        Ok(merged
            .into_iter()
            .map(|(key, value)| KeyValue::new(key, value))
            .collect())
    }

    /// Layers the branch's own writes for `bucket` within `range` over `merged`.
    fn apply_branch_writes(
        &self,
        bucket: &BucketName,
        range: &KeyRange,
        merged: &mut BTreeMap<Vec<u8>, Value>,
    ) -> Result<()> {
        match &self.backing {
            Backing::Ephemeral(overlay) => {
                for ((overlay_bucket, key), write) in overlay {
                    if overlay_bucket != bucket || !range_contains(range, key) {
                        continue;
                    }
                    match write {
                        OverlayWrite::Put(value) => {
                            merged.insert(key.clone(), value.clone());
                        }
                        OverlayWrite::Delete => {
                            merged.remove(key);
                        }
                    }
                }
            }
            Backing::Durable {
                name,
                written_buckets,
            } => {
                if !written_buckets.contains(bucket.as_str()) {
                    return Ok(());
                }
                // The branch data bucket holds the branch's own writes; read its
                // own latest state (it is not versioned against the parent fork).
                let data = self.db.bucket_sync(data_bucket(name, bucket.as_str()))?;
                for row in data.range_sync(range)? {
                    let row = row?;
                    match decode_branch_value(&row.value) {
                        Some(value) => {
                            merged.insert(row.key, value);
                        }
                        None => {
                            merged.remove(&row.key);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Whether `key` falls within `range`.
fn range_contains(range: &KeyRange, key: &[u8]) -> bool {
    let after_start = match &range.start {
        Bound::Unbounded => true,
        Bound::Included(start) => key >= start.as_slice(),
        Bound::Excluded(start) => key > start.as_slice(),
    };
    let before_end = match &range.end {
        Bound::Unbounded => true,
        Bound::Included(end) => key <= end.as_slice(),
        Bound::Excluded(end) => key < end.as_slice(),
    };
    after_start && before_end
}

/// Decodes a durable branch data value: `Some(value)` for a present write,
/// `None` for a tombstone (deleted on the branch) or a malformed/empty record.
fn decode_branch_value(raw: &[u8]) -> Option<Value> {
    match raw.first() {
        Some(&TAG_PRESENT) => Some(raw[1..].to_vec()),
        _ => None,
    }
}

fn persist_registry(db: &Db, name: &str, entry: &RegistryEntry) -> Result<()> {
    db.bucket_sync(registry_bucket())?
        .put_sync(name.as_bytes().to_vec(), entry.encode())
}

/// The checkpoint name pinning a durable branch's fork. A checkpoint is durable
/// metadata that the retained-history floor and GC respect, so the parent keeps
/// the branch's fork history across restarts.
fn fork_checkpoint(branch: &str) -> String {
    format!("{RESERVED}fork{SEP}{branch}")
}

impl Db {
    /// Forks an **ephemeral** copy-on-write [`Branch`] from a past `version` — an
    /// `AS OF` read view with an in-memory write overlay that vanishes with the
    /// handle. O(1) and copies no data; the parent is unaffected.
    ///
    /// The fork pins `version`'s history for the branch's lifetime, so it is
    /// subject to the same retained-history floor as [`Db::snapshot_at`].
    ///
    /// # Errors
    ///
    /// Returns an error if `version` is newer than the latest committed version
    /// or older than the retained-history floor.
    pub fn branch_at(&self, version: ReadVersion) -> Result<Branch<'_>> {
        Ok(Branch::ephemeral(self, self.snapshot_at(version)?))
    }

    /// Forks an ephemeral branch from the latest committed version — an instant
    /// in-memory clone of the current state.
    ///
    /// # Errors
    ///
    /// Returns an error if a snapshot at the latest version cannot be pinned.
    pub fn branch_from_latest(&self) -> Result<Branch<'_>> {
        self.branch_at(self.latest_read_version())
    }

    /// Creates a **durable** named branch forked at `from`. The name is recorded
    /// so the branch can be reopened later with [`Db::open_branch`]; its writes
    /// persist in its own buckets. O(1) and copies no data.
    ///
    /// Creating an existing name with the same fork is idempotent; with a
    /// different fork it is an error.
    ///
    /// The fork is pinned with a durable checkpoint, so the parent keeps the
    /// branch's fork history — and the branch stays openable — across restarts
    /// and aggressive retention, until the branch is deleted (no manual retention
    /// configuration needed).
    ///
    /// # Errors
    ///
    /// Returns an error if `from` is not a readable version, if the name already
    /// exists with a different fork, or if persisting the branch fails.
    pub fn create_branch(&self, name: &str, from: ReadVersion) -> Result<()> {
        if let Some(existing) = self.read_registry(name)? {
            if existing.fork == from {
                return Ok(());
            }
            return Err(Error::invalid_options(
                "branch already exists with a different fork version",
            ));
        }
        // Pin the fork durably (this also validates `from` is readable). The
        // checkpoint lives in the manifest, so the parent's GC cannot reclaim the
        // history the branch reads through, even after a restart.
        match self.create_checkpoint_at_sync(&fork_checkpoint(name), from) {
            Ok(()) | Err(Error::CheckpointAlreadyExists { .. }) => {}
            Err(error) => return Err(error),
        }
        persist_registry(
            self,
            name,
            &RegistryEntry {
                fork: from,
                written_buckets: BTreeSet::new(),
            },
        )
    }

    /// Opens a durable named branch created by [`Db::create_branch`], re-pinning
    /// its fork. The returned handle sees the parent as of the fork with the
    /// branch's persisted writes on top.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch does not exist, or if its fork version is no
    /// longer retained (see the module docs on retention).
    pub fn open_branch(&self, name: &str) -> Result<Branch<'_>> {
        let entry = self
            .read_registry(name)?
            .ok_or_else(|| Error::invalid_options("no such branch"))?;
        let fork = self.snapshot_at(entry.fork)?;
        Ok(Branch::durable(
            self,
            fork,
            name.to_owned(),
            entry.written_buckets,
        ))
    }

    /// Lists the durable branch names, in name order.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry cannot be scanned.
    pub fn list_branches(&self) -> Result<Vec<String>> {
        let registry = self.bucket_sync(registry_bucket())?;
        let mut names = Vec::new();
        for row in registry.range_sync(&KeyRange::all())? {
            let row = row?;
            names.push(String::from_utf8(row.key).map_err(|_| Error::Corruption {
                message: "branch registry holds a non-utf8 name".to_owned(),
            })?);
        }
        Ok(names)
    }

    /// Deletes a durable branch: releases its fork pin (so the parent can again
    /// GC that history) and forgets the branch, so it can no longer be opened.
    ///
    /// The branch's data buckets are left in place for now (a bucket-drop
    /// primitive is a later slice); they become unreachable once the branch is
    /// gone. Deleting a branch that does not exist is an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch does not exist or if releasing its state
    /// fails.
    pub fn delete_branch(&self, name: &str) -> Result<()> {
        if self.read_registry(name)?.is_none() {
            return Err(Error::invalid_options("no such branch"));
        }
        // Release the fork pin (the checkpoint may be absent if a prior delete was
        // interrupted after this step — tolerate that).
        match self.delete_checkpoint_sync(&fork_checkpoint(name)) {
            Ok(()) | Err(Error::CheckpointNotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        self.bucket_sync(registry_bucket())?
            .delete_sync(name.as_bytes().to_vec())
    }

    fn read_registry(&self, name: &str) -> Result<Option<RegistryEntry>> {
        match self
            .bucket_sync(registry_bucket())?
            .get_sync(name.as_bytes())?
        {
            Some(bytes) => Ok(Some(RegistryEntry::decode(&bytes)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Db, DbOptions, KeyRange};

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
        let got: Vec<(Vec<u8>, Vec<u8>)> = rows.into_iter().map(|kv| (kv.key, kv.value)).collect();
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
        let got: Vec<(Vec<u8>, Vec<u8>)> = rows.into_iter().map(|kv| (kv.key, kv.value)).collect();
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2-dev".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
            ]
        );
    }
}
