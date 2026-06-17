//! Copy-on-write branches and time travel (slice 1: read-only instant clones
//! plus an in-memory write overlay), built entirely on the existing MVCC
//! primitives (`Db::snapshot_at`, `Bucket::get_at_sync`/`range_at_sync`).
//!
//! See `docs/branching.md`. The design constraint is that branching must not
//! cost a database that never branches anything: this module adds a composition
//! layer *over* the public read API and touches none of the LSM read/write hot
//! path, so the root (non-branch) lineage is byte-for-byte unchanged.
//!
//! A [`Branch`] forks from a parent [`ReadVersion`] (a [`crate::Snapshot`] pinned
//! at the fork, which also keeps that history retained for the branch's
//! lifetime). It shares all parent history at or below the fork — creating one
//! is O(1), no data is copied — and keeps its own divergent writes in an
//! in-memory overlay. A read consults the overlay first and falls through to the
//! pinned parent snapshot; the fall-through is the only extra work, and it
//! happens solely for branch reads.
//!
//! Slice 1 scope: branch-local writes are **ephemeral** (overlay only) — they are
//! not persisted, compacted, or recovered. Durable, writable branches with their
//! own layer-set are a later slice (`docs/branching.md`).

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use crate::bucket::BucketName;
use crate::db::Db;
use crate::error::Result;
use crate::snapshot::Snapshot;
use crate::types::{KeyRange, KeyValue, ReadVersion, Value};

/// One branch-local write held in the overlay.
enum OverlayWrite {
    /// The key is set to this value on the branch (shadows the parent).
    Put(Value),
    /// The key is deleted on the branch (hides any parent value).
    Delete,
}

/// A copy-on-write branch forked from a parent database at a fixed
/// [`ReadVersion`]. Reads see the parent's state as of the fork with the
/// branch's own writes layered on top; the parent is unaffected.
///
/// Created with [`Db::branch_at`] / [`Db::branch_from_latest`]. The branch
/// borrows its parent [`Db`] and pins the fork version's history for as long as
/// it lives.
///
/// In slice 1 the branch's writes live only in memory (see the module docs); a
/// branch is a single-owner handle (its mutating methods take `&mut self`).
pub struct Branch<'db> {
    db: &'db Db,
    fork: Snapshot,
    overlay: HashMap<(BucketName, Vec<u8>), OverlayWrite>,
}

impl<'db> Branch<'db> {
    pub(crate) fn new(db: &'db Db, fork: Snapshot) -> Self {
        Self {
            db,
            fork,
            overlay: HashMap::new(),
        }
    }

    /// The parent version this branch forked from. Reads that fall through to the
    /// parent see its state as of exactly this version.
    #[must_use]
    pub const fn fork_version(&self) -> ReadVersion {
        self.fork.read_version()
    }

    /// Reads a key on the branch: the branch's own write if it has one, otherwise
    /// the parent's value as of the fork version.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket cannot be opened or the parent read fails.
    pub fn get(&self, bucket: impl Into<BucketName>, key: &[u8]) -> Result<Option<Value>> {
        let bucket = bucket.into();
        match self.overlay.get(&(bucket.clone(), key.to_vec())) {
            Some(OverlayWrite::Put(value)) => Ok(Some(value.clone())),
            Some(OverlayWrite::Delete) => Ok(None),
            None => self.db.bucket_sync(bucket)?.get_at_sync(&self.fork, key),
        }
    }

    /// Writes a key on the branch. The write is visible to this branch's reads
    /// and never touches the parent. Slice 1: in-memory only (not persisted).
    ///
    /// # Errors
    ///
    /// Slice 1 never fails; the result type is reserved for durable branches.
    pub fn put(
        &mut self,
        bucket: impl Into<BucketName>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Value>,
    ) -> Result<()> {
        self.overlay
            .insert((bucket.into(), key.into()), OverlayWrite::Put(value.into()));
        Ok(())
    }

    /// Deletes a key on the branch (hiding any parent value). The parent is
    /// unaffected. Slice 1: in-memory only.
    ///
    /// # Errors
    ///
    /// Slice 1 never fails; the result type is reserved for durable branches.
    pub fn delete(&mut self, bucket: impl Into<BucketName>, key: impl Into<Vec<u8>>) -> Result<()> {
        self.overlay
            .insert((bucket.into(), key.into()), OverlayWrite::Delete);
        Ok(())
    }

    /// Scans a key range on the branch, merging its overlay over the parent's
    /// state as of the fork: branch puts replace and branch deletes remove the
    /// parent's rows. Returns the merged rows in key order.
    ///
    /// Slice 1 returns an eager `Vec` (a clone is an in-memory working set); a
    /// lazy merging iterator is a later refinement.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket cannot be opened or the parent scan fails.
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
        // Layer the branch's own writes over the parent rows.
        for ((overlay_bucket, key), write) in &self.overlay {
            if *overlay_bucket != bucket || !range_contains(range, key) {
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
        Ok(merged
            .into_iter()
            .map(|(key, value)| KeyValue::new(key, value))
            .collect())
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

impl Db {
    /// Forks a copy-on-write [`Branch`] from a past `version`. Creating a branch
    /// is O(1) and copies no data; the branch shares the parent's history as of
    /// `version` and keeps its own writes separate. The parent is unaffected.
    ///
    /// The fork pins `version`'s history for the branch's lifetime, so it is
    /// subject to the same retained-history floor as [`Db::snapshot_at`]: a
    /// `version` older than [`Db::oldest_retained_read_version`] is rejected.
    ///
    /// # Errors
    ///
    /// Returns an error if `version` is newer than the latest committed version
    /// or older than the retained-history floor.
    pub fn branch_at(&self, version: ReadVersion) -> Result<Branch<'_>> {
        Ok(Branch::new(self, self.snapshot_at(version)?))
    }

    /// Forks a branch from the latest committed version — an instant clone of the
    /// current database state.
    ///
    /// # Errors
    ///
    /// Returns an error if a snapshot at the latest version cannot be pinned.
    pub fn branch_from_latest(&self) -> Result<Branch<'_>> {
        self.branch_at(self.latest_read_version())
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
        // Reads fall through to the parent.
        assert_eq!(
            branch.get("data", b"k1").expect("get"),
            Some(b"v1".to_vec())
        );

        // A branch write shadows the parent for the branch only.
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

        // Fork now: the branch pins the fork version's history, so it stays valid
        // even with the default (no extra) retention window.
        let branch = db.branch_from_latest().expect("branch");
        // The parent moves on.
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
        // Time travel to an arbitrary past version needs that history retained.
        let db = Db::open_sync(DbOptions::memory().with_keep_last_read_versions(8))
            .expect("open with retention");
        let bucket = db.bucket_sync("data").expect("bucket");
        bucket.put_sync(b"k".to_vec(), b"v1".to_vec()).expect("p1");
        let v1 = db.latest_read_version();
        bucket.put_sync(b"k".to_vec(), b"v2".to_vec()).expect("p2");

        // AS OF v1 reads the old value; latest reads the new one.
        let old = db.branch_at(v1).expect("branch at v1");
        assert_eq!(old.get("data", b"k").expect("get"), Some(b"v1".to_vec()));
        let now = db.branch_from_latest().expect("branch now");
        assert_eq!(now.get("data", b"k").expect("get"), Some(b"v2".to_vec()));
    }

    #[test]
    fn branch_range_merges_overlay_over_parent() {
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
            ],
            "a stays, b is overridden, c is deleted, d is added; parent order preserved"
        );

        // The parent range is unchanged.
        let parent: Vec<Vec<u8>> = bucket
            .range_sync(&KeyRange::all())
            .expect("parent range")
            .map(|kv| kv.expect("row").key)
            .collect();
        assert_eq!(parent, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }
}
