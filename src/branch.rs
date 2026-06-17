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
//! Branches nest: [`Db::create_branch_from`] forks a branch off another branch,
//! and a read walks the whole ancestor chain (branch → parent branch → … → root),
//! each ancestor seen frozen at the version its child forked it. This is the
//! git-style DAG. [`Db::delete_branch`] releases a branch's fork pin, drops its
//! divergent data buckets via [`Db::drop_bucket`] (reclaiming the space; on a
//! backend without bucket-drop it clears them instead), and forgets it; it
//! refuses while the branch still has children (they read through it).
//!
//! [`Branch::range`] is a lazy [`BranchRange`] iterator: the branch level, each
//! ancestor, and the root are streamed from their own sorted scans and k-way
//! merged on the fly (no full materialization).

use std::collections::{BTreeSet, HashMap};
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

/// A durable branch's persisted metadata: where it forked, the parent branch it
/// forked from (`None` = the root lineage), and which user buckets it has written
/// (so a read need not touch — or create — a data bucket the branch never wrote).
struct RegistryEntry {
    /// The global version this branch forked its parent at.
    fork: ReadVersion,
    /// The parent branch name, or `None` when forked from the root lineage.
    parent: Option<String>,
    written_buckets: BTreeSet<String>,
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}

impl RegistryEntry {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.fork.as_u64().to_le_bytes());
        let count = u32::try_from(self.written_buckets.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&count.to_le_bytes());
        for bucket in &self.written_buckets {
            put_str(&mut out, bucket);
        }
        // Parent is a trailing optional field (a flag byte then the name) so an
        // entry written before nesting existed still decodes (parent = None).
        match &self.parent {
            Some(parent) => {
                out.push(1);
                put_str(&mut out, parent);
            }
            None => out.push(0),
        }
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let corrupt = || Error::Corruption {
            message: "malformed branch registry entry".to_owned(),
        };
        let mut pos = 0usize;
        let take_u32 = |pos: &mut usize| -> Result<u32> {
            let raw: [u8; 4] = bytes
                .get(*pos..*pos + 4)
                .ok_or_else(corrupt)?
                .try_into()
                .expect("4");
            *pos += 4;
            Ok(u32::from_le_bytes(raw))
        };
        let fork_bytes: [u8; 8] = bytes.get(0..8).ok_or_else(corrupt)?.try_into().expect("8");
        let fork = ReadVersion::from_u64(u64::from_le_bytes(fork_bytes));
        pos += 8;
        let count = take_u32(&mut pos)?;
        let mut written_buckets = BTreeSet::new();
        for _ in 0..count {
            let len = take_u32(&mut pos)? as usize;
            let name = bytes.get(pos..pos + len).ok_or_else(corrupt)?;
            pos += len;
            written_buckets.insert(String::from_utf8(name.to_vec()).map_err(|_| corrupt())?);
        }
        // Trailing optional parent (absent in pre-nesting entries).
        let parent = match bytes.get(pos) {
            None | Some(&0) => None,
            Some(&1) => {
                pos += 1;
                let len = take_u32(&mut pos)? as usize;
                let name = bytes.get(pos..pos + len).ok_or_else(corrupt)?;
                Some(String::from_utf8(name.to_vec()).map_err(|_| corrupt())?)
            }
            Some(_) => return Err(corrupt()),
        };
        Ok(Self {
            fork,
            parent,
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

/// One level of a durable branch's read chain. The leaf (the opened branch
/// itself) reads its own latest writes (`at = None`); each ancestor is read
/// frozen at the version the child forked it (`at = Some`). `written` is that
/// branch's set of diverged user buckets, so untouched buckets are skipped
/// without opening (or creating) a data bucket.
struct DurableLayer {
    name: String,
    written: BTreeSet<String>,
    at: Option<Snapshot>,
}

/// A durable branch's read chain (leaf first, then each ancestor branch), plus
/// the leaf's own registry fields — needed to rewrite its entry when it first
/// writes a user bucket. The root fall-through below the chain is the branch's
/// pinned [`Branch::fork`] snapshot.
struct DurableState {
    chain: Vec<DurableLayer>,
    leaf_fork: ReadVersion,
    leaf_parent: Option<String>,
}

/// How a branch stores its divergent writes.
enum Backing {
    /// In-memory, lost with the handle (ephemeral clone / `AS OF` view).
    Ephemeral(HashMap<(BucketName, Vec<u8>), OverlayWrite>),
    /// Persisted in the branch's own buckets (durable named branch), as a read
    /// chain from the branch up through its ancestor branches.
    Durable(DurableState),
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

    fn durable(db: &'db Db, fork: Snapshot, state: DurableState) -> Self {
        Self {
            db,
            fork,
            backing: Backing::Durable(state),
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
        matches!(self.backing, Backing::Durable(_))
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
            Backing::Durable(state) => {
                // Walk the chain leaf-first: the first level that holds the key
                // (a present value or a tombstone) is definitive; otherwise fall
                // through to the next ancestor, and finally to the root snapshot.
                for layer in &state.chain {
                    if !layer.written.contains(bucket.as_str()) {
                        continue;
                    }
                    let data = self
                        .db
                        .bucket_sync(data_bucket(&layer.name, bucket.as_str()))?;
                    let raw = match &layer.at {
                        None => data.get_sync(key)?,
                        Some(at) => data.get_at_sync(at, key)?,
                    };
                    if let Some(raw) = raw {
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
        let db = self.db;
        match &mut self.backing {
            Backing::Ephemeral(overlay) => {
                overlay.insert((bucket, key), write);
                Ok(())
            }
            Backing::Durable(state) => {
                // Writes only ever touch the leaf (the opened branch's own data
                // bucket); ancestors are read-only fall-through.
                let leaf_fork = state.leaf_fork;
                let leaf_parent = state.leaf_parent.clone();
                let leaf = &mut state.chain[0];
                let data = db.bucket_sync(data_bucket(&leaf.name, bucket.as_str()))?;
                match write {
                    OverlayWrite::Put(value) => data.put_sync(key, encode_present(&value))?,
                    OverlayWrite::Delete => data.put_sync(key, vec![TAG_TOMBSTONE])?,
                }
                // Record the first write to a user bucket so reads consult it (and
                // so the parent is consulted directly for never-written buckets).
                if leaf.written.insert(bucket.as_str().to_owned()) {
                    persist_registry(
                        db,
                        &leaf.name,
                        &RegistryEntry {
                            fork: leaf_fork,
                            parent: leaf_parent,
                            written_buckets: leaf.written.clone(),
                        },
                    )?;
                }
                Ok(())
            }
        }
    }

    /// Scans a key range on the branch, lazily merging its writes over the
    /// parent's state as of the fork (and over each ancestor branch, for a nested
    /// branch): branch puts replace and branch deletes hide the parent's rows.
    /// Returns a [`BranchRange`] iterator yielding the merged rows in key order
    /// without materializing them — each branch level and the root are streamed
    /// from their own sorted scans and k-way merged on the fly.
    ///
    /// # Errors
    ///
    /// Returns an error if a bucket cannot be opened or a scan cannot be started;
    /// per-row scan errors surface from the iterator.
    pub fn range(&self, bucket: impl Into<BucketName>, range: &KeyRange) -> Result<BranchRange> {
        let bucket = bucket.into();
        // Sources in precedence order, highest first; the root is lowest.
        let mut sources: Vec<MergeSource> = Vec::new();
        match &self.backing {
            Backing::Ephemeral(overlay) => {
                // The overlay is unsorted in memory, so collect its in-range
                // entries for this bucket and sort them into one source.
                let mut entries: Vec<(Vec<u8>, Option<Value>)> = overlay
                    .iter()
                    .filter(|((overlay_bucket, key), _)| {
                        overlay_bucket == &bucket && range_contains(range, key)
                    })
                    .map(|((_, key), write)| {
                        let value = match write {
                            OverlayWrite::Put(value) => Some(value.clone()),
                            OverlayWrite::Delete => None,
                        };
                        (key.clone(), value)
                    })
                    .collect();
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                sources.push(MergeSource::new(Box::new(entries.into_iter().map(Ok))));
            }
            Backing::Durable(state) => {
                for layer in &state.chain {
                    if !layer.written.contains(bucket.as_str()) {
                        continue;
                    }
                    let data = self
                        .db
                        .bucket_sync(data_bucket(&layer.name, bucket.as_str()))?;
                    let rows = match &layer.at {
                        None => data.range_sync(range)?,
                        Some(at) => data.range_at_sync(at, range)?,
                    };
                    sources.push(MergeSource::new(Box::new(rows.map(|row| {
                        row.map(|kv| {
                            let value = decode_branch_value(&kv.value);
                            (kv.key, value)
                        })
                    }))));
                }
            }
        }
        // The root (lowest precedence): every row is a present value.
        let root = self
            .db
            .bucket_sync(bucket.clone())?
            .range_at_sync(&self.fork, range)?;
        sources.push(MergeSource::new(Box::new(
            root.map(|row| row.map(|kv| (kv.key, Some(kv.value)))),
        )));
        Ok(BranchRange { sources })
    }
}

/// One row a merge source yields: its key and either a value or a tombstone
/// (`None`, meaning the level deletes the key).
type MergeRow = Result<(Vec<u8>, Option<Value>)>;

/// A sorted merge source with one buffered head row, so the merge can compare
/// keys across sources before consuming them.
struct MergeSource {
    iter: Box<dyn Iterator<Item = MergeRow>>,
    head: Option<MergeRow>,
}

impl MergeSource {
    fn new(mut iter: Box<dyn Iterator<Item = MergeRow>>) -> Self {
        let head = iter.next();
        Self { iter, head }
    }

    /// The buffered head key, or `None` when the source is exhausted or its head
    /// is an error (handled separately).
    fn key(&self) -> Option<&[u8]> {
        match &self.head {
            Some(Ok((key, _))) => Some(key),
            _ => None,
        }
    }

    fn is_err(&self) -> bool {
        matches!(&self.head, Some(Err(_)))
    }

    /// Takes the head row and refills from the underlying iterator.
    fn take(&mut self) -> Option<MergeRow> {
        let row = self.head.take();
        self.head = self.iter.next();
        row
    }
}

/// A lazy k-way merge of a branch's read chain — the branch's own writes, each
/// ancestor branch, and the root — yielding the resolved rows in key order. The
/// nearest level holding a key wins; a tombstone there hides the key entirely.
/// Returned by [`Branch::range`].
pub struct BranchRange {
    /// Sources in precedence order: index 0 is highest (the branch itself), the
    /// last is the root.
    sources: Vec<MergeSource>,
}

impl Iterator for BranchRange {
    type Item = Result<KeyValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Surface a pending scan error from any source.
            for source in &mut self.sources {
                if source.is_err() {
                    if let Some(Err(error)) = source.take() {
                        return Some(Err(error));
                    }
                    unreachable!("is_err guarantees an error head");
                }
            }
            // The smallest head key across all sources.
            let mut smallest: Option<&[u8]> = None;
            for source in &self.sources {
                if let Some(key) = source.key() {
                    let replace = match smallest {
                        None => true,
                        Some(current) => key < current,
                    };
                    if replace {
                        smallest = Some(key);
                    }
                }
            }
            let key = smallest?.to_vec();
            // Consume that key from every source; the highest-precedence value
            // (first in source order) wins.
            let mut chosen: Option<Option<Value>> = None;
            for source in &mut self.sources {
                if source.key() == Some(key.as_slice()) {
                    if let Some(Ok((_, value))) = source.take() {
                        if chosen.is_none() {
                            chosen = Some(value);
                        }
                    }
                }
            }
            // A present value is emitted; a tombstone (or nothing) skips the key.
            if let Some(Some(value)) = chosen {
                return Some(Ok(KeyValue::new(key, value)));
            }
        }
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
                parent: None,
                written_buckets: BTreeSet::new(),
            },
        )
    }

    /// Creates a **durable** named branch forked from another branch `parent` at
    /// its current state — a branch of a branch (the git-style DAG). The new
    /// branch reads `parent`'s state (and `parent`'s own ancestors) with its own
    /// writes on top; `parent` is unaffected. O(1), copies no data.
    ///
    /// The fork is pinned with a checkpoint just like [`Db::create_branch`], so
    /// the chain stays readable. Do not delete `parent` while this branch exists
    /// (see [`Db::delete_branch`]).
    ///
    /// # Errors
    ///
    /// Returns an error if `parent` does not exist, if `name` already exists, or
    /// if persisting the branch fails.
    pub fn create_branch_from(&self, name: &str, parent: &str) -> Result<()> {
        if self.read_registry(parent)?.is_none() {
            return Err(Error::invalid_options("parent branch does not exist"));
        }
        if self.read_registry(name)?.is_some() {
            return Err(Error::invalid_options("branch already exists"));
        }
        // Fork at the current global version: the child sees the parent's state as
        // of now. Pinning it keeps the parent's (and its ancestors') history that
        // the chain reads through retained.
        let from = self.latest_read_version();
        match self.create_checkpoint_at_sync(&fork_checkpoint(name), from) {
            Ok(()) | Err(Error::CheckpointAlreadyExists { .. }) => {}
            Err(error) => return Err(error),
        }
        persist_registry(
            self,
            name,
            &RegistryEntry {
                fork: from,
                parent: Some(parent.to_owned()),
                written_buckets: BTreeSet::new(),
            },
        )
    }

    /// Opens a durable named branch, re-pinning its fork and assembling its read
    /// chain (the branch, then each ancestor branch, then the root). The returned
    /// handle sees that chain with the branch's persisted writes on top.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch (or an ancestor) does not exist, or if a
    /// fork version is no longer retained (see the module docs on retention).
    pub fn open_branch(&self, name: &str) -> Result<Branch<'_>> {
        let leaf = self
            .read_registry(name)?
            .ok_or_else(|| Error::invalid_options("no such branch"))?;
        let leaf_fork = leaf.fork;
        let leaf_parent = leaf.parent.clone();

        // The leaf reads its own latest writes; each ancestor is read frozen at
        // the version the child below it forked it.
        let mut chain = vec![DurableLayer {
            name: name.to_owned(),
            written: leaf.written_buckets,
            at: None,
        }];
        let mut child_fork = leaf.fork;
        let mut parent = leaf.parent;
        while let Some(parent_name) = parent {
            let entry = self
                .read_registry(&parent_name)?
                .ok_or_else(|| Error::Corruption {
                    message: format!("branch {parent_name} is missing (an ancestor of {name})"),
                })?;
            chain.push(DurableLayer {
                name: parent_name,
                written: entry.written_buckets,
                at: Some(self.snapshot_at(child_fork)?),
            });
            child_fork = entry.fork;
            parent = entry.parent;
        }
        // The base ancestor forked the root lineage at `child_fork`.
        let root_fork = self.snapshot_at(child_fork)?;
        Ok(Branch::durable(
            self,
            root_fork,
            DurableState {
                chain,
                leaf_fork,
                leaf_parent,
            },
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
    /// branches forked from it, is an error (a child depends on this branch's
    /// fork pin staying in place).
    ///
    /// The branch's divergent data is reclaimed: each data bucket it wrote is
    /// cleared, so the space is recovered by compaction and a future branch
    /// reusing the name starts clean. (The now-empty bucket shells themselves are
    /// removed only once the KV gains a bucket-drop primitive — a later slice —
    /// but they are gated by the registry's `written_buckets`, so they are never
    /// read after the branch is gone.)
    ///
    /// # Errors
    ///
    /// Returns an error if the branch does not exist, still has children, or if
    /// releasing its state fails.
    pub fn delete_branch(&self, name: &str) -> Result<()> {
        let entry = self
            .read_registry(name)?
            .ok_or_else(|| Error::invalid_options("no such branch"))?;
        // A child branch reads through this branch's history; refuse to drop the
        // pin out from under it.
        for other in self.list_branches()? {
            if other == name {
                continue;
            }
            if let Some(other_entry) = self.read_registry(&other)? {
                if other_entry.parent.as_deref() == Some(name) {
                    return Err(Error::invalid_options(
                        "cannot delete a branch that still has child branches",
                    ));
                }
            }
        }
        // Release the fork pin (the checkpoint may be absent if a prior delete was
        // interrupted after this step — tolerate that).
        match self.delete_checkpoint_sync(&fork_checkpoint(name)) {
            Ok(()) | Err(Error::CheckpointNotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        // Reclaim the branch's divergent data: drop each data bucket it wrote.
        // On a backend without bucket-drop, fall back to clearing the contents so
        // a same-named branch created later does not inherit stale rows (the empty
        // shell remains there).
        for user_bucket in &entry.written_buckets {
            let data = data_bucket(name, user_bucket);
            match self.drop_bucket_sync(data.clone()) {
                Ok(()) => {}
                Err(Error::UnsupportedBackend { .. }) => {
                    self.bucket_sync(data)?.delete_range_sync(KeyRange::all())?;
                }
                Err(error) => return Err(error),
            }
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
}
