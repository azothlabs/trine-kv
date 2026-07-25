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
//! merged on the fly (no full copy).

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use crate::bucket::BucketName;
use crate::db::Db;
use crate::error::{Error, Result};
use crate::snapshot::Snapshot;
use crate::state_transition::DurableTransition;
use crate::transaction::TransactionOptions;
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

/// A durable branch's lineage: the global version it forked at and its parent
/// branch (`None` = forked from the root lineage). Returned by
/// [`Db::branch_info`] for a higher layer that manages its **own** divergent
/// storage (e.g. a SQL/document engine whose writes must be one atomic
/// multi-bucket batch) and only needs the fork point — to read the parent
/// through [`Db::snapshot_at`] — and the parent link — to walk a nested branch's
/// ancestry — while still relying on the durable fork pin, registry, and nesting
/// this crate maintains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchInfo {
    fork: ReadVersion,
    parent: Option<String>,
}

impl BranchInfo {
    /// The global version this branch forked at. Read the parent's state as of
    /// this version (via [`Db::snapshot_at`]) to resolve a branch read that the
    /// branch's own storage does not hold (the fall-through).
    #[must_use]
    pub const fn fork(&self) -> ReadVersion {
        self.fork
    }

    /// The parent branch's name, or `None` when this branch forked the root
    /// lineage. Walk it to assemble a nested branch's ancestor chain.
    #[must_use]
    pub fn parent(&self) -> Option<&str> {
        self.parent.as_deref()
    }
}

/// A durable branch's persisted metadata: where it forked, the parent branch it
/// forked from (`None` = the root lineage), and which user buckets it has written
/// (so a read need not touch — or create — a data bucket the branch never wrote).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchLifecycle {
    Active,
    Deleting,
}

impl BranchLifecycle {
    fn require_active(self) -> Result<()> {
        match self {
            Self::Active => Ok(()),
            Self::Deleting => Err(Error::invalid_options("branch deletion is in progress")),
        }
    }

    fn begin_delete(self) -> DurableTransition<Self> {
        match self {
            Self::Active => DurableTransition::Apply(Self::Deleting),
            Self::Deleting => DurableTransition::AlreadyApplied(Self::Deleting),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegistryEntry {
    /// The global version this branch forked its parent at.
    fork: ReadVersion,
    /// The parent branch name, or `None` when forked from the root lineage.
    parent: Option<String>,
    written_buckets: BTreeSet<String>,
    lifecycle: BranchLifecycle,
    generation: [u8; 16],
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
        out.push(match self.lifecycle {
            BranchLifecycle::Active => 0,
            BranchLifecycle::Deleting => 1,
        });
        out.extend_from_slice(&self.generation);
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
            None => None,
            Some(&0) => {
                pos += 1;
                None
            }
            Some(&1) => {
                pos += 1;
                let len = take_u32(&mut pos)? as usize;
                let name = bytes.get(pos..pos + len).ok_or_else(corrupt)?;
                pos += len;
                Some(String::from_utf8(name.to_vec()).map_err(|_| corrupt())?)
            }
            Some(_) => return Err(corrupt()),
        };
        let lifecycle = match bytes.get(pos) {
            None => BranchLifecycle::Active,
            Some(&0) => {
                pos += 1;
                BranchLifecycle::Active
            }
            Some(&1) => {
                pos += 1;
                BranchLifecycle::Deleting
            }
            Some(_) => return Err(corrupt()),
        };
        let generation = match bytes.get(pos..) {
            Some([]) => [0; 16],
            Some(raw) if raw.len() == 16 => raw.try_into().expect("checked generation length"),
            _ => return Err(corrupt()),
        };
        Ok(Self {
            fork,
            parent,
            written_buckets,
            lifecycle,
            generation,
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
    leaf_generation: [u8; 16],
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
                require_branch_generation_active(
                    self.db,
                    &state.chain[0].name,
                    state.leaf_generation,
                )?;
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
                        return decode_branch_value(&raw);
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
                let leaf = &mut state.chain[0];
                let registry_name = registry_bucket();
                let data_name = data_bucket(&leaf.name, bucket.as_str());
                // Bucket creation may publish metadata, so complete it before
                // starting the transaction. The branch data and registry value
                // themselves are then one atomic commit.
                db.bucket_sync(registry_name.as_str())?;
                db.bucket_sync(data_name.as_str())?;
                let encoded = match write {
                    OverlayWrite::Put(value) => encode_present(&value),
                    OverlayWrite::Delete => vec![TAG_TOMBSTONE],
                };
                let mut transaction = db.transaction(TransactionOptions::default());
                let raw = transaction
                    .get_bucket_sync(&registry_name, leaf.name.as_bytes())?
                    .ok_or_else(|| Error::invalid_options("branch no longer exists"))?;
                let mut current = RegistryEntry::decode(&raw)?;
                current.lifecycle.require_active()?;
                if current.generation != state.leaf_generation
                    || current.fork != state.leaf_fork
                    || current.parent != state.leaf_parent
                {
                    return Err(Error::invalid_options(
                        "branch handle belongs to a replaced branch generation",
                    ));
                }
                current.written_buckets.insert(bucket.as_str().to_owned());
                transaction.put_bucket(&data_name, key, encoded)?;
                transaction.put_bucket(
                    &registry_name,
                    leaf.name.as_bytes().to_vec(),
                    current.encode(),
                )?;
                transaction.commit_sync()?;
                leaf.written = current.written_buckets;
                Ok(())
            }
        }
    }

    /// Scans a key range on the branch, lazily merging its writes over the
    /// parent's state as of the fork (and over each ancestor branch, for a nested
    /// branch): branch puts replace and branch deletes hide the parent's rows.
    /// Returns a [`BranchRange`] iterator yielding the merged rows in key order
    /// without building a full copy — each branch level and the root are streamed
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
                require_branch_generation_active(
                    self.db,
                    &state.chain[0].name,
                    state.leaf_generation,
                )?;
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
                        row.and_then(|kv| {
                            decode_branch_value(&kv.value).map(|value| (kv.key, value))
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
/// `None` for a tombstone (deleted on the branch).
fn decode_branch_value(raw: &[u8]) -> Result<Option<Value>> {
    match raw.first() {
        Some(&TAG_PRESENT) => Ok(Some(raw[1..].to_vec())),
        Some(&TAG_TOMBSTONE) if raw.len() == 1 => Ok(None),
        _ => Err(Error::Corruption {
            message: "malformed durable branch value".to_owned(),
        }),
    }
}

fn require_branch_generation_active(db: &Db, name: &str, generation: [u8; 16]) -> Result<()> {
    let current = db
        .read_registry(name)?
        .ok_or_else(|| Error::invalid_options("branch no longer exists"))?;
    current.lifecycle.require_active()?;
    if current.generation != generation {
        return Err(Error::invalid_options(
            "branch handle belongs to a replaced branch generation",
        ));
    }
    Ok(())
}

fn new_branch_generation() -> Result<[u8; 16]> {
    let mut generation = [0; 16];
    getrandom::fill(&mut generation)
        .map_err(|error| Error::runtime_busy(format!("branch generation entropy: {error}")))?;
    Ok(generation)
}

fn begin_branch_delete(db: &Db, name: &str) -> Result<Option<RegistryEntry>> {
    let registry = registry_bucket();
    db.bucket_sync(registry.as_str())?;
    let mut transaction = db.transaction(TransactionOptions::default());
    let mut rows = transaction.range_bucket_sync(&registry, KeyRange::all())?;
    let mut target = None;
    while let Some(row) = rows.next_sync() {
        let row = row?;
        let entry = RegistryEntry::decode(&row.value)?;
        let row_name = String::from_utf8(row.key).map_err(|_| Error::Corruption {
            message: "branch registry holds a non-utf8 name".to_owned(),
        })?;
        if entry.lifecycle == BranchLifecycle::Active && entry.parent.as_deref() == Some(name) {
            return Err(Error::invalid_options(
                "cannot delete a branch that still has child branches",
            ));
        }
        if row_name == name {
            target = Some(entry);
        }
    }
    drop(rows);
    let Some(mut target) = target else {
        return Ok(None);
    };
    match target.lifecycle.begin_delete() {
        DurableTransition::AlreadyApplied(_) => Ok(Some(target)),
        DurableTransition::Apply(lifecycle) => {
            target.lifecycle = lifecycle;
            transaction.put_bucket(&registry, name.as_bytes().to_vec(), target.encode())?;
            transaction.commit_sync()?;
            Ok(Some(target))
        }
    }
}

async fn begin_branch_delete_async(db: &Db, name: &str) -> Result<Option<RegistryEntry>> {
    let registry = registry_bucket();
    db.bucket(registry.as_str()).await?;
    let mut transaction = db.transaction(TransactionOptions::default());
    let mut rows = transaction.range_bucket(&registry, KeyRange::all()).await?;
    let mut target = None;
    while let Some(row) = rows.next().await? {
        let entry = RegistryEntry::decode(&row.value)?;
        let row_name = String::from_utf8(row.key).map_err(|_| Error::Corruption {
            message: "branch registry holds a non-utf8 name".to_owned(),
        })?;
        if entry.lifecycle == BranchLifecycle::Active && entry.parent.as_deref() == Some(name) {
            return Err(Error::invalid_options(
                "cannot delete a branch that still has child branches",
            ));
        }
        if row_name == name {
            target = Some(entry);
        }
    }
    drop(rows);
    let Some(mut target) = target else {
        return Ok(None);
    };
    match target.lifecycle.begin_delete() {
        DurableTransition::AlreadyApplied(_) => Ok(Some(target)),
        DurableTransition::Apply(lifecycle) => {
            target.lifecycle = lifecycle;
            transaction.put_bucket(&registry, name.as_bytes().to_vec(), target.encode())?;
            transaction.commit().await?;
            Ok(Some(target))
        }
    }
}

fn finish_branch_delete(db: &Db, name: &str, generation: [u8; 16]) -> Result<()> {
    let registry = registry_bucket();
    let mut transaction = db.transaction(TransactionOptions::default());
    let Some(raw) = transaction.get_bucket_sync(&registry, name.as_bytes())? else {
        return Ok(());
    };
    let entry = RegistryEntry::decode(&raw)?;
    if entry.lifecycle != BranchLifecycle::Deleting || entry.generation != generation {
        return Err(Error::Corruption {
            message: "branch delete completion observed a different durable generation".to_owned(),
        });
    }
    transaction.delete_bucket(&registry, name.as_bytes().to_vec())?;
    transaction.commit_sync().map(|_| ())
}

async fn finish_branch_delete_async(db: &Db, name: &str, generation: [u8; 16]) -> Result<()> {
    let registry = registry_bucket();
    let mut transaction = db.transaction(TransactionOptions::default());
    let Some(raw) = transaction.get_bucket(&registry, name.as_bytes()).await? else {
        return Ok(());
    };
    let entry = RegistryEntry::decode(&raw)?;
    if entry.lifecycle != BranchLifecycle::Deleting || entry.generation != generation {
        return Err(Error::Corruption {
            message: "branch delete completion observed a different durable generation".to_owned(),
        });
    }
    transaction.delete_bucket(&registry, name.as_bytes().to_vec())?;
    transaction.commit().await.map(|_| ())
}

/// The checkpoint name pinning a durable branch's fork. A checkpoint is durable
/// metadata that the retained-history floor and GC respect, so the parent keeps
/// the branch's fork history across restarts.
fn fork_checkpoint(branch: &str) -> String {
    format!("{RESERVED}fork{SEP}{branch}")
}

fn ensure_fork_checkpoint(db: &Db, branch: &str, from: ReadVersion) -> Result<()> {
    let checkpoint = fork_checkpoint(branch);
    match db.create_checkpoint_at_sync(&checkpoint, from) {
        Ok(()) => Ok(()),
        Err(Error::CheckpointAlreadyExists { .. }) => {
            if db.checkpoint_read_version_sync(&checkpoint)? == from {
                Ok(())
            } else {
                Err(Error::invalid_options(
                    "branch fork checkpoint already pins a different version",
                ))
            }
        }
        Err(error) => Err(error),
    }
}

async fn ensure_fork_checkpoint_async(db: &Db, branch: &str, from: ReadVersion) -> Result<()> {
    let checkpoint = fork_checkpoint(branch);
    match db.create_checkpoint_at(&checkpoint, from).await {
        Ok(()) => Ok(()),
        Err(Error::CheckpointAlreadyExists { .. }) => {
            if db.checkpoint_read_version(&checkpoint).await? == from {
                Ok(())
            } else {
                Err(Error::invalid_options(
                    "branch fork checkpoint already pins a different version",
                ))
            }
        }
        Err(error) => Err(error),
    }
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
            existing.lifecycle.require_active()?;
            if existing.fork == from && existing.parent.is_none() {
                return Ok(());
            }
            return Err(Error::invalid_options(
                "branch already exists with a different fork version",
            ));
        }
        // Pin the fork durably (this also validates `from` is readable). The
        // checkpoint lives in the manifest, so the parent's GC cannot reclaim the
        // history the branch reads through, even after a restart.
        ensure_fork_checkpoint(self, name, from)?;
        let registry = registry_bucket();
        self.bucket_sync(registry.as_str())?;
        let mut transaction = self.transaction(TransactionOptions::default());
        if let Some(raw) = transaction.get_bucket_sync(&registry, name.as_bytes())? {
            let existing = RegistryEntry::decode(&raw)?;
            existing.lifecycle.require_active()?;
            if existing.fork == from && existing.parent.is_none() {
                return Ok(());
            }
            return Err(Error::invalid_options(
                "branch already exists with a different fork version",
            ));
        }
        let entry = RegistryEntry {
            fork: from,
            parent: None,
            written_buckets: BTreeSet::new(),
            lifecycle: BranchLifecycle::Active,
            generation: new_branch_generation()?,
        };
        transaction.put_bucket(&registry, name.as_bytes().to_vec(), entry.encode())?;
        transaction.commit_sync().map(|_| ())
    }

    /// Async-first form of [`Db::create_branch`]. Required for object-store
    /// backends because both the fork checkpoint and the branch registry are
    /// durable metadata writes.
    ///
    /// # Errors
    ///
    /// Same validation and persistence errors as [`Db::create_branch`].
    pub async fn create_branch_at(&self, name: &str, from: ReadVersion) -> Result<()> {
        if let Some(existing) = self.read_registry_async(name).await? {
            existing.lifecycle.require_active()?;
            if existing.fork == from && existing.parent.is_none() {
                return Ok(());
            }
            return Err(Error::invalid_options(
                "branch already exists with a different fork version",
            ));
        }
        ensure_fork_checkpoint_async(self, name, from).await?;
        let registry = registry_bucket();
        self.bucket(registry.as_str()).await?;
        let mut transaction = self.transaction(TransactionOptions::default());
        if let Some(raw) = transaction.get_bucket(&registry, name.as_bytes()).await? {
            let existing = RegistryEntry::decode(&raw)?;
            existing.lifecycle.require_active()?;
            if existing.fork == from && existing.parent.is_none() {
                return Ok(());
            }
            return Err(Error::invalid_options(
                "branch already exists with a different fork version",
            ));
        }
        let entry = RegistryEntry {
            fork: from,
            parent: None,
            written_buckets: BTreeSet::new(),
            lifecycle: BranchLifecycle::Active,
            generation: new_branch_generation()?,
        };
        transaction.put_bucket(&registry, name.as_bytes().to_vec(), entry.encode())?;
        transaction.commit().await.map(|_| ())
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
        self.read_registry(parent)?
            .ok_or_else(|| Error::invalid_options("parent branch does not exist"))?
            .lifecycle
            .require_active()?;
        if self.read_registry(name)?.is_some() {
            return Err(Error::invalid_options("branch already exists"));
        }
        // Fork at the current global version: the child sees the parent's state as
        // of now. Pinning it keeps the parent's (and its ancestors') history that
        // the chain reads through retained.
        let from = self.latest_read_version();
        ensure_fork_checkpoint(self, name, from)?;
        let registry = registry_bucket();
        self.bucket_sync(registry.as_str())?;
        let mut transaction = self.transaction(TransactionOptions::default());
        let parent_raw = transaction
            .get_bucket_sync(&registry, parent.as_bytes())?
            .ok_or_else(|| Error::invalid_options("parent branch does not exist"))?;
        RegistryEntry::decode(&parent_raw)?
            .lifecycle
            .require_active()?;
        if transaction
            .get_bucket_sync(&registry, name.as_bytes())?
            .is_some()
        {
            return Err(Error::invalid_options("branch already exists"));
        }
        let entry = RegistryEntry {
            fork: from,
            parent: Some(parent.to_owned()),
            written_buckets: BTreeSet::new(),
            lifecycle: BranchLifecycle::Active,
            generation: new_branch_generation()?,
        };
        transaction.put_bucket(&registry, name.as_bytes().to_vec(), entry.encode())?;
        transaction.commit_sync().map(|_| ())
    }

    /// Async-first form of [`Db::create_branch_from`].
    ///
    /// The child forks `parent` at the latest globally committed
    /// [`ReadVersion`] observed before the checkpoint is created. Its registry
    /// entry stores that exact fork coordinate and the parent name, so reopening
    /// walks the same frozen ancestor chain. The parent is not modified.
    ///
    /// This form publishes both the durable fork checkpoint and branch registry
    /// through async metadata APIs. It is therefore required for object-storage
    /// and browser backends that reject synchronous manifest updates. If the
    /// checkpoint succeeds but registry publication fails, retrying is safe: the
    /// conservative checkpoint remains and no named child is visible yet.
    ///
    /// # Parameters
    ///
    /// - `name`: new durable child-branch name.
    /// - `parent`: existing durable parent-branch name.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidOptions`] when `parent` does not exist or `name`
    /// is already registered. It also returns retained-history, checkpoint,
    /// conditional-write, durability, and backend errors from publishing the
    /// fork metadata. A failed call never publishes a partial registry entry.
    pub async fn create_branch_from_async(&self, name: &str, parent: &str) -> Result<()> {
        self.read_registry_async(parent)
            .await?
            .ok_or_else(|| Error::invalid_options("parent branch does not exist"))?
            .lifecycle
            .require_active()?;
        if self.read_registry_async(name).await?.is_some() {
            return Err(Error::invalid_options("branch already exists"));
        }
        let from = self.latest_read_version();
        ensure_fork_checkpoint_async(self, name, from).await?;
        let registry = registry_bucket();
        self.bucket(registry.as_str()).await?;
        let mut transaction = self.transaction(TransactionOptions::default());
        let parent_raw = transaction
            .get_bucket(&registry, parent.as_bytes())
            .await?
            .ok_or_else(|| Error::invalid_options("parent branch does not exist"))?;
        RegistryEntry::decode(&parent_raw)?
            .lifecycle
            .require_active()?;
        if transaction
            .get_bucket(&registry, name.as_bytes())
            .await?
            .is_some()
        {
            return Err(Error::invalid_options("branch already exists"));
        }
        let entry = RegistryEntry {
            fork: from,
            parent: Some(parent.to_owned()),
            written_buckets: BTreeSet::new(),
            lifecycle: BranchLifecycle::Active,
            generation: new_branch_generation()?,
        };
        transaction.put_bucket(&registry, name.as_bytes().to_vec(), entry.encode())?;
        transaction.commit().await.map(|_| ())
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
        leaf.lifecycle.require_active()?;
        let leaf_fork = leaf.fork;
        let leaf_parent = leaf.parent.clone();
        let leaf_generation = leaf.generation;

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
            entry.lifecycle.require_active()?;
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
                leaf_generation,
            },
        ))
    }

    /// Lists active durable branch names, in name order. A branch whose
    /// recoverable deletion is in progress is intentionally omitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry cannot be scanned.
    pub fn list_branches(&self) -> Result<Vec<String>> {
        let registry = self.bucket_sync(registry_bucket())?;
        let mut names = Vec::new();
        for row in registry.range_sync(&KeyRange::all())? {
            let row = row?;
            let name = String::from_utf8(row.key).map_err(|_| Error::Corruption {
                message: "branch registry holds a non-utf8 name".to_owned(),
            })?;
            if RegistryEntry::decode(&row.value)?.lifecycle == BranchLifecycle::Active {
                names.push(name);
            }
        }
        Ok(names)
    }

    /// Deletes a durable branch through a recoverable persisted lifecycle.
    ///
    /// The registry first changes atomically from active to deleting while the
    /// same transaction verifies that no active child exists. From that point,
    /// opens, reads, writes, lineage lookup, and listing reject or hide the
    /// branch. Cleanup then removes divergent data, releases the fork
    /// checkpoint, and removes the deleting marker last.
    ///
    /// Every cleanup step is idempotent. Retrying after a process or storage
    /// failure resumes from the deleting marker; deleting an already absent
    /// branch also succeeds. Backends without bucket drop clear the divergent
    /// bucket contents before releasing the checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch still has active children, if its durable
    /// state is malformed, or if a cleanup step fails.
    pub fn delete_branch(&self, name: &str) -> Result<()> {
        let Some(entry) = begin_branch_delete(self, name)? else {
            return Ok(());
        };
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
        // The branch is already durably invisible. Release the history pin only
        // after its data is gone, then remove the Deleting marker last.
        match self.delete_checkpoint_sync(&fork_checkpoint(name)) {
            Ok(()) | Err(Error::CheckpointNotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        finish_branch_delete(self, name, entry.generation)
    }

    /// Async-first form of [`Db::delete_branch`]. Required for object-store
    /// backends because branch metadata and bucket deletion publish durable
    /// metadata through async compare-and-swap calls.
    ///
    /// # Errors
    ///
    /// Same validation and persistence errors as [`Db::delete_branch`].
    pub async fn delete_branch_async(&self, name: &str) -> Result<()> {
        let Some(entry) = begin_branch_delete_async(self, name).await? else {
            return Ok(());
        };
        for user_bucket in &entry.written_buckets {
            let data = data_bucket(name, user_bucket);
            match self.drop_bucket(data.clone()).await {
                Ok(()) => {}
                Err(Error::UnsupportedBackend { .. }) => {
                    self.bucket(data)
                        .await?
                        .delete_range(KeyRange::all())
                        .await?;
                }
                Err(error) => return Err(error),
            }
        }
        match self.delete_checkpoint(&fork_checkpoint(name)).await {
            Ok(()) | Err(Error::CheckpointNotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        finish_branch_delete_async(self, name, entry.generation).await
    }

    /// Returns an active durable branch's lineage (its fork version and parent
    /// branch), or `None` when no such active branch exists — without assembling
    /// a read chain or opening any data bucket. A branch being deleted is
    /// reported as `None`.
    ///
    /// This lets a higher layer reuse this crate's durable branch lifecycle (the
    /// fork pin that survives restarts and aggressive GC, the registry, and
    /// nesting) while storing its **own** divergent data and doing its own
    /// fall-through reads against [`Db::snapshot_at`] of the returned
    /// [`BranchInfo::fork`]. Combine with [`Db::create_branch`] /
    /// [`Db::create_branch_from`] / [`Db::list_branches`] / [`Db::delete_branch`].
    ///
    /// # Errors
    ///
    /// Returns an error if the registry cannot be read or a stored entry is
    /// malformed.
    pub fn branch_info(&self, name: &str) -> Result<Option<BranchInfo>> {
        Ok(self
            .read_registry(name)?
            .filter(|entry| entry.lifecycle == BranchLifecycle::Active)
            .map(|entry| BranchInfo {
                fork: entry.fork,
                parent: entry.parent,
            }))
    }

    /// Async-first form of [`Db::branch_info`].
    ///
    /// Reads only the durable branch registry; it does not open branch data
    /// buckets, create a snapshot, or change the fork pin. This is the supported
    /// lineage lookup for object-storage and browser backends whose registry
    /// reads use async I/O.
    ///
    /// # Parameters
    ///
    /// - `name`: durable branch name to resolve.
    ///
    /// # Returns
    ///
    /// `Some(BranchInfo)` contains the exact global fork coordinate and optional
    /// parent name. `None` means no registry entry is visible under `name`; it
    /// does not report checkpoint-only leftovers from an interrupted create.
    ///
    /// # Errors
    ///
    /// Returns storage and backend read errors, or [`Error::Corruption`] when a
    /// present registry entry has an invalid format.
    pub async fn branch_info_async(&self, name: &str) -> Result<Option<BranchInfo>> {
        Ok(self
            .read_registry_async(name)
            .await?
            .filter(|entry| entry.lifecycle == BranchLifecycle::Active)
            .map(|entry| BranchInfo {
                fork: entry.fork,
                parent: entry.parent,
            }))
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

    async fn read_registry_async(&self, name: &str) -> Result<Option<RegistryEntry>> {
        match self
            .bucket(registry_bucket())
            .await?
            .get(name.as_bytes())
            .await?
        {
            Some(bytes) => Ok(Some(RegistryEntry::decode(&bytes)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests;
