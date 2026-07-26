use super::{
    BTreeSet, Branch, BucketName, Db, Error, HashMap, ReadVersion, RegistryEntry, Result, Snapshot,
    Value,
};

/// Value tag in a durable branch's data bucket: a present value or a tombstone
/// (the branch deleted a key the parent still has). Distinguishes "the branch
/// wrote nothing here, fall through to the parent" (key absent) from "the branch
/// deleted it" (tombstone).
pub(super) const TAG_PRESENT: u8 = 0;
pub(super) const TAG_TOMBSTONE: u8 = 1;

pub(super) fn encode_present(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 1);
    out.push(TAG_PRESENT);
    out.extend_from_slice(value);
    out
}

/// One ephemeral branch-local write held in the in-memory overlay.
pub(super) enum OverlayWrite {
    Put(Value),
    Delete,
}

/// One level of a durable branch's read chain. The leaf (the opened branch
/// itself) reads its own latest writes (`at = None`); each ancestor is read
/// frozen at the version the child forked it (`at = Some`). `written` is that
/// branch's set of diverged user buckets, so untouched buckets are skipped
/// without opening (or creating) a data bucket.
pub(super) struct DurableLayer {
    pub(super) name: String,
    pub(super) written: BTreeSet<String>,
    pub(super) at: Option<Snapshot>,
}

/// A durable branch's read chain (leaf first, then each ancestor branch), plus
/// the leaf's own registry fields — needed to rewrite its entry when it first
/// writes a user bucket. The root fall-through below the chain is the branch's
/// pinned [`Branch::fork`] snapshot.
pub(super) struct DurableState {
    pub(super) chain: Vec<DurableLayer>,
    pub(super) leaf_fork: ReadVersion,
    pub(super) leaf_parent: Option<String>,
    pub(super) leaf_generation: [u8; 16],
}

/// Builds one durable branch ancestry while keeping lineage validation and
/// snapshot coordinates independent from the storage path used to read registry
/// entries.
pub(super) struct BranchLineageBuilder {
    leaf_fork: ReadVersion,
    leaf_parent: Option<String>,
    leaf_generation: [u8; 16],
    chain: Vec<DurableLayer>,
    child_fork: ReadVersion,
    parent: Option<String>,
    visited: BTreeSet<String>,
}

impl BranchLineageBuilder {
    pub(super) fn new(name: &str, leaf: RegistryEntry) -> Result<Self> {
        leaf.lifecycle.require_active()?;
        Ok(Self {
            leaf_fork: leaf.fork,
            leaf_parent: leaf.parent.clone(),
            leaf_generation: leaf.generation,
            chain: vec![DurableLayer {
                name: name.to_owned(),
                written: leaf.written_buckets,
                at: None,
            }],
            child_fork: leaf.fork,
            parent: leaf.parent,
            visited: BTreeSet::from([name.to_owned()]),
        })
    }

    pub(super) fn next_parent(&self) -> Option<String> {
        self.parent.clone()
    }

    pub(super) fn push_parent(
        &mut self,
        db: &Db,
        name: String,
        entry: RegistryEntry,
    ) -> Result<()> {
        if !self.visited.insert(name.clone()) {
            return Err(Error::Corruption {
                message: format!("branch lineage contains a cycle at {name}"),
            });
        }
        entry.lifecycle.require_active()?;
        self.chain.push(DurableLayer {
            name,
            written: entry.written_buckets,
            at: Some(db.snapshot_at(self.child_fork)?),
        });
        self.child_fork = entry.fork;
        self.parent = entry.parent;
        Ok(())
    }

    pub(super) fn finish(self, db: &Db) -> Result<Branch<'_>> {
        let root_fork = db.snapshot_at(self.child_fork)?;
        Ok(Branch::durable(
            db,
            root_fork,
            DurableState {
                chain: self.chain,
                leaf_fork: self.leaf_fork,
                leaf_parent: self.leaf_parent,
                leaf_generation: self.leaf_generation,
            },
        ))
    }
}

/// How a branch stores its divergent writes.
pub(super) enum Backing {
    /// In-memory, lost with the handle (ephemeral clone / `AS OF` view).
    Ephemeral(HashMap<(BucketName, Vec<u8>), OverlayWrite>),
    /// Persisted in the branch's own buckets (durable named branch), as a read
    /// chain from the branch up through its ancestor branches.
    Durable(DurableState),
}
