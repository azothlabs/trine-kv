use super::transitions::{
    BranchDeletePlan, activate_prepared_branch, active_registry_name, begin_branch_delete,
    finish_branch_delete, prepare_branch_create, select_child_fork,
};
use super::{
    Branch, BranchInfo, BranchLineageBuilder, Db, Error, KeyRange, ReadVersion, RegistryEntry,
    Result, registry_bucket, validate_branch_name,
};

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
    /// Returns an error if `from` is not a readable version, if `name` is empty
    /// or exceeds 1024 UTF-8 bytes, if the name already exists with a different
    /// fork, or if persisting the branch fails.
    pub fn create_branch_sync(&self, name: &str, from: ReadVersion) -> Result<()> {
        self.block_on_sync_api(self.create_branch(name, from))
    }

    /// Creates a durable named branch through the async storage boundary.
    ///
    /// This is the primary form of [`Db::create_branch_sync`]. It is required
    /// for object-store
    /// backends because both the fork checkpoint and the branch registry are
    /// durable metadata writes.
    ///
    /// # Errors
    ///
    /// Same validation and persistence errors as [`Db::create_branch_sync`].
    pub async fn create_branch(&self, name: &str, from: ReadVersion) -> Result<()> {
        let prepared = prepare_branch_create(self, name, from, None).await?;
        activate_prepared_branch(self, name, &prepared).await
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
    /// Returns an error if either name is empty or exceeds 1024 UTF-8 bytes, if
    /// `parent` does not exist, if `name` already exists with different lineage,
    /// or if persisting the branch fails.
    pub fn create_branch_from_sync(&self, name: &str, parent: &str) -> Result<()> {
        self.block_on_sync_api(self.create_branch_from(name, parent))
    }

    /// Async-first form of [`Db::create_branch_from_sync`].
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
    pub async fn create_branch_from(&self, name: &str, parent: &str) -> Result<()> {
        let existing = self.read_registry(name).await?;
        let from = select_child_fork(existing.as_ref(), parent, self.latest_read_version());
        let prepared = prepare_branch_create(self, name, from, Some(parent)).await?;
        activate_prepared_branch(self, name, &prepared).await
    }

    /// Opens a durable named branch, re-pinning its fork and assembling its read
    /// chain (the branch, then each ancestor branch, then the root). The returned
    /// handle sees that chain with the branch's persisted writes on top.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch (or an ancestor) does not exist, or if a
    /// fork version is no longer retained (see the module docs on retention).
    pub fn open_branch_sync(&self, name: &str) -> Result<Branch<'_>> {
        self.block_on_sync_api(self.open_branch(name))
    }

    /// Asynchronously opens a durable named branch and pins every ancestor at
    /// the version captured by its child.
    ///
    /// The returned handle uses async branch reads and writes on every backend.
    /// Use [`Db::open_branch_sync`] only with a backend that supports synchronous
    /// storage access.
    ///
    /// # Errors
    ///
    /// Returns an error if the branch is absent or inactive, an ancestor is
    /// missing or cyclic, a fork version is no longer retained, or registry
    /// storage cannot be read.
    pub async fn open_branch(&self, name: &str) -> Result<Branch<'_>> {
        validate_branch_name(name)?;
        let leaf = self
            .read_registry(name)
            .await?
            .ok_or_else(|| Error::invalid_options("no such branch"))?;
        let mut builder = BranchLineageBuilder::new(name, leaf)?;
        while let Some(parent_name) = builder.next_parent() {
            let entry =
                self.read_registry(&parent_name)
                    .await?
                    .ok_or_else(|| Error::Corruption {
                        message: format!("branch {parent_name} is missing (an ancestor of {name})"),
                    })?;
            builder.push_parent(self, parent_name, entry)?;
        }
        builder.finish(self)
    }

    /// Lists active durable branch names, in name order. A branch whose
    /// recoverable deletion is in progress is intentionally omitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry cannot be scanned.
    pub fn list_branches_sync(&self) -> Result<Vec<String>> {
        self.block_on_sync_api(self.list_branches())
    }

    /// Asynchronously lists active durable branch names in bytewise name order.
    ///
    /// Branches with an in-progress create or delete transition are omitted.
    ///
    /// # Errors
    ///
    /// Returns storage or format errors encountered while scanning the durable
    /// registry.
    pub async fn list_branches(&self) -> Result<Vec<String>> {
        let registry = self.internal_bucket(registry_bucket()).await?;
        let mut rows = registry.range(&KeyRange::all()).await?;
        let mut names = Vec::new();
        while let Some(row) = rows.next().await? {
            if let Some(name) = active_registry_name(row)? {
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
    pub fn delete_branch_sync(&self, name: &str) -> Result<()> {
        self.block_on_sync_api(self.delete_branch(name))
    }

    /// Async-first form of [`Db::delete_branch_sync`]. Required for object-store
    /// backends because branch metadata and bucket deletion publish durable
    /// metadata through async compare-and-swap calls.
    ///
    /// # Errors
    ///
    /// Same validation and persistence errors as [`Db::delete_branch_sync`].
    pub async fn delete_branch(&self, name: &str) -> Result<()> {
        validate_branch_name(name)?;
        let Some(entry) = begin_branch_delete(self, name).await? else {
            return Ok(());
        };
        let plan = BranchDeletePlan::new(name, &entry);
        for data in plan.data_buckets {
            match self.drop_internal_bucket(data.clone()).await {
                Ok(()) => {}
                Err(Error::UnsupportedBackend { .. }) => {
                    self.internal_bucket(data)
                        .await?
                        .delete_range(KeyRange::all())
                        .await?;
                }
                Err(error) => return Err(error),
            }
        }
        match self.delete_internal_checkpoint(&plan.checkpoint).await {
            Ok(()) | Err(Error::CheckpointNotFound { .. }) => {}
            Err(error) => return Err(error),
        }
        finish_branch_delete(self, name, plan.generation).await
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
    pub fn branch_info_sync(&self, name: &str) -> Result<Option<BranchInfo>> {
        self.block_on_sync_api(self.branch_info(name))
    }

    /// Async-first form of [`Db::branch_info_sync`].
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
    pub async fn branch_info(&self, name: &str) -> Result<Option<BranchInfo>> {
        validate_branch_name(name)?;
        Ok(self
            .read_registry(name)
            .await?
            .and_then(BranchInfo::from_active))
    }

    pub(super) async fn read_registry(&self, name: &str) -> Result<Option<RegistryEntry>> {
        match self
            .internal_bucket(registry_bucket())
            .await?
            .get(name.as_bytes())
            .await?
        {
            Some(bytes) => Ok(Some(RegistryEntry::decode(&bytes)?)),
            None => Ok(None),
        }
    }
}
