use super::{
    BTreeSet, BranchLifecycle, Db, DurableTransition, Error, KeyRange, KeyValue, RESERVED,
    ReadVersion, RegistryEntry, Result, SEP, TAG_PRESENT, TAG_TOMBSTONE, TransactionOptions, Value,
    data_bucket, decode_registry_name, encode_name_component, registry_bucket,
    validate_branch_name,
};

pub(super) struct BranchCreateRequest<'name> {
    name: &'name str,
    fork: ReadVersion,
    parent: Option<&'name str>,
}

impl<'name> BranchCreateRequest<'name> {
    fn new(name: &'name str, fork: ReadVersion, parent: Option<&'name str>) -> Result<Self> {
        validate_branch_name(name)?;
        if let Some(parent) = parent {
            validate_branch_name(parent)?;
        }
        Ok(Self { name, fork, parent })
    }

    fn resolve_existing(&self, existing: Option<RegistryEntry>) -> Result<Option<RegistryEntry>> {
        resolve_existing_create(existing, self.fork, self.parent)
    }

    fn new_entry(&self) -> Result<RegistryEntry> {
        Ok(RegistryEntry {
            fork: self.fork,
            parent: self.parent.map(str::to_owned),
            written_buckets: BTreeSet::new(),
            lifecycle: BranchLifecycle::Creating,
            generation: new_branch_generation()?,
        })
    }
}

pub(super) struct BranchDeletePlan {
    pub(super) data_buckets: Vec<String>,
    pub(super) checkpoint: String,
    pub(super) generation: [u8; 16],
}

impl BranchDeletePlan {
    pub(super) fn new(name: &str, entry: &RegistryEntry) -> Self {
        Self {
            data_buckets: entry
                .written_buckets
                .iter()
                .map(|bucket| data_bucket(name, bucket))
                .collect(),
            checkpoint: fork_checkpoint(name),
            generation: entry.generation,
        }
    }
}

pub(super) struct BranchDeleteScan<'name> {
    target_name: &'name str,
    target: Option<RegistryEntry>,
}

impl<'name> BranchDeleteScan<'name> {
    fn new(target_name: &'name str) -> Self {
        Self {
            target_name,
            target: None,
        }
    }

    fn observe(&mut self, row: KeyValue) -> Result<()> {
        let (name, entry) = decode_registry_row(row)?;
        reject_active_child(self.target_name, &entry)?;
        if name == self.target_name {
            self.target = Some(entry);
        }
        Ok(())
    }

    fn finish(self) -> Option<RegistryEntry> {
        self.target
    }
}

pub(super) fn decode_registry_row(row: KeyValue) -> Result<(String, RegistryEntry)> {
    Ok((
        decode_registry_name(row.key)?,
        RegistryEntry::decode(&row.value)?,
    ))
}

pub(super) fn active_registry_name(row: KeyValue) -> Result<Option<String>> {
    let (name, entry) = decode_registry_row(row)?;
    Ok((entry.lifecycle == BranchLifecycle::Active).then_some(name))
}

pub(super) fn select_child_fork(
    existing: Option<&RegistryEntry>,
    parent: &str,
    latest: ReadVersion,
) -> ReadVersion {
    existing
        .filter(|entry| {
            entry.parent.as_deref() == Some(parent)
                && matches!(
                    entry.lifecycle,
                    BranchLifecycle::Creating | BranchLifecycle::Active
                )
        })
        .map_or(latest, |entry| entry.fork)
}

/// Decodes a durable branch data value: `Some(value)` for a present write,
/// `None` for a tombstone (deleted on the branch).
pub(super) fn decode_branch_value(raw: &[u8]) -> Result<Option<Value>> {
    match raw.first() {
        Some(&TAG_PRESENT) => Ok(Some(raw[1..].to_vec())),
        Some(&TAG_TOMBSTONE) if raw.len() == 1 => Ok(None),
        _ => Err(Error::Corruption {
            message: "malformed durable branch value".to_owned(),
        }),
    }
}

pub(super) async fn require_branch_generation_active(
    db: &Db,
    name: &str,
    generation: [u8; 16],
) -> Result<()> {
    let current = db
        .read_registry(name)
        .await?
        .ok_or_else(|| Error::invalid_options("branch no longer exists"))?;
    current.require_generation(generation)
}

pub(super) fn new_branch_generation() -> Result<[u8; 16]> {
    let mut generation = [0; 16];
    getrandom::fill(&mut generation)
        .map_err(|error| Error::runtime_busy(format!("branch generation entropy: {error}")))?;
    Ok(generation)
}

pub(super) fn reject_active_child(parent: &str, entry: &RegistryEntry) -> Result<()> {
    if entry.lifecycle != BranchLifecycle::Deleting && entry.parent.as_deref() == Some(parent) {
        return Err(Error::invalid_options(
            "cannot delete a branch that still has child branches",
        ));
    }
    Ok(())
}

pub(super) fn resolve_existing_create(
    existing: Option<RegistryEntry>,
    from: ReadVersion,
    parent: Option<&str>,
) -> Result<Option<RegistryEntry>> {
    let Some(existing) = existing else {
        return Ok(None);
    };
    if existing.fork != from || existing.parent.as_deref() != parent {
        return Err(Error::invalid_options(
            "branch already exists with different lineage",
        ));
    }
    match existing.lifecycle {
        BranchLifecycle::Creating | BranchLifecycle::Active => Ok(Some(existing)),
        BranchLifecycle::Deleting => Err(Error::invalid_options("branch deletion is in progress")),
    }
}

pub(super) fn plan_branch_activation(
    mut current: RegistryEntry,
    prepared: &RegistryEntry,
) -> Result<DurableTransition<RegistryEntry>> {
    if current.generation != prepared.generation
        || current.fork != prepared.fork
        || current.parent != prepared.parent
    {
        return Err(Error::Corruption {
            message: "prepared branch registry entry changed identity".to_owned(),
        });
    }
    match current.lifecycle {
        BranchLifecycle::Active => Ok(DurableTransition::AlreadyApplied(current)),
        BranchLifecycle::Creating => {
            current.lifecycle = BranchLifecycle::Active;
            Ok(DurableTransition::Apply(current))
        }
        BranchLifecycle::Deleting => Err(Error::invalid_options(
            "branch deletion started before creation completed",
        )),
    }
}

pub(super) fn require_delete_completion(entry: &RegistryEntry, generation: [u8; 16]) -> Result<()> {
    if entry.lifecycle != BranchLifecycle::Deleting || entry.generation != generation {
        return Err(Error::Corruption {
            message: "branch delete completion observed a different durable generation".to_owned(),
        });
    }
    Ok(())
}

pub(super) async fn begin_branch_delete(db: &Db, name: &str) -> Result<Option<RegistryEntry>> {
    let registry = registry_bucket();
    db.internal_bucket(registry.as_str()).await?;
    let mut transaction = db.transaction(TransactionOptions::default());
    let mut rows = transaction
        .range_internal_bucket(&registry, KeyRange::all())
        .await?;
    let mut scan = BranchDeleteScan::new(name);
    while let Some(row) = rows.next().await? {
        scan.observe(row)?;
    }
    drop(rows);
    let Some(mut target) = scan.finish() else {
        return Ok(None);
    };
    match target.lifecycle.begin_delete() {
        DurableTransition::AlreadyApplied(_) => Ok(Some(target)),
        DurableTransition::Apply(lifecycle) => {
            target.lifecycle = lifecycle;
            transaction.put_internal_bucket(
                &registry,
                name.as_bytes().to_vec(),
                target.encode()?,
            )?;
            transaction.commit().await?;
            Ok(Some(target))
        }
    }
}

pub(super) async fn finish_branch_delete(db: &Db, name: &str, generation: [u8; 16]) -> Result<()> {
    let registry = registry_bucket();
    let mut transaction = db.transaction(TransactionOptions::default());
    let Some(raw) = transaction
        .get_internal_bucket(&registry, name.as_bytes())
        .await?
    else {
        return Ok(());
    };
    let entry = RegistryEntry::decode(&raw)?;
    require_delete_completion(&entry, generation)?;
    transaction.delete_internal_bucket(&registry, name.as_bytes().to_vec())?;
    transaction.commit().await.map(|_| ())
}

/// The checkpoint name pinning a durable branch's fork. A checkpoint is durable
/// metadata that the retained-history floor and GC respect, so the parent keeps
/// the branch's fork history across restarts.
pub(super) fn fork_checkpoint(branch: &str) -> String {
    format!("{RESERVED}fork-v1{SEP}{}", encode_name_component(branch))
}

pub(super) async fn ensure_fork_checkpoint(db: &Db, branch: &str, from: ReadVersion) -> Result<()> {
    let checkpoint = fork_checkpoint(branch);
    match db.create_internal_checkpoint_at(&checkpoint, from).await {
        Ok(()) => Ok(()),
        Err(Error::CheckpointAlreadyExists { .. }) => {
            if db.internal_checkpoint_read_version(&checkpoint)? == from {
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

pub(super) async fn delete_checkpoint_if_present(db: &Db, name: &str) -> Result<()> {
    match db.delete_internal_checkpoint(name).await {
        Ok(()) | Err(Error::CheckpointNotFound { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) async fn ensure_authoritative_fork_checkpoint(
    db: &Db,
    branch: &str,
    entry: &RegistryEntry,
) -> Result<()> {
    match ensure_fork_checkpoint(db, branch, entry.fork).await {
        Ok(()) => Ok(()),
        Err(Error::InvalidOptions { .. }) => {
            let checkpoint = fork_checkpoint(branch);
            delete_checkpoint_if_present(db, &checkpoint).await?;
            ensure_fork_checkpoint(db, branch, entry.fork).await
        }
        Err(error) => Err(error),
    }
}

pub(super) async fn prepare_branch_create(
    db: &Db,
    name: &str,
    from: ReadVersion,
    parent: Option<&str>,
) -> Result<RegistryEntry> {
    let request = BranchCreateRequest::new(name, from, parent)?;
    // Keep an in-process pin until the durable checkpoint exists. Otherwise a
    // registry commit can advance an aggressive retention floor past `from`
    // before the checkpoint is created.
    let fork_pin = db.snapshot_at(from)?;

    let registry = registry_bucket();
    db.internal_bucket(registry.as_str()).await?;
    let mut transaction = db.transaction(TransactionOptions::default());
    if let Some(parent) = request.parent {
        let parent_raw = transaction
            .get_internal_bucket(&registry, parent.as_bytes())
            .await?
            .ok_or_else(|| Error::invalid_options("parent branch does not exist"))?;
        RegistryEntry::decode(&parent_raw)?
            .lifecycle
            .require_active()?;
    }
    let existing = transaction
        .get_internal_bucket(&registry, name.as_bytes())
        .await?
        .map(|raw| RegistryEntry::decode(&raw))
        .transpose()?;
    if let Some(existing) = request.resolve_existing(existing)? {
        ensure_authoritative_fork_checkpoint(db, request.name, &existing).await?;
        return Ok(existing);
    }

    let entry = request.new_entry()?;
    ensure_authoritative_fork_checkpoint(db, request.name, &entry).await?;
    transaction.put_internal_bucket(
        &registry,
        request.name.as_bytes().to_vec(),
        entry.encode()?,
    )?;
    transaction.commit().await?;
    drop(fork_pin);
    Ok(entry)
}

pub(super) async fn activate_prepared_branch(
    db: &Db,
    name: &str,
    prepared: &RegistryEntry,
) -> Result<()> {
    let registry = registry_bucket();
    let mut transaction = db.transaction(TransactionOptions::default());
    let raw = transaction
        .get_internal_bucket(&registry, name.as_bytes())
        .await?
        .ok_or_else(|| Error::Corruption {
            message: "prepared branch registry entry disappeared".to_owned(),
        })?;
    let current = RegistryEntry::decode(&raw)?;
    match plan_branch_activation(current, prepared)? {
        DurableTransition::AlreadyApplied(_) => Ok(()),
        DurableTransition::Apply(current) => {
            transaction.put_internal_bucket(
                &registry,
                name.as_bytes().to_vec(),
                current.encode()?,
            )?;
            transaction.commit().await.map(|_| ())
        }
    }
}
