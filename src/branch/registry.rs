use super::{BTreeSet, DurableTransition, Error, ReadVersion, Result, validate_user_named_bucket};

/// Prefix reserving the buckets branching keeps its own state in.
pub(super) const RESERVED: &str = "\u{1}trine-branch\u{1}";
const ENCODED_DATA_RESERVED: &str = "\u{1}trine-branch-data-v1\u{1}";
const REGISTRY_ENTRY_MAGIC: &[u8; 8] = b"TRNBRG01";
pub(super) const SEP: char = '\u{1}';
const MAX_BRANCH_NAME_BYTES: usize = 1_024;

/// The bucket holding the branch registry: branch name → [`RegistryEntry`].
pub(super) fn registry_bucket() -> String {
    format!("{RESERVED}registry")
}

/// The bucket holding a durable branch's divergent writes for one user bucket.
pub(super) fn encode_name_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(super) fn validate_branch_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::invalid_options("branch name cannot be empty"));
    }
    if name.len() > MAX_BRANCH_NAME_BYTES {
        return Err(Error::invalid_options(
            "branch name exceeds the 1024-byte limit",
        ));
    }
    Ok(())
}

pub(super) fn data_bucket(branch: &str, user_bucket: &str) -> String {
    format!(
        "{ENCODED_DATA_RESERVED}{}{SEP}{}",
        encode_name_component(branch),
        encode_name_component(user_bucket)
    )
}

/// A durable branch's lineage: the global version it forked at and its parent
/// branch (`None` = forked from the root lineage). Returned by
/// [`Db::branch_info`](crate::Db::branch_info) for a higher layer that manages its **own** divergent
/// storage (e.g. a SQL/document engine whose writes must be one atomic
/// multi-bucket batch) and only needs the fork point — to read the parent
/// through [`Db::snapshot_at`](crate::Db::snapshot_at) — and the parent link — to walk a nested branch's
/// ancestry — while still relying on the durable fork pin, registry, and nesting
/// this crate maintains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchInfo {
    fork: ReadVersion,
    parent: Option<String>,
}

impl BranchInfo {
    pub(super) fn from_active(entry: RegistryEntry) -> Option<Self> {
        (entry.lifecycle == BranchLifecycle::Active).then_some(Self {
            fork: entry.fork,
            parent: entry.parent,
        })
    }

    /// The global version this branch forked at. Read the parent's state as of
    /// this version (via [`Db::snapshot_at`](crate::Db::snapshot_at)) to resolve a branch read that the
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
pub(super) enum BranchLifecycle {
    Creating,
    Active,
    Deleting,
}

impl BranchLifecycle {
    pub(super) fn require_active(self) -> Result<()> {
        match self {
            Self::Active => Ok(()),
            Self::Creating => Err(Error::runtime_busy("branch creation is in progress")),
            Self::Deleting => Err(Error::invalid_options("branch deletion is in progress")),
        }
    }

    pub(super) fn begin_delete(self) -> DurableTransition<Self> {
        match self {
            Self::Creating | Self::Active => DurableTransition::Apply(Self::Deleting),
            Self::Deleting => DurableTransition::AlreadyApplied(Self::Deleting),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RegistryEntry {
    /// The global version this branch forked its parent at.
    pub(super) fork: ReadVersion,
    /// The parent branch name, or `None` when forked from the root lineage.
    pub(super) parent: Option<String>,
    pub(super) written_buckets: BTreeSet<String>,
    pub(super) lifecycle: BranchLifecycle,
    pub(super) generation: [u8; 16],
}

fn put_str(out: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| Error::invalid_options("branch registry string exceeds u32::MAX bytes"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_registry_bytes<'a>(
    bytes: &'a [u8],
    position: &mut usize,
    length: usize,
) -> Result<&'a [u8]> {
    let end = position
        .checked_add(length)
        .ok_or_else(|| Error::Corruption {
            message: "branch registry field offset overflow".to_owned(),
        })?;
    let value = bytes.get(*position..end).ok_or_else(|| Error::Corruption {
        message: "malformed branch registry entry".to_owned(),
    })?;
    *position = end;
    Ok(value)
}

impl RegistryEntry {
    pub(super) fn require_generation(&self, generation: [u8; 16]) -> Result<()> {
        self.lifecycle.require_active()?;
        if self.generation != generation {
            return Err(Error::invalid_options(
                "branch handle belongs to a replaced branch generation",
            ));
        }
        Ok(())
    }

    pub(super) fn require_identity(
        &self,
        generation: [u8; 16],
        fork: ReadVersion,
        parent: Option<&str>,
    ) -> Result<()> {
        self.lifecycle.require_active()?;
        if self.generation != generation || self.fork != fork || self.parent.as_deref() != parent {
            return Err(Error::invalid_options(
                "branch handle belongs to a replaced branch generation",
            ));
        }
        Ok(())
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>> {
        if let Some(parent) = &self.parent {
            validate_branch_name(parent)?;
        }
        for bucket in &self.written_buckets {
            validate_user_named_bucket(bucket)?;
        }

        let mut out = Vec::new();
        out.extend_from_slice(REGISTRY_ENTRY_MAGIC);
        out.extend_from_slice(&self.fork.as_u64().to_le_bytes());
        let count = u32::try_from(self.written_buckets.len())
            .map_err(|_| Error::invalid_options("branch registry bucket count exceeds u32::MAX"))?;
        out.extend_from_slice(&count.to_le_bytes());
        for bucket in &self.written_buckets {
            put_str(&mut out, bucket)?;
        }
        // Parent uses a flag byte followed by its length-prefixed name.
        match &self.parent {
            Some(parent) => {
                out.push(1);
                put_str(&mut out, parent)?;
            }
            None => out.push(0),
        }
        out.push(match self.lifecycle {
            BranchLifecycle::Active => 0,
            BranchLifecycle::Deleting => 1,
            BranchLifecycle::Creating => 2,
        });
        out.extend_from_slice(&self.generation);
        Ok(out)
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        let corrupt = || Error::Corruption {
            message: "malformed branch registry entry".to_owned(),
        };
        if bytes.get(..REGISTRY_ENTRY_MAGIC.len()) != Some(REGISTRY_ENTRY_MAGIC) {
            return Err(corrupt());
        }
        let mut pos = REGISTRY_ENTRY_MAGIC.len();
        let take_u32 = |pos: &mut usize| -> Result<u32> {
            let raw: [u8; 4] = take_registry_bytes(bytes, pos, 4)?.try_into().expect("4");
            Ok(u32::from_le_bytes(raw))
        };
        let fork_bytes: [u8; 8] = take_registry_bytes(bytes, &mut pos, 8)?
            .try_into()
            .expect("8");
        let fork = ReadVersion::from_u64(u64::from_le_bytes(fork_bytes));
        let count = take_u32(&mut pos)?;
        let mut written_buckets = BTreeSet::new();
        for _ in 0..count {
            let len = take_u32(&mut pos)? as usize;
            let name = take_registry_bytes(bytes, &mut pos, len)?;
            let name = String::from_utf8(name.to_vec()).map_err(|_| corrupt())?;
            validate_user_named_bucket(&name).map_err(|_| corrupt())?;
            if !written_buckets.insert(name) {
                return Err(corrupt());
            }
        }
        let parent = match bytes.get(pos) {
            Some(&0) => {
                pos += 1;
                None
            }
            Some(&1) => {
                pos += 1;
                let len = take_u32(&mut pos)? as usize;
                let name = take_registry_bytes(bytes, &mut pos, len)?;
                let name = String::from_utf8(name.to_vec()).map_err(|_| corrupt())?;
                validate_branch_name(&name).map_err(|_| corrupt())?;
                Some(name)
            }
            Some(_) | None => return Err(corrupt()),
        };
        let lifecycle = match bytes.get(pos) {
            Some(&0) => {
                pos += 1;
                BranchLifecycle::Active
            }
            Some(&1) => {
                pos += 1;
                BranchLifecycle::Deleting
            }
            Some(&2) => {
                pos += 1;
                BranchLifecycle::Creating
            }
            Some(_) | None => return Err(corrupt()),
        };
        let generation = bytes
            .get(pos..)
            .filter(|raw| raw.len() == 16)
            .ok_or_else(corrupt)?
            .try_into()
            .expect("checked generation length");
        Ok(Self {
            fork,
            parent,
            written_buckets,
            lifecycle,
            generation,
        })
    }
}

pub(super) fn decode_registry_name(bytes: Vec<u8>) -> Result<String> {
    let name = String::from_utf8(bytes).map_err(|_| Error::Corruption {
        message: "branch registry holds a non-utf8 name".to_owned(),
    })?;
    validate_branch_name(&name).map_err(|_| Error::Corruption {
        message: "branch registry holds an invalid branch name".to_owned(),
    })?;
    Ok(name)
}
