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
//! [`Branch::range`] returns an [`AsyncBranchRange`], while
//! [`Branch::range_sync`] returns a [`BranchRange`]. Both stream the branch
//! level, each ancestor, and the root from sorted scans and merge them without
//! building a full copy.

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use crate::bucket::{BucketName, validate_user_named_bucket};
use crate::db::Db;
use crate::error::{Error, Result};
use crate::iterator::Iter;
use crate::snapshot::Snapshot;
use crate::state_transition::DurableTransition;
use crate::transaction::TransactionOptions;
use crate::types::{KeyRange, KeyValue, ReadVersion, Value};

mod api;
mod handle;
mod range;
mod registry;
mod state;
mod transitions;

pub use handle::Branch;
pub use range::{AsyncBranchRange, BranchRange};
use range::{AsyncMergeRows, AsyncMergeSource, range_contains};
pub use registry::BranchInfo;
use registry::{
    BranchLifecycle, RESERVED, RegistryEntry, SEP, data_bucket, decode_registry_name,
    encode_name_component, registry_bucket, validate_branch_name,
};
use state::{
    Backing, BranchLineageBuilder, DurableState, OverlayWrite, TAG_PRESENT, TAG_TOMBSTONE,
    encode_present,
};
#[cfg(test)]
use transitions::fork_checkpoint;
use transitions::{decode_branch_value, require_branch_generation_active};

#[cfg(test)]
mod tests;
