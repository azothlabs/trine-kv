//! Small trusted predicates shared by runtime validation and bounded proofs.
//!
//! Keep this module free of I/O, allocation, clocks, and backend types. Higher
//! layers may translate these decisions into domain errors, but must not
//! redefine the predicates.

#[must_use]
pub(crate) const fn durable_fence_advances(current: u64, next: u64) -> bool {
    next >= current
}

#[must_use]
pub(crate) const fn checked_sequence_successor(current: u64) -> Option<u64> {
    current.checked_add(1)
}

#[must_use]
pub(crate) const fn gc_delete_allowed(is_pending: bool, is_reachable: bool) -> bool {
    is_pending && !is_reachable
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotReachability {
    Reachable,
    TooOld,
    TooNew,
}

#[must_use]
pub(crate) const fn classify_snapshot_reachability(
    requested: u64,
    oldest_retained: u64,
    latest: u64,
) -> SnapshotReachability {
    if requested > latest {
        SnapshotReachability::TooNew
    } else if requested < oldest_retained {
        SnapshotReachability::TooOld
    } else {
        SnapshotReachability::Reachable
    }
}

#[must_use]
pub(crate) const fn immutable_retry_allowed(object_exists: bool, bytes_equal: bool) -> bool {
    object_exists && bytes_equal
}

#[cfg(test)]
mod tests {
    use super::{
        SnapshotReachability, checked_sequence_successor, classify_snapshot_reachability,
        durable_fence_advances, gc_delete_allowed, immutable_retry_allowed,
    };

    #[test]
    fn durable_fence_accepts_equal_or_larger_and_rejects_regression() {
        assert!(durable_fence_advances(7, 7));
        assert!(durable_fence_advances(7, 8));
        assert!(!durable_fence_advances(7, 6));
    }

    #[test]
    fn sequence_successor_is_exact_and_rejects_overflow() {
        assert_eq!(checked_sequence_successor(0), Some(1));
        assert_eq!(checked_sequence_successor(u64::MAX - 1), Some(u64::MAX));
        assert_eq!(checked_sequence_successor(u64::MAX), None);
    }

    #[test]
    fn garbage_collection_requires_pending_and_unreachable() {
        assert!(!gc_delete_allowed(false, false));
        assert!(!gc_delete_allowed(false, true));
        assert!(gc_delete_allowed(true, false));
        assert!(!gc_delete_allowed(true, true));
    }

    #[test]
    fn snapshot_reachability_classifies_every_interval_boundary() {
        assert_eq!(
            classify_snapshot_reachability(4, 5, 9),
            SnapshotReachability::TooOld
        );
        assert_eq!(
            classify_snapshot_reachability(5, 5, 9),
            SnapshotReachability::Reachable
        );
        assert_eq!(
            classify_snapshot_reachability(9, 5, 9),
            SnapshotReachability::Reachable
        );
        assert_eq!(
            classify_snapshot_reachability(10, 5, 9),
            SnapshotReachability::TooNew
        );
        assert_eq!(
            classify_snapshot_reachability(6, 9, 5),
            SnapshotReachability::TooNew
        );
    }

    #[test]
    fn immutable_retry_requires_an_equal_existing_object() {
        assert!(!immutable_retry_allowed(false, false));
        assert!(!immutable_retry_allowed(false, true));
        assert!(!immutable_retry_allowed(true, false));
        assert!(immutable_retry_allowed(true, true));
    }
}

#[cfg(kani)]
mod proofs {
    use super::{
        SnapshotReachability, checked_sequence_successor, classify_snapshot_reachability,
        durable_fence_advances, gc_delete_allowed, immutable_retry_allowed,
    };

    #[kani::proof]
    fn a_sequence_successor_is_exactly_one_and_strictly_larger() {
        let current = kani::any::<u64>();
        match checked_sequence_successor(current) {
            Some(next) => {
                assert_eq!(next - current, 1);
                assert!(next > current);
            }
            None => assert_eq!(current, u64::MAX),
        }
    }

    #[kani::proof]
    fn durable_fence_order_is_transitive() {
        let first = kani::any::<u64>();
        let second = kani::any::<u64>();
        let third = kani::any::<u64>();
        kani::assume(durable_fence_advances(first, second));
        kani::assume(durable_fence_advances(second, third));
        assert!(durable_fence_advances(first, third));
    }

    #[kani::proof]
    fn garbage_collection_never_authorizes_a_reachable_object() {
        let pending = kani::any::<bool>();
        let reachable = kani::any::<bool>();
        if gc_delete_allowed(pending, reachable) {
            assert!(pending);
            assert!(!reachable);
        }
    }

    #[kani::proof]
    fn reachable_snapshot_is_exactly_within_the_retained_interval() {
        let requested = kani::any::<u64>();
        let oldest = kani::any::<u64>();
        let latest = kani::any::<u64>();
        let classification = classify_snapshot_reachability(requested, oldest, latest);
        assert_eq!(
            classification == SnapshotReachability::Reachable,
            requested >= oldest && requested <= latest
        );
    }

    #[kani::proof]
    fn immutable_retry_never_accepts_absent_or_different_bytes() {
        let exists = kani::any::<bool>();
        let equal = kani::any::<bool>();
        if immutable_retry_allowed(exists, equal) {
            assert!(exists);
            assert!(equal);
        }
    }
}
