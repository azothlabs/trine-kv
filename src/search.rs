use std::cmp::Ordering;

use crate::options::IndexSearchPolicy;

/// Returns the first index whose item is greater than or equal to `target`.
#[must_use]
pub fn seek_ge<T: Ord>(items: &[T], target: &T) -> Option<usize> {
    match items.binary_search(target) {
        Ok(index) => Some(index),
        Err(index) if index < items.len() => Some(index),
        Err(_) => None,
    }
}

/// Returns the first index whose item is greater than `target`.
#[must_use]
pub fn seek_gt<T: Ord>(items: &[T], target: &T) -> Option<usize> {
    let index = items.partition_point(|item| item <= target);
    (index < items.len()).then_some(index)
}

/// Returns the last index whose item is less than or equal to `target`.
#[must_use]
pub fn seek_le<T: Ord>(items: &[T], target: &T) -> Option<usize> {
    let index = items.partition_point(|item| item <= target);
    index.checked_sub(1)
}

/// Advances from `current` to the first index greater than or equal to `target`.
#[must_use]
pub fn advance_to<T: Ord>(items: &[T], current: usize, target: &T) -> Option<usize> {
    if current >= items.len() {
        return None;
    }

    match items[current].cmp(target) {
        Ordering::Greater | Ordering::Equal => Some(current),
        Ordering::Less => seek_ge(&items[current + 1..], target).map(|index| current + 1 + index),
    }
}

pub(crate) fn partition_point_by(
    len: usize,
    policy: IndexSearchPolicy,
    predicate: impl FnMut(usize) -> bool,
) -> usize {
    // Callers keep canonical sorted arrays for validation and scans. Policy
    // variants can get specialized layouts later behind this single boundary.
    match policy {
        IndexSearchPolicy::Linear => linear_partition_point(len, predicate),
        IndexSearchPolicy::Auto if len <= 8 => linear_partition_point(len, predicate),
        IndexSearchPolicy::Auto | IndexSearchPolicy::Binary => {
            binary_partition_point(len, predicate)
        }
    }
}

fn linear_partition_point(len: usize, mut predicate: impl FnMut(usize) -> bool) -> usize {
    for index in 0..len {
        if !predicate(index) {
            return index;
        }
    }
    len
}

fn binary_partition_point(len: usize, mut predicate: impl FnMut(usize) -> bool) -> usize {
    let mut left = 0;
    let mut right = len;

    while left < right {
        let middle = left + (right - left) / 2;
        if predicate(middle) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }

    left
}

#[cfg(test)]
mod tests {
    use super::{advance_to, partition_point_by, seek_ge, seek_gt, seek_le};
    use crate::options::IndexSearchPolicy;

    #[test]
    fn sorted_search_boundaries_are_stable() {
        let items = [1, 3, 5, 7];

        assert_eq!(seek_ge(&items, &0), Some(0));
        assert_eq!(seek_ge(&items, &4), Some(2));
        assert_eq!(seek_ge(&items, &8), None);

        assert_eq!(seek_gt(&items, &5), Some(3));
        assert_eq!(seek_gt(&items, &7), None);

        assert_eq!(seek_le(&items, &0), None);
        assert_eq!(seek_le(&items, &6), Some(2));

        assert_eq!(advance_to(&items, 1, &6), Some(3));
    }

    #[test]
    fn partition_point_policy_dispatch_keeps_sorted_boundaries() {
        let items = [1, 3, 5, 7, 9, 11, 13, 15, 17];

        for policy in [
            IndexSearchPolicy::Linear,
            IndexSearchPolicy::Binary,
            IndexSearchPolicy::Auto,
        ] {
            let index = partition_point_by(items.len(), policy, |index| items[index] <= 10);
            assert_eq!(index, 5, "policy {policy:?} changed the boundary");
        }
    }
}
