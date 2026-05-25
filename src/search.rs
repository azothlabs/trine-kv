use std::cmp::Ordering;

#[must_use]
pub fn seek_ge<T: Ord>(items: &[T], target: &T) -> Option<usize> {
    match items.binary_search(target) {
        Ok(index) => Some(index),
        Err(index) if index < items.len() => Some(index),
        Err(_) => None,
    }
}

#[must_use]
pub fn seek_gt<T: Ord>(items: &[T], target: &T) -> Option<usize> {
    let index = items.partition_point(|item| item <= target);
    (index < items.len()).then_some(index)
}

#[must_use]
pub fn seek_le<T: Ord>(items: &[T], target: &T) -> Option<usize> {
    let index = items.partition_point(|item| item <= target);
    index.checked_sub(1)
}

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

#[cfg(test)]
mod tests {
    use super::{advance_to, seek_ge, seek_gt, seek_le};

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
}
