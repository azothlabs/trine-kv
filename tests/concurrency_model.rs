//! Exhaustive small-state concurrency models for synchronization patterns used
//! by the commit frontier.
//!
//! This intentionally models the same lock/atomic/condition-variable order as
//! `CommitTracker`: terminal slots are changed while holding the slot lock, the
//! contiguous frontier is released before notification, and waiters re-check
//! the frontier while holding that same lock.

use std::collections::BTreeMap;

use loom::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

#[derive(Debug)]
struct CommitFrontierModel {
    visible: AtomicUsize,
    slots: Mutex<BTreeMap<usize, bool>>,
    changed: Condvar,
}

impl CommitFrontierModel {
    fn new() -> Self {
        Self {
            visible: AtomicUsize::new(0),
            slots: Mutex::new(BTreeMap::from([(1, false), (2, false)])),
            changed: Condvar::new(),
        }
    }

    fn mark_terminal(&self, sequence: usize) {
        let mut slots = self.slots.lock().expect("model slot lock");
        *slots.get_mut(&sequence).expect("reserved model slot") = true;
        let mut visible = self.visible.load(Ordering::Acquire);
        while slots.get(&(visible + 1)).copied() == Some(true) {
            slots.remove(&(visible + 1));
            visible += 1;
            self.visible.store(visible, Ordering::Release);
        }
        drop(slots);
        self.changed.notify_all();
    }

    fn wait_until_visible(&self, sequence: usize) {
        let mut slots = self.slots.lock().expect("model slot lock");
        while self.visible.load(Ordering::Acquire) < sequence {
            slots = self.changed.wait(slots).expect("model wait lock");
        }
    }
}

#[test]
fn commit_frontier_has_no_lost_wakeup_under_all_small_interleavings() {
    loom::model(|| {
        let frontier = Arc::new(CommitFrontierModel::new());

        let later = Arc::clone(&frontier);
        let publish_later = thread::spawn(move || later.mark_terminal(2));

        let earlier = Arc::clone(&frontier);
        let publish_earlier = thread::spawn(move || earlier.mark_terminal(1));

        let waiter = Arc::clone(&frontier);
        let wait = thread::spawn(move || {
            waiter.wait_until_visible(2);
            assert_eq!(waiter.visible.load(Ordering::Acquire), 2);
        });

        publish_later.join().expect("later publisher joins");
        publish_earlier.join().expect("earlier publisher joins");
        wait.join().expect("waiter joins");
        assert_eq!(frontier.visible.load(Ordering::Acquire), 2);
        assert!(frontier.slots.lock().expect("final model lock").is_empty());
    });
}
