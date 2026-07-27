//! Commit visibility sequencing and the close-aware publication barrier.

use super::{
    AtomicU64, BTreeMap, Condvar, Error, Mutex, MutexGuard, Ordering, Result, Sequence,
    lock_poisoned,
};
#[cfg(any(not(target_os = "wasi"), test))]
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
pub(super) struct CommitTracker {
    last_reserved_sequence: AtomicU64,
    visible_sequence: AtomicU64,
    skipped_slots: AtomicU64,
    slots: Mutex<BTreeMap<u64, CommitSlotState>>,
    visible_changed: Condvar,
    #[cfg(any(not(target_os = "wasi"), test))]
    visible_wakers: Mutex<Vec<Waker>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CommitSlot {
    sequence: Sequence,
}

#[derive(Debug)]
pub(super) struct PublishBarrier {
    sequence_lock: Mutex<()>,
    activity: Mutex<PublishBarrierActivity>,
    idle: Condvar,
}

#[derive(Debug)]
pub(super) struct PublishBarrierGuard<'barrier> {
    _activity: PublishActivityGuard<'barrier>,
    _sequence: PublishSequenceGuard<'barrier>,
}

#[derive(Debug)]
pub(crate) struct PublishActivityGuard<'barrier> {
    barrier: &'barrier PublishBarrier,
}

#[derive(Debug)]
pub(super) struct PublishSequenceGuard<'barrier> {
    _guard: MutexGuard<'barrier, ()>,
}

#[derive(Debug, Default)]
pub(super) struct PublishBarrierActivity {
    active: usize,
    closing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommitSlotState {
    Open,
    Visible,
    Skipped,
}

impl CommitTracker {
    pub(super) fn new(visible_sequence: Sequence) -> Self {
        Self {
            last_reserved_sequence: AtomicU64::new(visible_sequence.get()),
            visible_sequence: AtomicU64::new(visible_sequence.get()),
            skipped_slots: AtomicU64::new(0),
            slots: Mutex::new(BTreeMap::new()),
            visible_changed: Condvar::new(),
            #[cfg(any(not(target_os = "wasi"), test))]
            visible_wakers: Mutex::new(Vec::new()),
        }
    }

    #[must_use]
    pub(super) fn visible_sequence(&self) -> Sequence {
        Sequence::new(self.visible_sequence.load(Ordering::Acquire))
    }

    pub(super) fn reset_visible_boundary(&self, visible_sequence: Sequence) -> Result<()> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        slots.clear();
        self.visible_sequence
            .store(visible_sequence.get(), Ordering::Release);
        self.last_reserved_sequence
            .store(visible_sequence.get(), Ordering::Release);
        self.skipped_slots.store(0, Ordering::Release);
        Ok(())
    }

    pub(super) fn last_reserved_sequence(&self) -> Sequence {
        Sequence::new(self.last_reserved_sequence.load(Ordering::Acquire))
    }

    pub(super) fn open_slot_count(&self) -> usize {
        self.slots.lock().map_or(0, |slots| {
            slots
                .values()
                .filter(|state| **state == CommitSlotState::Open)
                .count()
        })
    }

    pub(super) fn skipped_slot_count(&self) -> u64 {
        self.skipped_slots.load(Ordering::Acquire)
    }

    pub(super) fn reserve_slot(&self) -> Result<CommitSlot> {
        let reserved = self
            .last_reserved_sequence
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| Error::Corruption {
                message: "sequence counter overflow".to_owned(),
            })?
            .checked_add(1)
            .ok_or_else(|| Error::Corruption {
                message: "sequence counter overflow".to_owned(),
            })?;
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        if slots.insert(reserved, CommitSlotState::Open).is_some() {
            return Err(Error::Corruption {
                message: format!("commit slot {reserved} was reserved twice"),
            });
        }
        Ok(CommitSlot {
            sequence: Sequence::new(reserved),
        })
    }

    pub(super) fn mark_visible(&self, slot: CommitSlot) -> Result<()> {
        self.mark_terminal(slot, CommitSlotState::Visible)
    }

    pub(super) fn mark_skipped(&self, slot: CommitSlot) -> Result<()> {
        self.mark_terminal(slot, CommitSlotState::Skipped)
    }

    pub(super) fn mark_terminal(
        &self,
        slot: CommitSlot,
        terminal_state: CommitSlotState,
    ) -> Result<()> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        let state = slots
            .get_mut(&slot.sequence.get())
            .ok_or_else(|| Error::Corruption {
                message: format!("commit slot {} is missing", slot.sequence.get()),
            })?;
        match *state {
            CommitSlotState::Open => {
                *state = terminal_state;
                if terminal_state == CommitSlotState::Skipped {
                    self.skipped_slots.fetch_add(1, Ordering::AcqRel);
                }
                let advanced = self.advance_visible_sequence(&mut slots);
                drop(slots);
                if advanced {
                    self.notify_visible_waiters();
                }
                Ok(())
            }
            CommitSlotState::Visible | CommitSlotState::Skipped => Err(Error::Corruption {
                message: format!("commit slot {} is already terminal", slot.sequence.get()),
            }),
        }
    }

    pub(super) fn advance_visible_sequence(
        &self,
        slots: &mut BTreeMap<u64, CommitSlotState>,
    ) -> bool {
        let mut visible = self.visible_sequence.load(Ordering::Acquire);
        let previous = visible;
        while let Some(next) = visible.checked_add(1) {
            match slots.get(&next).copied() {
                Some(CommitSlotState::Visible | CommitSlotState::Skipped) => {
                    slots.remove(&next);
                    visible = next;
                    self.visible_sequence.store(visible, Ordering::Release);
                }
                Some(CommitSlotState::Open) | None => break,
            }
        }
        visible != previous
    }

    pub(super) fn wait_until_visible(&self, sequence: Sequence) -> Result<()> {
        let mut slots = self
            .slots
            .lock()
            .map_err(|_| lock_poisoned("commit tracker slots"))?;
        while self.visible_sequence().get() < sequence.get() {
            slots = self
                .visible_changed
                .wait(slots)
                .map_err(|_| lock_poisoned("commit tracker visible wait"))?;
        }
        Ok(())
    }

    #[cfg(any(not(target_os = "wasi"), test))]
    pub(super) async fn wait_until_visible_async(&self, sequence: Sequence) -> Result<()> {
        std::future::poll_fn(|context| self.poll_until_visible(sequence, context)).await
    }

    #[cfg(any(not(target_os = "wasi"), test))]
    pub(super) fn poll_until_visible(
        &self,
        sequence: Sequence,
        context: &Context<'_>,
    ) -> Poll<Result<()>> {
        if self.visible_sequence().get() >= sequence.get() {
            return Poll::Ready(Ok(()));
        }

        let mut wakers = self
            .visible_wakers
            .lock()
            .map_err(|_| lock_poisoned("commit tracker visible wakers"))?;
        if self.visible_sequence().get() >= sequence.get() {
            return Poll::Ready(Ok(()));
        }
        if !wakers
            .iter()
            .any(|registered| registered.will_wake(context.waker()))
        {
            wakers.push(context.waker().clone());
        }
        Poll::Pending
    }

    pub(super) fn notify_visible_waiters(&self) {
        self.visible_changed.notify_all();
        #[cfg(any(not(target_os = "wasi"), test))]
        {
            let wakers = {
                let mut wakers = self
                    .visible_wakers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                std::mem::take(&mut *wakers)
            };
            for waker in wakers {
                waker.wake();
            }
        }
    }
}

impl PublishBarrier {
    pub(super) fn new() -> Self {
        Self {
            sequence_lock: Mutex::new(()),
            activity: Mutex::new(PublishBarrierActivity::default()),
            idle: Condvar::new(),
        }
    }

    pub(super) fn enter(&self) -> Result<PublishBarrierGuard<'_>> {
        let activity = self.begin_activity()?;
        match self.enter_sequence() {
            Ok(sequence) => Ok(PublishBarrierGuard {
                _activity: activity,
                _sequence: sequence,
            }),
            Err(error) => {
                drop(activity);
                Err(error)
            }
        }
    }

    pub(super) fn begin_activity(&self) -> Result<PublishActivityGuard<'_>> {
        let mut activity = self
            .activity
            .lock()
            .map_err(|_| lock_poisoned("publish activity"))?;
        if activity.closing {
            return Err(Error::Closed);
        }
        activity.active = activity
            .active
            .checked_add(1)
            .ok_or_else(|| Error::Corruption {
                message: "publish activity counter overflow".to_owned(),
            })?;
        Ok(PublishActivityGuard { barrier: self })
    }

    pub(super) fn enter_sequence(&self) -> Result<PublishSequenceGuard<'_>> {
        self.sequence_lock
            .lock()
            .map(|guard| PublishSequenceGuard { _guard: guard })
            .map_err(|_| lock_poisoned("publish sequence barrier"))
    }

    pub(super) fn close(&self) -> Result<()> {
        let mut activity = self
            .activity
            .lock()
            .map_err(|_| lock_poisoned("publish activity"))?;
        activity.closing = true;
        while activity.active != 0 {
            activity = self
                .idle
                .wait(activity)
                .map_err(|_| lock_poisoned("publish activity"))?;
        }
        Ok(())
    }
}

impl Drop for PublishActivityGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut activity) = self.barrier.activity.lock() {
            if activity.active == 0 {
                debug_assert!(false, "publish activity guard count underflow");
                return;
            }
            activity.active -= 1;
            if activity.active == 0 {
                self.barrier.idle.notify_all();
            }
        }
    }
}

impl CommitSlot {
    #[must_use]
    pub(super) const fn sequence(self) -> Sequence {
        self.sequence
    }
}
