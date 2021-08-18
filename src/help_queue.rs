use crate::OperationRecordBox;
use crossbeam_epoch::{self as epoch, Atomic, CompareExchangeError, Guard, Owned, Shared};
use std::sync::atomic::Ordering;

struct Node<T> {
    value: Option<T>,
    next: Atomic<Self>,
    enq_id: Option<usize>,
}

impl<T> Node<T> {
    fn new(value: T, enq_id: usize) -> Atomic<Self> {
        Atomic::new(Self {
            value: Some(value),
            next: Atomic::null(),
            enq_id: Some(enq_id),
        })
    }

    fn sentinel() -> Self {
        Self {
            value: None,
            next: Atomic::null(),
            enq_id: None,
        }
    }
}

struct OpDesc<T> {
    phase: Option<u64>,
    pending: bool,
    enqueue: bool,
    node: Option<Atomic<Node<T>>>,
}

/// Operations are linear in N.
pub(crate) struct WaitFreeHelpQueue<T, const N: usize> {
    head: Atomic<Node<T>>,
    tail: Atomic<Node<T>>,
    state: [Atomic<OpDesc<T>>; N],
}

impl<T, const N: usize> Drop for WaitFreeHelpQueue<T, N> {
    fn drop(&mut self) {
        let guard = &epoch::pin();
        let mut head = self.head.load(Ordering::SeqCst, guard);
        while !head.is_null() {
            let next = unsafe { head.deref() }.next.load(Ordering::SeqCst, guard);
            let prev_head = self.head.swap(next, Ordering::SeqCst, guard);
            unsafe {
                prev_head.into_owned();
            }
            head = self.head.load(Ordering::SeqCst, guard);
        }

        for desc_atomic in &self.state {
            unsafe {
                let _ = desc_atomic.clone().into_owned();
            }
        }
    }
}

impl<T, const N: usize> WaitFreeHelpQueue<T, N>
where
    T: Copy + PartialEq + Eq,
{
    pub(crate) fn new() -> Self {
        use std::convert::TryInto;

        let sentinel = Node::sentinel();
        let head = Atomic::new(sentinel);
        let tail = head.clone();
        // TODO: Once consts can depend on T, make this constant instead of going via Vec
        let state: [Atomic<OpDesc<T>>; N] = (0..N)
            .map(|_| {
                Atomic::new(OpDesc {
                    phase: None,
                    pending: false,
                    enqueue: true,
                    node: None,
                })
            })
            .collect::<Vec<_>>()
            .try_into()
            .expect("gave N elements");

        Self { head, tail, state }
    }

    pub(crate) fn enqueue<'g>(&self, id: usize, value: T, guard: &'g Guard) {
        let phase = self.max_phase(guard).map_or(0, |p| p + 1);
        // Old code used `store` here, discarding the old value saved in `state[id]`.
        // We want to reclaim that value, and drop it.
        // Though maybe somehow `store` is okay. Check with dear Jon.
        let old_desc = self.state[id].swap(
            Owned::new(OpDesc {
                phase: Some(phase),
                pending: true,
                enqueue: true,
                node: Some(Node::new(value, id)),
            }),
            Ordering::SeqCst,
            guard,
        );

        // Safety: We swapped the descriptor, it is no longer reachable.
        unsafe {
            guard.defer_destroy(old_desc);
        }

        self.help(phase, guard);
        self.help_finish_enq(guard);
    }

    pub(crate) fn peek<'g>(&self, guard: &'g Guard) -> Option<T> {
        // Safety: head always points to valid memory.
        let node = unsafe { self.head.load(Ordering::SeqCst, guard).deref() };
        let next = node.next.load(Ordering::SeqCst, guard);
        if next.is_null() {
            None
        } else {
            Some(unsafe { next.deref() }.value.expect("not a sentinel Node"))
        }
    }

    pub(crate) fn try_remove_front<'g>(&self, front: T, guard: &'g Guard) -> Result<(), ()> {
        let curr_head_ptr = self.head.load(Ordering::SeqCst, guard);
        // Safety: head always points to valid memory.
        let curr_head = unsafe { curr_head_ptr.deref() };
        let next = curr_head.next.load(Ordering::SeqCst, guard);
        // Safety: `next` is not null.
        if next.is_null() || (unsafe { next.deref() }.value.expect("not a sentinel node")) != front
        {
            return Err(());
        }

        match self.head.compare_exchange(
            curr_head_ptr,
            next,
            Ordering::SeqCst,
            Ordering::Relaxed,
            guard,
        ) {
            Ok(_) => {
                self.help_finish_enq(guard);
                // TODO: is this needed?
                curr_head.next.store(Shared::null(), Ordering::SeqCst);
                // TODO
                unsafe {
                    guard.defer_destroy(curr_head_ptr);
                }
                Ok(())
            }
            Err(_) => Err(()),
        }
    }

    fn help<'g>(&self, phase: u64, guard: &'g Guard) {
        for (id, desc_atomic) in self.state.iter().enumerate() {
            let desc_ptr = desc_atomic.load(Ordering::SeqCst, guard);
            // Safety:
            // `state`'s elements are always allocated.
            let desc = unsafe { desc_ptr.deref() };
            if desc.pending && desc.phase.unwrap_or(0) <= phase {
                // This operation needs help.
                // Currently, the only helpable operation is enqueue.
                if desc.enqueue {
                    self.help_enq(id, phase, guard)
                }
            }
        }
    }

    fn help_enq<'g>(&self, id: usize, phase: u64, guard: &'g Guard) {
        while self.is_still_pending(id, phase, guard) {
            let last_ptr = self.tail.load(Ordering::SeqCst, guard);
            // Safety: tail always points to valid memory.
            let last = unsafe { last_ptr.deref() };
            let next_ptr = last.next.load(Ordering::SeqCst, guard);
            if last_ptr != self.tail.load(Ordering::SeqCst, guard) {
                // Tail was concurrently updated.
                continue;
            }

            if !next_ptr.is_null() {
                // Tail is not up to date -- help update it.
                self.help_finish_enq(guard);
                continue;
            }

            if !self.is_still_pending(id, phase, guard) {
                // Phase is already over.
                // TODO: Can this just return?
                continue;
            }

            // We know we have a consistent (tail, tail.next) pair, and that it likely still needs
            // to be updated, so let's try to actually execute the to-be-enqueued node from the
            // enqueuing thread's descriptor.

            let curr_desc_ptr = self.state[id].load(Ordering::SeqCst, guard);
            // Safety:
            // `state`'s elements are always allocated.
            let curr_desc = unsafe { curr_desc_ptr.deref() };

            if !curr_desc.pending {
                // TODO: Can we continue? Can we assert this is still pending?
            }
            debug_assert!(curr_desc.enqueue);

            let curr_node = curr_desc
                .node
                .as_ref()
                .expect("node should always be Some for pending enqueue")
                .load(Ordering::SeqCst, guard);

            if last
                .next
                .compare_exchange(
                    next_ptr,
                    curr_node,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                self.help_finish_enq(guard);
                return;
            }
        }
    }

    fn help_finish_enq<'g>(&self, guard: &'g Guard) {
        let last_ptr = self.tail.load(Ordering::SeqCst, guard);
        // Safety: tail always points to valid memory.
        let last = unsafe { last_ptr.deref() };
        let next_ptr = last.next.load(Ordering::SeqCst, guard);
        if next_ptr.is_null() {
            // Tail pointer is already up to date, so nothing to do.
            return;
        }

        // Safety: `next_ptr` is not null.
        let next = unsafe { next_ptr.deref() };
        let id = next.enq_id.expect("next is never the sentinel");
        let cur_desc_ptr = self.state[id].load(Ordering::SeqCst, guard);
        // Safety:
        // `state`'s elements are always allocated.
        let cur_desc = unsafe { cur_desc_ptr.deref() };

        if last_ptr != self.tail.load(Ordering::SeqCst, guard) {
            // Tail pointer has already been updated.
            return;
        }

        let null = Atomic::null();
        if cur_desc
            .node
            .as_ref()
            .unwrap_or(&null)
            .load(Ordering::SeqCst, guard)
            != next_ptr
        {
            // Owner of the next node is now working on a subsequent operation,
            // the enqueue must have finished.
            return;
        }

        // This is really just setting pending = false.
        let new_desc_ptr = Owned::new(OpDesc {
            phase: cur_desc.phase,
            pending: false,
            enqueue: true,
            node: cur_desc.node.clone(),
        });

        match self.state[id].compare_exchange(
            cur_desc_ptr,
            new_desc_ptr,
            Ordering::SeqCst,
            Ordering::Relaxed,
            guard,
        ) {
            Ok(_) => {
                // `new_desc_ptr` was CASed into the state array -> free `cur_desc_ptr`
                // Safety: TODO
                unsafe {
                    guard.defer_destroy(cur_desc_ptr);
                }
            }
            Err(CompareExchangeError { new, .. }) => {
                // Someone else already replaced the descriptor, free `new_desc_ptr`
                drop(new);
            }
        }

        let _ = self.tail.compare_exchange(
            last_ptr,
            next_ptr,
            Ordering::SeqCst,
            Ordering::Relaxed,
            guard,
        );
    }

    fn max_phase<'g>(&self, guard: &'g Guard) -> Option<u64> {
        self.state
            .iter()
            .filter_map(|s| {
                // Safety:
                // `state`'s elements are always allocated.
                unsafe { s.load(Ordering::SeqCst, guard).deref() }.phase
            })
            .max()
    }

    fn is_still_pending<'g>(&self, id: usize, phase: u64, guard: &'g Guard) -> bool {
        // Safety:
        // `state`'s elements are always allocated.
        let state = unsafe { self.state[id].load(Ordering::SeqCst, guard).deref() };
        state.pending && state.phase.unwrap_or(0) <= phase
    }
}

// A wait-free queue.
pub(crate) type HelpQueue<LF, const N: usize> = WaitFreeHelpQueue<*const OperationRecordBox<LF>, N>;

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_epoch as epoch;

    #[test]
    fn single_threaded() {
        const ID: usize = 0usize;
        let queue = WaitFreeHelpQueue::<_, 1>::new();

        let guard = &epoch::pin();

        queue.enqueue(ID, 1, guard);

        drop(guard);
        let guard = &epoch::pin();

        let elem = queue.peek(guard);
        assert_eq!(elem, Some(1));

        drop(guard);
        let guard = &epoch::pin();

        let res = queue.try_remove_front(1, guard);
        assert!(res.is_ok());

        drop(guard);
        let guard = &epoch::pin();

        let res = queue.try_remove_front(1, guard);
        assert!(res.is_err());
    }

    #[test]
    fn single_threaded_2() {
        const ID: usize = 0usize;
        let queue = WaitFreeHelpQueue::<_, 1>::new();

        let guard = &epoch::pin();
        queue.enqueue(ID, 1, guard);

        drop(guard);
        let guard = &epoch::pin();

        queue.enqueue(ID, 2, guard);

        drop(guard);
        let guard = &epoch::pin();

        let elem = queue.peek(guard);
        assert_eq!(elem, Some(1));

        drop(guard);
        let guard = &epoch::pin();

        // Fail to enqueue element not on top
        let res = queue.try_remove_front(2, guard);
        assert!(res.is_err());

        drop(guard);
        let guard = &epoch::pin();

        let res = queue.try_remove_front(1, guard);
        assert!(res.is_ok());

        drop(guard);
        let guard = &epoch::pin();

        let elem = queue.peek(guard);
        assert_eq!(elem, Some(2));

        drop(guard);
        let guard = &epoch::pin();

        let res = queue.try_remove_front(2, guard);
        assert!(res.is_ok());
    }
}
