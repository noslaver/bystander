mod lib;

use crossbeam_epoch::Guard;
use lib::{LinkedList as LockFreeLinkedList, Node};
use std::sync::atomic::Ordering;

use bystander::{
    Atomic, CasState, Contention, ContentionMeasure, NormalizedLockFree, VersionedCas,
    WaitFreeSimulator,
};

const N: usize = 20; // 🤔

// in a consuming crate (wait-free-linked-list crate)
pub struct WaitFreeLinkedList<T: Clone> {
    _simulator: WaitFreeSimulator<LockFreeLinkedList<T>, N>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum InputOp<T> {
    Insert(T),
    Delete(T),
    Find(T),
}

struct ReferenceQuartet<T> {
    reference: &T,
    mark_bit: bool,
    help_bit: bool,
}

impl<T> ReferenceQuartet<T> {
    fn new(reference: &T, mark_bit: bool, help_bit: bool) -> Self {
        Self(reference, mark_bit, help_bit)
    }
}
struct DoubleMarkReference<T> {
    atomic_ref: Atomic<ReferenceQuartet<T>>,
}

impl<T> DoubleMarkReference<T> {
    fn new(reference: &T, mark_bit: bool, help_bit: bool) -> Self {
        Self(Atomic::<ReferenceQuartet<T>>::new(
            ReferenceQuartet::<T>::new(reference, mark_bit, help_bit),
        ))
    }

    fn get_version(&self) -> u64 {
        let (_, old_version) = self
            .atomic_ref
            .with(|current, version| return (current, version), guard);
        old_version
    }

    fn compare_and_set(
        &self,
        expected: &T,
        new: &T,
        expected_mark: bool,
        new_mark: bool,
        expected_help: bool,
        new_help: bool,
    ) -> bool {
        let (current, new_version) = self
            .atomic_ref
            .with(|current, version| return (current, version + 1), guard);
        expected == current.reference
            && expected_mark == current.mark_bit
            && expected_help == current.help_bit
            && (new == current.reference
                && new_mark == current.mark_bit
                && new_help == current.help_bit
                || self.atomic_ref.compare_and_set(
                    current,
                    ReferenceQuartet::new(new, new_mark, new_help),
                    contention,
                    new_version,
                    guard,
                ))
    }
}

#[derive(Clone)]
pub struct ListCasDescriptor<T> {
    holder: DoubleMarkReference<Node<T>>,
    old_ref: &'g Node<T>,
    new_ref: &'g Node<T>,
    old_mark: bool,
    new_mark: bool,
    old_version: u64,
    state: CasState,
}

impl<T> ListCasDescriptor<T> {
    fn new(
        holder: DoubleMarkReference<Node<T>>,
        old_ref: &'g Node<T>,
        new_ref: &'g Node<T>,
        old_mark: bool,
        new_mark: bool,
        guard: &Guard,
    ) -> Self {
        Self(
            holder,
            old_ref,
            new_ref,
            old_mark,
            new_mark,
            holder.get_version(),
            CasState::Pending,
        )
    }
}

impl VersionedCas for ListCasDescriptor {
    fn execute(&self, _contention: &mut ContentionMeasure) -> Result<bool, Contention> {
        Ok(self.holder.compare_and_set(
            self.old_ref,
            self.new_ref,
            self.old_mark,
            self.new_mark,
            false,
            true,
        ))
    }

    fn has_modified_bit(&self) -> bool {
        let (current, new_version) = self
            .holder
            .atomic_ref
            .with(|current, version| return (current, version), guard);
        current.help_bit && self.old_version + 1 == new_version
    }

    fn clear_bit(&self) -> bool {
        self.holder.compare_and_set(
            self.new_ref,
            self.new_ref,
            self.new_mark,
            self.new_mark,
            true,
            false,
        )
    }

    fn state(&self) -> CasState {
        self.state
    }

    fn set_state(&self, _new: CasState) {
        self.state = _new;
    }
}

impl<T: Clone> NormalizedLockFree for LockFreeLinkedList<T> {
    type Input = InputOp<T>;
    type Output = bool;

    type CommitDescriptor = Vec<ListCasDescriptor<T>>;

    fn generator(
        &self,
        _op: &Self::Input,
        _contention: &mut ContentionMeasure,
        _guard: &'g Guard,
    ) -> Result<Self::CommitDescriptor, Contention> {
        match _op {
            InputOp::Insert(key) => {
                let (left_ptr, right_ptr) = self.search(key, _guard);

                let right = unsafe { right_ptr.deref() };
                let left = unsafe { left_ptr.deref() };

                // Key already in list
                if right_ptr != self.tail.load(Ordering::SeqCst, guard) && right.key == Some(key) {
                    return Ok(Vec::<ListCasDescriptor<T>>::new());
                }

                let mut new = Node::new(key);
                new.next = Atomic::<Node<T>>::from(right_ptr);

                // Fill the details
                let update_cas_descriptor =
                    ListCasDescriptor::new(holder, old_ref, new_ref, old_mark, new_mark);

                // make it more elegant
                Vec::new().push(update_cas_descriptor)
            }
            InputOp::Delete(node) => {
                let (left_ptr, right_ptr) = self.search(key, _guard);

                let right = unsafe { right_ptr.deref() };

                if (right_ptr == tail) || (right.key != Some(key)) {
                    return Ok(Vec::<ListCasDescriptor<T>>::new());
                }

                // Fill the details
                let update_cas_descriptor =
                    ListCasDescriptor::new(holder, old_ref, new_ref, old_mark, new_mark);

                // make it more elegant
                Vec::new().push(update_cas_descriptor)
            }
            InputOp::Find(key) => {
                return Ok(Vec::<ListCasDescriptor<T>>::new());
            }
        }
    }

    fn wrap_up(
        &self,
        _op: Self::Input,
        _executed: Result<(), usize>,
        _performed: &Self::CommitDescriptor,
        _contention: &mut ContentionMeasure,
        _guard: &'g Guard,
    ) -> Result<Option<Self::Output>, Contention> {
        match _op {
            InputOp::Delete(key) | InputOp::Insert(key) => {
                if _performed.is_empty() {
                    // Operation failed
                    Ok(Some(false));
                }
                match _executed {
                    Ok(()) => Ok(Some(true)),

                    // Need to restart operation
                    Err(_) => Ok(None),
                }
            }
            InputOp::Find(key) => {
                let res = self.find(key, _guard);
                Ok(Some(res.unwrap()))
            }
        }
    }

    fn fast_path(
        &self,
        _op: &Self::Input,
        _contention: &mut ContentionMeasure,
        _guard: &'g Guard,
    ) -> Result<Self::Output, Contention> {
        // On fast path, just use the existing algorithm API
        // If fails return contention
        match _op {
            InputOp::Insert(key) => self.insert(key, _contention, _guard),
            InputOp::Delete(key) => self.delete(key, _contention, _guard),
            InputOp::Find(key) => self.find(key, _guard),
        }
    }
}

//impl<T: Clone> WaitFreeLinkedList<T> {
//    pub fn insert(&self, t: T) {
//        // self.simulator.run(Insert(t))
//    }
//
//    pub fn find(&self, t: T) {
//        // self.simulator.run(Insert(t))
//    }
//
//    pub fn delete(&self, t: T) {
//        // self.simulator.run(Insert(t))
//    }
//}

#[test]
fn wait_free_sim() {}
