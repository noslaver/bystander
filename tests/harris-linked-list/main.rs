mod linked_list;

use crossbeam_epoch::{self as epoch, Guard, Owned, Shared};
use linked_list::{LinkedList as LockFreeLinkedList, Node};
use std::sync::atomic::Ordering;

use bystander::{
    Atomic, CasState, Contention, ContentionMeasure, NormalizedLockFree, VersionedCas,
    WaitFreeSimulator,
};

const N: usize = 20; // 🤔

// in a consuming crate (wait-free-linked-list crate)
pub struct WaitFreeLinkedList<T>
where
    T: Clone + Copy + PartialEq + Eq + PartialOrd + Ord,
{
    _simulator: WaitFreeSimulator<LockFreeLinkedList<T>, N>,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum InputOp<T>
where
    T: Clone + Copy + PartialEq + Eq + PartialOrd + Ord,
{
    Insert(T),
    Delete(T),
    Find(T),
}

struct ReferenceQuartet<T> {
    reference: T,
    mark_bit: bool,
    help_bit: bool,
}

impl<T> Eq for ReferenceQuartet<T> where T: PartialEq {}

impl<T> PartialEq for ReferenceQuartet<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference
            && self.mark_bit == other.mark_bit
            && self.help_bit == other.help_bit
    }
}

impl<T> ReferenceQuartet<T> {
    fn new(reference: T, mark_bit: bool, help_bit: bool) -> Self {
        Self {
            reference,
            mark_bit,
            help_bit,
        }
    }
}

struct DoubleMarkReference<T>(Atomic<ReferenceQuartet<T>>);

impl<T> DoubleMarkReference<T>
where
    T: PartialEq,
{
    fn new(reference: T, mark_bit: bool, help_bit: bool) -> Self {
        Self(Atomic::<ReferenceQuartet<T>>::new(
            ReferenceQuartet::<T>::new(reference, mark_bit, help_bit),
        ))
    }

    fn get_version<'g>(&self, guard: &'g Guard) -> u64 {
        self.0.with(|_, version| version, guard)
    }

    fn compare_and_set<'g>(
        &self,
        expected: T,
        new: T,
        expected_mark: bool,
        new_mark: bool,
        expected_help: bool,
        new_help: bool,
        contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> bool {
        let (current, new_version) = self
            .0
            .with(|current, version| (current, version + 1), guard);

        expected == current.reference
            && expected_mark == current.mark_bit
            && expected_help == current.help_bit
            && (new == current.reference
                && new_mark == current.mark_bit
                && new_help == current.help_bit
                || self
                    .0
                    .compare_and_set(
                        current,
                        ReferenceQuartet::new(new, new_mark, new_help),
                        contention,
                        Some(new_version),
                        guard,
                    )
                    .unwrap()) // TODO
    }
}

pub struct ListCasDescriptor<'g, T> {
    holder: DoubleMarkReference<Node<T>>,
    old_ref: Shared<'g, Node<T>>, // TODO
    new_ref: Owned<Node<T>>,
    old_mark: bool,
    new_mark: bool,
    old_version: u64,
    state: CasState,
}

impl<'g, T> ListCasDescriptor<'g, T>
where
    T: PartialEq,
{
    fn new(
        holder: DoubleMarkReference<Node<T>>,
        old_ref: Node<T>,
        new_ref: Node<T>,
        old_mark: bool,
        new_mark: bool,
    ) -> Self {
        Self {
            holder,
            old_ref,
            new_ref,
            old_mark,
            new_mark,
            old_version: holder.get_version(),
            state: CasState::Pending,
        }
    }
}

impl<'g, T> VersionedCas for ListCasDescriptor<'g, T> {
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
        // TODO
        let guard = &epoch::pin();
        self.holder.0.with(
            |current, version| current.help_bit && self.old_version + 1 == version,
            guard,
        )
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

impl<T> NormalizedLockFree for LockFreeLinkedList<T>
where
    T: Clone + Copy + PartialEq + Eq + PartialOrd + Ord,
{
    type Input = InputOp<T>;
    type Output = bool;

    type CommitDescriptor<'g> = Option<ListCasDescriptor<'g, T>>;

    fn generator<'g>(
        &self,
        op: &Self::Input,
        _contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> Result<Self::CommitDescriptor, Contention> {
        match op {
            InputOp::Insert(key) => {
                let (left_ptr, right_ptr) = self.search(key, guard);

                let right = unsafe { right_ptr.deref() };
                let left = unsafe { left_ptr.deref() };

                // Key already in list
                if right_ptr != self.tail.load(Ordering::SeqCst, guard) && right.key == Some(key) {
                    return Ok(None);
                }

                let mut new = Node::new(key);
                new.next = Atomic::<Node<T>>::from(right_ptr);

                Ok(Some(todo!()))
                //ListCasDescriptor::new(
                //self.holder,
                //self.old_ref,
                //self.new_ref,
                //self.old_mark,
                //self.new_mark)
            }
            InputOp::Delete(key) => {
                let (left_ptr, right_ptr) = self.search(key, guard);

                let right = unsafe { right_ptr.deref() };

                if right_ptr == self.tail.load(Ordering::SeqCst, guard) || right.key != Some(key) {
                    return Ok(None);
                }

                Ok(Some(todo!()))
                //ListCasDescriptor::new(
                //self.holder,
                //self.old_ref,
                //self.new_ref,
                //self.old_mark,
                //self.new_mark)
            }
            InputOp::Find(key) => {
                return Ok(None);
            }
        }
    }

    fn wrap_up<'g>(
        &self,
        op: &Self::Input,
        _executed: Result<(), usize>,
        performed: &Self::CommitDescriptor,
        _contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> Result<Option<Self::Output>, Contention> {
        match op {
            InputOp::Delete(key) | InputOp::Insert(key) => {
                if performed.is_empty() {
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
                let res = self.find(key, guard)?;
                Ok(Some(res))
            }
        }
    }

    fn fast_path<'g>(
        &self,
        op: &Self::Input,
        contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> Result<Self::Output, Contention> {
        // On fast path, just use the existing algorithm API
        // If fails return contention
        match op {
            InputOp::Insert(key) => self.insert(key, contention, guard),
            InputOp::Delete(key) => self.delete(key, contention, guard),
            InputOp::Find(key) => self.find(key, guard),
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
