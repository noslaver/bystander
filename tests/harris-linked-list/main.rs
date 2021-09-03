mod linked_list;

use crossbeam_epoch::{self as epoch, Guard};
use linked_list::{LinkedList as LockFreeLinkedList, Node};

use bystander::{
    Atomic, CasState, Contention, ContentionMeasure, NormalizedLockFree, VersionedCas,
    WaitFreeSimulator,
};

// in a consuming crate (wait-free-linked-list crate)
pub struct WaitFreeLinkedList<const N: usize> {
    _simulator: WaitFreeSimulator<LockFreeLinkedList, N>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum InputOp {
    Insert(usize),
    Delete(usize),
    Find(usize),
}

#[derive(Clone, PartialEq, Eq)]
struct ReferenceQuartet {
    reference: Node,
    mark_bit: bool,
    help_bit: bool,
}

impl ReferenceQuartet {
    fn new(reference: Node, mark_bit: bool, help_bit: bool) -> Self {
        Self {
            reference,
            mark_bit,
            help_bit,
        }
    }
}

#[derive(Clone)]
struct DoubleMarkReference(Atomic<ReferenceQuartet>);

impl DoubleMarkReference {
    fn new(reference: Node, mark_bit: bool, help_bit: bool) -> Self {
        Self(Atomic::<ReferenceQuartet>::new(ReferenceQuartet::new(
            reference, mark_bit, help_bit,
        )))
    }

    fn get_version<'g>(&self, guard: &'g Guard) -> u64 {
        self.0.with(|_, version| version, guard)
    }

    fn compare_and_set<'g>(
        &self,
        expected: Node,
        new: Node,
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

#[derive(Clone)]
pub struct ListCasDescriptor {
    holder: DoubleMarkReference,
    // old_ref: Shared<'g, Node>, // TODO
    old_ref: *const Node, // TODO
    new_ref: Node,
    old_mark: bool,
    new_mark: bool,
    old_version: u64,
    state: CasState,
}

impl ListCasDescriptor {
    fn new(
        holder: DoubleMarkReference,
        old_ref: *const Node,
        new_ref: Node,
        old_mark: bool,
        new_mark: bool,
        old_version: u64,
    ) -> Self {
        Self {
            holder,
            old_ref,
            new_ref,
            old_mark,
            new_mark,
            old_version,
            state: CasState::Pending,
        }
    }
}

impl VersionedCas for ListCasDescriptor {
    fn execute(&self, contention: &mut ContentionMeasure) -> Result<bool, Contention> {
        // TODO
        let guard = &epoch::pin();
        Ok(self.holder.compare_and_set(
            self.old_ref,
            self.new_ref,
            self.old_mark,
            self.new_mark,
            false,
            true,
            contention,
            guard,
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
        // TODO
        let guard = &epoch::pin();
        self.holder.compare_and_set(
            self.new_ref,
            self.new_ref,
            self.new_mark,
            self.new_mark,
            true,
            false,
            &mut ContentionMeasure::new(),
            guard,
        )
    }

    fn state(&self) -> CasState {
        self.state
    }

    fn set_state(&self, _new: CasState) {
        self.state = _new;
    }
}

impl NormalizedLockFree for LockFreeLinkedList {
    type Input = InputOp;
    type Output = bool;

    type CommitDescriptor = Option<ListCasDescriptor>;

    fn generator<'g>(
        &self,
        op: &Self::Input,
        _contention: &mut ContentionMeasure, // TODO
        guard: &'g Guard,
    ) -> Result<Self::CommitDescriptor, Contention> {
        match *op {
            InputOp::Insert(key) => {
                let (left_ptr, right_ptr) = self.search(Some(key), guard);

                let right = unsafe { right_ptr.deref() };
                let left = unsafe { left_ptr.deref() };

                // Key already in list
                if right != self.tail.with(|tail, _| tail, guard) && right.key == Some(key) {
                    return Ok(None);
                }

                let mut new = Node::new(key);
                // TODO - get Atomic from Shared and then clone
                // new.next = Atomic::<Node>::from(right_ptr);

                todo!()
                //Ok(Some(ListCasDescriptor::new(
                //self.holder,
                //self.old_ref,
                //self.new_ref,
                //self.old_mark,
                //self.new_mark,
                //self.holder.get_version(guard))))
            }
            InputOp::Delete(key) => {
                let (left_ptr, right_ptr) = self.search(Some(key), guard);

                let right = unsafe { right_ptr.deref() };

                if right == self.tail.with(|tail, _| tail, guard) || right.key != Some(key) {
                    return Ok(None);
                }

                todo!()
                //Ok(Some(ListCasDescriptor::new(
                //self.holder,
                //self.old_ref,
                //self.new_ref,
                //self.old_mark,
                //self.new_mark,
                //self.holder.get_version(guard))))
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
        match *op {
            InputOp::Delete(key) | InputOp::Insert(key) => {
                if performed.is_none() {
                    // Operation failed
                    return Ok(Some(false));
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
        match *op {
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
