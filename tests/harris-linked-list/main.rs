use crossbeam_epoch::{self as epoch, Guard};
use std::sync::atomic::Ordering;

use bystander::{
    Atomic, CasState, Contention, ContentionMeasure, NormalizedLockFree, VersionedCas,
    WaitFreeSimulator,
};

// in a consuming crate (wait-free-linked-list crate)
pub struct WaitFreeLinkedList<const N: usize> {
    _simulator: WaitFreeSimulator<LinkedList, N>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd)]
pub enum InputOp {
    Insert(usize),
    Delete(usize),
    Find(usize),
}

#[derive(Clone, PartialEq, Eq)]
struct RefTriple {
    reference: Node,
    mark: bool,
    help: bool,
}

impl RefTriple {
    fn new(reference: Node, mark: bool, help: bool) -> Self {
        Self {
            reference,
            mark,
            help,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
#[repr(transparant)]
struct DoubleMarkRef(Atomic<RefTriple>);

impl DoubleMarkRef {
    fn new(reference: Node, mark: bool, help: bool) -> Self {
        Self(Atomic::<RefTriple>::new(RefTriple::new(
            reference, mark, help,
        )))
    }

    fn get_version<'g>(&self, guard: &'g Guard) -> u64 {
        self.0.with(|_, version| version, guard)
    }

    fn compare_and_set<'g>(
        &self,
        expected: &Node,
        new: Node,
        expected_mark: bool,
        new_mark: bool,
        expected_help: bool,
        new_help: bool,
        contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> Result<bool, Contention> {
        let (current, new_version) = self
            .0
            .with(|current, version| (current, version + 1), guard);

        if !(expected == &current.reference
            && expected_mark == current.mark
            && expected_help == current.help)
        {
            return Ok(false);
        }

        if new == current.reference && new_mark == current.mark && new_help == current.help {
            return Ok(true);
        }

        self.0.compare_and_set(
            current,
            RefTriple::new(new, new_mark, new_help),
            contention,
            Some(new_version),
            guard,
        )
    }
}

#[derive(Clone)]
pub struct ListCasDescriptor {
    holder: DoubleMarkRef,
    old_ref: *const Node, // TODO
    new_ref: Node,
    old_mark: bool,
    new_mark: bool,
    old_version: u64,
    state: CasState,
}

impl ListCasDescriptor {
    fn new(
        holder: DoubleMarkRef,
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
        self.holder.compare_and_set(
            unsafe { &*self.old_ref }, // TODO
            self.new_ref,
            self.old_mark,
            self.new_mark,
            false,
            true,
            contention,
            guard,
        )
    }

    fn has_modified_bit(&self) -> bool {
        // TODO
        let guard = &epoch::pin();
        self.holder.0.with(
            |current, version| current.help && self.old_version + 1 == version,
            guard,
        )
    }

    fn clear_bit(&self) -> bool {
        // TODO
        let guard = &epoch::pin();
        match self.holder.compare_and_set(
            &self.new_ref,
            self.new_ref,
            self.new_mark,
            self.new_mark,
            true,
            false,
            &mut ContentionMeasure::new(),
            guard,
        ) {
            Ok(b) => b,
            Err(_) => false,
        }
    }

    fn state(&self) -> CasState {
        self.state
    }

    fn set_state(&self, new: CasState) {
        self.state = new;
    }
}

#[derive(Clone)]
pub struct Node {
    pub key: Option<usize>,
    pub next: DoubleMarkRef,
}

impl Eq for Node {}
impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        let guard = &epoch::pin();
        self.key == other.key && self.next == other.next
    }
}

impl Node {
    pub fn new(key: usize) -> Self {
        Self {
            key: Some(key),
            next: DoubleMarkRef(Atomic::null()),
        }
    }

    pub fn sentinel() -> Self {
        Self {
            key: None,
            next: DoubleMarkRef(Atomic::null()),
        }
    }
}

pub struct LinkedList {
    pub head: DoubleMarkRef,
    pub tail: DoubleMarkRef,
}

impl LinkedList {
    pub fn new() -> Self {
        let tail = DoubleMarkRef::new(Node::sentinel(), false, false);
        let mut head = Node::sentinel();
        head.next = tail.clone();
        let head = DoubleMarkRef::new(head, false, false);
        Self { head, tail }
    }

    pub fn insert<'g>(&self, key: usize, guard: &'g Guard) -> bool {
        match self.insert_impl(key, &mut ContentionMeasure::new(), guard) {
            Ok(b) => b,
            Err(Contention) => false,
        }
    }

    fn insert_impl<'g>(
        &self,
        key: usize,
        contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> Result<bool, Contention> {
        loop {
            // Find nodes that will be before and after the new key
            let (pred, curr) = self.search(Some(key), guard)?;

            // Key already in list
            if curr != self.tail.0.with(|tail, _| &tail.reference, guard) && curr.key == Some(key) {
                return Ok(false);
            }

            let mut new = Node::new(key);
            // new.next = DoubleMarkRef::new(curr, false, false);
            // TODO - change to atomic (?) and return proper values

            match left.next.compare_and_set(
                &right, new, contention, None, // TODO - version?
                guard,
            ) {
                Ok(true) => break Ok(true),
                Err(Contention) => break Err(Contention),
            }
        }
    }

    pub fn delete<'g>(
        &self,
        key: usize,
        contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> Result<bool, Contention> {
        let mut right_ptr;
        let mut right;
        let mut right_next;
        let mut left;

        let tail = self.tail.0.with(|tail, _| tail, guard);

        loop {
            (left, right) = self.search(Some(key), guard)?;
            right_ptr = r_ptr;

            if (right == tail) || (right.key != Some(key)) {
                return Ok(false);
            }

            right_next = right.next.load(Ordering::SeqCst, guard);

            // TODO - change to atomic and return proper values
            if right_next.tag() == 0 {
                if right
                    .next
                    .compare_exchange(
                        right_next,
                        right_next.with_tag(1),
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                        guard,
                    )
                    .is_ok()
                {
                    break;
                }
            }
        }

        if left
            .next
            .compare_exchange(
                right_ptr,
                right_next,
                Ordering::SeqCst,
                Ordering::Relaxed,
                guard,
            )
            .is_err()
        {
            let _ = self.search(right.key, guard);
        }

        Ok(true)
    }

    pub fn find<'g>(&self, key: usize, guard: &'g Guard) -> bool {
        match self.find_impl(key, guard) {
            Ok(b) => b,
            Err(Contention) => false,
        }
    }

    fn find_impl<'g>(&self, key: usize, guard: &'g Guard) -> Result<bool, Contention> {
        let (_, right) = self.search(Some(key), guard)?;

        let tail = self.tail.0.with(|curr, _| curr.reference, guard);
        if (right == tail) || (right.key != Some(key)) {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn search<'g>(
        &self,
        key: Option<usize>,
        guard: &'g Guard,
    ) -> Result<(&'g Node, &'g Node), Contention> {
        let mut contention = ContentionMeasure::new();

        'retry: loop {
            let mut pred = self.head.0.with(|curr, _| &curr.reference, guard);
            let mut curr = pred.next.0.with(|curr, _| &curr.reference, guard);

            loop {
                let (mut succ, mark) = curr
                    .next
                    .0
                    .with(|curr, _| (&curr.reference, curr.mark), guard);
                while mark {
                    let res = pred.next.compare_and_set(
                        &curr,
                        succ.clone(),
                        false,
                        false,
                        false,
                        false,
                        &mut contention,
                        guard,
                    );

                    if res.is_err() || !res.unwrap() {
                        break 'retry;
                    }

                    curr = succ;
                    succ = curr.next.0.with(|curr, _| &curr.reference, guard);
                }

                if curr.key >= key {
                    return Ok((pred, curr));
                }

                pred = curr;
                curr = succ;
            }
        }

        Err(Contention)
    }
}

impl NormalizedLockFree for LinkedList {
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
                let (left_ptr, right_ptr) = self.search(Some(key), guard)?;

                let right = unsafe { right_ptr.deref() };
                let left = unsafe { left_ptr.deref() };

                // Key already in list
                if right != self.tail.0.with(|tail, _| tail, guard) && right.key == Some(key) {
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
                let (left_ptr, right_ptr) = self.search(Some(key), guard)?;

                let right = unsafe { right_ptr.deref() };

                if right == self.tail.0.with(|tail, _| tail, guard) || right.key != Some(key) {
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
                let res = self.find_impl(key, guard)?;
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
            InputOp::Insert(key) => self.insert_impl(key, contention, guard),
            InputOp::Delete(key) => self.delete(key, contention, guard),
            InputOp::Find(key) => self.find_impl(key, guard),
        }
    }
}
