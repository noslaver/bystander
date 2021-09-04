use crossbeam_epoch::{self as epoch, Guard};
use std::sync::atomic::{AtomicU8, Ordering};

use bystander::{
    Atomic, CasState, Contention, ContentionMeasure, NormalizedLockFree, VersionedCas,
    WaitFreeSimulator,
};

// in a consuming crate (wait-free-linked-list crate)
pub struct WaitFreeLinkedList<const N: usize> {
    sim: WaitFreeSimulator<LinkedList, N>,
}

impl<const N: usize> WaitFreeLinkedList<N> {
    fn new() -> Self {
        Self {
            sim: WaitFreeSimulator::new(LinkedList::new()),
        }
    }

    pub fn insert(&self, key: usize) -> bool {
        let guard = &epoch::pin();
        self.sim.run(InputOp::Insert(key), guard)
    }

    pub fn delete(&self, key: usize) -> bool {
        let guard = &epoch::pin();
        self.sim.run(InputOp::Delete(key), guard)
    }

    pub fn find(&self, key: usize) -> bool {
        let guard = &epoch::pin();
        self.sim.run(InputOp::Find(key), guard)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd)]
pub enum InputOp {
    Insert(usize),
    Delete(usize),
    Find(usize),
}

#[derive(Clone, PartialEq, Eq)]
struct DoubleMarkNode {
    node: Node,
    mark: bool,
    help: bool,
}

impl DoubleMarkNode {
    fn new(node: Node, mark: bool, help: bool) -> Self {
        Self { node, mark, help }
    }
}

impl std::ops::Deref for DoubleMarkNode {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.node
    }
}

#[derive(Clone, PartialEq, Eq)]
// #[repr(transparent)]
struct DoubleMarkRef(Atomic<DoubleMarkNode>);

impl DoubleMarkRef {
    fn new(node: Node, mark: bool, help: bool) -> Self {
        Self(Atomic::<DoubleMarkNode>::new(DoubleMarkNode::new(
            node, mark, help,
        )))
    }

    fn get_version(&self, guard: &Guard) -> u64 {
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

        if !(expected == &current.node
            && expected_mark == current.mark
            && expected_help == current.help)
        {
            return Ok(false);
        }

        if new == current.node && new_mark == current.mark && new_help == current.help {
            return Ok(true);
        }

        self.0.compare_and_set(
            current,
            DoubleMarkNode::new(new, new_mark, new_help),
            contention,
            Some(new_version),
            guard,
        )
    }
}

pub struct ListCasDescriptor {
    holder: DoubleMarkRef,
    old_ref: *const Node, // TODO
    new_ref: Node,
    old_mark: bool,
    new_mark: bool,
    old_version: u64,
    state: AtomicU8, // This is actually CasState
}

impl Clone for ListCasDescriptor {
    fn clone(&self) -> Self {
        Self {
            holder: self.holder.clone(),
            old_ref: self.old_ref,
            new_ref: self.new_ref.clone(),
            old_mark: self.old_mark,
            new_mark: self.new_mark,
            old_version: self.old_version,
            state: self.state.load(Ordering::SeqCst).into(),
        }
    }
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
            state: AtomicU8::new(CasState::Pending as u8),
        }
    }
}

impl VersionedCas for ListCasDescriptor {
    fn execute(
        &self,
        contention: &mut ContentionMeasure,
        guard: &Guard,
    ) -> Result<bool, Contention> {
        self.holder.compare_and_set(
            unsafe { &*self.old_ref }, // TODO
            self.new_ref.clone(),
            self.old_mark,
            self.new_mark,
            false,
            true,
            contention,
            guard,
        )
    }

    fn has_modified_bit(&self, guard: &Guard) -> bool {
        self.holder.0.with(
            |current, version| current.help && self.old_version + 1 == version,
            guard,
        )
    }

    fn clear_bit(&self, guard: &Guard) -> bool {
        self.holder
            .compare_and_set(
                &self.new_ref,
                self.new_ref.clone(),
                self.new_mark,
                self.new_mark,
                true,
                false,
                &mut ContentionMeasure::new(),
                guard,
            )
            .unwrap_or(false)
    }

    fn state(&self) -> CasState {
        match self.state.load(Ordering::SeqCst) {
            0 => CasState::Success,
            1 => CasState::Failure,
            2 => CasState::Pending,
            _ => unreachable!(),
        }
    }

    fn set_state(&self, new: CasState) {
        self.state.store(new as u8, Ordering::SeqCst);
    }
}

#[derive(Clone)]
pub struct Node {
    key: Option<usize>,
    next: DoubleMarkRef,
}

impl Eq for Node {}
impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
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
    head: DoubleMarkRef,
    tail: DoubleMarkRef,
}

impl LinkedList {
    pub fn new() -> Self {
        let tail = DoubleMarkRef::new(Node::sentinel(), false, false);
        let mut head = Node::sentinel();
        head.next = tail.clone();
        let head = DoubleMarkRef::new(head, false, false);
        Self { head, tail }
    }

    pub fn insert(&self, key: usize, guard: &Guard) -> bool {
        match self.insert_impl(key, &mut ContentionMeasure::new(), guard) {
            Ok(b) => b,
            Err(Contention) => false,
        }
    }

    fn insert_impl(
        &self,
        key: usize,
        contention: &mut ContentionMeasure,
        guard: &Guard,
    ) -> Result<bool, Contention> {
        loop {
            // Find nodes that will be before and after the new key
            let (pred, curr) = self.search(Some(key), guard)?;

            // Key already in list
            if self.tail.0.with(|tail, _| curr.node != tail.node, guard) && curr.key == Some(key) {
                return Ok(false);
            }

            let mut new = Node::new(key);
            new.next = DoubleMarkRef(unsafe { Atomic::from_raw(curr as *const _) });

            match pred
                .next
                .compare_and_set(curr, new, false, false, false, false, contention, guard)
            {
                Ok(true) => break Ok(true),
                Ok(false) => continue,
                Err(Contention) => break Err(Contention),
            }
        }
    }

    pub fn delete(&self, key: usize, guard: &Guard) -> bool {
        match self.delete_impl(key, &mut ContentionMeasure::new(), guard) {
            Ok(b) => b,
            Err(Contention) => false,
        }
    }

    fn delete_impl(
        &self,
        key: usize,
        contention: &mut ContentionMeasure,
        guard: &Guard,
    ) -> Result<bool, Contention> {
        'retry: loop {
            let (pred, curr) = self.search(Some(key), guard)?;

            if self.tail.0.with(|tail, _| curr.node == tail.node, guard) || curr.key != Some(key) {
                return Ok(false);
            }

            let succ = curr.next.0.with(|curr, _| &curr.node, guard);

            if curr.next.compare_and_set(
                succ,
                succ.clone(),
                false,
                true,
                false,
                false,
                contention,
                guard,
            )? {
                continue 'retry;
            }

            // success of failure, node was deleted.
            let _ = pred.next.compare_and_set(
                curr,
                succ.clone(),
                false,
                false,
                false,
                false,
                contention,
                guard,
            );

            return Ok(true);
        }
    }

    pub fn find(&self, key: usize, guard: &Guard) -> bool {
        match self.find_impl(key, guard) {
            Ok(b) => b,
            Err(Contention) => false,
        }
    }

    fn find_impl(&self, key: usize, guard: &Guard) -> Result<bool, Contention> {
        let (_, curr) = self.search(Some(key), guard)?;

        if self.tail.0.with(|tail, _| curr.node == tail.node, guard) || curr.key != Some(key) {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn search<'g>(
        &self,
        key: Option<usize>,
        guard: &'g Guard,
    ) -> Result<(&'g DoubleMarkNode, &'g DoubleMarkNode), Contention> {
        let mut contention = ContentionMeasure::new();

        'retry: loop {
            let mut pred = self.head.0.with(|curr, _| curr, guard);
            let mut curr = pred.node.next.0.with(|curr, _| curr, guard);

            loop {
                let mut succ = curr.next.0.with(|curr, _| curr, guard);
                while succ.mark {
                    let res = pred.node.next.compare_and_set(
                        &curr.node,
                        succ.node.clone(),
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
                    succ = curr.node.next.0.with(|curr, _| curr, guard);
                }

                if curr.node.key >= key {
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

    type CommitDescriptor = ListCasDescriptor;

    fn generator<'g>(
        &self,
        op: &Self::Input,
        _contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> Result<Option<Self::CommitDescriptor>, Contention> {
        match *op {
            InputOp::Insert(key) => {
                let (pred, curr) = self.search(Some(key), guard)?;

                // Key already in list
                if self.tail.0.with(|tail, _| curr.node != tail.node, guard)
                    && curr.key == Some(key)
                {
                    return Ok(None);
                }

                let mut new = Node::new(key);
                new.next = DoubleMarkRef(unsafe { Atomic::from_raw(curr as *const _) });

                Ok(Some(ListCasDescriptor::new(
                    pred.next.clone(),
                    curr as *const _ as *const _,
                    new,
                    false,
                    false,
                    pred.next.get_version(guard),
                )))
            }
            InputOp::Delete(key) => {
                let (_pred, curr) = self.search(Some(key), guard)?;

                if self.tail.0.with(|tail, _| tail.node == curr.node, guard)
                    || curr.key != Some(key)
                {
                    return Ok(None);
                }

                Ok(Some(ListCasDescriptor::new(
                    curr.next.clone(),
                    curr.next.0.as_raw() as *const _,
                    curr.next.0.with(|curr, _| curr.node.clone(), guard),
                    false,
                    true,
                    curr.next.get_version(guard),
                )))
            }
            InputOp::Find(_key) => Ok(None),
        }
    }

    fn wrap_up<'g>(
        &self,
        op: &Self::Input,
        executed: Result<(), usize>,
        performed: &Option<Self::CommitDescriptor>,
        _contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> Result<Option<Self::Output>, Contention> {
        match *op {
            InputOp::Delete(_key) | InputOp::Insert(_key) => {
                if performed.is_none() {
                    // Operation failed
                    return Ok(Some(false));
                }
                match executed {
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
            InputOp::Delete(key) => self.delete_impl(key, contention, guard),
            InputOp::Find(key) => self.find_impl(key, guard),
        }
    }
}

#[test]
fn nothing_works() {
    let linked_list = WaitFreeLinkedList::<1>::new();

    linked_list.insert(1);
}
