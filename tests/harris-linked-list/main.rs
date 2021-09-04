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

#[derive(Clone, PartialEq, Eq, Debug)]
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

#[derive(Clone, PartialEq, Eq, Debug)]
struct DoubleMarkRef(Atomic<DoubleMarkNode>);

impl DoubleMarkRef {
    fn new(node: Node, mark: bool, help: bool) -> Self {
        Self(Atomic::<DoubleMarkNode>::new(DoubleMarkNode::new(
            node, mark, help,
        )))
    }

    fn get_version(&self, guard: &Guard) -> u64 {
        self.0.with(|_, version| version, guard).unwrap()
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
        let (current, version) = self
            .0
            .with(|current, version| (current, version), guard)
            .unwrap();

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
            Some(version),
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
        self.holder
            .0
            .with(
                |current, version| current.help && self.old_version + 1 == version,
                guard,
            )
            .unwrap()
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

#[derive(Clone, Debug)]
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
            if self
                .tail
                .0
                .with(|tail, _| curr.node != tail.node, guard)
                .unwrap()
                && curr.key == Some(key)
            {
                return Ok(false);
            }

            let mut new = Node::new(key);
            new.next = DoubleMarkRef(unsafe { Atomic::from_raw(curr as *const _) });

            match pred
                .next
                .compare_and_set(curr, new, false, false, false, false, contention, guard)
            {
                Ok(true) => break Ok(true),
                Ok(false) => {
                    continue;
                }
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

            if self
                .tail
                .0
                .with(|tail, _| curr.node == tail.node, guard)
                .unwrap()
                || curr.key != Some(key)
            {
                return Ok(false);
            }

            let succ = curr
                .next
                .0
                .with(|curr, _| &curr.node, guard)
                .expect("curr is not that tail, so we have a next");

            if !curr.next.compare_and_set(
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

        if self
            .tail
            .0
            .with(|tail, _| curr.node == tail.node, guard)
            .unwrap()
            || curr.key != Some(key)
        {
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
            let mut pred = self
                .head
                .0
                .with(|curr, _| curr, guard)
                .expect("head is not null");
            let mut curr = pred
                .node
                .next
                .0
                .with(|curr, _| curr, guard)
                .expect("head's next is not null");

            loop {
                let mut succ = if let Some(succ) = curr.next.0.with(|curr, _| curr, guard) {
                    succ
                } else {
                    // reached end of list
                    return Ok((pred, curr));
                };

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
                    succ = if let Some(succ) = curr.next.0.with(|curr, _| curr, guard) {
                        succ
                    } else {
                        // reached end of list
                        return Ok((pred, curr));
                    };
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

impl Drop for LinkedList {
    fn drop(&mut self) {
        let guard = &epoch::pin();

        let mut curr = self.head.clone();
        let mut next = curr
            .0
            .with(|curr, _| curr.node.clone(), guard)
            .unwrap()
            .next;

        loop {
            unsafe { curr.0.drop() };
            curr = next;
            next = if let Some(curr) = curr.0.with(|curr, _| curr.node.clone(), guard) {
                curr.next
            } else {
                break;
            };
        }

        // unsafe { self.tail.0.clone().drop() };
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
                if self
                    .tail
                    .0
                    .with(|tail, _| curr.node != tail.node, guard)
                    .unwrap()
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

                if self
                    .tail
                    .0
                    .with(|tail, _| tail.node == curr.node, guard)
                    .unwrap()
                    || curr.key != Some(key)
                {
                    return Ok(None);
                }

                Ok(Some(ListCasDescriptor::new(
                    curr.next.clone(),
                    curr.next.0.as_raw() as *const _,
                    curr.next
                        .0
                        .with(|next, _| next.node.clone(), guard)
                        .expect("curr is not the tail"),
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
fn fast_path() {
    let list = WaitFreeLinkedList::<1>::new();

    // no elements yet
    assert!(!list.delete(1));

    // insert `1`
    assert!(list.insert(1));
    assert!(list.find(1));
    // can't insert `1` twice
    assert!(!list.insert(1));
    assert!(list.find(1));

    // insert `2`
    assert!(list.insert(2));
    assert!(list.find(2));

    // remove `1`
    assert!(list.delete(1));
    assert!(!list.find(1));

    // remove `2`
    assert!(list.delete(2));
    assert!(!list.find(2));
}

#[test]
fn dropping_one_element_list() {
    let list = WaitFreeLinkedList::<1>::new();

    assert!(list.insert(0));
    assert!(list.find(0));
}

#[test]
#[ignore]
fn dropping_many_element_list() {
    let list = WaitFreeLinkedList::<1>::new();

    for i in 0..127 {
        assert!(list.insert(i));
        assert!(list.find(i));
    }
}

use std::sync::Arc;
use std::thread;

#[test]
#[ignore]
fn concurrent_delete() {
    let list = Arc::new(WaitFreeLinkedList::<2>::new());

    for val in 0..100 {
        list.insert(val);
    }

    let mut handles = vec![];
    for _ in 0..2 {
        let list = list.clone();
        handles.push(thread::spawn(move || {
            let mut counter = 0;
            for val in 0..100 {
                if list.delete(val) {
                    counter += 1;
                }
            }
            println!("counter: {}", counter);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
#[ignore]
fn concurrent_insert() {
    const N: usize = 50;
    let list = Arc::new(WaitFreeLinkedList::<N>::new());

    let mut handles = vec![];
    for _ in 0..N {
        let list = list.clone();
        handles.push(thread::spawn(move || {
            let mut counter = 0;
            for val in 0..1500 {
                if list.insert(val) {
                    counter += 1;
                }
            }
            println!("counter: {}", counter);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
