mod lib;
use lib::LinkedList as LockFreeLinkedList;

use bystander::{
    CasState, Contention, ContentionMeasure, NormalizedLockFree, VersionedCas, WaitFreeSimulator,
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
}

#[derive(Clone)]
pub struct CasDescriptor;

impl VersionedCas for CasDescriptor {
    fn execute(&self, _contention: &mut ContentionMeasure) -> Result<bool, Contention> {
        todo!()
    }

    fn has_modified_bit(&self) -> bool {
        todo!()
    }

    fn clear_bit(&self) -> bool {
        todo!()
    }

    fn state(&self) -> CasState {
        todo!()
    }

    fn set_state(&self, _new: CasState) {
        todo!()
    }
}

impl<T: Clone> NormalizedLockFree for LockFreeLinkedList<T> {
    type Input = InputOp<T>;
    type Output = bool;

    type CommitDescriptor = CasDescriptor;

    fn generator(
        &self,
        _op: &Self::Input,
        _contention: &mut ContentionMeasure,
    ) -> Result<Self::CommitDescriptor, Contention> {
        todo!()
    }

    fn wrap_up(
        &self,
        _executed: Result<(), usize>,
        _performed: &Self::CommitDescriptor,
        _contention: &mut ContentionMeasure,
    ) -> Result<Option<Self::Output>, Contention> {
        todo!()
    }

    fn fast_path(
        &self,
        _op: &Self::Input,
        _contention: &mut ContentionMeasure,
    ) -> Result<Self::Output, Contention> {
        todo!()
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
