mod help_queue;
use help_queue::WaitFreeHelpQueue;

use crossbeam_epoch::{
    Atomic as EpochAtomic, CompareExchangeError, Guard, Owned, Shared as EpochShared,
};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

const CONTENTION_THRESHOLD: usize = 2;
const RETRY_THRESHOLD: usize = 2;

pub struct Contention;

// in bystander
pub struct ContentionMeasure(usize);
impl ContentionMeasure {
    pub fn detected(&mut self) -> Result<(), Contention> {
        self.0 += 1;
        if self.0 < CONTENTION_THRESHOLD {
            Ok(())
        } else {
            Err(Contention)
        }
    }

    pub fn use_slow_path(&self) -> bool {
        self.0 > CONTENTION_THRESHOLD
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CasState {
    Success,
    Failure,
    Pending,
}

struct CasByRcu<T> {
    version: u64,

    /// The value that will actually be CASed.
    value: T,
}

pub struct Atomic<T>(EpochAtomic<CasByRcu<T>>);

pub trait VersionedCas {
    fn execute(&self, contention: &mut ContentionMeasure) -> Result<bool, Contention>;
    fn has_modified_bit(&self) -> bool;
    fn clear_bit(&self) -> bool;
    fn state(&self) -> CasState;
    fn set_state(&self, new: CasState);
}

impl<T> Atomic<T>
where
    T: PartialEq + Eq,
{
    pub fn new(initial: T) -> Self {
        Self(EpochAtomic::new(CasByRcu {
            version: 0,
            value: initial,
        }))
    }

    fn get<'g>(&self, guard: &'g Guard) -> EpochShared<'g, CasByRcu<T>> {
        self.0.load(Ordering::SeqCst, guard)
    }

    pub fn with<'g, F, R>(&self, f: F, guard: &'g Guard) -> R
    where
        F: FnOnce(&T, u64) -> R,
    {
        // Safety: We always point to a valid memory.
        let this = unsafe { self.get(guard).deref() };
        f(&this.value, this.version)
    }

    pub fn set<'g>(&self, value: T, guard: &'g Guard) {
        let this_ptr = self.get(guard);
        // Safety: We always point to a valid memory.
        let this = unsafe { this_ptr.deref() };
        if this.value != value {
            self.0.store(
                Owned::new(CasByRcu {
                    version: this.version + 1,
                    value,
                }),
                Ordering::SeqCst,
            );

            // Safety: we replaced `this_ptr` with a new value, can no longer be reached.
            unsafe { guard.defer_destroy(this_ptr) };
        }
    }

    pub fn compare_and_set<'g>(
        &self,
        expected: &T,
        value: T,
        contention: &mut ContentionMeasure,
        version: Option<u64>,
        guard: &'g Guard,
    ) -> Result<bool, Contention> {
        let this_ptr = self.get(guard);
        // Safety: We always point to a valid memory.
        let this = unsafe { this_ptr.deref() };
        if &this.value == expected {
            if let Some(v) = version {
                if v != this.version {
                    contention.detected()?;
                    return Ok(false);
                }
            }

            if expected == &value {
                Ok(true)
            } else {
                let new_ptr = Owned::new(CasByRcu {
                    version: this.version + 1,
                    value,
                });
                match self.0.compare_exchange(
                    this_ptr,
                    new_ptr,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    guard,
                ) {
                    Ok(_) => {
                        // Safety: `this_ptr` was CASed and can no longer be read.
                        unsafe { guard.defer_destroy(this_ptr) };

                        Ok(true)
                    }
                    Err(CompareExchangeError { new, .. }) => {
                        // Safety: new was never shared.
                        drop(new);
                        contention.detected()?;
                        Ok(false)
                    }
                }
            }
        } else {
            Ok(false)
        }
    }
}

pub trait NormalizedLockFree {
    type Input: Clone;
    type Output: Clone;
    type CommitDescriptor: Clone;

    fn generator(
        &self,
        op: &Self::Input,
        contention: &mut ContentionMeasure,
    ) -> Result<Self::CommitDescriptor, Contention>;
    fn wrap_up(
        &self,
        executed: Result<(), usize>,
        performed: &Self::CommitDescriptor,
        contention: &mut ContentionMeasure,
    ) -> Result<Option<Self::Output>, Contention>;

    fn fast_path(
        &self,
        op: &Self::Input,
        contention: &mut ContentionMeasure,
    ) -> Result<Self::Output, Contention>;
}

struct OperationRecordBox<LF: NormalizedLockFree> {
    val: EpochAtomic<OperationRecord<LF>>,
}

enum OperationState<LF: NormalizedLockFree> {
    PreCas,
    ExecuteCas(LF::CommitDescriptor),
    PostCas(LF::CommitDescriptor, Result<(), usize>),
    Completed(LF::Output),
}

struct OperationRecord<LF: NormalizedLockFree> {
    owner: std::thread::ThreadId,
    input: LF::Input,
    state: OperationState<LF>,
}

impl<LF: NormalizedLockFree> OperationRecord<LF> {
    fn new(input: LF::Input) -> Self {
        Self {
            owner: std::thread::current().id(),
            input,
            state: OperationState::PreCas,
        }
    }
}

type HelpQueue<LF, const N: usize> = WaitFreeHelpQueue<*const OperationRecordBox<LF>, N>;

struct Shared<LF: NormalizedLockFree, const N: usize> {
    algorithm: LF,
    help: HelpQueue<LF, N>,
    free_ids: Mutex<Vec<usize>>,
}

pub struct WaitFreeSimulator<LF: NormalizedLockFree, const N: usize> {
    shared: Arc<Shared<LF, N>>,
    id: usize,
}

pub struct TooManyHandles;
impl<LF: NormalizedLockFree, const N: usize> WaitFreeSimulator<LF, N> {
    pub fn new(algorithm: LF) -> Self {
        assert_ne!(N, 0);
        Self {
            shared: Arc::new(Shared {
                algorithm,
                help: HelpQueue::new(),
                // NOTE: The Self we return has already claimed 0, therefore 1..
                free_ids: Mutex::new((1..N).collect()),
            }),
            id: 0,
        }
    }

    pub fn fork(&self) -> Result<Self, TooManyHandles> {
        if let Some(id) = self.shared.free_ids.lock().unwrap().pop() {
            Ok(Self {
                shared: Arc::clone(&self.shared),
                id,
            })
        } else {
            Err(TooManyHandles)
        }
    }
}

impl<LF: NormalizedLockFree, const N: usize> Drop for WaitFreeSimulator<LF, N> {
    fn drop(&mut self) {
        self.shared.free_ids.lock().unwrap().push(self.id);
    }
}

enum CasExecuteFailure {
    CasFailed(usize),
    Contention,
}

impl From<Contention> for CasExecuteFailure {
    fn from(_: Contention) -> Self {
        Self::Contention
    }
}

impl<LF: NormalizedLockFree, const N: usize> WaitFreeSimulator<LF, N>
where
    for<'a> &'a LF::CommitDescriptor: IntoIterator<Item = &'a dyn VersionedCas>,
{
    fn cas_execute(
        &self,
        descriptors: &LF::CommitDescriptor,
        contention: &mut ContentionMeasure,
    ) -> Result<(), CasExecuteFailure> {
        for (i, cas) in descriptors.into_iter().enumerate() {
            match cas.state() {
                CasState::Success => {
                    cas.clear_bit();
                }
                CasState::Failure => {
                    return Err(CasExecuteFailure::CasFailed(i));
                }
                CasState::Pending => {
                    cas.execute(contention)?;
                    if cas.has_modified_bit() {
                        // XXX: Paper and code diverge here.
                        cas.set_state(CasState::Success);
                        cas.clear_bit();
                    }
                    if cas.state() != CasState::Success {
                        cas.set_state(CasState::Failure);
                        return Err(CasExecuteFailure::CasFailed(i));
                    }
                }
            }
        }
        Ok(())
    }

    // Guarantees that on return, orb is no longer in help queue.
    fn help_op<'g>(&self, orb: &OperationRecordBox<LF>, guard: &'g Guard) {
        loop {
            let or_ptr = orb.val.load(Ordering::SeqCst, guard);
            // Safety: An `OperationRecordBox` is always initialized with valid memory.
            let or = unsafe { or_ptr.deref() };
            let updated_or = match &or.state {
                OperationState::Completed(..) => {
                    let _ = self.shared.help.try_remove_front(orb, guard);
                    return;
                }
                OperationState::PreCas => {
                    let cas_list = match self
                        .shared
                        .algorithm
                        .generator(&or.input, &mut ContentionMeasure(0))
                    {
                        Ok(cas_list) => cas_list,
                        Err(Contention) => continue,
                    };
                    OperationRecord {
                        owner: or.owner.clone(),
                        input: or.input.clone(),
                        state: OperationState::ExecuteCas(cas_list),
                    }
                }
                OperationState::ExecuteCas(cas_list) => {
                    let outcome = match self.cas_execute(cas_list, &mut ContentionMeasure(0)) {
                        Ok(outcome) => Ok(outcome),
                        Err(CasExecuteFailure::CasFailed(i)) => Err(i),
                        Err(CasExecuteFailure::Contention) => continue,
                    };
                    OperationRecord {
                        owner: or.owner.clone(),
                        input: or.input.clone(),
                        state: OperationState::PostCas(cas_list.clone(), outcome),
                    }
                }
                OperationState::PostCas(cas_list, outcome) => {
                    match self.shared.algorithm.wrap_up(
                        *outcome,
                        cas_list,
                        &mut ContentionMeasure(0),
                    ) {
                        Ok(Some(result)) => OperationRecord {
                            owner: or.owner.clone(),
                            input: or.input.clone(),
                            state: OperationState::Completed(result),
                        },
                        Ok(None) => {
                            // We need to re-start from the generator.
                            OperationRecord {
                                owner: or.owner.clone(),
                                input: or.input.clone(),
                                state: OperationState::PreCas,
                            }
                        }
                        Err(Contention) => {
                            // Not up to us to re-start.
                            continue;
                        }
                    }
                }
            };
            let updated_or = Owned::new(updated_or);

            match orb.val.compare_exchange_weak(
                or_ptr,
                updated_or,
                Ordering::SeqCst,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(_) => {
                    // Safety: `or_ptr` was CASed and can no longer be read.
                    unsafe { guard.defer_destroy(or_ptr) };
                }
                Err(CompareExchangeError { new, .. }) => {
                    // Never got shared, so safe to drop.
                    drop(new);
                }
            }
        }
    }

    fn help_first<'g>(&self, guard: &'g Guard) {
        if let Some(help) = self.shared.help.peek(&guard) {
            // Safety: The operation still exists in the queue, which means it hasn't been
            // completed yet, and thereby wasn't dropped.
            // TODO - is it though??
            let help = unsafe { &*help };
            self.help_op(help, guard);
        }
    }

    pub fn run<'g>(&self, op: LF::Input, guard: &'g Guard) -> LF::Output {
        let help = /* once in a while */ true;
        if help {
            self.help_first(guard);
        }

        // fast path
        for retry in 0.. {
            let mut contention = ContentionMeasure(0);
            match self.shared.algorithm.fast_path(&op, &mut contention) {
                Ok(result) => return result,
                Err(Contention) => {}
            }

            if retry > RETRY_THRESHOLD {
                break;
            }
        }

        // slow path: ask for help.
        let orb = OperationRecordBox {
            val: EpochAtomic::new(OperationRecord::new(op)),
        };
        self.shared.help.enqueue(self.id, &orb, guard);
        loop {
            // Safety: orb.val points to valid memory, and we break after we destroy him.
            let or_ptr = orb.val.load(Ordering::SeqCst, guard);
            let or = unsafe { or_ptr.deref() };
            if let OperationState::Completed(t) = &or.state {
                // Safety: When the operation is completed, it is removed from the queue and can no
                // longer be accessed by other threads.
                unsafe { guard.defer_destroy(or_ptr) };

                break t.clone();
            } else {
                self.help_first(guard);
            }
        }
    }
}

/*
// in a consuming crate (wait-free-linked-list crate)
pub struct WaitFreeLinkedList<T> {
    simulator: WaitFreeSimulator<LockFreeLinkedList<T>>,
}

struct LockFreeLinkedList<T> {
    t: T,
}

// impl<T> NormalizedLockFree for LockFreeLinkedList<T> {}

impl<T> WaitFreeLinkedList<T> {
    pub fn push_front(&self, t: T) {
        // self.simulator.run(Insert(t))
    }
}
*/
