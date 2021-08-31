use bystander::{Atomic, ContentionMeasure};
use crossbeam_epoch::Guard;
use std::sync::atomic::Ordering;

pub(crate) struct Node<T> {
    key: Option<T>,
    next: Atomic<Self>,
}

impl<T> Eq for Node<T> where T: PartialEq + Eq {}
impl<T> PartialEq for Node<T>
where
    T: PartialEq + Eq,
{
    fn eq(&self, other: &Self) -> bool {
        // self.key == other.key || (self.key.is_none() && self.next.get)
        true
    }
}

impl<T> Node<T>
where
    T: PartialEq + Eq,
{
    fn new(key: T) -> Self {
        Self {
            key: Some(key),
            next: Atomic::null(),
        }
    }

    fn sentinel() -> Self {
        Self {
            key: None,
            next: Atomic::null(),
        }
    }
}

pub struct LinkedList<T> {
    head: Atomic<Node<T>>,
    tail: Atomic<Node<T>>,
}

impl<T> LinkedList<T>
where
    T: Copy + Ord + PartialOrd + Eq + PartialEq,
{
    pub fn new() -> Self {
        let tail = Atomic::new(Node::<T>::sentinel());
        let mut head = Node::<T>::sentinel();
        head.next = tail.clone();
        let head = Atomic::new(head);
        Self { head, tail }
    }

    pub fn insert<'g>(
        &self,
        key: T,
        contention: &mut ContentionMeasure,
        guard: &'g Guard,
    ) -> Result<bool, Contention> {
        // It is guaranteed that on returning, either the key is in the list
        loop {
            // Find nodes that will be before and after the new key
            let (left_ptr, right_ptr) = self.search(Some(key), guard);
            let right = unsafe { right_ptr.deref() };
            let left = unsafe { left_ptr.deref() };

            // Key already in list
            if right_ptr != self.tail.load(Ordering::SeqCst, guard) && right.key == Some(key) {
                return Ok(false);
            }

            let mut new = Node::new(key);
            new.next = Atomic::<Node<T>>::from(right_ptr);
            let new_ptr = Owned::new(new);
            // TODO - change to atomic (?) and return proper values
            match left
                .next
                .compare_exchange(
                    right_ptr,
                    new_ptr,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                    guard,
                ) {
                Ok(true) => return Ok(true),
                Err(Contention) => return Err(Contention),
            }
        }
        }
    }

    pub fn delete<'g>(&self, key: T, contention: &mut ContentionMeasure, guard: &'g Guard) -> Result<bool, Contention> {
        let mut right_ptr;
        let mut right;
        let mut right_next;
        let mut left;

        let tail = self.tail.load(Ordering::SeqCst, guard);

        loop {
            let (left_ptr, r_ptr) = self.search(Some(key), guard);
            right_ptr = r_ptr;
            right = unsafe { right_ptr.deref() };
            left = unsafe { left_ptr.deref() };

            if (right_ptr == tail) || (right.key != Some(key)) {
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

    pub fn find<'g>(&self, key: T, guard: &'g Guard) -> Result<bool, _> {
        let (_, right_ptr) = self.search(Some(key), guard);
        let right = unsafe { right_ptr.deref() };

        let tail = self.tail.load(Ordering::SeqCst, guard);
        if (right_ptr == tail) || (right.key != Some(key)) {
            Ok(false)
        } else {
            Ok(true)
        }
    }

    pub(crate) fn search<'g>(
        &self,
        key: Option<T>,
        guard: &'g Guard,
    ) -> (Shared<'g, Node<T>>, Shared<'g, Node<T>>) {
        let mut left_ptr = Shared::null();
        let mut left_next = Shared::null();
        let mut right_ptr;

        loop {
            let mut t_ptr = self.head.load(Ordering::SeqCst, guard);
            let t = unsafe { t_ptr.deref() };
            let mut t_next = t.next.load(Ordering::SeqCst, guard);

            let tail = self.tail.load(Ordering::SeqCst, guard);

            loop {
                if t_next.tag() == 0 {
                    left_ptr = t_ptr;
                    left_next = t_next;
                }

                t_ptr = t_next.with_tag(0);
                if t_ptr == tail {
                    break;
                }

                let t = unsafe { t_ptr.deref() };
                t_next = t.next.load(Ordering::SeqCst, guard);
                if !(t_next.tag() == 1 || t.key < key) {
                    break;
                }
            }

            right_ptr = t_ptr;

            let right = unsafe { right_ptr.deref() };
            if left_next == right_ptr {
                if (right_ptr != tail) && right.next.load(Ordering::SeqCst, guard).tag() == 1 {
                    continue;
                } else {
                    break;
                }
            }

            if let Ok(_) = unsafe { left_ptr.deref() }.next.compare_exchange(
                left_next,
                right_ptr,
                Ordering::SeqCst,
                Ordering::Relaxed,
                guard,
            ) {
                unsafe { guard.defer_destroy(left_next) };

                if (right_ptr != tail) && right.next.load(Ordering::SeqCst, guard).tag() == 1 {
                    continue;
                } else {
                    break;
                }
            }
        }

        (left_ptr, right_ptr)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crossbeam_epoch::{self as epoch};
    use std::{sync::Arc, thread, time::Duration};

    #[test]
    fn new() {
        let _ = LinkedList::<i32>::new();
    }

    #[test]
    fn insert() {
        let list = LinkedList::<i32>::new();
        let guard = &epoch::pin();
        assert!(list.insert(1, guard));
    }

    #[test]
    fn insert_find() {
        let list = LinkedList::<i32>::new();
        let guard = &epoch::pin();
        assert!(list.insert(1, guard));
        assert!(list.find(1, guard));
    }

    #[test]
    fn insert_find_remove() {
        let list = LinkedList::<i32>::new();
        let guard = &epoch::pin();
        assert!(list.insert(1, guard));
        assert!(list.find(1, guard));
        assert!(list.delete(1, guard));
        assert!(!list.find(1, guard));
    }

    #[test]
    fn remove_empty() {
        let list = LinkedList::<i32>::new();
        let guard = &epoch::pin();
        assert!(!list.delete(1, guard));
    }

    #[test]
    fn insert_concurrent() {
        let list = Arc::new(LinkedList::<i32>::new());
        let mut handles = vec![];

        for id in 0..1000 {
            let list = list.clone();
            handles.push(thread::spawn(move || {
                let guard = &epoch::pin();
                assert!(list.insert(id, guard));
                assert!(list.find(id, guard));
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let guard = &epoch::pin();
        for id in 0..1000 {
            assert!(list.delete(id, guard));
        }
    }

    #[test]
    fn delete_concurrent() {
        let list = Arc::new(LinkedList::<i32>::new());
        let mut handles = vec![];

        let guard = &epoch::pin();
        for id in 0..1000 {
            assert!(list.insert(id, guard));
        }
        drop(guard);

        for id in 0..1000 {
            let list = list.clone();
            handles.push(thread::spawn(move || {
                let guard = &epoch::pin();
                assert!(list.find(id, guard));
                thread::yield_now();
                assert!(list.delete(id, guard));
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn two_threads() {
        let list = Arc::new(LinkedList::<i32>::new());
        let mut handles = vec![];

        let guard = &epoch::pin();
        for id in 0..10000 {
            assert!(list.insert(id, guard));
        }
        drop(guard);

        for _ in 0..2 {
            let list = list.clone();
            handles.push(thread::spawn(move || {
                let mut counter = 0;
                for key in 0..10000 {
                    if key % 500 == 0 {
                        thread::sleep(Duration::from_millis(50));
                    }
                    let guard = &epoch::pin();
                    if list.delete(key, guard) {
                        counter += 1;
                    }
                }
                print!("counter: {}\n", counter);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let guard = &epoch::pin();
        for val in 0..10000 {
            assert!(!list.find(val, guard))
        }
    }
}
