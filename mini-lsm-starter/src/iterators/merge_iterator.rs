use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;
use bytes::Bytes;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (index, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(index, iter));
            }
        }
        let min = heap.pop();
        MergeIterator {
            iters: heap,
            current: min,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        let iter = self.current.as_ref().unwrap().1.as_ref();
        iter.key()
    }

    fn show_key(&self) -> Bytes {
        Bytes::copy_from_slice(self.key().inner())
    }

    fn value(&self) -> &[u8] {
        let iter = self.current.as_ref().unwrap().1.as_ref();
        iter.value()
    }

    fn is_valid(&self) -> bool {
        if let Some(ref iter) = self.current {
            iter.1.is_valid()
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        // Pop the item out of the heap if they have the same value.
        while let Some(mut min_in_heap) = self.iters.peek_mut() {
            if min_in_heap.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = min_in_heap.1.next() {
                    PeekMut::pop(min_in_heap);
                    return e;
                }
                // Case 2: iter is no longer valid.
                if !min_in_heap.1.is_valid() {
                    PeekMut::pop(min_in_heap);
                }
            } else {
                break;
            }
        }

        current.1.next()?;
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
            return Ok(());
        }
        if let Some(mut min_in_heap) = self.iters.peek_mut() {
            if *current < *min_in_heap {
                std::mem::swap(&mut *min_in_heap, current);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let mut num = 0;
        if let Some(iter) = &self.current {
            num += iter.1.num_active_iterators();
        }
        for iter in &self.iters {
            num += iter.1.num_active_iterators();
        }
        num
    }
}
