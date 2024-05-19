use anyhow::{bail, Result};
use bytes::Bytes;
use std::collections::Bound;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeyBytes;
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator.
pub type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<KeyBytes>,
    prev_key: Bytes,
    read_ts: u64,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: Bound<KeyBytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            prev_key: Bytes::default(),
            read_ts,
            end_bound,
        };
        if iter.is_valid() {
            iter.move_to_valid_ts()?;

            if iter.is_valid() {
                iter.update_prev_key_with_current_key();
                if iter.value().is_empty() {
                    iter.next()?;
                }
            }
        }
        Ok(iter)
    }

    fn update_prev_key_with_current_key(&mut self) {
        self.prev_key = Bytes::copy_from_slice(self.key());
    }

    fn move_to_valid_ts(&mut self) -> Result<()> {
        loop {
            if !self.is_valid() {
                return Ok(());
            }
            if self.read_ts >= self.inner.key().ts() {
                return Ok(());
            }
            self.inner.next()?;
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn key(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Invalid state")
        }
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Invalid state")
        }
        self.inner.value()
    }

    fn is_valid(&self) -> bool {
        self.is_valid && self.inner.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        // valid <--> key != prev_key && key's ts <= read_ts && value not empty
        loop {
            self.inner.next()?;
            self.move_to_valid_ts()?;
            if !self.is_valid() {
                return Ok(());
            }
            if self.inner.key().key_ref() != self.prev_key {
                // update prev_key regardless of being empty or not.
                self.update_prev_key_with_current_key();
                if !self.value().is_empty() {
                    break;
                }
            }
        }
        match &self.end_bound {
            Bound::Included(end_bound) => {
                self.is_valid = self.inner.key().key_ref() <= end_bound.key_ref()
            }
            Bound::Excluded(end_bound) => {
                self.is_valid = self.inner.key().key_ref() < end_bound.key_ref()
            }
            Bound::Unbounded => {}
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("invalid state")
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid state")
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("this iterator has error")
        }
        if self.is_valid() {
            if let e @ Err(_) = self.iter.next() {
                self.has_errored = true;
                return e;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
