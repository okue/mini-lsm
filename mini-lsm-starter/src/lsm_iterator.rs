use anyhow::{bail, Result};

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner) -> Result<Self> {
        let mut iter = Self { inner: iter };
        if iter.value().is_empty() {
            iter.next()?;
        }
        Ok(iter)
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn key(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Invalid state")
        }
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Invalid state")
        }
        self.inner.value()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }

        loop {
            if let e @ Err(_) = self.inner.next() {
                return e;
            }

            if !self.is_valid() {
                return Ok(());
            }

            if !self.value().is_empty() {
                return Ok(());
            }
            // empty value => As this key's data is deleted, call next() one more time.
        }
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
}
