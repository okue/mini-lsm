#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    current_is_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    fn next_is_a(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            false
        } else if !b.is_valid() {
            true
        } else {
            a.key() <= b.key()
        }
    }
    
    fn skip_b(a: &A, b: &mut B) -> Result<()> {
        if a.is_valid() && b.is_valid() && a.key() == b.key() {
            b.next()?;
        }
        Ok(())
    }

    pub fn create(a: A, mut b: B) -> Result<Self> {
        let current_is_a = Self::next_is_a(&a, &b);
        Self::skip_b(&a, &mut b)?;
        Ok(Self { a, b, current_is_a })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.current_is_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.current_is_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.current_is_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.current_is_a {
            self.a.next()?
        } else {
            self.b.next()?
        }

        self.current_is_a = Self::next_is_a(&self.a, &self.b);
        Self::skip_b(&self.a, &mut self.b)?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
