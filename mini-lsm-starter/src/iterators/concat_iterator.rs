use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Ok(SstConcatIterator {
            current: sstables
                .first()
                .map(|t| SsTableIterator::create_and_seek_to_first(t.clone()))
                .transpose()?,
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let idx = sstables
            .binary_search_by(|t| t.first_key().as_key_slice().cmp(&key))
            .unwrap_or_else(|idx| idx.saturating_sub(1));
        let mut iter = SstConcatIterator {
            current: None,
            next_sst_idx: idx,
            sstables,
        };
        iter.update_current(Some(key))?;
        if !iter.is_valid() && !iter.is_last_table() {
            // Move to head of next sstable.
            iter.update_current(None)?;
        }
        Ok(iter)
    }

    fn is_last_table(&self) -> bool {
        self.next_sst_idx == self.sstables.len()
    }

    fn update_current(&mut self, key: Option<KeySlice>) -> Result<()> {
        self.current = self
            .sstables
            .get(self.next_sst_idx)
            .map(|t| match key {
                None => SsTableIterator::create_and_seek_to_first(t.clone()),
                Some(key) => SsTableIterator::create_and_seek_to_key(t.clone(), key),
            })
            .transpose()?;
        self.next_sst_idx += 1;
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if !self.is_valid() {
            panic!("invalid iterator")
        }
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid iterator")
        }
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        match &self.current {
            None => false,
            Some(iter) => iter.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        let iter = self.current.as_mut().unwrap();
        iter.next()?;
        if iter.is_valid() {
            return Ok(());
        }
        if self.is_last_table() {
            return Ok(());
        }

        // Move to the next sstable.
        self.update_current(None)?;

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
