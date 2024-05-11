use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

use super::SsTable;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        Ok(Self {
            blk_iter: Self::seek_to_first_inner(&table)?,
            blk_idx: 0,
            table,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        if self.blk_idx == 0 {
            self.blk_iter.seek_to_first();
        } else {
            self.blk_iter = Self::seek_to_first_inner(&self.table)?;
            self.blk_idx = 0;
        }
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&table, key)?;
        Ok(Self {
            blk_iter,
            blk_idx,
            table,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    fn seek_to_first_inner(table: &SsTable) -> Result<BlockIterator> {
        let block = table.read_block_cached(0)?;
        Ok(BlockIterator::create_and_seek_to_first(block))
    }

    fn seek_to_key_inner(table: &SsTable, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let idx = table.find_block_idx(key);
        let block = table.read_block_cached(idx)?;
        let iter = BlockIterator::create_and_seek_to_key(block, key);
        Ok((idx, iter))
    }

    fn is_last_block(&self) -> bool {
        self.blk_idx == self.table.num_of_blocks() - 1
    }

    fn move_to_next_block(&mut self) -> Result<()> {
        assert!(!self.is_last_block());

        let next_block_idx = self.blk_idx + 1;
        self.blk_idx = next_block_idx;
        self.blk_iter =
            BlockIterator::create_and_seek_to_first(self.table.read_block_cached(next_block_idx)?);

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    fn show_key(&self) -> Bytes {
        Bytes::copy_from_slice(self.key().inner())
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        // Case: Move to the next key in the current block.
        if self.blk_iter.is_valid() {
            return Ok(());
        }
        // Case: All data blocks in this SST are traversed.
        if self.is_last_block() {
            return Ok(());
        }
        // Case: Move to the next block.
        self.move_to_next_block()?;
        Ok(())
    }
}
