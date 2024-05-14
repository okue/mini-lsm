use std::sync::Arc;

use bytes::Buf;

use crate::block::builder::SIZEOF_U16;
use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.first_key.clone(),
            idx: 0,
            key: KeyVec::new(),  // dummy
            value_range: (0, 0), // dummy
            block,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        if !self.is_valid() {
            panic!("Invalid state!")
        }
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Invalid state!")
        }
        let (start_pos, end_pos) = self.value_range;
        &self.block.data[start_pos..end_pos]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seek to the given index.
    ///
    /// See [Block]
    fn seek(&mut self, index: usize) {
        if index >= self.block.offsets.len() {
            self.key = KeyVec::default();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[index] as usize;
        let mut data = &self.block.data[offset..];

        // key-value format:
        // key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len) | value_len (u16) | value (len)
        self.key.clear();
        let key_overlap_len = data.get_u16() as usize;
        let rest_key_len = data.get_u16() as usize;
        self.key
            .append(&self.first_key.raw_ref()[..key_overlap_len]);
        self.key.append(&data[..rest_key_len]);
        data.advance(rest_key_len);

        let val_len = data.get_u16() as usize;

        self.idx = index;
        self.value_range = (
            offset + SIZEOF_U16 * 3 + rest_key_len,
            offset + SIZEOF_U16 * 3 + rest_key_len + val_len,
        );
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek(self.idx + 1);
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek(0);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // naive search
        let mut index = 0;
        while index < self.block.offsets.len() {
            self.seek(index);
            if self.key.raw_ref() >= key.raw_ref() {
                return;
            }
            index += 1;
        }
        // Case the key that >= `key` is not found.
        self.seek(index);
    }
}
