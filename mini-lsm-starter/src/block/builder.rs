#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

use super::Block;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::default(),
            data: Vec::default(),
            block_size,
            first_key: KeyVec::default(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    ///
    /// -----------------------------------------------------------------------
    /// |                           Entry #1                            | ... |
    /// -----------------------------------------------------------------------
    /// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
    /// -----------------------------------------------------------------------
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty()
            && self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 > self.block_size
        {
            return false;
        }

        // add offset for the previous entry.
        self.offsets.push(self.data.len() as u16);
        // add key and value
        self.data.put_u16(key.len() as u16);
        self.data.put_slice(key.raw_ref());
        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    pub fn estimated_size(&self) -> usize {
        SIZEOF_U16 // number of key-value pairs in the block
            +  self.offsets.len() * SIZEOF_U16 // offsets
            + self.data.len() // key-value pairs
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
