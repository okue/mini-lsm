use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

use super::Block;

pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZEOF_U64: usize = std::mem::size_of::<u64>();

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
    /// Encoding format:
    /// | key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len) | ts (u64) | value_len (u16) | value (len) |
    ///
    /// See also [Block::encode]
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if !self.is_empty()
            && self.estimated_size() + key.raw_len() + value.len() + SIZEOF_U16 * 4
                > self.block_size
        {
            return false;
        }
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        // add offset
        self.offsets.push(self.data.len() as u16);
        // add key and value
        let key_overlap_len = self.key_overlap_length(key);
        self.data.put_u16(key_overlap_len);
        self.data.put_u16((key.key_len() as u16) - key_overlap_len);
        self.data
            .put_slice(&key.key_ref()[(key_overlap_len as usize)..]);
        self.data.put_u64(key.ts());
        self.data.put_u16(value.len() as u16);
        self.data.put_slice(value);
        true
    }

    pub fn estimated_size(&self) -> usize {
        // - key-value pairs
        // - offsets
        // - number of key-value pairs in the block
        self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16
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
            first_key: self.first_key,
        }
    }

    fn key_overlap_length(&self, key: KeySlice) -> u16 {
        let first_key = self.first_key.key_ref();
        let key = key.key_ref();
        let mut overlap_len = 0;
        for idx in 0..first_key.len() {
            if first_key[idx] != key[idx] {
                return overlap_len;
            }
            overlap_len += 1;
        }
        overlap_len
    }
}
