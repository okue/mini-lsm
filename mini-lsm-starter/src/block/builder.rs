use bytes::BufMut;
use crate::key::{KeySlice, KeyVec};

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
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        SIZEOF_U16 /* number of key-value pairs in the block */ + self.offsets.len() * SIZEOF_U16 /* offsets */ + self.data.len()
        // key-value pairs
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 /* key_len, value_len and offset */ > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // Add the offset of the data into the offset array.
        self.offsets.push(self.data.len() as u16);
        // Encode key length.
        self.data.put_u16(key.len() as u16);
        // Encode key content.
        self.data.put(key.raw_ref());
        // Encode value length.
        self.data.put_u16(value.len() as u16);
        // Encode value content.
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            offsets: self.offsets,
            data: self.data,
        }
    }
}
