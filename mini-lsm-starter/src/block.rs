#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::{Buf, BufMut, Bytes};

use crate::block::builder::SIZEOF_U16;
pub use builder::BlockBuilder;
pub use iterator::BlockIterator;

mod builder;
mod iterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    pub fn get_key(&self, index: usize) -> &[u8] {
        let offset = self.offsets[index] as usize;
        let key_len = (&self.data[offset..]).get_u16() as usize;
        &self.data[offset + SIZEOF_U16..offset + SIZEOF_U16 + key_len]
    }

    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    ///
    /// ----------------------------------------------------------------------------------------------------
    /// |             Data Section             |              Offset Section             |      Extra      |
    /// ----------------------------------------------------------------------------------------------------
    /// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
    /// ----------------------------------------------------------------------------------------------------
    ///
    ///
    /// See [BlockBuilder::add].
    pub fn encode(&self) -> Bytes {
        let mut buffer = self.data.clone();
        for offset in &self.offsets {
            buffer.put_u16(*offset);
        }
        buffer.put_u16(self.offsets.len() as u16);
        Bytes::from(buffer)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_entries = (&data[data.len() - 2..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 * (num_of_entries + 1);

        let block_data: Vec<u8> = Vec::from(&data[0..data_end]);
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();

        Block {
            data: block_data,
            offsets,
        }
    }
}
