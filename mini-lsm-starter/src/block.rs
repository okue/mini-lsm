use bytes::{Buf, BufMut, Bytes};

pub use builder::BlockBuilder;
pub use iterator::BlockIterator;

use crate::block::builder::SIZEOF_U16;
use crate::key::KeyVec;

mod builder;
mod iterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    first_key: KeyVec,
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    ///
    /// ------------------------------------------------------------------------------------------------------
    /// |       Header              |        Data Section       |     Offset Section          |      Extra   |
    /// ------------------------------------------------------------------------------------------------------
    /// | first_key_len | first_key | Entry #1 | ... | Entry #N | Offset #1 | ... | Offset #N | num_of_elems |
    /// ------------------------------------------------------------------------------------------------------
    ///
    /// See [BlockBuilder::add].
    pub fn encode(&self) -> Bytes {
        let mut buffer = Vec::with_capacity(self.data.len() + self.offsets.len() * 2);
        // first key
        buffer.put_u16(self.first_key.len() as u16);
        buffer.extend_from_slice(self.first_key.raw_ref());
        // data
        buffer.extend_from_slice(&self.data[..]);
        // offset
        for offset in &self.offsets {
            buffer.put_u16(*offset);
        }
        // extra
        buffer.put_u16(self.offsets.len() as u16);
        Bytes::from(buffer)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(mut data: &[u8]) -> Self {
        let first_key_len = data.get_u16() as usize;
        let first_key = KeyVec::from_vec(data[..first_key_len].to_vec());
        data.advance(first_key_len);

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
            first_key,
        }
    }
}
