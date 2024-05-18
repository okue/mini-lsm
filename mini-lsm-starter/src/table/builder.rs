use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use crate::key::KeyVec;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

use super::{BlockMeta, FileObject, SsTable};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    // currently being built
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    // built data
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::default(),
            last_key: KeyVec::default(),
            data: Vec::default(),
            meta: Vec::default(),
            block_size,
            key_hashes: Vec::default(),
        }
    }

    /// Adds a key-value pair to SSTable.
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));

        // Case: the current block is not full.
        if self.builder.add(key, value) {
            if self.first_key.is_empty() {
                self.first_key.set_from_slice(key);
            }
            self.last_key.set_from_slice(key);
            return;
        }

        // Case: the current block is full.
        self.finish_current_block();
        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.builder.estimated_size()
    }

    pub fn num_of_entries(&self) -> usize {
        self.key_hashes.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    ///
    /// --------------------------------------------------------------------------------------------------------------------------
    /// |         Block Section         |                            Meta Section                           |          Extra     |
    /// --------------------------------------------------------------------------------------------------------------------------
    /// | data block | ... | data block | metadata | meta block offset | bloom filter | bloom filter offset | meta block offset  |
    /// |                               |  varlen  |         u32       |    varlen    |        u32          |        u32         |
    /// --------------------------------------------------------------------------------------------------------------------------
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_current_block();

        // Block section
        let mut buf = self.data;
        // Meta section - block metadata
        let meta_section_offset = buf.len();
        {
            let mut block_meta_buf = Vec::new();
            BlockMeta::encode_block_meta(&self.meta[..], &mut block_meta_buf);
            buf.put_u32(block_meta_buf.len() as u32);
            buf.append(&mut block_meta_buf);
        }
        // Meta section - bloom filter
        let bloom = Self::build_bloom_filter(self.key_hashes);
        {
            let mut bloom_buf = Vec::new();
            bloom.encode(&mut bloom_buf);
            buf.put_u32(bloom_buf.len() as u32);
            buf.append(&mut bloom_buf);
        }
        // Extra
        buf.put_u32(meta_section_offset as u32);

        let file = FileObject::create(path.as_ref(), buf)?;
        let sstable = SsTable {
            file,
            block_meta_offset: meta_section_offset,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: Some(bloom),
            // TODO: timestamp
            max_ts: 0,
        };
        Ok(sstable)
    }

    fn build_bloom_filter(key_hashes: Vec<u32>) -> Bloom {
        let bits_per_key = Bloom::bloom_bits_per_key(key_hashes.len(), 0.01);
        Bloom::build_from_key_hashes(&key_hashes[..], bits_per_key)
    }

    fn finish_current_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let first_key = std::mem::take(&mut self.first_key).into_key_bytes();
        let last_key = std::mem::take(&mut self.last_key).into_key_bytes();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key,
            last_key,
        });
        let block = builder.build().encode();
        self.data.put(block);
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
