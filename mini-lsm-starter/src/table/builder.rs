use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use crate::key::KeyVec;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

use super::{BlockMeta, FileObject, SsTable};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
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
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
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

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.builder.estimated_size()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    ///
    /// -------------------------------------------------------------------------------------------
    /// |         Block Section         |          Meta Section         |          Extra          |
    /// -------------------------------------------------------------------------------------------
    /// | data block | ... | data block |            metadata           | meta block offset (u32) |
    /// -------------------------------------------------------------------------------------------
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_current_block();

        // Block section
        let mut buf = self.data;
        // Meta section
        let block_meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta[..], &mut buf);
        // Extra
        buf.put_u32(block_meta_offset as u32);

        let file = FileObject::create(path.as_ref(), buf)?;
        let sstable = SsTable {
            file,
            block_meta_offset,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            // TODO: bloom flter
            bloom: None,
            // TODO: timestamp
            max_ts: 0,
        };
        Ok(sstable)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
