use std::fmt::Debug;
use std::fmt::Formatter;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut};

pub use builder::SsTableBuilder;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

pub(crate) mod bloom;
mod builder;
mod iterator;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        buf.put_u16(block_meta.len() as u16);
        for item in block_meta {
            buf.put_u16(item.offset as u16);
            buf.put_u16(item.first_key.key_len() as u16);
            buf.put_u64(item.first_key.ts());
            buf.put(item.first_key.key_ref());
            buf.put_u16(item.last_key.key_len() as u16);
            buf.put_u64(item.first_key.ts());
            buf.put(item.last_key.key_ref());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Vec<BlockMeta> {
        let block_meta_count = buf.get_u16();
        let mut block_meta = Vec::new();

        for _ in 0..block_meta_count {
            let offset = buf.get_u16() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = {
                let ts = buf.get_u64();
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(first_key_len), ts)
            };
            let last_key_len = buf.get_u16() as usize;
            let last_key = {
                let ts = buf.get_u64();
                KeyBytes::from_bytes_with_ts(buf.copy_to_bytes(last_key_len), ts)
            };
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            })
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject {
    file: Option<File>,
    size: u64,
}

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.file
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Create a new file object and write the file to the disk.
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject {
            file: Some(File::options().read(true).write(false).open(path)?),
            size: data.len() as u64,
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject {
            file: Some(file),
            size,
        })
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    ///
    /// See [SsTableBuilder::build].
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let block_meta_offset = (&file.read(file.size - 4, 4)?[..]).get_u32() as u64;
        let mut meta_section =
            &file.read(block_meta_offset, file.size - 4 - block_meta_offset)?[..];
        // read block metadata
        let block_meta_len = meta_section.get_u32() as usize;
        let block_meta = BlockMeta::decode_block_meta(&meta_section[..block_meta_len]);
        meta_section.advance(block_meta_len);
        // read bloom filter
        let bloom_len = meta_section.get_u32() as usize;
        let bloom = Bloom::decode(&meta_section[..bloom_len])?;

        let sstable = Self {
            file,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            bloom: Some(bloom),
            max_ts: 0,
        };
        Ok(sstable)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject {
                file: None,
                size: file_size,
            },
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_offset = self.block_meta[block_idx].offset as u64;
        let end_offset = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset) as u64;
        Ok(Arc::new(Block::decode(
            &self.file.read(block_offset, end_offset - block_offset)?[..],
        )))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note:
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let key = key.to_key_vec().into_key_bytes();
        self.block_meta
            .partition_point(|b| b.last_key < key)
            .min(self.num_of_blocks() - 1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.size
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}

impl Debug for SsTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SsTable")
            .field("sst_id", &self.sst_id())
            .field("table_size", &self.table_size())
            .field("num_of_blocks", &self.num_of_blocks())
            .field("first_key", &self.first_key())
            .field("last_key", &self.last_key())
            .field("max_ts", &self.max_ts())
            .field("bloom", &self.bloom)
            .finish()
    }
}
