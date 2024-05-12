use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::{fs, mem};

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::logger;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{map_bound, MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    #[allow(dead_code)]
    path: PathBuf,
    #[allow(dead_code)]
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    #[allow(dead_code)]
    pub(crate) compaction_controller: CompactionController,
    #[allow(dead_code)]
    pub(crate) manifest: Option<Manifest>,
    #[allow(dead_code)]
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for [LsmStorageInner] and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    #[allow(dead_code)]
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    #[allow(dead_code)]
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        unimplemented!()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        logger::setup();
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            fs::create_dir(path)?;
        }

        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let state = self.state.read().clone();
        if let Some(v) = state.memtable.get(key) {
            return if v.is_empty() { Ok(None) } else { Ok(Some(v)) };
        }
        for memtable in &state.imm_memtables {
            if let Some(v) = memtable.get(key) {
                return if v.is_empty() { Ok(None) } else { Ok(Some(v)) };
            }
        }
        let iter = Self::create_l0_sstable_iter_for_get(state, key)?;
        if iter.is_valid() && iter.key().into_inner() == key && !iter.value().is_empty() {
            Ok(Some(Bytes::copy_from_slice(iter.value())))
        } else {
            Ok(None)
        }
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let size = {
            let guard = self.state.read();
            guard.memtable.put(_key, _value)?;
            guard.memtable.approximate_size()
        };

        if size > self.options.target_sst_size {
            let lock = self.state_lock.lock();
            let size = self.state.read().memtable.approximate_size();
            if size > self.options.target_sst_size {
                self.force_freeze_memtable(&lock)?;
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    #[allow(dead_code)]
    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    #[allow(dead_code)]
    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    #[allow(dead_code)]
    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // let memtable = MemTable::create_with_wal()?;
        let new_memtable = MemTable::create(self.next_sst_id());
        {
            let mut guard = self.state.write();

            let mut snapshot = guard.as_ref().clone();
            let old_memtable = mem::replace(&mut snapshot.memtable, Arc::new(new_memtable));
            snapshot.imm_memtables.insert(0, old_memtable);

            *guard = Arc::new(snapshot);
        };
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        let earliest_memtable = snapshot.imm_memtables.last();
        if earliest_memtable.is_none() {
            // skip
            return Ok(());
        }
        let earliest_memtable = earliest_memtable.cloned().unwrap();
        let memtable_id = earliest_memtable.id();

        let mut sst = SsTableBuilder::new(self.options.block_size);
        earliest_memtable.flush(&mut sst)?;
        let sst = sst.build(
            memtable_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(memtable_id),
        )?;

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.imm_memtables.pop();
            snapshot.sstables.insert(memtable_id, Arc::new(sst));
            snapshot.l0_sstables.insert(0, memtable_id);
            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    fn range_overlap(
        scan_lower: Bound<&[u8]>,
        scan_upper: Bound<&[u8]>,
        first_key: &[u8],
        last_key: &[u8],
    ) -> bool {
        match scan_lower {
            Bound::Included(lower) => {
                if last_key < lower {
                    return false;
                }
            }
            Bound::Excluded(lower) => {
                if last_key <= lower {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }
        match scan_upper {
            Bound::Included(upper) => {
                if upper < first_key {
                    return false;
                }
            }
            Bound::Excluded(upper) => {
                if upper <= first_key {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }
        true
    }

    fn create_memtable_iter(
        state: Arc<LsmStorageState>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MergeIterator<MemTableIterator> {
        let mut iters: Vec<Box<MemTableIterator>> = Vec::new();
        iters.push(Box::new(state.memtable.scan(lower, upper)));
        for mem_table in &state.imm_memtables {
            iters.push(Box::new(mem_table.scan(lower, upper)));
        }
        MergeIterator::create(iters)
    }

    fn create_l0_sstable_iter_for_get(
        state: Arc<LsmStorageState>,
        key: &[u8],
    ) -> Result<MergeIterator<SsTableIterator>> {
        let key_hash = farmhash::fingerprint32(key);
        let mut iters: Vec<Box<SsTableIterator>> = Vec::new();
        for idx in &state.l0_sstables {
            if let Some(sstable) = state.sstables.get(idx).cloned() {
                // Check if the scan range overlaps sstable.
                if !Self::range_overlap(
                    Bound::Included(key),
                    Bound::Included(key),
                    sstable.first_key().raw_ref(),
                    sstable.last_key().raw_ref(),
                ) {
                    continue;
                }
                // Check if the key never exists in this SSTable.
                if let Some(ref bloom) = sstable.bloom {
                    if !bloom.may_contain(key_hash) {
                        if log::log_enabled!(log::Level::Debug) {
                            log::debug!(
                                "[bloom filter] Skip SSTable {sst_id} as {key:?}",
                                sst_id = idx,
                                key = Bytes::copy_from_slice(key)
                            );
                        }
                        continue;
                    }
                }
                let sstable_iter =
                    SsTableIterator::create_and_seek_to_key(sstable, KeySlice::from_slice(key))?;
                iters.push(Box::new(sstable_iter))
            }
        }
        Ok(MergeIterator::create(iters))
    }

    fn create_l0_sstable_iter_for_scan(
        state: Arc<LsmStorageState>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<MergeIterator<SsTableIterator>> {
        let mut iters: Vec<Box<SsTableIterator>> = Vec::new();
        for idx in &state.l0_sstables {
            if let Some(sstable) = state.sstables.get(idx).cloned() {
                // Check if the scan range overlaps sstable.
                if !Self::range_overlap(
                    lower,
                    upper,
                    sstable.first_key().raw_ref(),
                    sstable.last_key().raw_ref(),
                ) {
                    continue;
                }
                let sstable_iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(sstable, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sstable,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key().into_inner() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sstable)?,
                };
                iters.push(Box::new(sstable_iter))
            }
        }
        Ok(MergeIterator::create(iters))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = {
            let guard = self.state.read();
            guard.clone()
        };
        let memtable_iter = Self::create_memtable_iter(state.clone(), lower, upper);
        let sstable_iter = Self::create_l0_sstable_iter_for_scan(state, lower, upper)?;
        Ok(FusedIterator::new(
            LsmIterator::new(
                TwoMergeIterator::create(memtable_iter, sstable_iter)?,
                map_bound(upper),
            )
            .unwrap(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::Bound;
    use std::sync::Arc;

    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::iterators::StorageIterator;
    use crate::lsm_storage::{LsmStorageInner, LsmStorageOptions};

    #[test]
    fn test_1() {
        let dir = tempdir().unwrap();
        let storage = Arc::new(
            LsmStorageInner::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap(),
        );
        storage.put(b"0", b"2333333").unwrap();
        storage.put(b"00", b"2333333").unwrap();
        storage.put(b"4", b"23").unwrap();
        sync(&storage);

        storage.delete(b"4").unwrap();
        sync(&storage);

        storage.put(b"1", b"233").unwrap();
        storage.put(b"2", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"00", b"2333").unwrap();
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.put(b"3", b"23333").unwrap();
        storage.delete(b"1").unwrap();

        let snapshot = storage.state.read().clone();
        let mut iter = LsmStorageInner::create_l0_sstable_iter_for_scan(
            snapshot,
            Bound::Unbounded,
            Bound::Unbounded,
        )
        .unwrap();
        println!("num iter: {}", iter.num_active_iterators());
        while iter.is_valid() {
            println!(
                "key={:?} value={:?}",
                iter.show_key(),
                Bytes::copy_from_slice(iter.value())
            );
            let _ = iter.next();
        }
    }

    fn sync(storage: &LsmStorageInner) {
        storage
            .force_freeze_memtable(&storage.state_lock.lock())
            .unwrap();
        storage.force_flush_next_imm_memtable().unwrap();
    }
}
