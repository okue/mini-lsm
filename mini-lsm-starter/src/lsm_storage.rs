use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::{fs, mem};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::logger;
use crate::lsm_iterator::{FusedIterator, LsmIterator, LsmIteratorInner};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_key_bound_plus_ts, MemTable, MemTableIterator};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

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

    pub fn get_sst_ids(&self, level: usize) -> Vec<usize> {
        self.levels
            .iter()
            .find(|item| item.0 == level)
            .map(|(_, ids)| ids)
            .cloned()
            .unwrap_or(Vec::new())
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
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for [LsmStorageInner] and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
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
        log::info!("Close MiniLsm...");

        self.flush_notifier.send(())?;
        let mut flush_thread = self.flush_thread.lock();
        if let Some(thread) = flush_thread.take() {
            thread.join().map_err(|e| anyhow!("error: {:?}", e))?;
            log::info!("Close flush thread...");
        }

        self.compaction_notifier.send(())?;
        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(thread) = compaction_thread.take() {
            thread.join().map_err(|e| anyhow!("error: {:?}", e))?;
            log::info!("Close compaction thread...");
        }

        if !self.inner.options.enable_wal {
            // Make sure no memtables.
            self.force_flush()?;
            while !self.inner.state.read().imm_memtables.is_empty() {
                self.force_flush()?;
            }
        }
        self.sync()?;
        self.inner.sync_dir()?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let _ = logger::setup();
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

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
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

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
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

        let block_cache = Arc::new(BlockCache::new(1024));
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

        let mut state = LsmStorageState::create(&options);

        // Read manifest to recover memtables and sstables.
        let manifest_path = Self::path_of_manifest_static(path);
        let mut memtable_ids = BTreeSet::new();
        let manifest = if manifest_path.exists() {
            let (manifest, records) = Manifest::recover(manifest_path)?;
            for record in records {
                match record {
                    ManifestRecord::NewMemtable(memtable_id) => {
                        memtable_ids.insert(memtable_id);
                    }
                    ManifestRecord::Flush(sst_id) => {
                        log::debug!("Found record: Flush {}", sst_id);
                        memtable_ids.remove(&sst_id);
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                    }
                    ManifestRecord::Compaction(compaction_tasks, new_sst_ids) => {
                        log::debug!(
                            "Found record: Compaction {:?}, {:?}",
                            new_sst_ids,
                            compaction_tasks
                        );
                        state = compaction_controller
                            .apply_compaction_result(&state, &compaction_tasks, &new_sst_ids[..])
                            .0;
                    }
                }
            }
            manifest
        } else {
            Manifest::create(manifest_path)?
        };
        // Recover immutable memtables from disk.
        if options.enable_wal {
            for sst_id in memtable_ids.clone() {
                let memtable =
                    MemTable::recover_from_wal(sst_id, Self::path_of_wal_static(path, sst_id))?;
                state.imm_memtables.insert(0, Arc::new(memtable))
            }
        }
        // Recover sstables from disk.
        for sst_id in state
            .levels
            .iter()
            .flat_map(|(_, sst_ids)| sst_ids.clone())
            .chain(state.l0_sstables.clone())
        {
            state.sstables.insert(
                sst_id,
                Arc::new(SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, sst_id))?,
                )?),
            );
        }
        // Decide next sst_id
        let next_sst_id = memtable_ids
            .iter()
            .chain(state.sstables.keys())
            .chain(state.l0_sstables.iter())
            .max()
            .cloned()
            .map(|sst_id| sst_id + 1)
            .unwrap_or(0);
        // Initialize memtable
        if options.enable_wal {
            state.memtable = MemTable::create_with_wal(
                next_sst_id,
                Self::path_of_wal_static(path, next_sst_id),
            )?
            .into();
        } else {
            state.memtable = MemTable::create(next_sst_id).into();
        };
        manifest.add_record_when_init(ManifestRecord::NewMemtable(next_sst_id))?;

        let max_ts = state
            .imm_memtables
            .iter()
            .map(|t| t.max_ts())
            .chain(state.sstables.values().map(|t| t.max_ts()))
            .max()
            .unwrap_or(0);
        let mvcc = LsmMvccInner::new(max_ts);

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(mvcc),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()?;
        Ok(())
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage.
    pub fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let iter = LsmIterator::new(
            self.get_inner(key)?,
            map_key_bound_plus_ts(Bound::Included(key), TS_RANGE_END),
            read_ts,
        )
        .unwrap();
        if iter.is_valid() && iter.key() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.mvcc().new_txn(self.clone(), false);
        txn.get(key)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let memtable_size: usize;
        {
            // Only one thread can write to the storage engine at the same time.
            let _write_lock = self.mvcc().write_lock.lock();
            let ts = self.mvcc().latest_commit_ts() + 1;
            let guard = self.state.read();

            for batch_record in batch {
                match batch_record {
                    WriteBatchRecord::Put(key, value) => {
                        guard
                            .memtable
                            .put(KeySlice::from_slice(key.as_ref(), ts), value.as_ref())?;
                    }
                    WriteBatchRecord::Del(key) => {
                        guard
                            .memtable
                            .put(KeySlice::from_slice(key.as_ref(), ts), &[])?;
                    }
                }
            }

            memtable_size = guard.memtable.approximate_size();
            self.mvcc().update_commit_ts(ts);
        }

        // Freeze memtable to immutable if the size > target_sst_size.
        if memtable_size > self.options.target_sst_size {
            let lock = self.state_lock.lock();
            let size = self.state.read().memtable.approximate_size();
            if size > self.options.target_sst_size {
                self.force_freeze_memtable(&lock)?;
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(crate) fn path_of_manifest_static(path: impl AsRef<Path>) -> PathBuf {
        path.as_ref().join("MANIFEST")
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let next_id = self.next_sst_id();
        let new_memtable = MemTable::create_with_wal(next_id, self.path_of_wal(next_id))?;

        let mut guard = self.state.write();
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = mem::replace(&mut snapshot.memtable, Arc::new(new_memtable));
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        *guard = Arc::new(snapshot);
        drop(guard);

        log::debug!(
            "Freeze memtable: frozen={}, new={}",
            old_memtable.id(),
            next_id
        );

        // update manifest and wal
        if let Some(manifest) = self.manifest.as_ref() {
            manifest.add_record(state_lock_observer, ManifestRecord::NewMemtable(next_id))?;
        }
        old_memtable.sync_wal()?;
        self.sync_dir()?;

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        let Some(earliest_memtable) = snapshot.imm_memtables.last().cloned() else {
            return Ok(());
        };
        let sst_id = earliest_memtable.id();

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        earliest_memtable.flush(&mut sst_builder)?;
        let sst = sst_builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            if snapshot.l0_sstables.contains(&sst_id) {
                log::debug!(
                    "Skip because imm memtable (id={}) is already flushed to SSTable.",
                    sst_id
                );
                return Ok(());
            } else {
                snapshot.imm_memtables.pop();
                snapshot.sstables.insert(sst_id, Arc::new(sst));
                snapshot.l0_sstables.insert(0, sst_id);
                log::debug!("Flush imm memtable (id={}) to SSTable.", sst_id);
                *guard = Arc::new(snapshot);
            }
        }
        {
            let lock = self.state_lock.lock();
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&lock, ManifestRecord::Flush(sst_id))?;
        }
        if self.options.enable_wal {
            fs::remove_file(self.path_of_wal(sst_id))
                .map_err(|e| anyhow!("failed to remove wal file of {}: {}", sst_id, e))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), false))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.mvcc().new_txn(self.clone(), false);
        txn.scan(lower, upper)
    }

    pub fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        Ok(FusedIterator::new(
            LsmIterator::new(
                self.scan_inner(lower, upper)?,
                map_key_bound_plus_ts(upper, TS_RANGE_END),
                read_ts,
            )
            .unwrap(),
        ))
    }

    pub(crate) fn scan_inner(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<LsmIteratorInner> {
        let state = {
            let guard = self.state.read();
            guard.clone()
        };
        let memtable_iter = Self::create_memtable_iter_for_scan(state.clone(), lower, upper);
        let l0_sstable_iter = Self::create_l0_sstable_iter_for_scan(state.clone(), lower, upper)?;
        let ln_sstable_iter = Self::create_ln_sstable_iter_for_scan(state, lower)?;
        TwoMergeIterator::create(
            TwoMergeIterator::create(memtable_iter, l0_sstable_iter)?,
            ln_sstable_iter,
        )
    }

    pub(crate) fn get_inner(&self, key: &[u8]) -> Result<LsmIteratorInner> {
        let state = {
            let guard = self.state.read();
            guard.clone()
        };
        let memtable_iter = Self::create_memtable_iter_for_get(state.clone(), key);
        let l0_sstable_iter = Self::create_l0_sstable_iter_for_get(state.clone(), key)?;
        let ln_sstable_iter = Self::create_ln_sstable_iter_for_get(state, key)?;
        TwoMergeIterator::create(
            TwoMergeIterator::create(memtable_iter, l0_sstable_iter)?,
            ln_sstable_iter,
        )
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    fn create_memtable_iter_for_get(
        state: Arc<LsmStorageState>,
        key: &[u8],
    ) -> MergeIterator<MemTableIterator> {
        Self::create_memtable_iter_for_scan(state, Bound::Included(key), Bound::Included(key))
    }

    fn create_l0_sstable_iter_for_get(
        state: Arc<LsmStorageState>,
        key: &[u8],
    ) -> Result<MergeIterator<SsTableIterator>> {
        let key_hash = farmhash::fingerprint32(key);
        let mut iters: Vec<Box<SsTableIterator>> = Vec::new();
        let key_bound = Bound::Included(key);

        for idx in &state.l0_sstables {
            if let Some(sstable) = state.sstables.get(idx).cloned() {
                // Check if the scan range overlaps sstable.
                if !Self::range_overlap(key_bound, key_bound, sstable.clone()) {
                    continue;
                }
                // Check if the key never exists in this SSTable.
                if !Self::check_bloom_filter(sstable.clone(), key_hash) {
                    continue;
                }
                let sstable_iter = SsTableIterator::create_and_seek_to_key(
                    sstable,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?;
                iters.push(Box::new(sstable_iter))
            }
        }
        Ok(MergeIterator::create(iters))
    }

    fn create_ln_sstable_iter_for_get(
        state: Arc<LsmStorageState>,
        key: &[u8],
    ) -> Result<MergeIterator<SstConcatIterator>> {
        let key_hash = farmhash::fingerprint32(key);
        let key = KeySlice::from_slice(key, TS_RANGE_BEGIN);
        let mut iters: Vec<Box<SstConcatIterator>> = Vec::new();

        for (_, ids) in &state.levels {
            if ids.is_empty() {
                continue;
            }
            let sstables = ids
                .iter()
                .filter_map(|sst_id| state.sstables.get(sst_id).cloned())
                .filter(|sst| Self::check_bloom_filter(sst.clone(), key_hash))
                .collect::<Vec<_>>();
            let iter = SstConcatIterator::create_and_seek_to_key(sstables, key)?;
            iters.push(Box::new(iter));
        }
        Ok(MergeIterator::create(iters))
    }

    fn create_memtable_iter_for_scan(
        state: Arc<LsmStorageState>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MergeIterator<MemTableIterator> {
        let lower = map_key_bound_plus_ts(lower, TS_RANGE_BEGIN);
        let upper = map_key_bound_plus_ts(upper, TS_RANGE_END);

        let mut iters: Vec<Box<MemTableIterator>> = Vec::new();
        iters.push(Box::new(state.memtable.scan(lower.clone(), upper.clone())));
        for mem_table in &state.imm_memtables {
            iters.push(Box::new(mem_table.scan(lower.clone(), upper.clone())));
        }
        MergeIterator::create(iters)
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
                if !Self::range_overlap(lower, upper, sstable.clone()) {
                    continue;
                }

                let sstable_iter = match lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        sstable,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            sstable,
                            KeySlice::from_slice(key, TS_RANGE_BEGIN),
                        )?;
                        while iter.is_valid() && iter.key().key_ref() == key {
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

    fn create_ln_sstable_iter_for_scan(
        state: Arc<LsmStorageState>,
        lower: Bound<&[u8]>,
    ) -> Result<MergeIterator<SstConcatIterator>> {
        let mut iters: Vec<Box<SstConcatIterator>> = Vec::new();
        for (_, ids) in &state.levels {
            let sstables = ids
                .iter()
                .map(|sst_id| {
                    state
                        .sstables
                        .get(sst_id)
                        .cloned()
                        .ok_or_else(|| anyhow!("sst_id {} not found", sst_id))
                })
                .collect::<Result<Vec<_>>>()?;

            let sstable_concat_iter = match lower {
                Bound::Included(key) => {
                    let key_slice = KeySlice::from_slice(key, TS_RANGE_BEGIN);
                    SstConcatIterator::create_and_seek_to_key(sstables, key_slice)?
                }
                Bound::Excluded(key) => {
                    let key_slice = KeySlice::from_slice(key, TS_RANGE_BEGIN);
                    let mut iter = SstConcatIterator::create_and_seek_to_key(sstables, key_slice)?;
                    while iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(sstables)?,
            };
            iters.push(Box::new(sstable_concat_iter));
        }
        Ok(MergeIterator::create(iters))
    }

    /// Returns: True if the table may contain the key. False if the key is never contained.
    fn check_bloom_filter(sstable: Arc<SsTable>, key_hash: u32) -> bool {
        if let Some(ref bloom) = sstable.bloom {
            return bloom.may_contain(key_hash);
        }
        true
    }

    fn range_overlap(
        scan_lower: Bound<&[u8]>,
        scan_upper: Bound<&[u8]>,
        sstable: Arc<SsTable>,
    ) -> bool {
        let first_key = sstable.first_key().key_ref();
        let last_key = sstable.last_key().key_ref();
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
}
