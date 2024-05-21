use std::sync::atomic::Ordering;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::lsm_storage::WriteBatchRecord;
use crate::mem_table::map_bound;
use crate::mvcc::CommittedTxnData;
use crate::{
    iterators::{two_merge_iterator::TwoMergeIterator, StorageIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// 0: read set
    /// 1: write set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::Relaxed) {
            bail!("already committed!")
        }

        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let mut guard = key_hashes.lock();
            guard.0.insert(farmhash::fingerprint32(key));
        }

        if let Some(entry) = self.local_storage.get(key) {
            let value = entry.value();
            return if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value.clone()))
            };
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    // Note: `scan` doesn't guarantee serializable.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::Relaxed) {
            bail!("already committed!")
        }

        let local_storage_iter = TxnLocalIterator::create(
            self.local_storage.clone(),
            map_bound(lower),
            map_bound(upper),
        )?;
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_storage_iter,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::Relaxed) {
            panic!("already committed!")
        }

        if let Some(key_hashes) = self.key_hashes.as_ref() {
            let mut guard = key_hashes.lock();
            guard.1.insert(farmhash::fingerprint32(key));
        }

        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.put(key, &[]);
    }

    pub fn commit(&self) -> Result<()> {
        let _commit_lock = self.inner.mvcc().commit_lock.lock();

        let serializable = self.key_hashes.is_some();
        let mut written_in_this_txn: bool = false;

        if serializable {
            let key_hashes = self.key_hashes.as_ref().unwrap().lock();
            let (read_set, write_set) = &*key_hashes;
            written_in_this_txn = !write_set.is_empty();

            if written_in_this_txn {
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                for (_, commit_tx) in committed_txns.range((self.read_ts + 1)..) {
                    if !commit_tx.key_hashes.is_disjoint(read_set) {
                        bail!("Commit abort for serializable violation!")
                    }
                }
            }
        }

        let write_batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();
        let commit_ts = self.inner.write_batch_inner(&write_batch)?;

        if serializable && written_in_this_txn {
            let mut key_hashes = self.key_hashes.as_ref().unwrap().lock();
            let (_, write_set) = &mut *key_hashes;

            let mut committed_txns = self.inner.mvcc().committed_txns.lock();
            committed_txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );

            let watermark = self.inner.mvcc().watermark();
            while let Some(entry) = committed_txns.first_entry() {
                if entry.key() < &watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }
        self.committed.store(true, Ordering::Relaxed);
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn create(
        local_storage: Arc<SkipMap<Bytes, Bytes>>,
        lower: Bound<Bytes>,
        upper: Bound<Bytes>,
    ) -> Result<Self> {
        let mut iter = TxnLocalIterator::new(
            local_storage,
            |map| map.range((lower, upper)),
            (Bytes::default(), Bytes::default()),
        );
        iter.next()?;
        Ok(iter)
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn key(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid iterator")
        }
        self.borrow_item().0.as_ref()
    }

    // may return empty.
    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid iterator")
        }
        self.borrow_item().1.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let new_item = self.with_iter_mut(|iter| {
            iter.next()
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or((Bytes::default(), Bytes::default()))
        });
        self.with_item_mut(|item| *item = new_item);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let iter = Self { txn, iter };
        iter.add_to_read_set();
        Ok(iter)
    }

    fn add_to_read_set(&self) {
        if self.is_valid() {
            if let Some(key_hashes) = self.txn.key_hashes.as_ref() {
                let mut guard = key_hashes.lock();
                guard.0.insert(farmhash::fingerprint32(self.key()));
            }
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a> = &'a [u8] where Self: 'a;

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    // must not return empty
    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        loop {
            self.iter.next()?;
            if !self.iter.is_valid() {
                return Ok(());
            }
            if !self.value().is_empty() {
                self.add_to_read_set();
                return Ok(());
            }
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
