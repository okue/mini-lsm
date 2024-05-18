use std::fs::remove_file;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::iterators::concat_iterator::SstConcatIterator;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

mod leveled;
mod simple_leveled;
mod tiered;

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    #[allow(dead_code)]
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

#[allow(dead_code)]
pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            _ => unreachable!(),
        }
    }

    /// Updates `l0_sstables` and `levels`.
    /// Returns new LSM state and SSTable ids to be deleted.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        // new SSTable ids
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    #[allow(dead_code)]
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode, always flush to L0
    NoCompaction,
}

// Compaction
impl LsmStorageInner {
    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();

        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.get_sst_ids(1);
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        let sstables = self.compact(&compaction_task)?;
        let new_sst_ids = sstables.iter().map(|t| t.sst_id()).collect::<Vec<_>>();
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            // update `levels`
            let new_ids = sstables.iter().map(|t| t.sst_id()).collect::<Vec<_>>();
            snapshot.levels[0].1 = new_ids;

            // update `l0_sstables`
            snapshot.l0_sstables.retain(|e| !&l0_sstables.contains(e));

            // update `sstables`
            for table in sstables {
                snapshot.sstables.insert(table.sst_id(), table);
            }
            for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
                snapshot.sstables.remove(sst_id);
            }
            *guard = Arc::new(snapshot);
        };

        // Deleted needless SST files.
        for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
            remove_file(self.path_of_sst(*sst_id))?
        }
        {
            let lock = self.state_lock.lock();
            self.manifest.as_ref().unwrap().add_record(
                &lock,
                ManifestRecord::Compaction(compaction_task, new_sst_ids),
            )?;
        }
        self.sync_dir()?;
        Ok(())
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(_task) => {
                unimplemented!()
            }
            CompactionTask::Tiered(_task) => {
                unimplemented!()
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level,
                ..
            }) => {
                log::debug!(
                    "SimpleLeveledCompaction:\n\
                       upper_level: L{:?}={:?}\n\
                       lower_level: L{}={:?}",
                    upper_level.unwrap_or(0),
                    upper_level_sst_ids,
                    lower_level,
                    lower_level_sst_ids,
                );
                let snapshot = self.state.read().clone();
                let lower_iter = SstConcatIterator::create_and_seek_to_first(Self::get_sstables(
                    &snapshot,
                    lower_level_sst_ids,
                ))?;
                let upper_tables = Self::get_sstables(&snapshot, upper_level_sst_ids);

                match upper_level {
                    // L0 compaction
                    None => {
                        let upper_iter = MergeIterator::create(
                            upper_tables
                                .iter()
                                .map(|t| {
                                    Ok(Box::new(SsTableIterator::create_and_seek_to_first(
                                        t.clone(),
                                    )?))
                                })
                                .collect::<Result<Vec<_>>>()?,
                        );
                        let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.generate_new_sstables(iter, *is_lower_level_bottom_level)
                    }
                    Some(_) => {
                        let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_tables)?;
                        let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.generate_new_sstables(iter, *is_lower_level_bottom_level)
                    }
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                log::debug!(
                    "ForceFullCompaction: L0 [{:?}], L1 [{:?}]",
                    l0_sstables,
                    l1_sstables
                );
                let snapshot = self.state.read().clone();

                // Create L0+L1 iterator
                let l0_iter = MergeIterator::create(
                    Self::get_sstables(&snapshot, l0_sstables)
                        .into_iter()
                        .map(|table| {
                            Ok(Box::new(SsTableIterator::create_and_seek_to_first(table)?))
                        })
                        .collect::<Result<Vec<_>>>()?,
                );
                let l1_iter = SstConcatIterator::create_and_seek_to_first(Self::get_sstables(
                    &snapshot,
                    l1_sstables,
                ))?;
                let iter = TwoMergeIterator::create(l0_iter, l1_iter)?;

                // Create new SSTables
                Ok(self.generate_new_sstables(iter, true)?)
            }
        }
    }

    fn generate_new_sstables<
        A: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    >(
        &self,
        mut iter: TwoMergeIterator<A, B>,
        is_lower_level_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let build_new_sst = |sst_builder: SsTableBuilder| {
            let sst_id = self.next_sst_id();
            sst_builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )
        };

        let mut sstables = Vec::new();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        while iter.is_valid() {
            if is_lower_level_bottom_level {
                // Skip deleted key
                if !iter.value().is_empty() {
                    sst_builder.add(iter.key(), iter.value());
                }
            } else {
                sst_builder.add(iter.key(), iter.value());
            }
            if sst_builder.estimated_size() >= self.options.target_sst_size {
                sstables.push(Arc::new(build_new_sst(sst_builder)?));
                sst_builder = SsTableBuilder::new(self.options.block_size);
            }
            iter.next()?;
        }
        if sst_builder.num_of_entries() > 0 {
            sstables.push(Arc::new(build_new_sst(sst_builder)?));
        }
        Ok(sstables)
    }

    fn get_sstables(snapshot: &LsmStorageState, sst_ids: &[usize]) -> Vec<Arc<SsTable>> {
        sst_ids
            .iter()
            .filter_map(|i| snapshot.sstables.get(i).cloned())
            .collect()
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();
        let Some(compaction_task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };

        // Execute compaction (create new SSTables).
        let new_ssts = self.compact(&compaction_task)?;
        let new_sst_ids = &new_ssts.iter().map(|s| s.sst_id()).collect::<Vec<_>>()[..];
        log::debug!(
            "New SSTs: {:?}",
            new_ssts.iter().map(|t| t.sst_id()).collect::<Vec<_>>()
        );

        // Update the LSM state.
        let mut guard = self.state.write();
        let (mut new_state, sst_to_delete) = self.compaction_controller.apply_compaction_result(
            guard.as_ref(),
            &compaction_task,
            new_sst_ids,
        );
        for sst in &sst_to_delete {
            new_state.sstables.remove(sst);
        }
        for sst in new_ssts {
            new_state.sstables.insert(sst.sst_id(), sst);
        }
        *guard = Arc::new(new_state);
        drop(guard);

        // Deleted needless SST files.
        for sst_id in sst_to_delete {
            remove_file(self.path_of_sst(sst_id))?
        }
        {
            let guard = self.state_lock.lock();
            self.manifest.as_ref().unwrap().add_record(
                &guard,
                ManifestRecord::Compaction(compaction_task, new_sst_ids.to_vec()),
            )?;
        }
        self.sync_dir()?;
        Ok(())
    }
}

// Flush
impl LsmStorageInner {
    /// Flush the earliest memtable to the disk
    fn trigger_flush(&self) -> Result<()> {
        let snapshot = self.state.read().clone();
        let num_of_memtables = if snapshot.memtable.is_empty() {
            snapshot.imm_memtables.len()
        } else {
            snapshot.imm_memtables.len() + 1
        };
        if self.options.num_memtable_limit <= num_of_memtables {
            self.force_flush_next_imm_memtable()?
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::Builder::new()
            .name("flusher".to_string())
            .spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                            eprintln!("flush failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            })?;
        Ok(Some(handle))
    }
}
