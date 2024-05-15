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
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTableBuilder, SsTableIterator};

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
    #[allow(dead_code)]
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    #[allow(dead_code)]
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
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

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<()> {
        match task {
            CompactionTask::Leveled(_task) => {
                unimplemented!()
            }
            CompactionTask::Tiered(_task) => {
                unimplemented!()
            }
            CompactionTask::Simple(_task) => {
                unimplemented!()
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let snapshot = self.state.read().clone();
                let mut iter = {
                    let l0_iters = l0_sstables
                        .iter()
                        .filter_map(|i| snapshot.sstables.get(i).cloned())
                        .map(|table| {
                            Ok(Box::new(SsTableIterator::create_and_seek_to_first(table)?))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let l0_iter = MergeIterator::create(l0_iters);
                    let l1_iters = l1_sstables
                        .iter()
                        .filter_map(|i| snapshot.sstables.get(i).cloned())
                        .collect::<Vec<_>>();
                    let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_iters)?;
                    TwoMergeIterator::create(l0_iter, l1_iter)?
                };

                let sstables = {
                    let mut sstables = Vec::new();
                    let mut sst_builder = SsTableBuilder::new(self.options.block_size);
                    while iter.is_valid() {
                        // Skip deleted key
                        if !iter.value().is_empty() {
                            sst_builder.add(iter.key(), iter.value());
                        }
                        if sst_builder.estimated_size() >= self.options.target_sst_size {
                            let sst_id = self.next_sst_id();
                            let sst = sst_builder.build(
                                sst_id,
                                Some(self.block_cache.clone()),
                                self.path_of_sst(sst_id),
                            )?;
                            sstables.push(sst);
                            sst_builder = SsTableBuilder::new(self.options.block_size);
                        }
                        iter.next()?;
                    }
                    if sst_builder.num_of_entries() > 0 {
                        let sst_id = self.next_sst_id();
                        let sst = sst_builder.build(
                            sst_id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(sst_id),
                        )?;
                        sstables.push(sst);
                    }
                    sstables
                };

                {
                    let mut guard = self.state.write();
                    let mut snapshot = guard.as_ref().clone();
                    if let Some((_, sst_ids)) = snapshot.levels.get_mut(0) {
                        let ids = sstables.iter().map(|t| t.sst_id()).collect::<Vec<_>>();
                        *sst_ids = ids;
                    }
                    snapshot.l0_sstables.retain(|e| !l0_sstables.contains(e));
                    for table in sstables {
                        snapshot.sstables.insert(table.sst_id(), Arc::new(table));
                    }
                    *guard = Arc::new(snapshot);
                };
                // TODO: deleted unreachable SST files.
                Ok(())
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();
        self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: snapshot.l0_sstables.clone(),
            l1_sstables: snapshot
                .levels
                .first()
                .cloned()
                .map(|l| l.1)
                .unwrap_or(Vec::default()),
        })?;
        self.sync_dir()?;
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

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
