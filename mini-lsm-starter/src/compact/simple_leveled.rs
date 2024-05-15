use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    /// lower level number of files / upper level number of files.
    /// In reality, we should compute the actual size of the files.
    /// However, we simplified the equation to use number of files to make it easier to do the simulation.
    /// When the ratio is too low (upper level has too many files), we should trigger a compaction
    pub size_ratio_percent: usize,
    /// when the number of SSTs in L0 is larger than or equal to this number, trigger a compaction of L0 and L1.
    pub level0_file_num_compaction_trigger: usize,
    /// the number of levels (excluding L0) in the LSM tree.
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.get_sst_ids(1),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }
        for (upper_level, upper) in snapshot.levels.iter() {
            if *upper_level == self.options.max_levels || upper.is_empty() {
                continue;
            }
            let lower = snapshot.get_sst_ids(upper_level + 1);
            // when lower / upper <= ratio, then trigger compaction.
            if lower.len() * 100 <= upper.len() * self.options.size_ratio_percent {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(*upper_level),
                    upper_level_sst_ids: upper.clone(),
                    lower_level: upper_level + 1,
                    lower_level_sst_ids: lower,
                    is_lower_level_bottom_level: self.options.max_levels == upper_level + 1,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        SimpleLeveledCompactionTask {
            upper_level,
            upper_level_sst_ids,
            lower_level,
            lower_level_sst_ids,
            ..
        }: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        match upper_level {
            None => {
                // When L0 compaction
                snapshot
                    .l0_sstables
                    .retain(|id| !upper_level_sst_ids.contains(id));
            }
            Some(level) => {
                // Remove upper level sst
                snapshot.levels[level - 1].1.clear()
            }
        }
        snapshot.levels[lower_level - 1].1 = output.to_vec();

        let mut sst_ids_to_delete = Vec::new();
        sst_ids_to_delete.extend(upper_level_sst_ids);
        sst_ids_to_delete.extend(lower_level_sst_ids);
        (snapshot, sst_ids_to_delete)
    }
}
