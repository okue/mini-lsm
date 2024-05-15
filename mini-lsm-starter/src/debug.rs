use crate::lsm_storage::{LsmStorageInner, LsmStorageOptions, LsmStorageState, MiniLsm};
use std::fmt::Write;

impl LsmStorageState {
    pub fn dump_structure(&self, options: Option<&LsmStorageOptions>) -> anyhow::Result<()> {
        let mut message = String::new();

        writeln!(message, "dump structure...")?;
        writeln!(
            message,
            "memtable: {} KB",
            &self.memtable.approximate_size() >> 10
        )?;

        let ids = self
            .imm_memtables
            .iter()
            .map(|m| m.id())
            .collect::<Vec<usize>>();
        writeln!(
            message,
            "imm memtable ({}): {:?}",
            self.imm_memtables.len(),
            ids
        )?;

        writeln!(
            message,
            "L0 ({}): {:?}",
            self.l0_sstables.len(),
            self.l0_sstables,
        )?;
        for (level, files) in &self.levels {
            writeln!(message, "L{level} ({}): {:?}", files.len(), files)?;
        }

        let mut sstables = self.sstables.values().cloned().collect::<Vec<_>>();
        sstables.sort_by_key(|sst| sst.sst_id());
        for sst in sstables {
            writeln!(
                message,
                "sst_id: {} [{:?} ~ {:?}]",
                sst.sst_id(),
                sst.first_key().inner(),
                sst.last_key().inner()
            )?
        }

        if let Some(options) = options {
            writeln!(message, "Options: {:?}", options)?;
        }

        log::info!("{}", message);
        Ok(())
    }
}

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        let _ = snapshot.dump_structure(Some(&self.options));
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
