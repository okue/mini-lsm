use crate::lsm_storage::{LsmStorageInner, MiniLsm};

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();

        println!(
            "memtable: {} KB",
            &snapshot.memtable.approximate_size() >> 10
        );

        let ids = snapshot
            .imm_memtables
            .iter()
            .map(|m| m.id())
            .collect::<Vec<usize>>();
        println!("imm memtable ({}): {:?}", snapshot.imm_memtables.len(), ids);

        println!(
            "L0 ({}): {:?}",
            snapshot.l0_sstables.len(),
            snapshot.l0_sstables,
        );
        for (level, files) in &snapshot.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
        println!("Options: {:?}", self.options);
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }
}
