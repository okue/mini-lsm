use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::compact::CompactionTask;
use anyhow::{Context, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ManifestRecord {
    // flushed memtable id
    Flush(usize),
    // new memtable id
    NewMemtable(usize),
    // compaction task & new SST ids
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        create_dir_all(path.parent().unwrap())?;
        let file = File::options()
            .read(true)
            .create_new(true)
            .append(true)
            .open(path)
            .context(format!(
                "failed to create new manifest file: {}",
                path.display()
            ))?;
        let file = Arc::new(Mutex::new(file));
        Ok(Self { file })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = File::options()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to open manifest file on recovery")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let records = serde_json::Deserializer::from_slice(&buf)
            .into_iter::<ManifestRecord>()
            .collect::<serde_json::Result<Vec<_>>>()?;

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        file.write_all(&serde_json::to_vec(&record)?)
            .context("failed to write manifest record to the file")?;
        file.sync_all()?;
        Ok(())
    }
}

#[allow(deprecated)]
#[cfg(test)]
mod tests {
    use crate::manifest::{Manifest, ManifestRecord};
    use std::env::home_dir;

    #[ignore]
    #[test]
    fn test_create() -> anyhow::Result<()> {
        let manifest = Manifest::create(home_dir().unwrap().join("work/mini-lsm/lsm.db/MANIFEST"))?;
        manifest.add_record_when_init(ManifestRecord::Flush(1))?;
        manifest.add_record_when_init(ManifestRecord::NewMemtable(1))?;

        Ok(())
    }

    #[ignore]
    #[test]
    fn test_recovery() -> anyhow::Result<()> {
        let (_, records) =
            Manifest::recover(home_dir().unwrap().join("work/mini-lsm/lsm.db/MANIFEST"))?;
        println!("{:?}", records);
        Ok(())
    }
}
