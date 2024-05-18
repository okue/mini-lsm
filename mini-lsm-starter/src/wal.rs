use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::options().create_new(true).append(true).open(path)?;
        let file = BufWriter::new(file);
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skip_map: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = File::options().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut buf = &buf[..];
        while buf.has_remaining() {
            let key_len = buf.get_u16();
            let key = buf.copy_to_bytes(key_len as usize);
            let val_len = buf.get_u16();
            let val = buf.copy_to_bytes(val_len as usize);
            skip_map.insert(key, val);
        }

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Format:
        // | key_len | key | value_len | value |
        let mut buf = Vec::<u8>::new();
        buf.put_u16(key.len() as u16);
        buf.put_slice(key);
        buf.put_u16(value.len() as u16);
        buf.put_slice(value);
        {
            let mut writer = self.file.lock();
            writer.write_all(&buf)?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use crate::wal::Wal;
    use crossbeam_skiplist::SkipMap;
    use std::env::home_dir;

    #[ignore]
    #[test]
    fn test_recovery() -> anyhow::Result<()> {
        let wal = Wal::create(home_dir().unwrap().join("work/mini-lsm/lsm.db/0.wal"))?;
        for i in 0..10 {
            wal.put(i.to_string().as_bytes(), format!("value_{}", i).as_bytes())?;
        }
        wal.sync()?;

        let map = SkipMap::new();
        let _wal = Wal::recover(home_dir().unwrap().join("work/mini-lsm/lsm.db/0.wal"), &map)?;
        for (k, v) in map {
            println!("{:?}={:?}", k, v)
        }
        Ok(())
    }
}
