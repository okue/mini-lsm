use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

#[derive(Default)]
pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.entry(ts).and_modify(|c| *c += 1).or_insert(1);
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let entry = self.readers.entry(ts).and_modify(|c| {
            *c = c.saturating_sub(1);
        });
        if let Entry::Occupied(e) = entry {
            if *e.get() == 0 {
                e.remove();
            }
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.keys().min().cloned()
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}
