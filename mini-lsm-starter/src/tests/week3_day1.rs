use std::sync::Arc;

use crate::iterators::merge_iterator::MergeIterator;
use bytes::Bytes;
use tempfile::tempdir;

use crate::key::KeySlice;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};
use crate::tests::test_utils::show_iter;

use super::harness::{check_iter_result_by_key_and_ts, generate_sst_with_ts};

#[test]
fn test_sst_build_multi_version_simple() {
    let mut builder = SsTableBuilder::new(16);
    builder.add(
        KeySlice::for_testing_from_slice_with_ts(b"233", 233),
        b"233333",
    );
    builder.add(
        KeySlice::for_testing_from_slice_with_ts(b"233", 0),
        b"2333333",
    );
    let dir = tempdir().unwrap();
    builder.build_for_test(dir.path().join("1.sst")).unwrap();
}

fn generate_test_data(modulo: u64, until: u64) -> Vec<((Bytes, u64), Bytes)> {
    (0..until)
        .map(|id| {
            (
                (
                    Bytes::from(format!("key{:05}", id / modulo)),
                    5 - (id % modulo),
                ),
                Bytes::from(format!("value{:05}", id)),
            )
        })
        .collect()
}

#[test]
fn test_sst_build_multi_version_hard() {
    let dir = tempdir().unwrap();
    let data = generate_test_data(5, 100);
    generate_sst_with_ts(1, dir.path().join("1.sst"), data.clone(), None);
    let sst = Arc::new(
        SsTable::open(
            1,
            None,
            FileObject::open(&dir.path().join("1.sst")).unwrap(),
        )
        .unwrap(),
    );
    check_iter_result_by_key_and_ts(
        &mut SsTableIterator::create_and_seek_to_first(sst).unwrap(),
        data,
    );
}

#[test]
fn test_merge_iter() -> anyhow::Result<()> {
    let dir = tempdir().unwrap();
    let generate_sst = |modulo: u64, idx: usize| {
        let data = generate_test_data(modulo, 10);
        let file_path = dir.path().join(format!("{idx}.sst"));
        let sst = generate_sst_with_ts(1, file_path, data.clone(), None);
        Arc::new(sst)
    };
    let sst1 = generate_sst(5, 1);
    let sst2 = generate_sst(3, 2);

    println!("sst 1");
    show_iter(SsTableIterator::create_and_seek_to_first(sst1.clone())?)?;

    println!("sst 2");
    show_iter(SsTableIterator::create_and_seek_to_first(sst2.clone())?)?;

    println!("sst 1+2");
    show_iter(MergeIterator::create(vec![
        Box::new(SsTableIterator::create_and_seek_to_first(sst1.clone())?),
        Box::new(SsTableIterator::create_and_seek_to_first(sst2.clone())?),
    ]))?;

    Ok(())
}
