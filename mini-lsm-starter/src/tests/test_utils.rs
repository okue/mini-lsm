use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::tests::harness::as_bytes;

#[allow(unused)]
pub(crate) fn show_iter<T: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>>(
    mut iter: T,
) -> anyhow::Result<()> {
    let mut idx = 0;
    while iter.is_valid() {
        println!("{idx}: {:?}={:?}", iter.key(), as_bytes(iter.value()));
        iter.next()?;
        idx += 1;
    }
    Ok(())
}

#[allow(unused)]
pub(crate) fn show_lsm_iter<T: for<'a> StorageIterator<KeyType<'a> = &'a [u8]>>(
    mut iter: T,
) -> anyhow::Result<()> {
    let mut idx = 0;
    while iter.is_valid() {
        println!(
            "{idx}: {:?}={:?}",
            as_bytes(iter.key()),
            as_bytes(iter.value())
        );
        iter.next()?;
        idx += 1;
    }
    Ok(())
}
