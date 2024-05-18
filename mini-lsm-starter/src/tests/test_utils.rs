use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::tests::harness::as_bytes;

#[allow(unused)]
pub(crate) fn show_iter<T: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>>(
    mut iter: T,
) -> anyhow::Result<()> {
    while iter.is_valid() {
        println!("{:?}={:?}", iter.key(), as_bytes(iter.value()));
        iter.next()?;
    }
    Ok(())
}
