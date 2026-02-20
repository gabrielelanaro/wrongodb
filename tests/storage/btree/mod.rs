use std::path::Path;

use wrongodb::{
    BTree, LeafPage, PageStore, PageStoreTrait, StorageError, WrongoDBError, NONE_BLOCK_ID,
};

mod checkpoint;
mod cow;
mod multi_level_range;
mod recovery;
mod split_root;
mod wal;

pub(super) fn create_tree(path: &Path, page_size: usize) -> Result<BTree, WrongoDBError> {
    let mut page_store = PageStore::create(path, page_size)?;
    init_root_if_missing(&mut page_store)?;
    page_store.checkpoint()?;
    Ok(BTree::new(Box::new(page_store)))
}

pub(super) fn open_tree(path: &Path) -> Result<BTree, WrongoDBError> {
    let mut page_store = PageStore::open(path)?;
    init_root_if_missing(&mut page_store)?;
    Ok(BTree::new(Box::new(page_store)))
}

fn init_root_if_missing(page_store: &mut dyn PageStoreTrait) -> Result<(), WrongoDBError> {
    if page_store.root_page_id() != NONE_BLOCK_ID {
        return Ok(());
    }

    let payload_len = page_store.page_payload_len();
    let mut leaf_bytes = vec![0u8; payload_len];
    LeafPage::init(&mut leaf_bytes)
        .map_err(|e| StorageError(format!("init root leaf failed: {e}")))?;
    let leaf_id = page_store.write_new_page(&leaf_bytes)?;
    page_store.set_root_page_id(leaf_id)?;
    Ok(())
}
