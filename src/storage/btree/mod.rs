use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

mod iter;
mod layout;
mod mvcc;
pub mod page;
mod page_cache;
mod pager;

pub use iter::BTreeRangeIter;

use self::mvcc::MvccState;
use self::pager::{BTreeStore, PageRead, Pager, PinnedPageMut};
use crate::core::errors::{StorageError, WrongoDBError};
use crate::core::lock_stats::{begin_lock_hold, record_lock_wait, LockStatKind};
use crate::storage::block::file::NONE_BLOCK_ID;
use crate::storage::wal::GlobalWal;
use crate::txn::{GlobalTxnState, TxnId, UpdateType, TXN_NONE};
use layout::{
    build_internal_page, internal_entries, leaf_entries, map_internal_err, map_leaf_err, page_type,
    split_internal_entries, split_leaf_entries, PageType,
};
use page::{InternalPage, LeafPage, LeafPageError};

// Type aliases for B-tree operations to clarify intent and reduce complexity

/// Represents a key in the B-tree (stored as bytes)
type Key = Vec<u8>;

/// Represents a value in the B-tree (stored as bytes)
type Value = Vec<u8>;

/// A key-value pair for leaf node entries
type KeyValuePair = (Key, Value);

/// A key-child ID pair for internal node separators
type KeyChildId = (Key, u64);

/// Collection of key-value pairs from a leaf page
type LeafEntries = Vec<KeyValuePair>;

/// Internal page entries: (first_child_id, separators as key-child pairs)
type InternalEntries = (u64, Vec<KeyChildId>);

/// Iterator over key-value pairs, yielding results or errors
type KeyValueIter<'a> = BTreeRangeIter<'a>;

#[derive(Debug)]
pub struct BTree {
    pager: Box<dyn BTreeStore>,
    global_wal: Option<Arc<Mutex<GlobalWal>>>,
    wal_enabled: bool,
    wal_store_name: String,
    mvcc: MvccState,
}

impl BTree {
    /// Create a new BTree with WAL enabled or disabled.
    pub fn create<P: AsRef<Path>>(
        path: P,
        page_size: usize,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
    ) -> Result<Self, WrongoDBError> {
        Self::create_with_global_wal(path, page_size, wal_enabled, global_txn, None)
    }

    pub(crate) fn create_with_global_wal<P: AsRef<Path>>(
        path: P,
        page_size: usize,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
        global_wal: Option<Arc<Mutex<GlobalWal>>>,
    ) -> Result<Self, WrongoDBError> {
        let mut pager = Pager::create(path.as_ref(), page_size)?;
        init_root_if_missing(&mut pager)?;
        pager.checkpoint()?;
        let store_name = store_name_from_path(path.as_ref())?;
        Ok(Self {
            pager: Box::new(pager),
            global_wal,
            wal_enabled,
            wal_store_name: store_name,
            mvcc: MvccState::new(global_txn),
        })
    }

    /// Open an existing BTree with WAL enabled or disabled.
    pub fn open<P: AsRef<Path>>(
        path: P,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
    ) -> Result<Self, WrongoDBError> {
        Self::open_with_global_wal(path, wal_enabled, global_txn, None)
    }

    pub(crate) fn open_with_global_wal<P: AsRef<Path>>(
        path: P,
        wal_enabled: bool,
        global_txn: Arc<GlobalTxnState>,
        global_wal: Option<Arc<Mutex<GlobalWal>>>,
    ) -> Result<Self, WrongoDBError> {
        let mut pager = Pager::open(path.as_ref())?;
        init_root_if_missing(&mut pager)?;
        let store_name = store_name_from_path(path.as_ref())?;
        Ok(Self {
            pager: Box::new(pager),
            global_wal,
            wal_enabled,
            wal_store_name: store_name,
            mvcc: MvccState::new(global_txn),
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let mut node_id = self.pager.root_page_id();
        if node_id == NONE_BLOCK_ID {
            return Ok(None);
        }

        loop {
            let mut page = self.pager.pin_page(node_id)?;
            let page_id = page.page_id();
            let page_type = match page_type(page.payload()) {
                Ok(t) => t,
                Err(e) => {
                    self.pager.unpin_page(page_id);
                    return Err(e);
                }
            };
            match page_type {
                PageType::Leaf => {
                    let leaf = match LeafPage::open(page.payload_mut()) {
                        Ok(leaf) => leaf,
                        Err(e) => {
                            self.pager.unpin_page(page_id);
                            return Err(StorageError(format!("corrupt leaf {node_id}: {e}")).into());
                        }
                    };
                    let result = leaf.get(key).map_err(map_leaf_err);
                    self.pager.unpin_page(page_id);
                    return result;
                }
                PageType::Internal => {
                    let internal = match InternalPage::open(page.payload_mut()) {
                        Ok(internal) => internal,
                        Err(e) => {
                            self.pager.unpin_page(page_id);
                            return Err(
                                StorageError(format!("corrupt internal {node_id}: {e}")).into()
                            );
                        }
                    };
                    node_id = match internal
                        .child_for_key(key)
                        .map_err(|e| StorageError(format!("routing failed at {node_id}: {e}")))
                    {
                        Ok(id) => id,
                        Err(e) => {
                            self.pager.unpin_page(page_id);
                            return Err(e.into());
                        }
                    };
                    self.pager.unpin_page(page_id);
                }
            }
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        self.put_with_txn(key, value, TXN_NONE, true)
    }

    pub(crate) fn put_with_txn(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
        log_wal: bool,
    ) -> Result<(), WrongoDBError> {
        let root = self.pager.root_page_id();
        if root == NONE_BLOCK_ID {
            return Err(StorageError("btree missing root".into()).into());
        }

        let result = self.insert_recursive(root, key, value, InsertMode::Upsert)?;
        if let Some(split) = result.split {
            let payload_len = self.pager.page_payload_len();
            let mut root_internal_bytes = vec![0u8; payload_len];
            {
                let mut internal = InternalPage::init(&mut root_internal_bytes, result.new_node_id)
                    .map_err(|e| StorageError(format!("init new root internal failed: {e}")))?;
                internal
                    .put_separator(&split.sep_key, split.right_child)
                    .map_err(map_internal_err)?;
            }

            let new_root_id = self.pager.write_new_page(&root_internal_bytes)?;
            self.pager.set_root_page_id(new_root_id)?;
        } else {
            self.pager.set_root_page_id(result.new_node_id)?;
        }

        if log_wal {
            self.log_wal_put(key, value, txn_id)?;
        }

        Ok(())
    }

    pub fn insert_unique(&mut self, key: &[u8], value: &[u8]) -> Result<bool, WrongoDBError> {
        let root = self.pager.root_page_id();
        if root == NONE_BLOCK_ID {
            return Err(StorageError("btree missing root".into()).into());
        }

        let result = self.insert_recursive(root, key, value, InsertMode::Unique)?;
        if !result.inserted {
            return Ok(false);
        }

        if let Some(split) = result.split {
            let payload_len = self.pager.page_payload_len();
            let mut root_internal_bytes = vec![0u8; payload_len];
            {
                let mut internal = InternalPage::init(&mut root_internal_bytes, result.new_node_id)
                    .map_err(|e| StorageError(format!("init new root internal failed: {e}")))?;
                internal
                    .put_separator(&split.sep_key, split.right_child)
                    .map_err(map_internal_err)?;
            }

            let new_root_id = self.pager.write_new_page(&root_internal_bytes)?;
            self.pager.set_root_page_id(new_root_id)?;
        } else {
            self.pager.set_root_page_id(result.new_node_id)?;
        }

        self.log_wal_put(key, value, TXN_NONE)?;

        Ok(true)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool, WrongoDBError> {
        self.delete_with_txn(key, TXN_NONE, true)
    }

    pub(crate) fn delete_with_txn(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
        log_wal: bool,
    ) -> Result<bool, WrongoDBError> {
        let root = self.pager.root_page_id();
        if root == NONE_BLOCK_ID {
            return Ok(false);
        }

        let result = self.delete_recursive(root, key)?;
        self.pager.set_root_page_id(result.new_node_id)?;

        if log_wal {
            self.log_wal_delete(key, txn_id)?;
        }

        Ok(result.deleted)
    }

    pub fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.pager.sync_all()
    }

    /// Explicitly checkpoint the B+tree.
    ///
    /// This flushes all dirty pages to disk and atomically swaps the root.
    /// After this returns, all previous mutations are durable.
    ///
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.materialize_committed_updates()?;
        let root = self.pager.checkpoint_prepare();
        self.pager.checkpoint_flush_data()?;
        self.pager.checkpoint_commit(root)?;
        Ok(())
    }

    /// Run garbage collection on MVCC update chains.
    ///
    /// Removes obsolete updates that are no longer visible to any active transaction.
    /// Returns (chains_cleaned, updates_removed, chains_dropped).
    pub fn run_gc(&mut self) -> (usize, usize, usize) {
        self.mvcc.run_gc()
    }

    pub fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<KeyValueIter<'_>, WrongoDBError> {
        let root = self.pager.root_page_id();
        if root == NONE_BLOCK_ID {
            return Ok(BTreeRangeIter::empty());
        }
        BTreeRangeIter::new(self.pager.as_mut() as &mut dyn PageRead, root, start, end)
    }

    pub fn mvcc_keys_in_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<Vec<u8>> {
        self.mvcc.keys_in_range(start, end)
    }

    pub fn materialize_committed_updates(&mut self) -> Result<(), WrongoDBError> {
        let entries = self.mvcc.latest_committed_entries();
        if entries.is_empty() {
            return Ok(());
        }

        for (key, update_type, data) in entries {
            match update_type {
                UpdateType::Standard => {
                    self.put_with_txn(&key, &data, TXN_NONE, false)?;
                }
                UpdateType::Tombstone => {
                    let _ = self.delete_with_txn(&key, TXN_NONE, false)?;
                }
                UpdateType::Reserve => {}
            }
        }
        Ok(())
    }

    /// Delete a key from the subtree rooted at `node_id`.
    ///
    /// Returns:
    /// - `Ok(DeleteResult)` with the new subtree root id and a flag indicating deletion.
    ///   No merge/borrow is performed; empty pages may remain.
    fn delete_recursive(
        &mut self,
        node_id: u64,
        key: &[u8],
    ) -> Result<DeleteResult, WrongoDBError> {
        let mut page = self.pager.pin_page_mut(node_id)?;
        let page_type = match page_type(page.payload()) {
            Ok(t) => t,
            Err(e) => {
                self.pager.unpin_page_mut_abort(page)?;
                return Err(e);
            }
        };
        let result = match page_type {
            PageType::Leaf => self.delete_from_leaf(node_id, &mut page, key),
            PageType::Internal => self.delete_from_internal(&mut page, key),
        };
        match result {
            Ok(ok) => {
                self.pager.unpin_page_mut_commit(page)?;
                Ok(ok)
            }
            Err(err) => {
                self.pager.unpin_page_mut_abort(page)?;
                Err(err)
            }
        }
    }

    fn delete_from_leaf(
        &mut self,
        _node_id: u64,
        page: &mut PinnedPageMut,
        key: &[u8],
    ) -> Result<DeleteResult, WrongoDBError> {
        let page_id = page.page_id();
        let deleted = {
            let mut leaf = LeafPage::open(page.payload_mut())
                .map_err(|e| StorageError(format!("corrupt leaf {page_id}: {e}")))?;
            leaf.delete(key).map_err(map_leaf_err)?
        };

        Ok(DeleteResult {
            new_node_id: page_id,
            deleted,
        })
    }

    fn delete_from_internal(
        &mut self,
        page: &mut PinnedPageMut,
        key: &[u8],
    ) -> Result<DeleteResult, WrongoDBError> {
        let payload_len = page.payload().len();
        let page_id = page.page_id();
        let (mut first_child, mut entries) = internal_entries(page.payload_mut())?;
        let child_idx = child_index_for_key(&entries, key);
        let child_id = if child_idx == 0 {
            first_child
        } else {
            entries[child_idx - 1].1
        };

        let child_result = self.delete_recursive(child_id, key)?;
        if child_idx == 0 {
            first_child = child_result.new_node_id;
        } else {
            entries[child_idx - 1].1 = child_result.new_node_id;
        }

        let bytes = build_internal_page(first_child, &entries, payload_len)?;
        page.payload_mut().copy_from_slice(&bytes);

        Ok(DeleteResult {
            new_node_id: page_id,
            deleted: child_result.deleted,
        })
    }

    /// Insert into the subtree rooted at `node_id`.
    ///
    /// Returns:
    /// - `Ok(InsertResult)` with the new subtree root id and an optional split.
    ///   If `split` is `Some`, the caller must insert `(split.sep_key -> split.right_child)`
    ///   into its parent, and `new_node_id` is the left sibling's id.
    ///
    /// Separator invariant (Slice D/E):
    /// - For any split producing left+right siblings, `sep_key` is the **minimum key** in the
    ///   right sibling.
    ///
    /// Graphically:
    /// ```text
    /// parent inserts:  [ ... | sep_key -> right_child | ... ]
    ///                      ^
    ///                      min key of right sibling
    /// ```
    fn insert_recursive(
        &mut self,
        node_id: u64,
        key: &[u8],
        value: &[u8],
        mode: InsertMode,
    ) -> Result<InsertResult, WrongoDBError> {
        let mut page = self.pager.pin_page_mut(node_id)?;
        let page_type = match page_type(page.payload()) {
            Ok(t) => t,
            Err(e) => {
                self.pager.unpin_page_mut_abort(page)?;
                return Err(e);
            }
        };
        let result = match page_type {
            PageType::Leaf => self.insert_into_leaf(node_id, &mut page, key, value, mode),
            PageType::Internal => self.insert_into_internal(node_id, &mut page, key, value, mode),
        };
        match result {
            Ok(ok) => {
                if ok.inserted {
                    self.pager.unpin_page_mut_commit(page)?;
                } else {
                    self.pager.unpin_page_mut_abort(page)?;
                }
                Ok(ok)
            }
            Err(err) => {
                self.pager.unpin_page_mut_abort(page)?;
                Err(err)
            }
        }
    }

    pub(super) fn log_wal_put(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(global_wal) = self.global_wal.as_ref() {
            let wait_start = Instant::now();
            let mut wal = global_wal.lock();
            record_lock_wait(LockStatKind::Wal, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::Wal);
            wal.log_put(&self.wal_store_name, key, value, txn_id)?;
        }
        Ok(())
    }

    pub(super) fn log_wal_delete(
        &mut self,
        key: &[u8],
        txn_id: TxnId,
    ) -> Result<(), WrongoDBError> {
        if !self.wal_enabled {
            return Ok(());
        }
        if let Some(global_wal) = self.global_wal.as_ref() {
            let wait_start = Instant::now();
            let mut wal = global_wal.lock();
            record_lock_wait(LockStatKind::Wal, wait_start.elapsed());
            let _hold = begin_lock_hold(LockStatKind::Wal);
            wal.log_delete(&self.wal_store_name, key, txn_id)?;
        }
        Ok(())
    }

    /// Insert into a leaf page, splitting if it overflows.
    ///
    /// Leaf split is done by rebuilding two leaf pages from the post-insert sorted entry list:
    /// ```text
    /// entries (sorted):  [ e0 e1 e2 e3 e4 e5 ]
    ///                         ^
    ///                      split_idx
    /// left  = [ e0 .. e{split_idx-1} ]
    /// right = [ e{split_idx} .. end ]
    /// sep_key = first_key(right)
    /// ```
    fn insert_into_leaf(
        &mut self,
        node_id: u64,
        page: &mut PinnedPageMut,
        key: &[u8],
        value: &[u8],
        mode: InsertMode,
    ) -> Result<InsertResult, WrongoDBError> {
        let payload_len = page.payload().len();
        let page_id = page.page_id();
        {
            let mut leaf = LeafPage::open(page.payload_mut())
                .map_err(|e| StorageError(format!("corrupt leaf {node_id}: {e}")))?;
            if mode == InsertMode::Unique && leaf.contains_key(key).map_err(map_leaf_err)? {
                return Ok(InsertResult {
                    new_node_id: page_id,
                    split: None,
                    inserted: false,
                });
            }
            match leaf.put(key, value) {
                Ok(()) => {
                    return Ok(InsertResult {
                        new_node_id: page_id,
                        split: None,
                        inserted: true,
                    });
                }
                Err(LeafPageError::PageFull) => { /* split below */ }
                Err(e) => return Err(map_leaf_err(e)),
            }
        }

        let mut entries = leaf_entries(page.payload_mut())?;
        upsert_entry(&mut entries, key, value);
        let (left_bytes, right_bytes, split_key, _split_idx) =
            split_leaf_entries(&entries, payload_len)?;

        // Allocate right sibling first
        let right_leaf_id = self.pager.write_new_page(&right_bytes)?;

        page.payload_mut().copy_from_slice(&left_bytes);
        Ok(InsertResult {
            new_node_id: page_id,
            split: Some(SplitInfo {
                sep_key: split_key,
                right_child: right_leaf_id,
            }),
            inserted: true,
        })
    }

    /// Insert into an internal page.
    ///
    /// Steps:
    /// 1) Route to the appropriate child and recurse.
    /// 2) If the child split, attempt to insert the child's separator into this internal page.
    /// 3) If this internal page overflows, split it and return a promoted separator to the parent.
    ///
    /// Internal separator semantics:
    /// - The page stores `(sep_key_i -> child_{i+1})` plus a `first_child = child_0`.
    /// - `sep_key_i` is the **minimum key** of `child_{i+1}`.
    fn insert_into_internal(
        &mut self,
        _node_id: u64,
        page: &mut PinnedPageMut,
        key: &[u8],
        value: &[u8],
        mode: InsertMode,
    ) -> Result<InsertResult, WrongoDBError> {
        let payload_len = page.payload().len();
        let page_id = page.page_id();
        let (mut first_child, mut entries) = internal_entries(page.payload_mut())?;
        let child_idx = child_index_for_key(&entries, key);
        let child_id = if child_idx == 0 {
            first_child
        } else {
            entries[child_idx - 1].1
        };

        let child_result = self.insert_recursive(child_id, key, value, mode)?;
        if !child_result.inserted {
            return Ok(InsertResult {
                new_node_id: page_id,
                split: None,
                inserted: false,
            });
        }
        if child_idx == 0 {
            first_child = child_result.new_node_id;
        } else {
            entries[child_idx - 1].1 = child_result.new_node_id;
        }

        if let Some(split) = child_result.split {
            upsert_internal_entry(&mut entries, &split.sep_key, split.right_child);
        }

        if let Ok(bytes) = build_internal_page(first_child, &entries, payload_len) {
            page.payload_mut().copy_from_slice(&bytes);
            return Ok(InsertResult {
                new_node_id: page_id,
                split: None,
                inserted: true,
            });
        }

        let (
            left_bytes,
            right_bytes,
            promoted_key,
            _left_first_child,
            _left_separators,
            _promote_idx,
        ) = split_internal_entries(first_child, &entries, payload_len)?;

        // Allocate right sibling first
        let right_internal_id = self.pager.write_new_page(&right_bytes)?;

        page.payload_mut().copy_from_slice(&left_bytes);
        Ok(InsertResult {
            new_node_id: page_id,
            split: Some(SplitInfo {
                sep_key: promoted_key,
                right_child: right_internal_id,
            }),
            inserted: true,
        })
    }
}

#[derive(Debug, Clone)]
/// A split propagated upward during recursive insert.
///
/// The caller must insert `sep_key -> right_child` into its internal page.
///
/// `sep_key` is the minimum key in `right_child` (the new right sibling).
struct SplitInfo {
    sep_key: Vec<u8>,
    right_child: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InsertMode {
    Upsert,
    Unique,
}

#[derive(Debug, Clone)]
struct InsertResult {
    new_node_id: u64,
    split: Option<SplitInfo>,
    inserted: bool,
}

#[derive(Debug, Clone)]
struct DeleteResult {
    new_node_id: u64,
    deleted: bool,
}

fn init_root_if_missing(pager: &mut dyn BTreeStore) -> Result<(), WrongoDBError> {
    if pager.root_page_id() != NONE_BLOCK_ID {
        return Ok(());
    }
    let payload_len = pager.page_payload_len();
    let mut leaf_bytes = vec![0u8; payload_len];
    LeafPage::init(&mut leaf_bytes).map_err(map_leaf_err)?;
    let leaf_id = pager.write_new_page(&leaf_bytes)?;
    pager.set_root_page_id(leaf_id)?;
    Ok(())
}

fn store_name_from_path(path: &Path) -> Result<String, WrongoDBError> {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return Err(StorageError(format!(
            "unable to determine store name for path: {}",
            path.display()
        ))
        .into());
    };
    Ok(name.to_string())
}

fn upsert_entry(entries: &mut Vec<(Vec<u8>, Vec<u8>)>, key: &[u8], value: &[u8]) {
    match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
        Ok(i) => entries[i].1 = value.to_vec(),
        Err(i) => entries.insert(i, (key.to_vec(), value.to_vec())),
    }
}

fn upsert_internal_entry(entries: &mut Vec<(Vec<u8>, u64)>, key: &[u8], child: u64) {
    match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
        Ok(i) => entries[i].1 = child,
        Err(i) => entries.insert(i, (key.to_vec(), child)),
    }
}

fn child_index_for_key(entries: &[(Vec<u8>, u64)], key: &[u8]) -> usize {
    let mut idx = 0;
    for (i, (sep_key, _)) in entries.iter().enumerate() {
        if key < sep_key.as_slice() {
            break;
        }
        idx = i + 1;
    }
    idx
}
