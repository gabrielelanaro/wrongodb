use std::path::Path;

pub mod page;
mod pager;
mod layout;
mod iter;
pub mod wal;

pub use iter::BTreeRangeIter;

use layout::{
    build_internal_page, internal_entries, leaf_entries, map_internal_err, map_leaf_err, page_type,
    split_internal_entries, split_leaf_entries, PageType,
};
use self::pager::{PageStore, Pager, PinnedPageMut};
use self::wal::WalFile;
use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::block::file::NONE_BLOCK_ID;
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
    pager: Box<dyn PageStore>,
}

impl BTree {
    pub fn create<P: AsRef<Path>>(path: P, page_size: usize, wal_enabled: bool) -> Result<Self, WrongoDBError> {
        let mut pager = Pager::create(path, page_size, wal_enabled)?;
        init_root_if_missing(&mut pager)?;
        pager.checkpoint()?;
        Ok(Self {
            pager: Box::new(pager),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P, wal_enabled: bool) -> Result<Self, WrongoDBError> {
        let mut pager = Pager::open(path, wal_enabled)?;
        init_root_if_missing(&mut pager)?;
        let mut tree = Self {
            pager: Box::new(pager),
        };

        if wal_enabled {
            if let Err(e) = tree.recover_from_wal() {
                eprintln!("WAL recovery failed: {}. Database may be inconsistent.", e);
                // Continue anyway - corrupted WAL shouldn't prevent database open
            }
        }

        Ok(tree)
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
                                StorageError(format!("corrupt internal {node_id}: {e}")).into(),
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
        let root = self.pager.root_page_id();
        if root == NONE_BLOCK_ID {
            return Err(StorageError("btree missing root".into()).into());
        }

        let result = self.insert_recursive(root, key, value)?;
        if let Some(split) = result.split {
            let payload_len = self.pager.page_payload_len();
            let mut root_internal_bytes = vec![0u8; payload_len];
            {
                let mut internal =
                    InternalPage::init(&mut root_internal_bytes, result.new_node_id)
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

        self.log_wal_if_enabled(|w| w.log_put(key, value))?;

        // Auto-checkpoint if threshold reached
        if self.pager.checkpoint_requested() {
            self.checkpoint()?;
        }

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool, WrongoDBError> {
        let root = self.pager.root_page_id();
        if root == NONE_BLOCK_ID {
            return Ok(false);
        }

        let result = self.delete_recursive(root, key)?;
        self.pager.set_root_page_id(result.new_node_id)?;

        self.log_wal_if_enabled(|w| w.log_delete(key))?;

        if self.pager.checkpoint_requested() {
            self.checkpoint()?;
        }

        Ok(result.deleted)
    }

    /// Request automatic checkpointing after N updates.
    ///
    /// Once the threshold is reached, `put()` will automatically call `checkpoint()`
    /// after the operation completes. This provides durability guarantees without
    /// manual checkpoint management.
    ///
    /// # Example
    /// ```no_run
    /// # use wrongodb::BTree;
    /// # let mut tree = BTree::create("/tmp/db", 4096, false).unwrap();
    /// tree.request_checkpoint_after_updates(100);  // Auto-checkpoint every 100 puts
    /// ```
    pub fn request_checkpoint_after_updates(&mut self, count: usize) {
        self.pager.request_checkpoint_after_updates(count);
    }

    pub fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.pager.sync_all()
    }

    /// Explicitly checkpoint the B+tree.
    ///
    /// This flushes all dirty pages to disk and atomically swaps the root.
    /// After this returns, all previous mutations are durable.
    ///
    /// Note: This is called automatically if `request_checkpoint_after_updates`
    /// is configured and the threshold is reached.
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        // Log checkpoint record BEFORE preparing checkpoint (if WAL is enabled)
        let root = self.pager.checkpoint_prepare();

        // Only log checkpoint if WAL is enabled
        if self.pager.wal().is_some() {
            if let Ok(wal) = self.pager.wal_mut() {
                // Get current generation (simplified - using 0 for now)
                let generation = 0;

                // Log checkpoint record
                let checkpoint_lsn = wal.log_checkpoint(root, generation)?;

                // Update checkpoint_lsn in WAL header so recovery knows where to start
                wal.set_checkpoint_lsn(checkpoint_lsn)?;

                // Sync WAL to ensure checkpoint record is durable
                wal.sync()?;
            }
        }

        self.pager.checkpoint_flush_data()?;
        self.pager.checkpoint_commit(root)?;

        // Truncate WAL after successful checkpoint to reclaim space
        if self.pager.wal().is_some() {
            if let Ok(wal) = self.pager.wal_mut() {
                wal.truncate_to_checkpoint()?;
            }
        }

        Ok(())
    }

    /// Configure WAL batch sync threshold (sync every N operations).
    ///
    /// When configured, the WAL will be synced to disk every N operations
    /// instead of on every operation. This improves performance at the cost
    /// of potentially losing up to N operations on a crash.
    ///
    /// # Example
    /// ```no_run
    /// # use wrongodb::BTree;
    /// # let mut tree = BTree::create("/tmp/db", 4096, true).unwrap();
    /// tree.set_wal_sync_threshold(100);  // Sync every 100 operations
    /// ```
    pub fn set_wal_sync_threshold(&mut self, threshold: usize) {
        self.pager.set_wal_sync_threshold(threshold);
    }

    /// Force WAL sync to disk.
    ///
    /// This ensures all pending WAL records are written to stable storage.
    /// Call this explicitly if you need durability before a checkpoint.
    pub fn sync_wal(&mut self) -> Result<(), WrongoDBError> {
        self.pager.sync_wal()
    }

    /// Recover from WAL after a crash.
    ///
    /// This is called automatically during BTree::open() if WAL is enabled.
    /// It replays all WAL records from the checkpoint LSN via logical BTree operations.
    fn recover_from_wal(&mut self) -> Result<(), WrongoDBError> {
        // Get the data file path from the blockfile
        let data_path = self.pager.data_path();

        let wal_path = wal::wal_path_from_data_path(data_path);

        if !wal_path.exists() {
            // No WAL file - nothing to recover
            return Ok(());
        }

        // Open WAL reader
        let mut wal_reader = wal::WalReader::open(&wal_path)
            .map_err(|e| StorageError(format!("Failed to open WAL for recovery: {}", e)))?;

        let checkpoint_lsn = wal_reader.checkpoint_lsn();

        if checkpoint_lsn.is_valid() {
            eprintln!("Starting WAL recovery from checkpoint LSN: {:?}", checkpoint_lsn);
        } else {
            eprintln!("Starting WAL recovery from beginning");
        }

        let saved_wal = self.pager.take_wal();

        let result = (|| {
            let mut stats = RecoveryStats::default();

            // Replay all records
            while let Some((_header, record)) = wal_reader
                .read_record()
                .map_err(|e| StorageError(format!("Failed to read WAL record: {}", e)))?
            {
                match record {
                    wal::WalRecord::Put { key, value } => {
                        self.put(&key, &value)?;
                        stats.puts += 1;
                    }
                    wal::WalRecord::Delete { key } => {
                        let _ = self.delete(&key)?;
                        stats.deletes += 1;
                    }
                    wal::WalRecord::Checkpoint { .. } => {
                        stats.checkpoints += 1;
                    }
                }
                stats.operations_replayed += 1;
            }

            eprintln!(
                "WAL recovery complete: {} operations replayed (puts={}, deletes={}, checkpoints={})",
                stats.operations_replayed, stats.puts, stats.deletes, stats.checkpoints
            );

            Ok(())
        })();

        self.pager.restore_wal(saved_wal);
        result
    }

    pub fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<KeyValueIter<'_>, WrongoDBError>
    {
        let root = self.pager.root_page_id();
        if root == NONE_BLOCK_ID {
            return Ok(BTreeRangeIter::empty());
        }
        BTreeRangeIter::new(self.pager.as_mut(), root, start, end)
    }

    /// Delete a key from the subtree rooted at `node_id`.
    ///
    /// Returns:
    /// - `Ok(DeleteResult)` with the new subtree root id and a flag indicating deletion.
    ///   No merge/borrow is performed; empty pages may remain.
    fn delete_recursive(&mut self, node_id: u64, key: &[u8]) -> Result<DeleteResult, WrongoDBError> {
        let mut page = self.pager.pin_page_mut(node_id)?;
        let page_type = match page_type(page.payload()) {
            Ok(t) => t,
            Err(e) => {
                if let Err(unpin_err) = self.pager.unpin_page_mut_abort(page) {
                    return Err(unpin_err);
                }
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
                if let Err(unpin_err) = self.pager.unpin_page_mut_abort(page) {
                    return Err(unpin_err);
                }
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
    ) -> Result<InsertResult, WrongoDBError> {
        let mut page = self.pager.pin_page_mut(node_id)?;
        let page_type = match page_type(page.payload()) {
            Ok(t) => t,
            Err(e) => {
                if let Err(unpin_err) = self.pager.unpin_page_mut_abort(page) {
                    return Err(unpin_err);
                }
                return Err(e);
            }
        };
        let result = match page_type {
            PageType::Leaf => self.insert_into_leaf(node_id, &mut page, key, value),
            PageType::Internal => self.insert_into_internal(node_id, &mut page, key, value),
        };
        match result {
            Ok(ok) => {
                self.pager.unpin_page_mut_commit(page)?;
                Ok(ok)
            }
            Err(err) => {
                if let Err(unpin_err) = self.pager.unpin_page_mut_abort(page) {
                    return Err(unpin_err);
                }
                Err(err)
            }
        }
    }

    /// Log a WAL record if WAL is enabled.
    ///
    /// This helper encapsulates the repetitive pattern of:
    /// 1. Checking if WAL is enabled
    /// 2. Logging the WAL record
    /// 3. Tracking the operation for batch sync
    fn log_wal_if_enabled<F>(&mut self, op: F) -> Result<(), WrongoDBError>
    where
        F: FnOnce(&mut WalFile) -> Result<wal::Lsn, WrongoDBError>,
    {
        if self.pager.wal().is_some() {
            op(self.pager.wal_mut()?)?;
            let _ = self.pager.log_wal_operation();
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
    ) -> Result<InsertResult, WrongoDBError> {
        let payload_len = page.payload().len();
        let page_id = page.page_id();
        {
            let mut leaf = LeafPage::open(page.payload_mut())
                .map_err(|e| StorageError(format!("corrupt leaf {node_id}: {e}")))?;
            match leaf.put(key, value) {
                Ok(()) => {
                    return Ok(InsertResult {
                        new_node_id: page_id,
                        split: None,
                    });
                }
                Err(LeafPageError::PageFull) => { /* split below */ }
                Err(e) => return Err(map_leaf_err(e)),
            }
        }

        let mut entries = leaf_entries(page.payload_mut())?;
        upsert_entry(&mut entries, key, value);
        let (left_bytes, right_bytes, split_key, _split_idx) = split_leaf_entries(&entries, payload_len)?;

        // Allocate right sibling first
        let right_leaf_id = self.pager.write_new_page(&right_bytes)?;

        page.payload_mut().copy_from_slice(&left_bytes);
        Ok(InsertResult {
            new_node_id: page_id,
            split: Some(SplitInfo {
                sep_key: split_key,
                right_child: right_leaf_id,
            }),
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

        let child_result = self.insert_recursive(child_id, key, value)?;
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
            });
        }

        let (left_bytes, right_bytes, promoted_key, _left_first_child, _left_separators, _promote_idx) =
            split_internal_entries(first_child, &entries, payload_len)?;

        // Allocate right sibling first
        let right_internal_id = self.pager.write_new_page(&right_bytes)?;

        page.payload_mut().copy_from_slice(&left_bytes);
        Ok(InsertResult {
            new_node_id: page_id,
            split: Some(SplitInfo {
                sep_key: promoted_key,
                right_child: right_internal_id,
            }),
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

#[derive(Debug, Clone)]
struct InsertResult {
    new_node_id: u64,
    split: Option<SplitInfo>,
}

#[derive(Debug, Default, Clone)]
struct RecoveryStats {
    operations_replayed: usize,
    puts: usize,
    deletes: usize,
    checkpoints: usize,
}

#[derive(Debug, Clone)]
struct DeleteResult {
    new_node_id: u64,
    deleted: bool,
}

fn init_root_if_missing(pager: &mut dyn PageStore) -> Result<(), WrongoDBError> {
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
