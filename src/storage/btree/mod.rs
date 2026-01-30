use std::path::Path;

pub mod page;
mod pager;
pub mod wal;

use self::pager::{Pager, PinnedPage, PinnedPageMut};
use self::wal::WalFile;
use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::block::file::NONE_BLOCK_ID;
use page::{InternalPage, InternalPageError, LeafPage, LeafPageError};

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

const PAGE_TYPE_LEAF: u8 = 1;
const PAGE_TYPE_INTERNAL: u8 = 2;

#[derive(Debug)]
pub struct BTree {
    pager: Pager,
}

impl BTree {
    pub fn create<P: AsRef<Path>>(path: P, page_size: usize, wal_enabled: bool) -> Result<Self, WrongoDBError> {
        let mut pager = Pager::create(path, page_size, wal_enabled)?;
        init_root_if_missing(&mut pager)?;
        pager.checkpoint()?;
        Ok(Self { pager })
    }

    pub fn open<P: AsRef<Path>>(path: P, wal_enabled: bool) -> Result<Self, WrongoDBError> {
        let pager = Pager::open(path, wal_enabled)?;
        let mut tree = Self { pager };
        init_root_if_missing(&mut tree.pager)?;

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
        let data_path = &self.pager.blockfile().path;

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
        BTreeRangeIter::new(&mut self.pager, root, start, end)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageType {
    Leaf,
    Internal,
}

fn page_type(payload: &[u8]) -> Result<PageType, WrongoDBError> {
    let Some(t) = payload.first().copied() else {
        return Err(StorageError("empty page payload".into()).into());
    };
    match t {
        PAGE_TYPE_LEAF => Ok(PageType::Leaf),
        PAGE_TYPE_INTERNAL => Ok(PageType::Internal),
        other => Err(StorageError(format!("unknown page type: {other}")).into()),
    }
}

fn init_root_if_missing(pager: &mut Pager) -> Result<(), WrongoDBError> {
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

fn map_leaf_err(e: LeafPageError) -> WrongoDBError {
    StorageError(e.to_string()).into()
}

fn map_internal_err(e: InternalPageError) -> WrongoDBError {
    StorageError(e.to_string()).into()
}

fn leaf_entries(buf: &mut [u8]) -> Result<LeafEntries, WrongoDBError> {
    let leaf = LeafPage::open(buf).map_err(|e| StorageError(format!("corrupt leaf: {e}")))?;
    let n = leaf.slot_count().map_err(map_leaf_err)?;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push((
            leaf.key_at(i).map_err(map_leaf_err)?.to_vec(),
            leaf.value_at(i).map_err(map_leaf_err)?.to_vec(),
        ));
    }
    Ok(out)
}

fn upsert_entry(entries: &mut Vec<(Vec<u8>, Vec<u8>)>, key: &[u8], value: &[u8]) {
    match entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
        Ok(i) => entries[i].1 = value.to_vec(),
        Err(i) => entries.insert(i, (key.to_vec(), value.to_vec())),
    }
}

fn internal_entries(buf: &mut [u8]) -> Result<InternalEntries, WrongoDBError> {
    let internal =
        InternalPage::open(buf).map_err(|e| StorageError(format!("corrupt internal: {e}")))?;
    let first_child = internal
        .first_child()
        .map_err(|e| StorageError(format!("corrupt internal: {e}")))?;
    let n = internal
        .slot_count()
        .map_err(|e| StorageError(format!("corrupt internal: {e}")))?;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        out.push((
            internal
                .key_at(i)
                .map_err(|e| StorageError(format!("corrupt internal: {e}")))?
                .to_vec(),
            internal
                .child_at(i)
                .map_err(|e| StorageError(format!("corrupt internal: {e}")))?,
        ));
    }
    Ok((first_child, out))
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

fn split_leaf_entries(
    entries: &[(Vec<u8>, Vec<u8>)],
    payload_len: usize,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, usize), WrongoDBError> {
    if entries.len() < 2 {
        return Err(StorageError("cannot split leaf with <2 entries".into()).into());
    }
    let mid = entries.len() / 2;

    let mut candidates = Vec::new();
    // split_idx must be in 1..len-1
    for delta in 0..entries.len() {
        let a = mid.saturating_sub(delta);
        let b = mid + delta;
        if a >= 1 && a < entries.len() {
            candidates.push(a);
        }
        if b >= 1 && b < entries.len() && b != a {
            candidates.push(b);
        }
    }
    candidates.retain(|&i| i < entries.len());
    candidates.dedup();

    for split_idx in candidates {
        if split_idx == 0 || split_idx >= entries.len() {
            continue;
        }
        let left = build_leaf_page(&entries[..split_idx], payload_len);
        let right = build_leaf_page(&entries[split_idx..], payload_len);
        if let (Ok(l), Ok(r)) = (left, right) {
            return Ok((l, r, entries[split_idx].0.clone(), split_idx));
        }
    }

    Err(StorageError("leaf split impossible (record too large?)".into()).into())
}

fn build_leaf_page(
    entries: &[(Vec<u8>, Vec<u8>)],
    payload_len: usize,
) -> Result<Vec<u8>, WrongoDBError> {
    let mut bytes = vec![0u8; payload_len];
    let mut page = LeafPage::init(&mut bytes).map_err(map_leaf_err)?;
    for (k, v) in entries {
        page.put(k, v).map_err(map_leaf_err)?;
    }
    Ok(bytes)
}

/// Split an internal node into (left_bytes, right_bytes, promoted_key).
///
/// We choose a separator entry at `promote_idx` to **promote** to the parent, and build left/right
/// internal pages on either side:
///
/// ```text
/// first_child = c0
/// entries = [(k0->c1), (k1->c2), (k2->c3), (k3->c4)]
///
/// promote_idx = 2  (promote k2->c3)
///
/// LEFT:
///   first_child = c0
///   entries = [(k0->c1), (k1->c2)]
///
/// RIGHT:
///   first_child = c3        (the promoted entry's child)
///   entries = [(k3->c4)]
///
/// parent inserts: (k2 -> RIGHT_NODE_ID)
/// ```
///
/// The implementation tries indices near the midpoint until both rebuilt pages fit in `payload_len`.
fn split_internal_entries(
    first_child: u64,
    entries: &[(Vec<u8>, u64)],
    payload_len: usize,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, u64, Vec<(Vec<u8>, u64)>, usize), WrongoDBError> {
    if entries.is_empty() {
        return Err(StorageError("cannot split internal with 0 separators".into()).into());
    }

    let mid = entries.len() / 2;
    let mut candidates = Vec::new();
    for delta in 0..entries.len() {
        let a = mid.saturating_sub(delta);
        let b = mid + delta;
        if a < entries.len() {
            candidates.push(a);
        }
        if b < entries.len() && b != a {
            candidates.push(b);
        }
    }
    candidates.dedup();

    for promote_idx in candidates {
        let promoted_key = entries[promote_idx].0.clone();
        let right_first_child = entries[promote_idx].1;
        let left_entries = &entries[..promote_idx];
        let right_entries = &entries[(promote_idx + 1)..];

        let left = build_internal_page(first_child, left_entries, payload_len);
        let right = build_internal_page(right_first_child, right_entries, payload_len);
        if let (Ok(l), Ok(r)) = (left, right) {
            return Ok((l, r, promoted_key, first_child, left_entries.to_vec(), promote_idx));
        }
    }

    Err(StorageError("internal split impossible (record too large?)".into()).into())
}

fn build_internal_page(
    first_child: u64,
    entries: &[(Vec<u8>, u64)],
    payload_len: usize,
) -> Result<Vec<u8>, WrongoDBError> {
    let mut bytes = vec![0u8; payload_len];
    let mut page = InternalPage::init(&mut bytes, first_child).map_err(map_internal_err)?;
    for (k, child) in entries {
        page.put_separator(k, *child).map_err(map_internal_err)?;
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Copy)]
struct CursorFrame {
    internal_id: u64,
    child_idx: usize,
}

/// Ordered range iterator over the B+tree (start inclusive, end exclusive).
///
/// Notes:
/// - Leaves are not linked (no sibling pointers), so advancing to the next leaf uses a parent stack.
///
/// ## Iterator Pinning Strategy
///
/// This iterator uses **leaf-only pinning** for memory efficiency:
/// - Only the current leaf page is pinned during iteration.
/// - Parent internal pages are unpinned after routing down to the leaf.
/// - When advancing to the next leaf, the current leaf is unpinned before climbing the stack.
///
/// This is safe because:
/// - Internal pages are immutable from the stable root perspective (COW semantics).
/// - If a parent page is evicted and re-read, it returns the same stable content.
/// - The iterator only needs stable access to the current leaf; mutations create new working pages.
///
/// Tradeoff: Parent pages may be re-read if evicted during iteration, but this is acceptable
/// for the current single-threaded design. Full-path pinning could be added later if needed.
///
/// Graphically:
/// ```text
/// current leaf exhausted
///   -> climb stack until a parent has a "next child"
///   -> go to that next child subtree
///   -> descend leftmost to the next leaf
/// ```
pub struct BTreeRangeIter<'a> {
    pager: Option<&'a mut Pager>,
    end: Option<Vec<u8>>,
    stack: Vec<CursorFrame>,
    leaf: Option<PinnedPage>,
    slot_idx: usize,
    done: bool,
}

impl<'a> BTreeRangeIter<'a> {
    fn empty() -> Self {
        Self {
            pager: None,
            end: None,
            stack: Vec::new(),
            leaf: None,
            slot_idx: 0,
            done: true,
        }
    }

    fn new(
        pager: &'a mut Pager,
        root: u64,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Self, WrongoDBError> {
        let mut it = Self {
            pager: Some(pager),
            end: end.map(|b| b.to_vec()),
            stack: Vec::new(),
            leaf: None,
            slot_idx: 0,
            done: false,
        };
        it.seek(root, start)?;
        Ok(it)
    }

    fn pager_mut(&mut self) -> Result<&mut Pager, WrongoDBError> {
        self.pager
            .as_deref_mut()
            .ok_or_else(|| StorageError("range iterator has no backing Pager".into()).into())
    }

    fn clear_leaf(&mut self) -> Result<(), WrongoDBError> {
        if let Some(leaf) = self.leaf.take() {
            self.pager_mut()?.unpin_page(leaf.page_id());
        }
        Ok(())
    }

    fn seek(&mut self, root: u64, start: Option<&[u8]>) -> Result<(), WrongoDBError> {
        self.clear_leaf()?;
        self.stack.clear();
        let mut node_id = root;
        loop {
            let mut page = self.pager_mut()?.pin_page(node_id)?;
            let page_id = page.page_id();
            let page_type = match page_type(page.payload()) {
                Ok(t) => t,
                Err(e) => {
                    self.pager_mut()?.unpin_page(page_id);
                    return Err(e);
                }
            };
            match page_type {
                PageType::Leaf => {
                    let leaf = match LeafPage::open(page.payload_mut()) {
                        Ok(leaf) => leaf,
                        Err(e) => {
                            self.pager_mut()?.unpin_page(page_id);
                            return Err(StorageError(format!("corrupt leaf {node_id}: {e}")).into());
                        }
                    };
                    let slot_idx = if let Some(start_key) = start {
                        match leaf_lower_bound(&leaf, start_key).map_err(map_leaf_err) {
                            Ok(idx) => idx,
                            Err(e) => {
                                self.pager_mut()?.unpin_page(page_id);
                                return Err(e);
                            }
                        }
                    } else {
                        0
                    };
                    self.leaf = Some(page);
                    self.slot_idx = slot_idx;
                    return Ok(());
                }
                PageType::Internal => {
                    let internal = match InternalPage::open(page.payload_mut()) {
                        Ok(internal) => internal,
                        Err(e) => {
                            self.pager_mut()?.unpin_page(page_id);
                            return Err(
                                StorageError(format!("corrupt internal {node_id}: {e}")).into(),
                            );
                        }
                    };
                    let child_idx = if let Some(start_key) = start {
                        match internal_child_index_for_key(&internal, start_key)
                            .map_err(map_internal_err)
                        {
                            Ok(idx) => idx,
                            Err(e) => {
                                self.pager_mut()?.unpin_page(page_id);
                                return Err(e);
                            }
                        }
                    } else {
                        0
                    };
                    let child_id = match internal_child_at(&internal, child_idx).map_err(map_internal_err) {
                        Ok(id) => id,
                        Err(e) => {
                            self.pager_mut()?.unpin_page(page_id);
                            return Err(e);
                        }
                    };
                    self.stack.push(CursorFrame {
                        internal_id: node_id,
                        child_idx,
                    });
                    self.pager_mut()?.unpin_page(page_id);
                    node_id = child_id;
                }
            }
        }
    }

    fn advance_to_next_leaf(&mut self) -> Result<bool, WrongoDBError> {
        self.clear_leaf()?;
        while let Some(frame) = self.stack.pop() {
            let mut page = self.pager_mut()?.pin_page(frame.internal_id)?;
            let page_id = page.page_id();
            let next_child_id = match (|| {
                let internal = InternalPage::open(page.payload_mut()).map_err(|e| {
                    StorageError(format!("corrupt internal {}: {e}", frame.internal_id))
                })?;

                let slots = internal.slot_count().map_err(map_internal_err)?;
                let children_count = slots + 1;
                if frame.child_idx + 1 >= children_count {
                    Ok(None)
                } else {
                    let next_child_idx = frame.child_idx + 1;
                    let next_child_id =
                        internal_child_at(&internal, next_child_idx).map_err(map_internal_err)?;
                    self.stack.push(CursorFrame {
                        internal_id: frame.internal_id,
                        child_idx: next_child_idx,
                    });
                    Ok(Some(next_child_id))
                }
            })() {
                Ok(next) => next,
                Err(e) => {
                    self.pager_mut()?.unpin_page(page_id);
                    return Err(e);
                }
            };
            self.pager_mut()?.unpin_page(page_id);
            if let Some(next_child_id) = next_child_id {
                self.descend_leftmost(next_child_id)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn descend_leftmost(&mut self, start_id: u64) -> Result<(), WrongoDBError> {
        let mut node_id = start_id;
        loop {
            let mut page = self.pager_mut()?.pin_page(node_id)?;
            let page_id = page.page_id();
            let page_type = match page_type(page.payload()) {
                Ok(t) => t,
                Err(e) => {
                    self.pager_mut()?.unpin_page(page_id);
                    return Err(e);
                }
            };
            match page_type {
                PageType::Leaf => {
                    self.leaf = Some(page);
                    self.slot_idx = 0;
                    return Ok(());
                }
                PageType::Internal => {
                    let internal = match InternalPage::open(page.payload_mut()) {
                        Ok(internal) => internal,
                        Err(e) => {
                            self.pager_mut()?.unpin_page(page_id);
                            return Err(
                                StorageError(format!("corrupt internal {node_id}: {e}")).into(),
                            );
                        }
                    };
                    let child_id = match internal.first_child().map_err(map_internal_err) {
                        Ok(id) => id,
                        Err(e) => {
                            self.pager_mut()?.unpin_page(page_id);
                            return Err(e);
                        }
                    };
                    self.stack.push(CursorFrame {
                        internal_id: node_id,
                        child_idx: 0,
                    });
                    self.pager_mut()?.unpin_page(page_id);
                    node_id = child_id;
                }
            }
        }
    }
}

impl<'a> Iterator for BTreeRangeIter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>), WrongoDBError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let Some(mut leaf_page) = self.leaf.take() else {
                self.done = true;
                return None;
            };
            let leaf_id = leaf_page.page_id();
            let leaf = match LeafPage::open(leaf_page.payload_mut()) {
                Ok(v) => v,
                Err(e) => {
                    self.done = true;
                    let _ = self.pager_mut().map(|pager| pager.unpin_page(leaf_id));
                    return Some(Err(StorageError(format!(
                        "corrupt leaf {}: {e}",
                        leaf_id
                    ))
                    .into()))
                }
            };

            let slot_count = match leaf.slot_count() {
                Ok(v) => v,
                Err(e) => {
                    self.done = true;
                    let _ = self.pager_mut().map(|pager| pager.unpin_page(leaf_id));
                    return Some(Err(map_leaf_err(e)));
                }
            };

            if self.slot_idx < slot_count {
                let k = match leaf.key_at(self.slot_idx) {
                    Ok(v) => v.to_vec(),
                    Err(e) => {
                        self.done = true;
                        let _ = self.pager_mut().map(|pager| pager.unpin_page(leaf_id));
                        return Some(Err(map_leaf_err(e)));
                    }
                };
                if let Some(end) = &self.end {
                    if k.as_slice() >= end.as_slice() {
                        self.done = true;
                        let _ = self.pager_mut().map(|pager| pager.unpin_page(leaf_id));
                        return None;
                    }
                }
                let v = match leaf.value_at(self.slot_idx) {
                    Ok(v) => v.to_vec(),
                    Err(e) => {
                        self.done = true;
                        let _ = self.pager_mut().map(|pager| pager.unpin_page(leaf_id));
                        return Some(Err(map_leaf_err(e)));
                    }
                };
                self.slot_idx += 1;
                self.leaf = Some(leaf_page);
                return Some(Ok((k, v)));
            }

            self.leaf = Some(leaf_page);
            match self.advance_to_next_leaf() {
                Ok(true) => continue,
                Ok(false) => {
                    self.done = true;
                    return None;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

impl<'a> Drop for BTreeRangeIter<'a> {
    fn drop(&mut self) {
        let _ = self.clear_leaf();
    }
}

fn leaf_lower_bound(leaf: &LeafPage<'_>, key: &[u8]) -> Result<usize, LeafPageError> {
    let n = leaf.slot_count()?;
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_key = leaf.key_at(mid)?;
        if mid_key < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn internal_child_index_for_key(
    internal: &InternalPage<'_>,
    key: &[u8],
) -> Result<usize, InternalPageError> {
    let n = internal.slot_count()?;
    if n == 0 {
        return Ok(0);
    }
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_key = internal.key_at(mid)?;
        if mid_key <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn internal_child_at(
    internal: &InternalPage<'_>,
    child_idx: usize,
) -> Result<u64, InternalPageError> {
    if child_idx == 0 {
        internal.first_child()
    } else {
        internal.child_at(child_idx - 1)
    }
}
