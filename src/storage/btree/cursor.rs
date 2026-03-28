use super::internal_ops::put_separator as put_internal_separator;
use super::iter::BTreeRangeIter;
use super::layout::{
    build_internal_page, internal_entries, leaf_entries, map_leaf_err, split_internal_entries,
    split_leaf_entries,
};
use super::leaf_ops::{
    contains_key as leaf_contains_key, delete as delete_from_leaf_page, put as put_in_leaf_page,
};
use super::page::{InternalPage, LeafPage, LeafPageError};
use super::search::{child_for_key, search_leaf};
use super::{visible_chain_value, KeyChildId, LeafEntries};
use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::history::{HistoryEntry, HistoryStore};
use crate::storage::mvcc::{ReconcileStats, Update, UpdateChain, UpdateHandle, UpdateType};
use crate::storage::page_store::{Page, PageEdit, PageRead, PageStore, PageType, RowInsert};
use crate::storage::reserved_store::StoreId;
use crate::txn::{ReadVisibility, Transaction, TxnId};

// ============================================================================
// Type Aliases (local to cursor module)
// ============================================================================

type KeyValueIter<'a> = BTreeRangeIter<'a>;
type ReconcileEntries = Vec<(Vec<u8>, UpdateType, Vec<u8>)>;

// ============================================================================
// Constants
// ============================================================================

const NONE_PAGE_ID: u64 = 0;

// ============================================================================
// Helper Types
// ============================================================================

#[derive(Debug, Clone)]
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

enum ReadStep {
    Found(Option<Vec<u8>>),
    Descend(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LeafPosition {
    page_id: u64,
    index: usize,
    found: bool,
}

#[derive(Debug, Clone, Copy)]
struct ReconcileContext {
    oldest_active_txn_id: TxnId,
    no_active_txns: bool,
    store_id: StoreId,
}

#[derive(Debug, Default)]
struct ReconcileAccumulator {
    entries: ReconcileEntries,
    stats: ReconcileStats,
}

// ============================================================================
// BTreeCursor
// ============================================================================

/// B+tree cursor providing read, transactional write, and checkpoint operations.
///
/// `BTreeCursor` is the main interface for B+tree operations, managing:
///
/// - **Point queries**: `get` retrieves a single key-value pair
/// - **Range scans**: `range` returns an iterator over key ranges
/// - **Transactional writes**: crate-private `put` and `delete` attach page-local MVCC updates
///   and register them with the active transaction
/// - **Base-row writes**: internal `write_base_row` and `delete_base_row` update the reconciled
///   page image without creating a new transactional update
/// - **Checkpointing**: `checkpoint` flushes dirty pages and creates a consistent recovery point
///
/// The cursor owns a [`PageStore`] which handles page caching, copy-on-write,
/// and coordination with the underlying block file.
#[derive(Debug)]
pub(crate) struct BTreeCursor {
    page_store: Box<dyn PageStore>,
}

impl BTreeCursor {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Creates a new B+tree cursor backed by the given page store.
    ///
    /// The cursor takes ownership of the page store, which manages
    /// page caching, copy-on-write, and checkpoint coordination.
    pub(crate) fn new(store: Box<dyn PageStore>) -> Self {
        Self { page_store: store }
    }

    // ------------------------------------------------------------------------
    // Public API: Read Operations
    // ------------------------------------------------------------------------

    pub(crate) fn get(
        &mut self,
        key: &[u8],
        visibility: &ReadVisibility,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let mut node_id = self.page_store.root_page_id();
        if node_id == NONE_PAGE_ID {
            return Ok(None);
        }

        loop {
            let pin = self.page_store.pin_page(node_id)?;
            let step = {
                let page = self.page_store.get_page(&pin);
                match page.header().page_type {
                    PageType::Leaf => ReadStep::Found(
                        self.resolve_visible_leaf_value(page, key, visibility)
                            .map_err(map_leaf_err)?,
                    ),
                    PageType::Internal => {
                        let internal = InternalPage::open(page).map_err(|e| {
                            StorageError(format!("corrupt internal {node_id}: {e}"))
                        })?;
                        let next = child_for_key(&internal, key).map_err(|e| {
                            StorageError(format!("routing failed at {node_id}: {e}"))
                        })?;
                        ReadStep::Descend(next)
                    }
                }
            };
            self.page_store.unpin_page(pin);

            match step {
                ReadStep::Found(result) => return Ok(result),
                ReadStep::Descend(next_id) => node_id = next_id,
            }
        }
    }

    pub(crate) fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        visibility: &ReadVisibility,
    ) -> Result<KeyValueIter<'_>, WrongoDBError> {
        let root = self.page_store.root_page_id();
        if root == NONE_PAGE_ID {
            return Ok(BTreeRangeIter::empty());
        }
        BTreeRangeIter::new(
            self.page_store.as_mut() as &mut dyn PageRead,
            root,
            start,
            end,
            visibility,
        )
    }

    // ------------------------------------------------------------------------
    // Public API: Write Operations
    // ------------------------------------------------------------------------

    pub(crate) fn put(
        &mut self,
        store_id: StoreId,
        key: &[u8],
        value: &[u8],
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        let update_handle = self.attach_update(
            key,
            Update::new(txn.id(), UpdateType::Standard, value.to_vec()),
            "leaf update failed",
        )?;
        txn.track_update(update_handle);
        txn.record_put_log(store_id, key, value);
        Ok(())
    }

    pub(crate) fn write_base_row(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        let root = self.page_store.root_page_id();
        if root == NONE_PAGE_ID {
            return Err(StorageError("btree missing root".into()).into());
        }

        let result = self.write_base_row_recursive(root, key, value, InsertMode::Upsert)?;
        if let Some(split) = result.split {
            let payload_len = self.page_store.page_payload_len();
            let mut root_page = Page::new_internal(payload_len, result.new_node_id)
                .map_err(|e| StorageError(format!("init new root internal failed: {e}")))?;
            put_internal_separator(&mut root_page, &split.sep_key, split.right_child)
                .map_err(|e| StorageError(format!("init new root internal failed: {e}")))?;

            let new_root_id = self.page_store.write_new_page(root_page)?;
            self.page_store.set_root_page_id(new_root_id)?;
        } else {
            self.page_store.set_root_page_id(result.new_node_id)?;
        }

        Ok(())
    }

    pub(crate) fn delete(
        &mut self,
        store_id: StoreId,
        key: &[u8],
        txn: &mut Transaction,
    ) -> Result<bool, WrongoDBError> {
        let update_handle = self.attach_update(
            key,
            Update::new(txn.id(), UpdateType::Tombstone, Vec::new()),
            "leaf tombstone failed",
        )?;
        txn.track_update(update_handle);
        txn.record_delete_log(store_id, key);
        Ok(true)
    }

    pub(crate) fn delete_base_row(&mut self, key: &[u8]) -> Result<bool, WrongoDBError> {
        let root = self.page_store.root_page_id();
        if root == NONE_PAGE_ID {
            return Ok(false);
        }

        let result = self.delete_base_row_recursive(root, key)?;
        self.page_store.set_root_page_id(result.new_node_id)?;
        Ok(result.deleted)
    }

    // ------------------------------------------------------------------------
    // Public API: Lifecycle Operations
    // ------------------------------------------------------------------------

    pub(crate) fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        let root = self.page_store.checkpoint_prepare();
        self.page_store.checkpoint_flush_data()?;
        self.page_store.checkpoint_commit(root)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Private Helpers: Read Operations
    // ------------------------------------------------------------------------

    fn resolve_visible_leaf_value(
        &self,
        page: &Page,
        key: &[u8],
        visibility: &ReadVisibility,
    ) -> Result<Option<Vec<u8>>, LeafPageError> {
        let leaf = LeafPage::open(page)?;
        let position = search_leaf(&leaf, key)?;

        if let Some(modify) = page.row_modify() {
            if position.found {
                if let Some(chain) = modify
                    .row_updates()
                    .get(position.index)
                    .and_then(|chain| chain.as_ref())
                {
                    if let Some(value) = visible_chain_value(chain, visibility) {
                        return Ok(value);
                    }
                }
            }

            if let Some(value) = find_visible_insert_value(modify.row_inserts(), key, visibility) {
                return Ok(value);
            }
        }

        if !position.found {
            return Ok(None);
        }

        Ok(Some(leaf.value_at(position.index)?.to_vec()))
    }

    fn attach_update(
        &mut self,
        key: &[u8],
        update: Update,
        error_context: &str,
    ) -> Result<UpdateHandle, WrongoDBError> {
        let position = self.find_leaf_position(key)?;
        let pin = self.page_store.pin_page(position.page_id)?;
        let update_handle = {
            let page = self.page_store.get_page_mut(&pin);
            attach_leaf_update(page, key, position, update).map_err(|e| {
                StorageError(format!("{error_context} at {}: {e}", position.page_id))
            })?
        };
        self.page_store.unpin_page(pin);
        Ok(update_handle)
    }

    pub(in crate::storage) fn reconcile_page_local_updates(
        &mut self,
        oldest_active_txn_id: TxnId,
        no_active_txns: bool,
        store_id: StoreId,
        history_store: Option<&mut HistoryStore>,
    ) -> Result<ReconcileStats, WrongoDBError> {
        let root = self.page_store.root_page_id();
        if root == NONE_PAGE_ID {
            return Ok(ReconcileStats::default());
        }

        let context = ReconcileContext {
            oldest_active_txn_id,
            no_active_txns,
            store_id,
        };
        let mut reconcile = ReconcileAccumulator::default();
        self.collect_page_local_entries(root, context, history_store, &mut reconcile)?;

        for (key, update_type, data) in reconcile.entries {
            match update_type {
                UpdateType::Standard => self.write_base_row(&key, &data)?,
                UpdateType::Tombstone => {
                    let _ = self.delete_base_row(&key)?;
                }
                UpdateType::Reserve => {}
            }
        }

        Ok(reconcile.stats)
    }

    fn find_leaf_position(&mut self, key: &[u8]) -> Result<LeafPosition, WrongoDBError> {
        let mut node_id = self.page_store.root_page_id();
        if node_id == NONE_PAGE_ID {
            return Err(StorageError("btree missing root".into()).into());
        }

        loop {
            let pin = self.page_store.pin_page(node_id)?;
            let step = {
                let page = self.page_store.get_page(&pin);
                match page.header().page_type {
                    PageType::Leaf => {
                        let leaf = LeafPage::open(page)
                            .map_err(|e| StorageError(format!("corrupt leaf {node_id}: {e}")))?;
                        let position = search_leaf(&leaf, key).map_err(|e| {
                            StorageError(format!("leaf search failed at {node_id}: {e}"))
                        })?;
                        Ok::<_, WrongoDBError>(Some(LeafPosition {
                            page_id: node_id,
                            index: position.index,
                            found: position.found,
                        }))
                    }
                    PageType::Internal => {
                        let internal = InternalPage::open(page).map_err(|e| {
                            StorageError(format!("corrupt internal {node_id}: {e}"))
                        })?;
                        let next = child_for_key(&internal, key).map_err(|e| {
                            StorageError(format!("routing failed at {node_id}: {e}"))
                        })?;
                        node_id = next;
                        Ok(None)
                    }
                }
            };
            self.page_store.unpin_page(pin);

            if let Some(position) = step? {
                return Ok(position);
            }
        }
    }

    fn collect_page_local_entries(
        &mut self,
        node_id: u64,
        context: ReconcileContext,
        mut history_store: Option<&mut HistoryStore>,
        reconcile: &mut ReconcileAccumulator,
    ) -> Result<(), WrongoDBError> {
        let pin = self.page_store.pin_page(node_id)?;
        let children = {
            let page_type = self.page_store.get_page(&pin).header().page_type;
            match page_type {
                PageType::Leaf => {
                    let page = self.page_store.get_page_mut(&pin);
                    collect_leaf_page_local_entries(
                        page,
                        context,
                        history_store.as_deref_mut(),
                        reconcile,
                    )?;
                    Vec::new()
                }
                PageType::Internal => {
                    let page = self.page_store.get_page(&pin);
                    let internal = InternalPage::open(page)
                        .map_err(|e| StorageError(format!("corrupt internal {node_id}: {e}")))?;
                    let mut children = Vec::with_capacity(internal.slot_count() + 1);
                    children.push(internal.first_child());
                    for index in 0..internal.slot_count() {
                        children.push(internal.child_at(index).map_err(|e| {
                            StorageError(format!("corrupt internal {node_id}: {e}"))
                        })?);
                    }
                    children
                }
            }
        };
        self.page_store.unpin_page(pin);

        for child_id in children {
            self.collect_page_local_entries(
                child_id,
                context,
                history_store.as_deref_mut(),
                reconcile,
            )?;
        }

        Ok(())
    }

    fn delete_base_row_recursive(
        &mut self,
        node_id: u64,
        key: &[u8],
    ) -> Result<DeleteResult, WrongoDBError> {
        let mut page = self.page_store.pin_page_mut(node_id)?;
        let result = match page.page().header().page_type {
            PageType::Leaf => self.delete_from_leaf(node_id, &mut page, key),
            PageType::Internal => self.delete_from_internal(&mut page, key),
        };

        match result {
            Ok(ok) => {
                self.page_store.commit_page_edit(page)?;
                Ok(ok)
            }
            Err(err) => {
                self.page_store.abort_page_edit(page)?;
                Err(err)
            }
        }
    }

    fn delete_from_leaf(
        &mut self,
        node_id: u64,
        page: &mut PageEdit,
        key: &[u8],
    ) -> Result<DeleteResult, WrongoDBError> {
        let page_id = page.page_id();
        let deleted = delete_from_leaf_page(page.page_mut(), key)
            .map_err(|e| StorageError(format!("corrupt leaf {node_id}: {e}")))?;

        Ok(DeleteResult {
            new_node_id: page_id,
            deleted,
        })
    }

    fn delete_from_internal(
        &mut self,
        page: &mut PageEdit,
        key: &[u8],
    ) -> Result<DeleteResult, WrongoDBError> {
        let payload_len = page.page().data().len();
        let page_id = page.page_id();
        let (mut first_child, mut entries) = internal_entries(page.page())?;
        let child_idx = child_index_for_key(&entries, key);
        let child_id = if child_idx == 0 {
            first_child
        } else {
            entries[child_idx - 1].1
        };

        let child_result = self.delete_base_row_recursive(child_id, key)?;
        if child_idx == 0 {
            first_child = child_result.new_node_id;
        } else {
            entries[child_idx - 1].1 = child_result.new_node_id;
        }

        let rebuilt = build_internal_page(first_child, &entries, payload_len)?;
        *page.page_mut() = rebuilt;

        Ok(DeleteResult {
            new_node_id: page_id,
            deleted: child_result.deleted,
        })
    }

    // ------------------------------------------------------------------------
    // Private Helpers: Insert Operations
    // ------------------------------------------------------------------------

    fn write_base_row_recursive(
        &mut self,
        node_id: u64,
        key: &[u8],
        value: &[u8],
        mode: InsertMode,
    ) -> Result<InsertResult, WrongoDBError> {
        let mut page = self.page_store.pin_page_mut(node_id)?;
        let result = match page.page().header().page_type {
            PageType::Leaf => self.insert_into_leaf(node_id, &mut page, key, value, mode),
            PageType::Internal => self.insert_into_internal(node_id, &mut page, key, value, mode),
        };

        match result {
            Ok(ok) => {
                if ok.inserted {
                    self.page_store.commit_page_edit(page)?;
                } else {
                    self.page_store.abort_page_edit(page)?;
                }
                Ok(ok)
            }
            Err(err) => {
                self.page_store.abort_page_edit(page)?;
                Err(err)
            }
        }
    }

    fn insert_into_leaf(
        &mut self,
        node_id: u64,
        page: &mut PageEdit,
        key: &[u8],
        value: &[u8],
        mode: InsertMode,
    ) -> Result<InsertResult, WrongoDBError> {
        let payload_len = page.page().data().len();
        let page_id = page.page_id();
        if mode == InsertMode::Unique
            && leaf_contains_key(page.page(), key)
                .map_err(|e| StorageError(format!("corrupt leaf {node_id}: {e}")))?
        {
            return Ok(InsertResult {
                new_node_id: page_id,
                split: None,
                inserted: false,
            });
        }
        match put_in_leaf_page(page.page_mut(), key, value) {
            Ok(()) => {
                return Ok(InsertResult {
                    new_node_id: page_id,
                    split: None,
                    inserted: true,
                });
            }
            Err(LeafPageError::PageFull) => {}
            Err(err) => return Err(map_leaf_err(err)),
        }

        let mut entries = leaf_entries(page.page())?;
        upsert_entry(&mut entries, key, value);
        let (left_page, right_page, split_key, _split_idx) =
            split_leaf_entries(&entries, payload_len)?;

        let right_leaf_id = self.page_store.write_new_page(right_page)?;
        *page.page_mut() = left_page;

        Ok(InsertResult {
            new_node_id: page_id,
            split: Some(SplitInfo {
                sep_key: split_key,
                right_child: right_leaf_id,
            }),
            inserted: true,
        })
    }

    fn insert_into_internal(
        &mut self,
        _node_id: u64,
        page: &mut PageEdit,
        key: &[u8],
        value: &[u8],
        mode: InsertMode,
    ) -> Result<InsertResult, WrongoDBError> {
        let payload_len = page.page().data().len();
        let page_id = page.page_id();
        let (mut first_child, mut entries) = internal_entries(page.page())?;
        let child_idx = child_index_for_key(&entries, key);
        let child_id = if child_idx == 0 {
            first_child
        } else {
            entries[child_idx - 1].1
        };

        let child_result = self.write_base_row_recursive(child_id, key, value, mode)?;
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

        if let Ok(rebuilt) = build_internal_page(first_child, &entries, payload_len) {
            *page.page_mut() = rebuilt;
            return Ok(InsertResult {
                new_node_id: page_id,
                split: None,
                inserted: true,
            });
        }

        let (
            left_page,
            right_page,
            promoted_key,
            _left_first_child,
            _left_separators,
            _promote_idx,
        ) = split_internal_entries(first_child, &entries, payload_len)?;

        let right_internal_id = self.page_store.write_new_page(right_page)?;
        *page.page_mut() = left_page;

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

// ============================================================================
// Helper Functions
// ============================================================================

fn find_visible_insert_value(
    buckets: &[Vec<RowInsert>],
    key: &[u8],
    visibility: &ReadVisibility,
) -> Option<Option<Vec<u8>>> {
    for bucket in buckets {
        for insert in bucket {
            if insert.key() != key {
                continue;
            }
            if let Some(value) = visible_chain_value(insert.updates(), visibility) {
                return Some(value);
            }
        }
    }

    None
}

fn attach_leaf_update(
    page: &mut Page,
    key: &[u8],
    position: LeafPosition,
    update: Update,
) -> Result<UpdateHandle, crate::storage::page_store::PageError> {
    let row_modify = page.ensure_row_modify()?;
    if position.found {
        let chain =
            row_modify.row_updates_mut()[position.index].get_or_insert_with(UpdateChain::default);
        return Ok(prepend_update(chain, update));
    }

    let bucket = &mut row_modify.row_inserts_mut()[position.index];
    match bucket.binary_search_by(|insert| insert.key().cmp(key)) {
        Ok(index) => Ok(prepend_update(bucket[index].updates_mut(), update)),
        Err(index) => {
            let mut chain = UpdateChain::default();
            let update_ref = prepend_update(&mut chain, update);
            bucket.insert(index, RowInsert::new(key.to_vec(), chain));
            Ok(update_ref)
        }
    }
}

fn prepend_update(chain: &mut UpdateChain, update: Update) -> UpdateHandle {
    if let Some(head) = chain.head() {
        head.write().mark_stopped(update.txn_id);
    }
    chain.prepend(update)
}

fn collect_leaf_page_local_entries(
    page: &mut Page,
    context: ReconcileContext,
    mut history_store: Option<&mut HistoryStore>,
    reconcile: &mut ReconcileAccumulator,
) -> Result<(), WrongoDBError> {
    let leaf = LeafPage::open(page).map_err(|e| StorageError(format!("corrupt leaf page: {e}")))?;
    let slot_keys = (0..leaf.slot_count())
        .map(|index| {
            leaf.key_at(index)
                .map(|key| key.to_vec())
                .map_err(map_leaf_err)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let Some(modify) = page.row_modify_mut() else {
        return Ok(());
    };

    for (index, slot) in modify.row_updates_mut().iter_mut().enumerate() {
        let Some(chain) = slot.as_mut() else {
            continue;
        };
        record_chain_reconcile(
            &slot_keys[index],
            chain,
            context,
            history_store.as_deref_mut(),
            reconcile,
        )?;
        if chain.is_empty() {
            *slot = None;
            reconcile.stats.chains_dropped += 1;
        }
    }

    for bucket in modify.row_inserts_mut() {
        let mut index = 0;
        while index < bucket.len() {
            let key = bucket[index].key().to_vec();
            let chain = bucket[index].updates_mut();
            record_chain_reconcile(
                &key,
                chain,
                context,
                history_store.as_deref_mut(),
                reconcile,
            )?;
            if chain.is_empty() {
                bucket.remove(index);
                reconcile.stats.chains_dropped += 1;
            } else {
                index += 1;
            }
        }
    }

    let empty_updates = modify.row_updates().iter().all(Option::is_none);
    let empty_inserts = modify.row_inserts().iter().all(Vec::is_empty);
    if empty_updates && empty_inserts {
        page.clear_row_modify();
    }

    Ok(())
}

fn record_chain_reconcile(
    key: &[u8],
    chain: &mut UpdateChain,
    context: ReconcileContext,
    history_store: Option<&mut HistoryStore>,
    reconcile: &mut ReconcileAccumulator,
) -> Result<(), WrongoDBError> {
    if let Some((update_type, data)) = chain.latest_committed_entry() {
        reconcile.entries.push((key.to_vec(), update_type, data));
        reconcile.stats.materialized_entries += 1;
    }

    if let Some(history_store) = history_store {
        let _ = save_obsolete_to_history(
            chain,
            context.store_id,
            key,
            context.oldest_active_txn_id,
            history_store,
        )?;
    }

    reconcile.stats.obsolete_updates_removed +=
        chain.truncate_obsolete(context.oldest_active_txn_id);
    let _ = chain.clear_if_materialized_current(context.no_active_txns);
    Ok(())
}

/// Save committed versions that are older than the reconciled base image.
///
/// The first committed entry in the chain becomes the reconciled base row and
/// is therefore excluded here. Every later committed entry is persisted into
/// the history store before reconciliation prunes the in-memory chain. That
/// preserves the invariant that each committed version lives in exactly one
/// place after reconciliation finishes.
fn save_obsolete_to_history(
    chain: &UpdateChain,
    store_id: StoreId,
    key: &[u8],
    _oldest_active_txn_id: TxnId,
    history_store: &mut HistoryStore,
) -> Result<usize, WrongoDBError> {
    let mut saved = 0;
    let mut skipped_latest_committed = false;

    for update_ref in chain.iter() {
        let update = update_ref.read();
        if !update.is_committed() {
            continue;
        }

        match update.type_ {
            UpdateType::Reserve => continue,
            UpdateType::Standard | UpdateType::Tombstone => {
                if !skipped_latest_committed {
                    skipped_latest_committed = true;
                    continue;
                }

                history_store.save_version(&HistoryEntry {
                    store_id,
                    key: key.to_vec(),
                    start_ts: update.time_window.start_ts,
                    stop_ts: update.time_window.stop_ts,
                    update_type: update.type_,
                    data: update.data.clone(),
                })?;
                saved += 1;
            }
        }
    }

    Ok(saved)
}

fn upsert_entry(entries: &mut LeafEntries, key: &[u8], value: &[u8]) {
    match entries.binary_search_by(|(existing_key, _)| existing_key.as_slice().cmp(key)) {
        Ok(index) => entries[index].1 = value.to_vec(),
        Err(index) => entries.insert(index, (key.to_vec(), value.to_vec())),
    }
}

fn upsert_internal_entry(entries: &mut Vec<KeyChildId>, key: &[u8], child: u64) {
    match entries.binary_search_by(|(existing_key, _)| existing_key.as_slice().cmp(key)) {
        Ok(index) => entries[index].1 = child,
        Err(index) => entries.insert(index, (key.to_vec(), child)),
    }
}

fn child_index_for_key(entries: &[KeyChildId], key: &[u8]) -> usize {
    let mut index = 0;
    for (entry_idx, (sep_key, _)) in entries.iter().enumerate() {
        if key < sep_key.as_slice() {
            break;
        }
        index = entry_idx + 1;
    }
    index
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::storage::page_store::{BlockFilePageStore, PageWrite, RootStore};
    use crate::storage::reserved_store::FIRST_DYNAMIC_STORE_ID;
    use crate::txn::{GlobalTxnState, TXN_NONE};

    const TEST_STORE_ID: u64 = FIRST_DYNAMIC_STORE_ID;

    fn create_tree(path: &Path) -> BTreeCursor {
        let mut store = BlockFilePageStore::create(path, 512).unwrap();
        let payload_len = store.page_payload_len();
        let root = Page::new_leaf(payload_len).unwrap();
        let root_id = store.write_new_page(root).unwrap();
        store.set_root_page_id(root_id).unwrap();
        BTreeCursor::new(Box::new(store))
    }

    #[test]
    fn put_shadows_existing_base_row() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("btree_put_existing.wt");
        let mut tree = create_tree(&path);
        let global_txn = Arc::new(GlobalTxnState::new());
        let mut txn = global_txn.begin_snapshot_txn();

        tree.write_base_row(b"k1", b"base").unwrap();
        tree.put(TEST_STORE_ID, b"k1", b"txn", &mut txn).unwrap();

        assert_eq!(
            tree.get(b"k1", &ReadVisibility::from_txn_id(7)).unwrap(),
            Some(b"txn".to_vec())
        );
        assert_eq!(
            tree.get(b"k1", &ReadVisibility::from_txn_id(TXN_NONE))
                .unwrap(),
            Some(b"base".to_vec())
        );
    }

    #[test]
    fn put_exposes_inserted_row_before_materialization() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("btree_put_insert.wt");
        let mut tree = create_tree(&path);
        let global_txn = Arc::new(GlobalTxnState::new());
        let mut txn = global_txn.begin_snapshot_txn();

        tree.put(TEST_STORE_ID, b"k1", b"txn", &mut txn).unwrap();

        assert_eq!(
            tree.get(b"k1", &ReadVisibility::from_txn_id(9)).unwrap(),
            Some(b"txn".to_vec())
        );
        assert_eq!(
            tree.get(b"k1", &ReadVisibility::from_txn_id(TXN_NONE))
                .unwrap(),
            None
        );
    }

    #[test]
    fn delete_hides_existing_base_row() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("btree_delete_existing.wt");
        let mut tree = create_tree(&path);
        let global_txn = Arc::new(GlobalTxnState::new());
        let mut txn = global_txn.begin_snapshot_txn();

        tree.write_base_row(b"k1", b"base").unwrap();
        tree.delete(TEST_STORE_ID, b"k1", &mut txn).unwrap();

        assert_eq!(
            tree.get(b"k1", &ReadVisibility::from_txn_id(11)).unwrap(),
            None
        );
        assert_eq!(
            tree.get(b"k1", &ReadVisibility::from_txn_id(TXN_NONE))
                .unwrap(),
            Some(b"base".to_vec())
        );
    }
}
