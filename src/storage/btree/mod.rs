mod internal_ops;
mod iter;
mod layout;
mod leaf_ops;
pub mod page;
mod search;

pub use iter::BTreeRangeIter;

use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::page_store::{Page, PageEdit, PageRead, PageStore, PageType};
use crate::txn::{ReadVisibility, UpdateChain, UpdateType};
use internal_ops::put_separator as put_internal_separator;
use layout::{
    build_internal_page, internal_entries, leaf_entries, map_leaf_err, split_internal_entries,
    split_leaf_entries,
};
use leaf_ops::{
    contains_key as leaf_contains_key, delete as delete_from_leaf_page, put as put_in_leaf_page,
};
use page::{InternalPage, LeafPage, LeafPageError};
use search::{child_for_key, search_leaf};

// ============================================================================
// Type Aliases
// ============================================================================

type Key = Vec<u8>;
type Value = Vec<u8>;
type KeyValuePair = (Key, Value);
type KeyChildId = (Key, u64);
type LeafEntries = Vec<KeyValuePair>;
type InternalEntries = (u64, Vec<KeyChildId>);
type KeyValueIter<'a> = BTreeRangeIter<'a>;

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

// ============================================================================
// BTreeCursor (Public API)
// ============================================================================

/// B+tree cursor providing read, write, and checkpoint operations.
///
/// `BTreeCursor` is the main interface for B+tree operations, managing:
///
/// - **Point queries**: `get` retrieves a single key-value pair
/// - **Range scans**: `range` returns an iterator over key ranges
/// - **Mutations**: `put` and `delete` modify tree structure with automatic splitting
/// - **Checkpointing**: `checkpoint` flushes dirty pages and creates a consistent recovery point
///
/// The cursor owns a [`PageStore`] which handles page caching, copy-on-write,
/// and coordination with the underlying block file.
#[derive(Debug)]
pub struct BTreeCursor {
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
    pub fn new(store: Box<dyn PageStore>) -> Self {
        Self { page_store: store }
    }

    // ------------------------------------------------------------------------
    // Public API: Read Operations
    // ------------------------------------------------------------------------

    pub fn get(
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

    pub fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
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
        )
    }

    // ------------------------------------------------------------------------
    // Public API: Write Operations
    // ------------------------------------------------------------------------

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError> {
        let root = self.page_store.root_page_id();
        if root == NONE_PAGE_ID {
            return Err(StorageError("btree missing root".into()).into());
        }

        let result = self.insert_recursive(root, key, value, InsertMode::Upsert)?;
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

    pub fn delete(&mut self, key: &[u8]) -> Result<bool, WrongoDBError> {
        let root = self.page_store.root_page_id();
        if root == NONE_PAGE_ID {
            return Ok(false);
        }

        let result = self.delete_recursive(root, key)?;
        self.page_store.set_root_page_id(result.new_node_id)?;
        Ok(result.deleted)
    }

    // ------------------------------------------------------------------------
    // Public API: Lifecycle Operations
    // ------------------------------------------------------------------------

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        let root = self.page_store.checkpoint_prepare();
        self.page_store.checkpoint_flush_data()?;
        self.page_store.checkpoint_commit(root)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Private Helpers: Delete Operations
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
                if let Some(chain) = modify.row_updates()[position.index].as_ref() {
                    if let Some(value) = visible_chain_value(chain, visibility) {
                        return Ok(value);
                    }
                }
            } else {
                for insert in &modify.row_inserts()[position.index] {
                    if insert.key() != key {
                        continue;
                    }
                    if let Some(value) = visible_chain_value(insert.updates(), visibility) {
                        return Ok(value);
                    }
                }
            }
        }

        if !position.found {
            return Ok(None);
        }

        Ok(Some(leaf.value_at(position.index)?.to_vec()))
    }

    fn delete_recursive(
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

        let child_result = self.delete_recursive(child_id, key)?;
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

    fn insert_recursive(
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

fn visible_chain_value(
    chain: &UpdateChain,
    visibility: &ReadVisibility,
) -> Option<Option<Vec<u8>>> {
    for update in chain.iter() {
        if !visibility.can_see(update) {
            continue;
        }

        return match update.type_ {
            UpdateType::Standard => Some(Some(update.data.clone())),
            UpdateType::Tombstone => Some(None),
            UpdateType::Reserve => continue,
        };
    }

    None
}

fn upsert_entry(entries: &mut Vec<(Vec<u8>, Vec<u8>)>, key: &[u8], value: &[u8]) {
    match entries.binary_search_by(|(existing_key, _)| existing_key.as_slice().cmp(key)) {
        Ok(index) => entries[index].1 = value.to_vec(),
        Err(index) => entries.insert(index, (key.to_vec(), value.to_vec())),
    }
}

fn upsert_internal_entry(entries: &mut Vec<(Vec<u8>, u64)>, key: &[u8], child: u64) {
    match entries.binary_search_by(|(existing_key, _)| existing_key.as_slice().cmp(key)) {
        Ok(index) => entries[index].1 = child,
        Err(index) => entries.insert(index, (key.to_vec(), child)),
    }
}

fn child_index_for_key(entries: &[(Vec<u8>, u64)], key: &[u8]) -> usize {
    let mut index = 0;
    for (entry_idx, (sep_key, _)) in entries.iter().enumerate() {
        if key < sep_key.as_slice() {
            break;
        }
        index = entry_idx + 1;
    }
    index
}
