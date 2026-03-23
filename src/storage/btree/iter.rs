use std::collections::BTreeMap;

use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::page_store::{Page, PageRead, PageType, ReadPin, RowInsert};
use crate::txn::{ReadVisibility, TXN_NONE};

use super::layout::{map_internal_err, map_leaf_err};
use super::page::{InternalPage, LeafPage};
use super::search::{internal_child_at, internal_child_index_for_key, leaf_lower_bound};
use super::visible_chain_value;

// ============================================================================
// BTreeRangeIter (Public API)
// ============================================================================

/// Range iterator over B+tree key-value pairs.
///
/// Yields entries in sorted order from `start` (inclusive) to `end` (exclusive).
/// Manages page pinning to prevent eviction during iteration.
///
/// Key design decisions:
/// - **Pager ownership**: Takes `&mut dyn PageRead` to allow unpinning pages during traversal
/// - **Lazy navigation**: Only pins pages as needed, releases internal nodes after descending
/// - **Leaf-local visibility merge**: Each leaf is materialized into a visible row view that merges
///   base rows, `row_updates`, and `row_inserts` before iteration continues
pub(crate) struct BTreeRangeIter<'a> {
    pager: Option<&'a mut (dyn PageRead + 'a)>,
    end: Option<Vec<u8>>,
    visibility: ReadVisibility,
    stack: Vec<CursorFrame>,
    leaf: Option<ReadPin>,
    leaf_entries: Vec<(Vec<u8>, Vec<u8>)>,
    entry_idx: usize,
    done: bool,
}

impl<'a> BTreeRangeIter<'a> {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Creates an empty iterator that yields nothing.
    ///
    /// Used when a B+tree is empty or when no matching range exists.
    pub(super) fn empty() -> Self {
        Self {
            pager: None,
            end: None,
            visibility: ReadVisibility::from_txn_id(TXN_NONE),
            stack: Vec::new(),
            leaf: None,
            leaf_entries: Vec::new(),
            entry_idx: 0,
            done: true,
        }
    }

    /// Creates a range iterator over the B+tree rooted at `root`.
    ///
    /// - `start`: Inclusive lower bound, or `None` to start from the first key
    /// - `end`: Exclusive upper bound, or `None` to iterate to the end
    ///
    /// The iterator seeks to the starting position during construction,
    /// pinning the initial leaf page.
    pub(super) fn new(
        pager: &'a mut (dyn PageRead + 'a),
        root: u64,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        visibility: &ReadVisibility,
    ) -> Result<Self, WrongoDBError> {
        let mut iter = Self {
            pager: Some(pager),
            end: end.map(|value| value.to_vec()),
            visibility: *visibility,
            stack: Vec::new(),
            leaf: None,
            leaf_entries: Vec::new(),
            entry_idx: 0,
            done: false,
        };
        iter.seek(root, start)?;
        Ok(iter)
    }

    // ------------------------------------------------------------------------
    // Private Helpers: Navigation
    // ------------------------------------------------------------------------

    fn clear_leaf(&mut self) -> Result<(), WrongoDBError> {
        if let Some(leaf_pin) = self.leaf.take() {
            let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
            pager.unpin_page(leaf_pin);
        }
        self.leaf_entries.clear();
        self.entry_idx = 0;
        Ok(())
    }

    fn seek(&mut self, root: u64, start: Option<&[u8]>) -> Result<(), WrongoDBError> {
        self.clear_leaf()?;
        self.stack.clear();

        let mut node_id = root;
        loop {
            let pin = {
                let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
                pager.pin_page(node_id)?
            };

            let step = {
                let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
                let page = pager.get_page(&pin);
                match page.header().page_type {
                    PageType::Leaf => {
                        let leaf = LeafPage::open(page)
                            .map_err(|e| StorageError(format!("corrupt leaf {node_id}: {e}")))?;
                        let slot_idx = if let Some(start_key) = start {
                            leaf_lower_bound(&leaf, start_key).map_err(map_leaf_err)?
                        } else {
                            0
                        };
                        SeekStep::Leaf { slot_idx }
                    }
                    PageType::Internal => {
                        let internal = InternalPage::open(page).map_err(|e| {
                            StorageError(format!("corrupt internal {node_id}: {e}"))
                        })?;
                        let child_idx = if let Some(start_key) = start {
                            internal_child_index_for_key(&internal, start_key)
                                .map_err(map_internal_err)?
                        } else {
                            0
                        };
                        let child_id =
                            internal_child_at(&internal, child_idx).map_err(map_internal_err)?;
                        SeekStep::Descend {
                            child_idx,
                            child_id,
                        }
                    }
                }
            };

            match step {
                SeekStep::Leaf { slot_idx } => {
                    self.leaf = Some(pin);
                    self.load_current_leaf_entries(start, Some(slot_idx))?;
                    return Ok(());
                }
                SeekStep::Descend {
                    child_idx,
                    child_id,
                } => {
                    self.stack.push(CursorFrame {
                        internal_id: node_id,
                        child_idx,
                    });
                    let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
                    pager.unpin_page(pin);
                    node_id = child_id;
                }
            }
        }
    }

    fn advance_to_next_leaf(&mut self) -> Result<bool, WrongoDBError> {
        self.clear_leaf()?;

        while let Some(frame) = self.stack.pop() {
            let pin = {
                let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
                pager.pin_page(frame.internal_id)?
            };

            let step = {
                let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
                let page = pager.get_page(&pin);
                let internal = InternalPage::open(page).map_err(|e| {
                    StorageError(format!("corrupt internal {}: {e}", frame.internal_id))
                })?;
                let children_count = internal.slot_count() + 1;
                if frame.child_idx + 1 >= children_count {
                    AdvanceStep::KeepClimbing
                } else {
                    let next_child_idx = frame.child_idx + 1;
                    let next_child_id =
                        internal_child_at(&internal, next_child_idx).map_err(map_internal_err)?;
                    self.stack.push(CursorFrame {
                        internal_id: frame.internal_id,
                        child_idx: next_child_idx,
                    });
                    AdvanceStep::Descend(next_child_id)
                }
            };

            let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
            pager.unpin_page(pin);

            match step {
                AdvanceStep::Descend(next_child_id) => {
                    self.descend_leftmost(next_child_id)?;
                    return Ok(true);
                }
                AdvanceStep::KeepClimbing => {}
            }
        }

        Ok(false)
    }

    fn descend_leftmost(&mut self, start_id: u64) -> Result<(), WrongoDBError> {
        let mut node_id = start_id;
        loop {
            let pin = {
                let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
                pager.pin_page(node_id)?
            };

            let step = {
                let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
                let page = pager.get_page(&pin);
                match page.header().page_type {
                    PageType::Leaf => AdvanceStep::Descend(node_id),
                    PageType::Internal => {
                        let internal = InternalPage::open(page).map_err(|e| {
                            StorageError(format!("corrupt internal {node_id}: {e}"))
                        })?;
                        let child_id = internal.first_child();
                        self.stack.push(CursorFrame {
                            internal_id: node_id,
                            child_idx: 0,
                        });
                        AdvanceStep::Descend(child_id)
                    }
                }
            };

            match step {
                AdvanceStep::Descend(next_id) if next_id == node_id => {
                    self.leaf = Some(pin);
                    self.load_current_leaf_entries(None, None)?;
                    return Ok(());
                }
                AdvanceStep::Descend(next_id) => {
                    let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
                    pager.unpin_page(pin);
                    node_id = next_id;
                }
                AdvanceStep::KeepClimbing => unreachable!(),
            }
        }
    }

    fn load_current_leaf_entries(
        &mut self,
        start: Option<&[u8]>,
        slot_idx_hint: Option<usize>,
    ) -> Result<(), WrongoDBError> {
        let leaf_pin = self
            .leaf
            .as_ref()
            .ok_or_else(|| StorageError("range iterator missing current leaf pin".into()))?;
        let pager = self.pager.as_deref_mut().ok_or_else(missing_pager)?;
        let page = pager.get_page(leaf_pin);
        self.leaf_entries = collect_visible_leaf_entries(page, &self.visibility)?;
        self.entry_idx = match start {
            Some(start_key) => lower_bound_entry(&self.leaf_entries, start_key),
            None => slot_idx_hint.unwrap_or(0).min(self.leaf_entries.len()),
        };
        Ok(())
    }
}

// ============================================================================
// Iterator Implementation
// ============================================================================

impl<'a> Iterator for BTreeRangeIter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>), WrongoDBError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let Some(leaf_pin) = self.leaf.take() else {
                self.done = true;
                return None;
            };

            if self.entry_idx < self.leaf_entries.len() {
                let (key, value) = self.leaf_entries[self.entry_idx].clone();
                if let Some(end) = &self.end {
                    if key.as_slice() >= end.as_slice() {
                        self.done = true;
                        if let Some(pager) = self.pager.as_deref_mut() {
                            pager.unpin_page(leaf_pin);
                        }
                        return None;
                    }
                }

                self.entry_idx += 1;
                self.leaf = Some(leaf_pin);
                return Some(Ok((key, value)));
            }

            self.leaf = Some(leaf_pin);
            match self.advance_to_next_leaf() {
                Ok(true) => continue,
                Ok(false) => {
                    self.done = true;
                    return None;
                }
                Err(err) => {
                    self.done = true;
                    return Some(Err(err));
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

// ============================================================================
// Helper Types (Internal)
// ============================================================================

/// Tracks position within an internal node during traversal.
#[derive(Debug, Clone, Copy)]
struct CursorFrame {
    internal_id: u64,
    child_idx: usize,
}

/// Result of seeking to a position in the tree.
enum SeekStep {
    Leaf { slot_idx: usize },
    Descend { child_idx: usize, child_id: u64 },
}

/// Result of advancing through internal nodes.
enum AdvanceStep {
    Descend(u64),
    KeepClimbing,
}

type KeyValue = (Vec<u8>, Vec<u8>);

// ============================================================================
// Helper Functions
// ============================================================================

fn collect_visible_leaf_entries(
    page: &Page,
    visibility: &ReadVisibility,
) -> Result<Vec<KeyValue>, WrongoDBError> {
    let leaf =
        LeafPage::open(page).map_err(|err| StorageError(format!("corrupt leaf page: {err}")))?;
    let mut by_key = BTreeMap::new();

    let Some(modify) = page.row_modify() else {
        let mut out = Vec::new();
        for slot_idx in 0..leaf.slot_count() {
            out.push((
                leaf.key_at(slot_idx).map_err(map_leaf_err)?.to_vec(),
                leaf.value_at(slot_idx).map_err(map_leaf_err)?.to_vec(),
            ));
        }
        return Ok(out);
    };

    for bucket in modify.row_inserts() {
        append_visible_bucket(&mut by_key, bucket, visibility);
    }

    for slot_idx in 0..leaf.slot_count() {
        let key = leaf.key_at(slot_idx).map_err(map_leaf_err)?.to_vec();
        let base_value = leaf.value_at(slot_idx).map_err(map_leaf_err)?.to_vec();
        match modify
            .row_updates()
            .get(slot_idx)
            .as_ref()
            .and_then(|chain| chain.as_ref())
            .and_then(|chain| visible_chain_value(chain, visibility))
        {
            Some(Some(value)) => {
                by_key.insert(key, Some(value));
            }
            Some(None) => {
                by_key.insert(key, None);
            }
            None => {
                by_key.entry(key).or_insert(Some(base_value));
            }
        }
    }

    Ok(by_key
        .into_iter()
        .filter_map(|(key, value)| value.map(|value| (key, value)))
        .collect())
}

fn append_visible_bucket(
    out: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    bucket: &[RowInsert],
    visibility: &ReadVisibility,
) {
    for insert in bucket {
        if let Some(value) = visible_chain_value(insert.updates(), visibility) {
            out.insert(insert.key().to_vec(), value);
        }
    }
}

fn lower_bound_entry(entries: &[(Vec<u8>, Vec<u8>)], key: &[u8]) -> usize {
    entries
        .binary_search_by(|(entry_key, _)| entry_key.as_slice().cmp(key))
        .unwrap_or_else(|index| index)
}

fn missing_pager() -> WrongoDBError {
    StorageError("range iterator has no backing page store".into()).into()
}
