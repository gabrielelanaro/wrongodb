use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::page_store::{PageRead, PageType, ReadPin};

use super::layout::{map_internal_err, map_leaf_err};
use super::page::{InternalPage, LeafPage};
use super::search::{internal_child_at, internal_child_index_for_key, leaf_lower_bound};

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
/// - **Stack-based cursor**: Tracks position through internal nodes for efficient advance
pub struct BTreeRangeIter<'a> {
    pager: Option<&'a mut (dyn PageRead + 'a)>,
    end: Option<Vec<u8>>,
    stack: Vec<CursorFrame>,
    leaf: Option<ReadPin>,
    slot_idx: usize,
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
            stack: Vec::new(),
            leaf: None,
            slot_idx: 0,
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
    ) -> Result<Self, WrongoDBError> {
        let mut iter = Self {
            pager: Some(pager),
            end: end.map(|value| value.to_vec()),
            stack: Vec::new(),
            leaf: None,
            slot_idx: 0,
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
                    self.slot_idx = slot_idx;
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
                    self.slot_idx = 0;
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

            let step = {
                let pager = match self.pager.as_deref_mut() {
                    Some(pager) => pager,
                    None => {
                        self.done = true;
                        return Some(Err(missing_pager()));
                    }
                };
                let page = pager.get_page(&leaf_pin);
                let leaf = match LeafPage::open(page) {
                    Ok(leaf) => leaf,
                    Err(err) => {
                        return Some(Err(StorageError(format!(
                            "corrupt leaf {}: {err}",
                            leaf_pin.page_id()
                        ))
                        .into()));
                    }
                };

                if self.slot_idx < leaf.slot_count() {
                    let key = match leaf.key_at(self.slot_idx) {
                        Ok(value) => value.to_vec(),
                        Err(err) => return Some(Err(map_leaf_err(err))),
                    };
                    if let Some(end) = &self.end {
                        if key.as_slice() >= end.as_slice() {
                            NextStep::End
                        } else {
                            let value = match leaf.value_at(self.slot_idx) {
                                Ok(value) => value.to_vec(),
                                Err(err) => return Some(Err(map_leaf_err(err))),
                            };
                            NextStep::Yield(key, value)
                        }
                    } else {
                        let value = match leaf.value_at(self.slot_idx) {
                            Ok(value) => value.to_vec(),
                            Err(err) => return Some(Err(map_leaf_err(err))),
                        };
                        NextStep::Yield(key, value)
                    }
                } else {
                    NextStep::Advance
                }
            };

            match step {
                NextStep::Yield(key, value) => {
                    self.slot_idx += 1;
                    self.leaf = Some(leaf_pin);
                    return Some(Ok((key, value)));
                }
                NextStep::End => {
                    self.done = true;
                    if let Some(pager) = self.pager.as_deref_mut() {
                        pager.unpin_page(leaf_pin);
                    }
                    return None;
                }
                NextStep::Advance => {
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

/// Result of processing a leaf during iteration.
enum NextStep {
    Yield(Vec<u8>, Vec<u8>),
    Advance,
    End,
}

// ============================================================================
// Helper Functions
// ============================================================================

fn missing_pager() -> WrongoDBError {
    StorageError("range iterator has no backing page store".into()).into()
}
