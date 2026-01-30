use crate::core::errors::{StorageError, WrongoDBError};

use super::layout::{map_internal_err, map_leaf_err, page_type, PageType};
use super::page::{InternalPage, InternalPageError, LeafPage, LeafPageError};
use super::pager::{PageStore, PinnedPage};

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
    pager: Option<&'a mut (dyn PageStore + 'a)>,
    end: Option<Vec<u8>>,
    stack: Vec<CursorFrame>,
    leaf: Option<PinnedPage>,
    slot_idx: usize,
    done: bool,
}

fn missing_pager() -> WrongoDBError {
    StorageError("range iterator has no backing Pager".into()).into()
}

impl<'a> BTreeRangeIter<'a> {
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

    pub(super) fn new(
        pager: &'a mut (dyn PageStore + 'a),
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

    fn clear_leaf(&mut self) -> Result<(), WrongoDBError> {
        if let Some(leaf) = self.leaf.take() {
            let pager = self
                .pager
                .as_deref_mut()
                .ok_or_else(missing_pager)?;
            pager.unpin_page(leaf.page_id());
        }
        Ok(())
    }

    fn seek(&mut self, root: u64, start: Option<&[u8]>) -> Result<(), WrongoDBError> {
        self.clear_leaf()?;
        self.stack.clear();
        let mut node_id = root;
        loop {
            let pager = self
                .pager
                .as_deref_mut()
                .ok_or_else(missing_pager)?;
            let mut page = pager.pin_page(node_id)?;
            let page_id = page.page_id();
            let page_type = match page_type(page.payload()) {
                Ok(t) => t,
                Err(e) => {
                    pager.unpin_page(page_id);
                    return Err(e);
                }
            };
            match page_type {
                PageType::Leaf => {
                    let leaf = match LeafPage::open(page.payload_mut()) {
                        Ok(leaf) => leaf,
                        Err(e) => {
                            pager.unpin_page(page_id);
                            return Err(StorageError(format!("corrupt leaf {node_id}: {e}")).into());
                        }
                    };
                    let slot_idx = if let Some(start_key) = start {
                        match leaf_lower_bound(&leaf, start_key).map_err(map_leaf_err) {
                            Ok(idx) => idx,
                            Err(e) => {
                                pager.unpin_page(page_id);
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
                            pager.unpin_page(page_id);
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
                                pager.unpin_page(page_id);
                                return Err(e);
                            }
                        }
                    } else {
                        0
                    };
                    let child_id = match internal_child_at(&internal, child_idx).map_err(map_internal_err) {
                        Ok(id) => id,
                        Err(e) => {
                            pager.unpin_page(page_id);
                            return Err(e);
                        }
                    };
                    self.stack.push(CursorFrame {
                        internal_id: node_id,
                        child_idx,
                    });
                    pager.unpin_page(page_id);
                    node_id = child_id;
                }
            }
        }
    }

    fn advance_to_next_leaf(&mut self) -> Result<bool, WrongoDBError> {
        self.clear_leaf()?;
        while let Some(frame) = self.stack.pop() {
            let pager = self
                .pager
                .as_deref_mut()
                .ok_or_else(missing_pager)?;
            let mut page = pager.pin_page(frame.internal_id)?;
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
                    pager.unpin_page(page_id);
                    return Err(e);
                }
            };
            pager.unpin_page(page_id);
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
            let pager = self
                .pager
                .as_deref_mut()
                .ok_or_else(missing_pager)?;
            let mut page = pager.pin_page(node_id)?;
            let page_id = page.page_id();
            let page_type = match page_type(page.payload()) {
                Ok(t) => t,
                Err(e) => {
                    pager.unpin_page(page_id);
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
                            pager.unpin_page(page_id);
                            return Err(
                                StorageError(format!("corrupt internal {node_id}: {e}")).into(),
                            );
                        }
                    };
                    let child_id = match internal.first_child().map_err(map_internal_err) {
                        Ok(id) => id,
                        Err(e) => {
                            pager.unpin_page(page_id);
                            return Err(e);
                        }
                    };
                    self.stack.push(CursorFrame {
                        internal_id: node_id,
                        child_idx: 0,
                    });
                    pager.unpin_page(page_id);
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
                    if let Some(pager) = self.pager.as_deref_mut() {
                        pager.unpin_page(leaf_id);
                    }
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
                    if let Some(pager) = self.pager.as_deref_mut() {
                        pager.unpin_page(leaf_id);
                    }
                    return Some(Err(map_leaf_err(e)));
                }
            };

            if self.slot_idx < slot_count {
                let k = match leaf.key_at(self.slot_idx) {
                    Ok(v) => v.to_vec(),
                    Err(e) => {
                        self.done = true;
                        if let Some(pager) = self.pager.as_deref_mut() {
                            pager.unpin_page(leaf_id);
                        }
                        return Some(Err(map_leaf_err(e)));
                    }
                };
                if let Some(end) = &self.end {
                    if k.as_slice() >= end.as_slice() {
                        self.done = true;
                        if let Some(pager) = self.pager.as_deref_mut() {
                            pager.unpin_page(leaf_id);
                        }
                        return None;
                    }
                }
                let v = match leaf.value_at(self.slot_idx) {
                    Ok(v) => v.to_vec(),
                    Err(e) => {
                        self.done = true;
                        if let Some(pager) = self.pager.as_deref_mut() {
                            pager.unpin_page(leaf_id);
                        }
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
