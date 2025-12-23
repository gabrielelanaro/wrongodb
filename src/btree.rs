use std::path::Path;

mod pager;

use self::pager::Pager;
use crate::errors::StorageError;
use crate::{
    InternalPage, InternalPageError, LeafPage, LeafPageError, WrongoDBError, NONE_BLOCK_ID,
};

// Type aliases for B-tree operations to clarify intent and reduce complexity

/// Represents a key in the B-tree (stored as bytes)
type Key = Vec<u8>;

/// Represents a value in the B-tree (stored as bytes)
type Value = Vec<u8>;

/// Represents serialized page data (stored as bytes)
type PageData = Vec<u8>;

/// A key-value pair for leaf node entries
type KeyValuePair = (Key, Value);

/// A key-child ID pair for internal node separators
type KeyChildId = (Key, u64);

/// Collection of key-value pairs from a leaf page
type LeafEntries = Vec<KeyValuePair>;

/// Internal page entries: (first_child_id, separators as key-child pairs)
type InternalEntries = (u64, Vec<KeyChildId>);

/// Result of splitting a page: (left_page_data, separator_key, right_page_data)
type SplitResult = (PageData, Key, PageData);

/// Iterator over key-value pairs, yielding results or errors
type KeyValueIter<'a> = BTreeRangeIter<'a>;

const PAGE_TYPE_LEAF: u8 = 1;
const PAGE_TYPE_INTERNAL: u8 = 2;

#[derive(Debug)]
pub struct BTree {
    pager: Pager,
}

impl BTree {
    pub fn create<P: AsRef<Path>>(path: P, page_size: usize) -> Result<Self, WrongoDBError> {
        let mut pager = Pager::create(path, page_size)?;
        init_root_if_missing(&mut pager)?;
        pager.checkpoint()?;
        Ok(Self { pager })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let mut pager = Pager::open(path)?;
        init_root_if_missing(&mut pager)?;
        Ok(Self { pager })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongoDBError> {
        let mut node_id = self.pager.root_page_id();
        if node_id == NONE_BLOCK_ID {
            return Ok(None);
        }

        loop {
            let mut payload = self.pager.read_page(node_id)?;
            match page_type(&payload)? {
                PageType::Leaf => {
                    let leaf = LeafPage::open(&mut payload)
                        .map_err(|e| StorageError(format!("corrupt leaf {node_id}: {e}")))?;
                    return leaf.get(key).map_err(map_leaf_err);
                }
                PageType::Internal => {
                    let internal = InternalPage::open(&mut payload)
                        .map_err(|e| StorageError(format!("corrupt internal {node_id}: {e}")))?;
                    node_id = internal
                        .child_for_key(key)
                        .map_err(|e| StorageError(format!("routing failed at {node_id}: {e}")))?;
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
        Ok(())
    }

    pub fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.pager.sync_all()
    }

    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.pager.checkpoint()
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
        let payload = self.pager.read_page(node_id)?;
        match page_type(&payload)? {
            PageType::Leaf => self.insert_into_leaf(node_id, payload, key, value),
            PageType::Internal => self.insert_into_internal(node_id, payload, key, value),
        }
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
        mut payload: Vec<u8>,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult, WrongoDBError> {
        {
            let mut leaf = LeafPage::open(&mut payload)
                .map_err(|e| StorageError(format!("corrupt leaf {node_id}: {e}")))?;
            match leaf.put(key, value) {
                Ok(()) => {
                    let new_leaf_id = self.pager.write_new_page(&payload)?;
                    self.pager.retire_page(node_id);
                    return Ok(InsertResult {
                        new_node_id: new_leaf_id,
                        split: None,
                    });
                }
                Err(LeafPageError::PageFull) => { /* split below */ }
                Err(e) => return Err(map_leaf_err(e)),
            }
        }

        let payload_len = payload.len();
        let mut entries = leaf_entries(&mut payload)?;
        upsert_entry(&mut entries, key, value);
        let (left_bytes, right_bytes, split_key) = split_leaf_entries(&entries, payload_len)?;

        let left_leaf_id = self.pager.write_new_page(&left_bytes)?;
        let right_leaf_id = self.pager.write_new_page(&right_bytes)?;
        self.pager.retire_page(node_id);
        Ok(InsertResult {
            new_node_id: left_leaf_id,
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
        node_id: u64,
        mut payload: Vec<u8>,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertResult, WrongoDBError> {
        let payload_len = payload.len();
        let (mut first_child, mut entries) = internal_entries(&mut payload)?;
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
            let new_internal_id = self.pager.write_new_page(&bytes)?;
            self.pager.retire_page(node_id);
            return Ok(InsertResult {
                new_node_id: new_internal_id,
                split: None,
            });
        }

        let (left_bytes, right_bytes, promoted_key) =
            split_internal_entries(first_child, &entries, payload_len)?;
        let left_internal_id = self.pager.write_new_page(&left_bytes)?;
        let right_internal_id = self.pager.write_new_page(&right_bytes)?;
        self.pager.retire_page(node_id);
        Ok(InsertResult {
            new_node_id: left_internal_id,
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
) -> Result<SplitResult, WrongoDBError> {
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
            return Ok((l, r, entries[split_idx].0.clone()));
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
) -> Result<SplitResult, WrongoDBError> {
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
            return Ok((l, r, promoted_key));
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
    leaf_id: u64,
    leaf_payload: Vec<u8>,
    slot_idx: usize,
    done: bool,
}

impl<'a> BTreeRangeIter<'a> {
    fn empty() -> Self {
        Self {
            pager: None,
            end: None,
            stack: Vec::new(),
            leaf_id: 0,
            leaf_payload: Vec::new(),
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
            leaf_id: 0,
            leaf_payload: Vec::new(),
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

    fn seek(&mut self, root: u64, start: Option<&[u8]>) -> Result<(), WrongoDBError> {
        self.stack.clear();
        let mut node_id = root;
        loop {
            let mut payload = self.pager_mut()?.read_page(node_id)?;
            match page_type(&payload)? {
                PageType::Leaf => {
                    let leaf = LeafPage::open(&mut payload)
                        .map_err(|e| StorageError(format!("corrupt leaf {node_id}: {e}")))?;
                    let slot_idx = if let Some(start_key) = start {
                        leaf_lower_bound(&leaf, start_key).map_err(map_leaf_err)?
                    } else {
                        0
                    };
                    self.leaf_id = node_id;
                    self.leaf_payload = payload;
                    self.slot_idx = slot_idx;
                    return Ok(());
                }
                PageType::Internal => {
                    let internal = InternalPage::open(&mut payload)
                        .map_err(|e| StorageError(format!("corrupt internal {node_id}: {e}")))?;
                    let child_idx = if let Some(start_key) = start {
                        internal_child_index_for_key(&internal, start_key)
                            .map_err(map_internal_err)?
                    } else {
                        0
                    };
                    let child_id =
                        internal_child_at(&internal, child_idx).map_err(map_internal_err)?;
                    self.stack.push(CursorFrame {
                        internal_id: node_id,
                        child_idx,
                    });
                    node_id = child_id;
                }
            }
        }
    }

    fn advance_to_next_leaf(&mut self) -> Result<bool, WrongoDBError> {
        while let Some(frame) = self.stack.pop() {
            let mut payload = self.pager_mut()?.read_page(frame.internal_id)?;
            let internal = InternalPage::open(&mut payload).map_err(|e| {
                StorageError(format!("corrupt internal {}: {e}", frame.internal_id))
            })?;

            let slots = internal.slot_count().map_err(map_internal_err)?;
            let children_count = slots + 1;
            if frame.child_idx + 1 >= children_count {
                continue;
            }

            let next_child_idx = frame.child_idx + 1;
            let next_child_id =
                internal_child_at(&internal, next_child_idx).map_err(map_internal_err)?;
            self.stack.push(CursorFrame {
                internal_id: frame.internal_id,
                child_idx: next_child_idx,
            });
            self.descend_leftmost(next_child_id)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn descend_leftmost(&mut self, start_id: u64) -> Result<(), WrongoDBError> {
        let mut node_id = start_id;
        loop {
            let payload = self.pager_mut()?.read_page(node_id)?;
            match page_type(&payload)? {
                PageType::Leaf => {
                    self.leaf_id = node_id;
                    self.leaf_payload = payload;
                    self.slot_idx = 0;
                    return Ok(());
                }
                PageType::Internal => {
                    let mut payload = payload;
                    let internal = InternalPage::open(&mut payload)
                        .map_err(|e| StorageError(format!("corrupt internal {node_id}: {e}")))?;
                    let child_id = internal.first_child().map_err(map_internal_err)?;
                    self.stack.push(CursorFrame {
                        internal_id: node_id,
                        child_idx: 0,
                    });
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
            let mut leaf_payload = std::mem::take(&mut self.leaf_payload);
            let leaf = match LeafPage::open(&mut leaf_payload) {
                Ok(v) => v,
                Err(e) => {
                    return Some(Err(StorageError(format!(
                        "corrupt leaf {}: {e}",
                        self.leaf_id
                    ))
                    .into()))
                }
            };

            let slot_count = match leaf.slot_count() {
                Ok(v) => v,
                Err(e) => return Some(Err(map_leaf_err(e))),
            };

            if self.slot_idx < slot_count {
                let k = match leaf.key_at(self.slot_idx) {
                    Ok(v) => v.to_vec(),
                    Err(e) => return Some(Err(map_leaf_err(e))),
                };
                if let Some(end) = &self.end {
                    if k.as_slice() >= end.as_slice() {
                        self.done = true;
                        self.leaf_payload = leaf_payload;
                        return None;
                    }
                }
                let v = match leaf.value_at(self.slot_idx) {
                    Ok(v) => v.to_vec(),
                    Err(e) => return Some(Err(map_leaf_err(e))),
                };
                self.slot_idx += 1;
                self.leaf_payload = leaf_payload;
                return Some(Ok((k, v)));
            }

            self.leaf_payload = leaf_payload;
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
