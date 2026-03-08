use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::page_store::Page;

use super::internal_ops;
use super::leaf_ops;
use super::page::{InternalPage, InternalPageError, LeafPage, LeafPageError};
use super::{InternalEntries, LeafEntries};

// ============================================================================
// Type Aliases
// ============================================================================

type InternalKeyChildEntries = Vec<(Vec<u8>, u64)>;
pub(super) type LeafSplit = (Page, Page, Vec<u8>, usize);
pub(super) type InternalSplit = (Page, Page, Vec<u8>, u64, InternalKeyChildEntries, usize);

// ============================================================================
// Public Functions: Error Mapping
// ============================================================================

pub(super) fn map_leaf_err(err: LeafPageError) -> WrongoDBError {
    StorageError(err.to_string()).into()
}

pub(super) fn map_internal_err(err: InternalPageError) -> WrongoDBError {
    StorageError(err.to_string()).into()
}

// ============================================================================
// Public Functions: Entry Extraction
// ============================================================================

pub(super) fn leaf_entries(page: &Page) -> Result<LeafEntries, WrongoDBError> {
    let leaf = LeafPage::open(page).map_err(|e| StorageError(format!("corrupt leaf: {e}")))?;
    let mut out = Vec::with_capacity(leaf.slot_count());
    for i in 0..leaf.slot_count() {
        out.push((
            leaf.key_at(i).map_err(map_leaf_err)?.to_vec(),
            leaf.value_at(i).map_err(map_leaf_err)?.to_vec(),
        ));
    }
    Ok(out)
}

pub(super) fn internal_entries(page: &Page) -> Result<InternalEntries, WrongoDBError> {
    let internal =
        InternalPage::open(page).map_err(|e| StorageError(format!("corrupt internal: {e}")))?;
    let mut out = Vec::with_capacity(internal.slot_count());
    for i in 0..internal.slot_count() {
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
    Ok((internal.first_child(), out))
}

// ============================================================================
// Public Functions: Page Building
// ============================================================================

pub(super) fn build_leaf_page(
    entries: &[(Vec<u8>, Vec<u8>)],
    payload_len: usize,
) -> Result<Page, WrongoDBError> {
    let mut page = Page::new_leaf(payload_len)
        .map_err(|e| StorageError(format!("init leaf page failed: {e}")))?;
    for (key, value) in entries {
        leaf_ops::put(&mut page, key, value).map_err(map_leaf_err)?;
    }
    Ok(page)
}

pub(super) fn build_internal_page(
    first_child: u64,
    entries: &[(Vec<u8>, u64)],
    payload_len: usize,
) -> Result<Page, WrongoDBError> {
    let mut page = Page::new_internal(payload_len, first_child)
        .map_err(|e| StorageError(format!("init internal page failed: {e}")))?;
    for (key, child) in entries {
        internal_ops::put_separator(&mut page, key, *child).map_err(map_internal_err)?;
    }
    Ok(page)
}

// ============================================================================
// Public Functions: Split Operations
// ============================================================================

pub(super) fn split_leaf_entries(
    entries: &[(Vec<u8>, Vec<u8>)],
    payload_len: usize,
) -> Result<LeafSplit, WrongoDBError> {
    if entries.len() < 2 {
        return Err(StorageError("cannot split leaf with <2 entries".into()).into());
    }
    let mid = entries.len() / 2;

    let mut candidates = Vec::new();
    for delta in 0..entries.len() {
        let left = mid.saturating_sub(delta);
        let right = mid + delta;
        if left >= 1 && left < entries.len() {
            candidates.push(left);
        }
        if right >= 1 && right < entries.len() && right != left {
            candidates.push(right);
        }
    }
    candidates.retain(|&index| index < entries.len());
    candidates.dedup();

    for split_idx in candidates {
        if split_idx == 0 || split_idx >= entries.len() {
            continue;
        }
        let left = build_leaf_page(&entries[..split_idx], payload_len);
        let right = build_leaf_page(&entries[split_idx..], payload_len);
        if let (Ok(left_page), Ok(right_page)) = (left, right) {
            return Ok((
                left_page,
                right_page,
                entries[split_idx].0.clone(),
                split_idx,
            ));
        }
    }

    Err(StorageError("leaf split impossible (record too large?)".into()).into())
}

pub(super) fn split_internal_entries(
    first_child: u64,
    entries: &[(Vec<u8>, u64)],
    payload_len: usize,
) -> Result<InternalSplit, WrongoDBError> {
    if entries.is_empty() {
        return Err(StorageError("cannot split internal with 0 separators".into()).into());
    }

    let mid = entries.len() / 2;
    let mut candidates = Vec::new();
    for delta in 0..entries.len() {
        let left = mid.saturating_sub(delta);
        let right = mid + delta;
        if left < entries.len() {
            candidates.push(left);
        }
        if right < entries.len() && right != left {
            candidates.push(right);
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
        if let (Ok(left_page), Ok(right_page)) = (left, right) {
            return Ok((
                left_page,
                right_page,
                promoted_key,
                first_child,
                left_entries.to_vec(),
                promote_idx,
            ));
        }
    }

    Err(StorageError("internal split impossible (record too large?)".into()).into())
}
