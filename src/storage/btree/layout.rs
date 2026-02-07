use crate::core::errors::{StorageError, WrongoDBError};

use super::page::{InternalPage, InternalPageError, LeafPage, LeafPageError};
use super::{InternalEntries, LeafEntries};

const PAGE_TYPE_LEAF: u8 = 1;
const PAGE_TYPE_INTERNAL: u8 = 2;

type PageBytes = Vec<u8>;
type InternalKeyChildEntries = Vec<(Vec<u8>, u64)>;
pub(super) type LeafSplit = (PageBytes, PageBytes, Vec<u8>, usize);
pub(super) type InternalSplit = (
    PageBytes,
    PageBytes,
    Vec<u8>,
    u64,
    InternalKeyChildEntries,
    usize,
);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PageType {
    Leaf,
    Internal,
}

pub(super) fn page_type(payload: &[u8]) -> Result<PageType, WrongoDBError> {
    let Some(t) = payload.first().copied() else {
        return Err(StorageError("empty page payload".into()).into());
    };
    match t {
        PAGE_TYPE_LEAF => Ok(PageType::Leaf),
        PAGE_TYPE_INTERNAL => Ok(PageType::Internal),
        other => Err(StorageError(format!("unknown page type: {other}")).into()),
    }
}

pub(super) fn map_leaf_err(e: LeafPageError) -> WrongoDBError {
    StorageError(e.to_string()).into()
}

pub(super) fn map_internal_err(e: InternalPageError) -> WrongoDBError {
    StorageError(e.to_string()).into()
}

pub(super) fn leaf_entries(buf: &mut [u8]) -> Result<LeafEntries, WrongoDBError> {
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

pub(super) fn internal_entries(buf: &mut [u8]) -> Result<InternalEntries, WrongoDBError> {
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

pub(super) fn split_leaf_entries(
    entries: &[(Vec<u8>, Vec<u8>)],
    payload_len: usize,
) -> Result<LeafSplit, WrongoDBError> {
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

pub(super) fn build_leaf_page(
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
            return Ok((
                l,
                r,
                promoted_key,
                first_child,
                left_entries.to_vec(),
                promote_idx,
            ));
        }
    }

    Err(StorageError("internal split impossible (record too large?)".into()).into())
}

pub(super) fn build_internal_page(
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
