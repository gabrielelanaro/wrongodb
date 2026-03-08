use super::page::{InternalPage, InternalPageError, LeafPage, LeafPageError};

// ============================================================================
// Helper Types
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LeafSearch {
    pub(super) index: usize,
    pub(super) found: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct InternalSearch {
    pub(super) index: usize,
    pub(super) found: bool,
}

// ============================================================================
// Public Functions
// ============================================================================

pub(super) fn search_leaf(leaf: &LeafPage<'_>, key: &[u8]) -> Result<LeafSearch, LeafPageError> {
    let slot_count = leaf.slot_count();
    let mut lo = 0usize;
    let mut hi = slot_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_key = leaf.key_at(mid)?;
        match mid_key.cmp(key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Equal => {
                return Ok(LeafSearch {
                    index: mid,
                    found: true,
                });
            }
            std::cmp::Ordering::Greater => hi = mid,
        }
    }

    Ok(LeafSearch {
        index: lo,
        found: false,
    })
}

pub(super) fn leaf_lower_bound(leaf: &LeafPage<'_>, key: &[u8]) -> Result<usize, LeafPageError> {
    Ok(search_leaf(leaf, key)?.index)
}

pub(super) fn internal_child_index_for_key(
    internal: &InternalPage<'_>,
    key: &[u8],
) -> Result<usize, InternalPageError> {
    let n = internal.slot_count();
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

pub(super) fn search_internal(
    internal: &InternalPage<'_>,
    key: &[u8],
) -> Result<InternalSearch, InternalPageError> {
    let slot_count = internal.slot_count();
    let mut lo = 0usize;
    let mut hi = slot_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let mid_key = internal.key_at(mid)?;
        match mid_key.cmp(key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Equal => {
                return Ok(InternalSearch {
                    index: mid,
                    found: true,
                });
            }
            std::cmp::Ordering::Greater => hi = mid,
        }
    }

    Ok(InternalSearch {
        index: lo,
        found: false,
    })
}

pub(super) fn internal_child_at(
    internal: &InternalPage<'_>,
    child_idx: usize,
) -> Result<u64, InternalPageError> {
    if child_idx == 0 {
        Ok(internal.first_child())
    } else {
        internal.child_at(child_idx - 1)
    }
}

pub(super) fn child_for_key(
    internal: &InternalPage<'_>,
    key: &[u8],
) -> Result<u64, InternalPageError> {
    let child_idx = internal_child_index_for_key(internal, key)?;
    internal_child_at(internal, child_idx)
}
