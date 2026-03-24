use crate::core::errors::{StorageError, WrongoDBError};

use super::skiplist::SkipList;

// ============================================================================
// Public types - Extent and ExtentLists (highest level)
// ============================================================================

/// Contiguous range of blocks with generation tracking for copy-on-write.
///
/// Extents represent allocated or free regions in the block file. The generation
/// field enables COW semantics by tracking which checkpoint allocated the extent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Extent {
    pub(super) offset: u64,
    pub(super) size: u64,
    pub(super) generation: u64,
}

/// Collection of extent lists tracking allocation state.
///
/// Three lists implement the extent-based allocation scheme:
/// - **alloc**: Extents reachable from the stable checkpoint (in use)
/// - **avail**: Free extents available for immediate allocation
/// - **discard**: Extents freed after the checkpoint, awaiting safe reclaim
#[derive(Debug, Clone, Default)]
pub(super) struct ExtentLists {
    pub(super) alloc: Vec<Extent>,
    pub(super) avail: Vec<Extent>,
    pub(super) discard: Vec<Extent>,
}

// ============================================================================
// BlockManager - Public API for extent allocation
// ============================================================================

/// Extent allocator with generation tracking for copy-on-write semantics.
///
/// `BlockManager` manages three extent lists (alloc/avail/discard) to support
/// crash recovery and checkpointing. Freed extents go to the discard list and
/// are only moved to avail after the stable checkpoint advances beyond their
/// generation, ensuring they're not referenced by any recoverable state.
///
/// Uses skip lists for efficient offset-based and size-based lookups during
/// allocation and coalescing operations.
#[derive(Debug, Clone)]
pub(super) struct BlockManager {
    alloc: ExtentIndex,
    avail: ExtentIndex,
    discard: ExtentIndex,
    stable_generation: u64,
}

impl BlockManager {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Create a new block manager with the given stable generation and initial extent lists.
    pub(super) fn new(stable_generation: u64, lists: ExtentLists) -> Self {
        let seed = stable_generation ^ 0xC0FFEE;
        let alloc = ExtentIndex::from_extents(seed ^ 0x11, lists.alloc);
        let avail = ExtentIndex::from_extents(seed ^ 0x22, lists.avail);
        let discard = ExtentIndex::from_extents(seed ^ 0x33, lists.discard);
        Self {
            alloc,
            avail,
            discard,
            stable_generation,
        }
    }

    // ------------------------------------------------------------------------
    // Getters/Setters
    // ------------------------------------------------------------------------

    pub(super) fn stable_generation(&self) -> u64 {
        self.stable_generation
    }

    pub(super) fn set_stable_generation(&mut self, generation: u64) {
        self.stable_generation = generation;
    }

    pub(super) fn extent_lists(&self) -> ExtentLists {
        ExtentLists {
            alloc: self.alloc.values_by_offset(),
            avail: self.avail.values_by_offset(),
            discard: self.discard.values_by_offset(),
        }
    }

    // ------------------------------------------------------------------------
    // Allocation API
    // ------------------------------------------------------------------------

    /// Allocate an extent of the requested size from the avail list using best-fit.
    ///
    /// Returns the smallest extent that satisfies the request. If a larger extent
    /// is used, any remainder is returned to the avail list. Returns None if no
    /// suitable extent is available.
    pub(super) fn allocate_from_avail(&mut self, size: u64) -> Option<Extent> {
        let candidate = self.avail.best_fit(size)?;
        let extent = self.avail.remove_by_offset(candidate.offset)?;

        if extent.size == size {
            let allocated = Extent {
                offset: extent.offset,
                size,
                generation: self.stable_generation,
            };
            self.insert_alloc(allocated);
            return Some(allocated);
        }

        let allocated = Extent {
            offset: extent.offset,
            size,
            generation: self.stable_generation,
        };
        let remainder = Extent {
            offset: extent.offset.saturating_add(size),
            size: extent.size.saturating_sub(size),
            generation: extent.generation,
        };
        if remainder.size > 0 {
            self.insert_avail(remainder);
        }
        self.insert_alloc(allocated);
        Some(allocated)
    }

    pub(super) fn add_alloc_extent(&mut self, extent: Extent) {
        self.insert_alloc(extent);
    }

    pub(super) fn add_avail_extent(&mut self, extent: Extent) {
        self.insert_avail(extent);
    }

    // ------------------------------------------------------------------------
    // Free and reclaim API
    // ------------------------------------------------------------------------

    /// Free an allocated extent, moving it to the discard list.
    ///
    /// The extent is not immediately available for reuse; it must wait for
    /// the stable checkpoint to advance beyond its generation before reclaim.
    /// Fails if the specified range is not currently allocated.
    pub(super) fn free_extent(&mut self, offset: u64, size: u64) -> Result<(), WrongoDBError> {
        if size == 0 {
            return Ok(());
        }

        if !self.alloc_contains(offset, size) {
            return Err(StorageError(format!(
                "free range {offset}..{} not allocated",
                offset.saturating_add(size)
            ))
            .into());
        }

        let discard_gen = next_generation(self.stable_generation);
        let discard = Extent {
            offset,
            size,
            generation: discard_gen,
        };
        self.discard.insert(discard);
        Ok(())
    }

    /// Reclaim discarded extents whose generation is <= stable generation.
    ///
    /// Moves discard-list extents that are no longer needed for recovery
    /// into the avail list, coalescing adjacent extents to minimize fragmentation.
    pub(super) fn reclaim_discarded(&mut self) {
        let mut avail_extents = self.avail.values_by_offset();
        let mut remaining_discard = Vec::new();

        for extent in self.discard.values_by_offset() {
            if extent.generation <= self.stable_generation {
                let _ = self.remove_alloc_range(extent.offset, extent.size);
                avail_extents.push(Extent {
                    generation: self.stable_generation,
                    ..extent
                });
            } else {
                remaining_discard.push(extent);
            }
        }

        let merged = coalesce_extents(avail_extents, self.stable_generation);
        let seed = self.stable_generation ^ 0xBEEF;
        self.avail = ExtentIndex::from_extents(seed ^ 0x44, merged);
        self.discard = ExtentIndex::from_extents(seed ^ 0x55, remaining_discard);
    }

    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------

    fn insert_alloc(&mut self, extent: Extent) {
        insert_with_coalescing(&mut self.alloc, extent);
    }

    fn insert_avail(&mut self, extent: Extent) {
        insert_with_coalescing(&mut self.avail, extent);
    }

    fn remove_alloc_range(&mut self, offset: u64, size: u64) -> Option<Extent> {
        let candidate = self.alloc.predecessor(offset)?;
        let candidate_end = candidate.offset.saturating_add(candidate.size);
        let end = offset.saturating_add(size);

        if offset < candidate.offset || end > candidate_end {
            return None;
        }

        self.alloc.remove_extent(candidate);

        if offset > candidate.offset {
            let left = Extent {
                offset: candidate.offset,
                size: offset - candidate.offset,
                generation: candidate.generation,
            };
            self.alloc.insert(left);
        }
        if end < candidate_end {
            let right = Extent {
                offset: end,
                size: candidate_end - end,
                generation: candidate.generation,
            };
            self.alloc.insert(right);
        }

        Some(Extent {
            offset,
            size,
            generation: candidate.generation,
        })
    }

    fn alloc_contains(&self, offset: u64, size: u64) -> bool {
        let Some(candidate) = self.alloc.predecessor(offset) else {
            return false;
        };
        let candidate_end = candidate.offset.saturating_add(candidate.size);
        let end = offset.saturating_add(size);
        offset >= candidate.offset && end <= candidate_end
    }
}

// ============================================================================
// ExtentIndex - Dual-index for offset and size lookups
// ============================================================================

#[derive(Debug, Clone)]
struct ExtentIndex {
    by_offset: SkipList<u64, Extent>,
    by_size: SkipList<SizeKey, Extent>,
}

impl ExtentIndex {
    fn new(seed: u64) -> Self {
        Self {
            by_offset: SkipList::new(12, seed ^ 0xA5A5_5A5A_1234_5678),
            by_size: SkipList::new(12, seed ^ 0x5A5A_A5A5_5678_1234),
        }
    }

    fn from_extents(seed: u64, extents: Vec<Extent>) -> Self {
        let mut index = Self::new(seed);
        for extent in extents {
            index.insert(extent);
        }
        index
    }

    fn insert(&mut self, extent: Extent) {
        self.by_offset.insert(extent.offset, extent);
        self.by_size.insert(
            SizeKey {
                size: extent.size,
                offset: extent.offset,
            },
            extent,
        );
    }

    fn remove_by_offset(&mut self, offset: u64) -> Option<Extent> {
        let extent = self.by_offset.remove(&offset)?;
        let size_key = SizeKey {
            size: extent.size,
            offset: extent.offset,
        };
        self.by_size.remove(&size_key);
        Some(extent)
    }

    fn remove_extent(&mut self, extent: Extent) {
        let _ = self.by_offset.remove(&extent.offset);
        let size_key = SizeKey {
            size: extent.size,
            offset: extent.offset,
        };
        let _ = self.by_size.remove(&size_key);
    }

    fn best_fit(&self, size: u64) -> Option<Extent> {
        let key = SizeKey { size, offset: 0 };
        self.by_size.find_ge(&key).cloned()
    }

    fn predecessor(&self, offset: u64) -> Option<Extent> {
        let key = offset.saturating_add(1);
        self.by_offset.find_lt(&key).cloned()
    }

    fn successor(&self, offset: u64) -> Option<Extent> {
        self.by_offset.find_ge(&offset).cloned()
    }

    fn values_by_offset(&self) -> Vec<Extent> {
        self.by_offset.values_in_order()
    }
}

// ============================================================================
// SizeKey - Sort key for best-fit allocation
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SizeKey {
    size: u64,
    offset: u64,
}

// ============================================================================
// Helper functions (lowest level)
// ============================================================================

fn coalesce_extents(mut extents: Vec<Extent>, generation: u64) -> Vec<Extent> {
    if extents.is_empty() {
        return extents;
    }
    extents.sort_by(|a, b| {
        if a.offset == b.offset {
            a.size.cmp(&b.size)
        } else {
            a.offset.cmp(&b.offset)
        }
    });
    let mut merged: Vec<Extent> = Vec::with_capacity(extents.len());
    for mut extent in extents {
        extent.generation = generation;
        if let Some(last) = merged.last_mut() {
            let last_end = last.offset.saturating_add(last.size);
            if last_end == extent.offset {
                last.size = last.size.saturating_add(extent.size);
                continue;
            }
        }
        merged.push(extent);
    }
    merged
}

fn insert_with_coalescing(index: &mut ExtentIndex, extent: Extent) {
    let (prev, next, merged) = compute_coalesced_extent(index, extent);
    if let Some(prev) = prev {
        index.remove_extent(prev);
    }
    if let Some(next) = next {
        index.remove_extent(next);
    }
    index.insert(merged);
}

fn compute_coalesced_extent(
    index: &ExtentIndex,
    extent: Extent,
) -> (Option<Extent>, Option<Extent>, Extent) {
    let mut merged = extent;
    let mut prev = None;
    let mut next = None;

    if let Some(before) = index.predecessor(merged.offset) {
        let end = before.offset.saturating_add(before.size);
        if end == merged.offset {
            merged.offset = before.offset;
            merged.size = merged.size.saturating_add(before.size);
            prev = Some(before);
        }
    }

    let merged_end = merged.offset.saturating_add(merged.size);
    if let Some(after) = index.successor(merged_end) {
        if merged.offset.saturating_add(merged.size) == after.offset {
            merged.size = merged.size.saturating_add(after.size);
            next = Some(after);
        }
    }

    (prev, next, merged)
}

fn next_generation(current: u64) -> u64 {
    let mut next = current.wrapping_add(1);
    if next == 0 {
        next = 1;
    }
    next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_best_fit_splits_extent() {
        let mut mgr = BlockManager::new(
            1,
            ExtentLists {
                alloc: vec![],
                avail: vec![
                    Extent {
                        offset: 10,
                        size: 10,
                        generation: 1,
                    },
                    Extent {
                        offset: 30,
                        size: 5,
                        generation: 1,
                    },
                ],
                discard: vec![],
            },
        );

        let allocated = mgr.allocate_from_avail(4).unwrap();
        assert_eq!(allocated.offset, 30);
        assert_eq!(allocated.size, 4);

        let avail = mgr.extent_lists().avail;
        assert!(avail.iter().any(|e| e.offset == 10 && e.size == 10));
        assert!(avail.iter().any(|e| e.offset == 34 && e.size == 1));
    }

    #[test]
    fn free_moves_to_discard_and_reclaim_coalesces() {
        let mut mgr = BlockManager::new(
            1,
            ExtentLists {
                alloc: vec![Extent {
                    offset: 5,
                    size: 3,
                    generation: 1,
                }],
                avail: vec![Extent {
                    offset: 20,
                    size: 2,
                    generation: 1,
                }],
                discard: vec![],
            },
        );

        mgr.free_extent(6, 1).unwrap();
        let lists = mgr.extent_lists();
        assert_eq!(lists.discard.len(), 1);
        assert_eq!(lists.alloc.len(), 1);

        mgr.set_stable_generation(2);
        mgr.reclaim_discarded();
        let lists = mgr.extent_lists();
        assert!(lists.discard.is_empty());
        assert!(lists.avail.iter().any(|e| e.offset == 6 && e.size == 1));
    }

    #[test]
    fn reclaim_skips_future_generation() {
        let mut mgr = BlockManager::new(
            1,
            ExtentLists {
                alloc: vec![],
                avail: vec![],
                discard: vec![Extent {
                    offset: 10,
                    size: 2,
                    generation: 3,
                }],
            },
        );

        mgr.set_stable_generation(2);
        mgr.reclaim_discarded();
        let lists = mgr.extent_lists();
        assert_eq!(lists.discard.len(), 1);
        assert!(lists.avail.is_empty());
    }

    #[test]
    fn fragmentation_reclamation_stress() {
        let mut mgr = BlockManager::new(
            1,
            ExtentLists {
                alloc: vec![],
                avail: vec![Extent {
                    offset: 1,
                    size: 200,
                    generation: 1,
                }],
                discard: vec![],
            },
        );

        let mut allocated = Vec::new();
        for _ in 0..200 {
            let extent = mgr.allocate_from_avail(1).unwrap();
            allocated.push(extent);
        }

        for extent in allocated {
            mgr.free_extent(extent.offset, extent.size).unwrap();
        }

        mgr.set_stable_generation(2);
        mgr.reclaim_discarded();

        let avail = mgr.extent_lists().avail;
        assert_eq!(avail.len(), 1);
        assert_eq!(avail[0].offset, 1);
        assert_eq!(avail[0].size, 200);
    }
}
