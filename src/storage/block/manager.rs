use crate::core::errors::{StorageError, WrongoDBError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    pub offset: u64,
    pub size: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ExtentLists {
    pub alloc: Vec<Extent>,
    pub avail: Vec<Extent>,
    pub discard: Vec<Extent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SizeKey {
    size: u64,
    offset: u64,
}

#[derive(Debug, Clone)]
struct Node<K, V> {
    key: Option<K>,
    value: Option<V>,
    forwards: Vec<Option<usize>>,
}

#[derive(Debug, Clone)]
struct SkipList<K, V> {
    max_level: usize,
    level: usize,
    head: usize,
    len: usize,
    nodes: Vec<Node<K, V>>,
    seed: u64,
}

impl<K, V> SkipList<K, V>
where
    K: Ord + Clone,
    V: Clone,
{
    fn new(max_level: usize, seed: u64) -> Self {
        let head = Node {
            key: None,
            value: None,
            forwards: vec![None; max_level],
        };
        Self {
            max_level,
            level: 1,
            head: 0,
            len: 0,
            nodes: vec![head],
            seed,
        }
    }

    fn random_level(&mut self) -> usize {
        // Simple LCG-based RNG; deterministic and dependency-free.
        self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let mut lvl = 1;
        let mut x = self.seed;
        while lvl < self.max_level && (x & 1) == 1 {
            lvl += 1;
            x >>= 1;
        }
        lvl
    }

    fn insert(&mut self, key: K, value: V) -> Option<V> {
        let mut update = vec![self.head; self.max_level];
        let mut current = self.head;

        for level in (0..self.level).rev() {
            loop {
                let next = self.nodes[current].forwards[level];
                if let Some(idx) = next {
                    let next_key = self.nodes[idx].key.as_ref().expect("node key");
                    if next_key < &key {
                        current = idx;
                        continue;
                    }
                }
                break;
            }
            update[level] = current;
        }

        if let Some(next) = self.nodes[current].forwards[0] {
            let next_key = self.nodes[next].key.as_ref().expect("node key");
            if next_key == &key {
                let old = self.nodes[next].value.replace(value);
                return old;
            }
        }

        let node_level = self.random_level();
        if node_level > self.level {
            for slot in update.iter_mut().take(node_level).skip(self.level) {
                *slot = self.head;
            }
            self.level = node_level;
        }

        let node = Node {
            key: Some(key),
            value: Some(value),
            forwards: vec![None; node_level],
        };
        let node_idx = self.nodes.len();
        self.nodes.push(node);

        for (level, prev) in update.iter().take(node_level).enumerate() {
            self.nodes[node_idx].forwards[level] = self.nodes[*prev].forwards[level];
            self.nodes[*prev].forwards[level] = Some(node_idx);
        }

        self.len += 1;
        None
    }

    fn remove(&mut self, key: &K) -> Option<V> {
        let mut update = vec![self.head; self.max_level];
        let mut current = self.head;

        for level in (0..self.level).rev() {
            loop {
                let next = self.nodes[current].forwards[level];
                if let Some(idx) = next {
                    let next_key = self.nodes[idx].key.as_ref().expect("node key");
                    if next_key < key {
                        current = idx;
                        continue;
                    }
                }
                break;
            }
            update[level] = current;
        }

        let target = self.nodes[current].forwards[0];
        if let Some(idx) = target {
            let target_key = self.nodes[idx].key.as_ref().expect("node key");
            if target_key == key {
                for (level, prev) in update.iter().take(self.level).enumerate() {
                    if self.nodes[*prev].forwards[level] == Some(idx) {
                        self.nodes[*prev].forwards[level] = self.nodes[idx].forwards[level];
                    }
                }
                while self.level > 1 && self.nodes[self.head].forwards[self.level - 1].is_none() {
                    self.level -= 1;
                }
                self.len = self.len.saturating_sub(1);
                return self.nodes[idx].value.take();
            }
        }
        None
    }

    fn find_ge(&self, key: &K) -> Option<&V> {
        let mut current = self.head;
        for level in (0..self.level).rev() {
            loop {
                let next = self.nodes[current].forwards[level];
                if let Some(idx) = next {
                    let next_key = self.nodes[idx].key.as_ref().expect("node key");
                    if next_key < key {
                        current = idx;
                        continue;
                    }
                }
                break;
            }
        }
        let next = self.nodes[current].forwards[0]?;
        let next_key = self.nodes[next].key.as_ref().expect("node key");
        if next_key >= key {
            self.nodes[next].value.as_ref()
        } else {
            None
        }
    }

    fn find_lt(&self, key: &K) -> Option<&V> {
        let mut current = self.head;
        for level in (0..self.level).rev() {
            loop {
                let next = self.nodes[current].forwards[level];
                if let Some(idx) = next {
                    let next_key = self.nodes[idx].key.as_ref().expect("node key");
                    if next_key < key {
                        current = idx;
                        continue;
                    }
                }
                break;
            }
        }
        if current == self.head {
            return None;
        }
        self.nodes[current].value.as_ref()
    }

    fn values_in_order(&self) -> Vec<V> {
        let mut values = Vec::with_capacity(self.len);
        let mut current = self.nodes[self.head].forwards[0];
        while let Some(idx) = current {
            if let Some(value) = self.nodes[idx].value.as_ref() {
                values.push(value.clone());
            }
            current = self.nodes[idx].forwards[0];
        }
        values
    }
}

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

#[derive(Debug, Clone)]
pub struct BlockManager {
    alloc: ExtentIndex,
    avail: ExtentIndex,
    discard: ExtentIndex,
    stable_generation: u64,
}

impl BlockManager {
    pub fn new(stable_generation: u64, lists: ExtentLists) -> Self {
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

    pub fn stable_generation(&self) -> u64 {
        self.stable_generation
    }

    pub fn set_stable_generation(&mut self, generation: u64) {
        self.stable_generation = generation;
    }

    pub fn extent_lists(&self) -> ExtentLists {
        ExtentLists {
            alloc: self.alloc.values_by_offset(),
            avail: self.avail.values_by_offset(),
            discard: self.discard.values_by_offset(),
        }
    }

    pub fn allocate_from_avail(&mut self, size: u64) -> Option<Extent> {
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

    pub fn add_alloc_extent(&mut self, extent: Extent) {
        self.insert_alloc(extent);
    }

    pub fn add_avail_extent(&mut self, extent: Extent) {
        self.insert_avail(extent);
    }

    pub fn free_extent(&mut self, offset: u64, size: u64) -> Result<(), WrongoDBError> {
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

    pub fn reclaim_discarded(&mut self) {
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

    fn insert_alloc(&mut self, extent: Extent) {
        let (prev, next, merged) = coalesce_with_neighbors(&self.alloc, extent);
        if let Some(prev) = prev {
            self.alloc.remove_extent(prev);
        }
        if let Some(next) = next {
            self.alloc.remove_extent(next);
        }
        self.alloc.insert(merged);
    }

    fn insert_avail(&mut self, extent: Extent) {
        let (prev, next, merged) = coalesce_with_neighbors(&self.avail, extent);
        if let Some(prev) = prev {
            self.avail.remove_extent(prev);
        }
        if let Some(next) = next {
            self.avail.remove_extent(next);
        }
        self.avail.insert(merged);
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

fn coalesce_with_neighbors(
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
