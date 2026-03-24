// ============================================================================
// Node - Skip list node (defined first as SkipList depends on it)
// ============================================================================

#[derive(Debug, Clone)]
struct Node<K, V> {
    key: Option<K>,
    value: Option<V>,
    forwards: Vec<Option<usize>>,
}

// ============================================================================
// SkipList - Probabilistic ordered map
// ============================================================================

/// Probabilistic ordered map with O(log n) expected operations.
///
/// Uses a simple LCG-based RNG for deterministic, dependency-free level
/// generation. Nodes are stored in a contiguous vector with index-based links.
#[derive(Debug, Clone)]
pub(super) struct SkipList<K, V> {
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
    // ------------------------------------------------------------------------
    // Constructor
    // ------------------------------------------------------------------------

    pub(super) fn new(max_level: usize, seed: u64) -> Self {
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

    // ------------------------------------------------------------------------
    // Mutations
    // ------------------------------------------------------------------------

    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let mut update = vec![self.head; self.max_level];
        let mut current = self.head;

        for level in (0..self.level).rev() {
            current = self.search_at_level(level, current, &key);
            update[level] = current;
        }

        if let Some(next) = self.nodes[current].forwards[0] {
            let next_key = self.nodes[next].key.as_ref().expect("node key");
            if next_key == &key {
                return self.nodes[next].value.replace(value);
            }
        }

        let node_level = self.random_level();
        if node_level > self.level {
            for slot in update.iter_mut().take(node_level).skip(self.level) {
                *slot = self.head;
            }
            self.level = node_level;
        }

        let node_idx = self.nodes.len();
        self.nodes.push(Node {
            key: Some(key),
            value: Some(value),
            forwards: vec![None; node_level],
        });

        for (level, prev) in update.iter().take(node_level).enumerate() {
            self.nodes[node_idx].forwards[level] = self.nodes[*prev].forwards[level];
            self.nodes[*prev].forwards[level] = Some(node_idx);
        }

        self.len += 1;
        None
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        let mut update = vec![self.head; self.max_level];
        let mut current = self.head;

        for level in (0..self.level).rev() {
            current = self.search_at_level(level, current, key);
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

    // ------------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------------

    pub(super) fn find_ge(&self, key: &K) -> Option<&V> {
        let mut current = self.head;
        for level in (0..self.level).rev() {
            current = self.search_at_level(level, current, key);
        }
        let next = self.nodes[current].forwards[0]?;
        let next_key = self.nodes[next].key.as_ref().expect("node key");
        if next_key >= key {
            self.nodes[next].value.as_ref()
        } else {
            None
        }
    }

    pub(super) fn find_lt(&self, key: &K) -> Option<&V> {
        let mut current = self.head;
        for level in (0..self.level).rev() {
            current = self.search_at_level(level, current, key);
        }
        if current == self.head {
            return None;
        }
        self.nodes[current].value.as_ref()
    }

    pub(super) fn values_in_order(&self) -> Vec<V> {
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

    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------

    /// Search forward at a single level, stopping at or before the key.
    fn search_at_level(&self, level: usize, mut current: usize, key: &K) -> usize {
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
        current
    }

    fn random_level(&mut self) -> usize {
        self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let mut lvl = 1;
        let mut x = self.seed;
        while lvl < self.max_level && (x & 1) == 1 {
            lvl += 1;
            x >>= 1;
        }
        lvl
    }
}
