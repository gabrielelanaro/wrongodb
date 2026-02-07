use std::collections::HashMap;

use crate::core::errors::{StorageError, WrongoDBError};

const DEFAULT_CACHE_CAPACITY_PAGES: usize = 256;

#[derive(Debug, Clone)]
pub(crate) struct PageCacheConfig {
    pub capacity_pages: usize,
}

impl Default for PageCacheConfig {
    fn default() -> Self {
        Self {
            capacity_pages: DEFAULT_CACHE_CAPACITY_PAGES,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PageCacheEntry {
    pub page_id: u64,
    pub payload: Vec<u8>,
    pub dirty: bool,
    pub pin_count: u32,
    pub last_access: u64,
}

impl PageCacheEntry {
    pub fn new(page_id: u64, payload: Vec<u8>) -> Self {
        Self {
            page_id,
            payload,
            dirty: false,
            pin_count: 0,
            last_access: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PageCache {
    config: PageCacheConfig,
    pub entries: HashMap<u64, PageCacheEntry>,
    access_counter: u64,
}

impl PageCache {
    pub fn new(config: PageCacheConfig) -> Self {
        Self {
            config,
            entries: HashMap::new(),
            access_counter: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn capacity(&self) -> usize {
        self.config.capacity_pages
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }

    pub fn contains(&self, page_id: u64) -> bool {
        self.entries.contains_key(&page_id)
    }

    pub fn get(&self, page_id: u64) -> Option<&PageCacheEntry> {
        self.entries.get(&page_id)
    }

    pub fn get_mut(&mut self, page_id: u64) -> Option<&mut PageCacheEntry> {
        let access = self.next_access();
        let entry = self.entries.get_mut(&page_id)?;
        entry.last_access = access;
        Some(entry)
    }

    pub fn insert(&mut self, page_id: u64, payload: Vec<u8>) -> &mut PageCacheEntry {
        let mut entry = PageCacheEntry::new(page_id, payload);
        entry.last_access = self.next_access();
        self.entries.insert(page_id, entry);
        self.entries
            .get_mut(&page_id)
            .expect("cache entry just inserted")
    }

    pub fn remove(&mut self, page_id: u64) -> Option<PageCacheEntry> {
        self.entries.remove(&page_id)
    }

    pub fn pin(&mut self, page_id: u64) -> Result<(), WrongoDBError> {
        let entry = self
            .get_mut(page_id)
            .ok_or_else(|| StorageError(format!("page cache miss for {page_id}")))?;
        entry.pin_count = entry.pin_count.saturating_add(1);
        Ok(())
    }

    pub fn unpin(&mut self, page_id: u64) -> Result<(), WrongoDBError> {
        let entry = self
            .get_mut(page_id)
            .ok_or_else(|| StorageError(format!("page cache miss for {page_id}")))?;
        if entry.pin_count == 0 {
            return Err(StorageError(format!("page cache pin underflow for {page_id}")).into());
        }
        entry.pin_count -= 1;
        Ok(())
    }

    pub fn lru_unpinned(&self) -> Option<u64> {
        let mut candidate: Option<(u64, u64)> = None;
        for (page_id, entry) in &self.entries {
            if entry.pin_count > 0 {
                continue;
            }
            match candidate {
                None => candidate = Some((*page_id, entry.last_access)),
                Some((_, best_access)) if entry.last_access < best_access => {
                    candidate = Some((*page_id, entry.last_access));
                }
                _ => {}
            }
        }
        candidate.map(|(page_id, _)| page_id)
    }

    pub fn evict_lru(&mut self) -> Result<Option<PageCacheEntry>, WrongoDBError> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        let candidate = self
            .lru_unpinned()
            .ok_or_else(|| StorageError("page cache eviction failed: all pages pinned".into()))?;
        Ok(self.entries.remove(&candidate))
    }

    fn next_access(&mut self) -> u64 {
        self.access_counter = self.access_counter.saturating_add(1);
        self.access_counter
    }

    /// Load a page from storage into cache and pin it.
    pub fn load_and_pin<F>(
        &mut self,
        page_id: u64,
        mut read_fn: F,
    ) -> Result<Vec<u8>, WrongoDBError>
    where
        F: FnMut(u64) -> Result<Vec<u8>, WrongoDBError>,
    {
        if self.contains(page_id) {
            self.pin(page_id)?;
            let payload = self
                .get(page_id)
                .expect("page cache entry just pinned")
                .payload
                .clone();
            return Ok(payload);
        }

        let payload = read_fn(page_id)?;
        let entry = self.insert(page_id, payload.clone());
        entry.pin_count = 1;
        Ok(payload)
    }

    /// Load payload for CoW without pinning.
    pub fn load_cow_payload<F>(
        &mut self,
        page_id: u64,
        mut read_fn: F,
    ) -> Result<Vec<u8>, WrongoDBError>
    where
        F: FnMut(u64) -> Result<Vec<u8>, WrongoDBError>,
    {
        if let Some(entry) = self.get_mut(page_id) {
            return Ok(entry.payload.clone());
        }
        read_fn(page_id)
    }

    /// Evict LRU entry if cache is at capacity.
    pub fn evict_if_full<F>(&mut self, mut write_fn: F) -> Result<(), WrongoDBError>
    where
        F: FnMut(u64, &[u8]) -> Result<(), WrongoDBError>,
    {
        if !self.is_full() {
            return Ok(());
        }
        let entry = match self.evict_lru()? {
            Some(entry) => entry,
            None => return Ok(()),
        };
        if entry.dirty {
            if let Err(err) = write_fn(entry.page_id, &entry.payload) {
                self.entries.insert(entry.page_id, entry);
                return Err(err);
            }
        }
        Ok(())
    }

    /// Flush all dirty pages to storage.
    pub fn flush<F>(&mut self, mut write_fn: F) -> Result<(), WrongoDBError>
    where
        F: FnMut(u64, &[u8]) -> Result<(), WrongoDBError>,
    {
        for entry in self.entries.values_mut() {
            if !entry.dirty {
                continue;
            }
            if entry.pin_count > 0 {
                return Err(StorageError(format!(
                    "cannot flush dirty pinned page {}",
                    entry.page_id
                ))
                .into());
            }
            write_fn(entry.page_id, &entry.payload)?;
            entry.dirty = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_skips_pinned_pages() {
        let mut cache = PageCache::new(PageCacheConfig { capacity_pages: 3 });
        cache.insert(1, vec![1]);
        cache.insert(2, vec![2]);
        cache.insert(3, vec![3]);

        cache.get_mut(2).unwrap().pin_count = 1;
        cache.get_mut(1);

        let candidate = cache.lru_unpinned();
        assert_eq!(candidate, Some(3));
    }

    #[test]
    fn evict_lru_errors_when_all_pinned() {
        let mut cache = PageCache::new(PageCacheConfig { capacity_pages: 2 });
        cache.insert(1, vec![1]).pin_count = 1;
        cache.insert(2, vec![2]).pin_count = 1;

        let err = cache.evict_lru().unwrap_err();
        assert!(matches!(err, WrongoDBError::Storage(_)));
    }
}
