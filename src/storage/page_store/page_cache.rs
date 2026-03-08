use std::collections::HashMap;

use crate::core::errors::{StorageError, WrongoDBError};

use super::page::Page;
use super::types::ReadPin;

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_CACHE_CAPACITY_PAGES: usize = 256;

// ============================================================================
// PageCacheConfig
// ============================================================================

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

// ============================================================================
// PageCacheEntry
// ============================================================================

#[derive(Debug)]
pub(crate) struct PageCacheEntry {
    pub page_id: u64,
    pub page: Page,
    pub dirty: bool,
    pub pin_count: u32,
    pub last_access: u64,
}

impl PageCacheEntry {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub fn new(page_id: u64, page: Page) -> Self {
        Self {
            page_id,
            page,
            dirty: false,
            pin_count: 0,
            last_access: 0,
        }
    }
}

// ============================================================================
// PageCache
// ============================================================================

#[derive(Debug)]
pub(crate) struct PageCache {
    config: PageCacheConfig,
    pub entries: HashMap<u64, PageCacheEntry>,
    access_counter: u64,
}

impl PageCache {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    pub fn new(config: PageCacheConfig) -> Self {
        Self {
            config,
            entries: HashMap::new(),
            access_counter: 0,
        }
    }

    // ------------------------------------------------------------------------
    // Public API - Query Operations
    // ------------------------------------------------------------------------

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

    pub fn get_mut(&mut self, page_id: u64) -> Option<&mut PageCacheEntry> {
        let access = self.next_access();
        let entry = self.entries.get_mut(&page_id)?;
        entry.last_access = access;
        Some(entry)
    }

    pub fn get_page(&self, page_id: u64) -> &Page {
        let entry = self
            .entries
            .get(&page_id)
            .expect("pinned page must exist in cache");
        debug_assert!(entry.pin_count > 0, "page {} must be pinned", page_id);
        &entry.page
    }

    // ------------------------------------------------------------------------
    // Public API - Modification Operations
    // ------------------------------------------------------------------------

    pub fn insert(&mut self, page_id: u64, page: Page) -> &mut PageCacheEntry {
        let mut entry = PageCacheEntry::new(page_id, page);
        entry.last_access = self.next_access();
        self.entries.insert(page_id, entry);
        self.entries
            .get_mut(&page_id)
            .expect("cache entry just inserted")
    }

    pub fn remove(&mut self, page_id: u64) -> Option<PageCacheEntry> {
        self.entries.remove(&page_id)
    }

    // ------------------------------------------------------------------------
    // Public API - Pin/Unpin Operations
    // ------------------------------------------------------------------------

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

    // ------------------------------------------------------------------------
    // Public API - Eviction Operations
    // ------------------------------------------------------------------------

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

    // ------------------------------------------------------------------------
    // Public API - I/O Operations
    // ------------------------------------------------------------------------

    /// Load a page from storage into cache and pin it.
    pub fn load_and_pin<F>(
        &mut self,
        page_id: u64,
        mut read_fn: F,
    ) -> Result<ReadPin, WrongoDBError>
    where
        F: FnMut(u64) -> Result<Vec<u8>, WrongoDBError>,
    {
        if self.contains(page_id) {
            self.pin(page_id)?;
            return Ok(ReadPin { page_id });
        }

        let payload = read_fn(page_id)?;
        let page = Page::from_bytes(payload)
            .map_err(|e| StorageError(format!("corrupt page {page_id}: {e}")))?;
        let entry = self.insert(page_id, page);
        entry.pin_count = 1;
        Ok(ReadPin { page_id })
    }

    /// Load page for CoW without pinning.
    pub fn load_cow_page<F>(&mut self, page_id: u64, mut read_fn: F) -> Result<Page, WrongoDBError>
    where
        F: FnMut(u64) -> Result<Vec<u8>, WrongoDBError>,
    {
        if let Some(entry) = self.get_mut(page_id) {
            return Ok(entry.page.clone());
        }

        let payload = read_fn(page_id)?;
        Page::from_bytes(payload)
            .map_err(|e| StorageError(format!("corrupt page {page_id}: {e}")).into())
    }

    /// Evict LRU entry if cache is at capacity.
    pub fn evict_if_full<F>(&mut self, mut write_fn: F) -> Result<(), WrongoDBError>
    where
        F: FnMut(u64, &Page) -> Result<(), WrongoDBError>,
    {
        if !self.is_full() {
            return Ok(());
        }
        let entry = match self.evict_lru()? {
            Some(entry) => entry,
            None => return Ok(()),
        };
        if entry.dirty {
            if let Err(err) = write_fn(entry.page_id, &entry.page) {
                self.entries.insert(entry.page_id, entry);
                return Err(err);
            }
        }
        Ok(())
    }

    /// Flush all dirty pages to storage.
    pub fn flush<F>(&mut self, mut write_fn: F) -> Result<(), WrongoDBError>
    where
        F: FnMut(u64, &Page) -> Result<(), WrongoDBError>,
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
            write_fn(entry.page_id, &entry.page)?;
            entry.dirty = false;
        }
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Private Helpers
    // ------------------------------------------------------------------------

    fn next_access(&mut self) -> u64 {
        self.access_counter = self.access_counter.saturating_add(1);
        self.access_counter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_skips_pinned_pages() {
        let mut cache = PageCache::new(PageCacheConfig { capacity_pages: 3 });
        cache.insert(1, Page::new_leaf(64).unwrap());
        cache.insert(2, Page::new_leaf(64).unwrap());
        cache.insert(3, Page::new_leaf(64).unwrap());

        cache.get_mut(2).unwrap().pin_count = 1;
        cache.get_mut(1);

        let candidate = cache.lru_unpinned();
        assert_eq!(candidate, Some(3));
    }

    #[test]
    fn evict_lru_errors_when_all_pinned() {
        let mut cache = PageCache::new(PageCacheConfig { capacity_pages: 2 });
        cache.insert(1, Page::new_leaf(64).unwrap()).pin_count = 1;
        cache.insert(2, Page::new_leaf(64).unwrap()).pin_count = 1;

        let err = cache.evict_lru().unwrap_err();
        assert!(matches!(err, WrongoDBError::Storage(_)));
    }
}
