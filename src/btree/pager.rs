use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::{BlockFile, StorageError, WrongoDBError, NONE_BLOCK_ID};

#[allow(dead_code)]
const DEFAULT_CACHE_CAPACITY_PAGES: usize = 256;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictionPolicy {
    Lru,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PageCacheConfig {
    capacity_pages: usize,
    eviction_policy: EvictionPolicy,
}

#[allow(dead_code)]
impl Default for PageCacheConfig {
    fn default() -> Self {
        Self {
            capacity_pages: DEFAULT_CACHE_CAPACITY_PAGES,
            eviction_policy: EvictionPolicy::Lru,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct PageCacheEntry {
    page_id: u64,
    payload: Vec<u8>,
    dirty: bool,
    pin_count: u32,
    last_access: u64,
}

#[allow(dead_code)]
impl PageCacheEntry {
    fn new(page_id: u64, payload: Vec<u8>) -> Self {
        Self {
            page_id,
            payload,
            dirty: false,
            pin_count: 0,
            last_access: 0,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct PageCache {
    config: PageCacheConfig,
    entries: HashMap<u64, PageCacheEntry>,
    access_counter: u64,
}

#[allow(dead_code)]
impl PageCache {
    fn new(config: PageCacheConfig) -> Self {
        Self {
            config,
            entries: HashMap::new(),
            access_counter: 0,
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn capacity(&self) -> usize {
        self.config.capacity_pages
    }

    fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }

    fn contains(&self, page_id: u64) -> bool {
        self.entries.contains_key(&page_id)
    }

    fn get(&self, page_id: u64) -> Option<&PageCacheEntry> {
        self.entries.get(&page_id)
    }

    fn get_mut(&mut self, page_id: u64) -> Option<&mut PageCacheEntry> {
        let access = self.next_access();
        let entry = self.entries.get_mut(&page_id)?;
        entry.last_access = access;
        Some(entry)
    }

    fn insert(&mut self, page_id: u64, payload: Vec<u8>) -> &mut PageCacheEntry {
        let mut entry = PageCacheEntry::new(page_id, payload);
        entry.last_access = self.next_access();
        self.entries.insert(page_id, entry);
        self.entries
            .get_mut(&page_id)
            .expect("cache entry just inserted")
    }

    fn remove(&mut self, page_id: u64) -> Option<PageCacheEntry> {
        self.entries.remove(&page_id)
    }

    fn pin(&mut self, page_id: u64) -> Result<(), WrongoDBError> {
        let entry = self
            .get_mut(page_id)
            .ok_or_else(|| StorageError(format!("page cache miss for {page_id}")))?;
        entry.pin_count = entry.pin_count.saturating_add(1);
        Ok(())
    }

    fn unpin(&mut self, page_id: u64) -> Result<(), WrongoDBError> {
        let entry = self
            .get_mut(page_id)
            .ok_or_else(|| StorageError(format!("page cache miss for {page_id}")))?;
        if entry.pin_count == 0 {
            return Err(StorageError(format!("page cache pin underflow for {page_id}")).into());
        }
        entry.pin_count -= 1;
        Ok(())
    }

    fn mark_dirty(&mut self, page_id: u64) -> Result<(), WrongoDBError> {
        let entry = self
            .get_mut(page_id)
            .ok_or_else(|| StorageError(format!("page cache miss for {page_id}")))?;
        entry.dirty = true;
        Ok(())
    }

    fn mark_clean(&mut self, page_id: u64) -> Result<(), WrongoDBError> {
        let entry = self
            .get_mut(page_id)
            .ok_or_else(|| StorageError(format!("page cache miss for {page_id}")))?;
        entry.dirty = false;
        Ok(())
    }

    fn lru_unpinned(&self) -> Option<u64> {
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

    fn evict_lru(&mut self) -> Result<Option<PageCacheEntry>, WrongoDBError> {
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
}

#[derive(Debug)]
pub(super) struct Pager {
    bf: BlockFile,
    working_root: u64,
    retired_blocks: HashSet<u64>,
    #[allow(dead_code)]
    cache: PageCache,
}

#[derive(Debug)]
pub(super) struct PinnedPage {
    page_id: u64,
    payload: Vec<u8>,
}

impl PinnedPage {
    pub(super) fn page_id(&self) -> u64 {
        self.page_id
    }

    pub(super) fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub(super) fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.payload
    }
}

#[derive(Debug)]
pub(super) struct PinnedPageMut {
    page_id: u64,
    payload: Vec<u8>,
}

impl PinnedPageMut {
    pub(super) fn page_id(&self) -> u64 {
        self.page_id
    }

    pub(super) fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub(super) fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.payload
    }
}

impl Pager {
    pub(super) fn create<P: AsRef<Path>>(path: P, page_size: usize) -> Result<Self, WrongoDBError> {
        let bf = BlockFile::create(path, page_size)?;
        let working_root = bf.root_block_id();
        Ok(Self {
            bf,
            working_root,
            retired_blocks: HashSet::new(),
            cache: PageCache::new(PageCacheConfig::default()),
        })
    }

    pub(super) fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let bf = BlockFile::open(path)?;
        let working_root = bf.root_block_id();
        Ok(Self {
            bf,
            working_root,
            retired_blocks: HashSet::new(),
            cache: PageCache::new(PageCacheConfig::default()),
        })
    }

    pub(super) fn page_payload_len(&self) -> usize {
        self.bf.page_payload_len()
    }

    pub(super) fn root_page_id(&self) -> u64 {
        self.working_root
    }

    pub(super) fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError> {
        self.working_root = root_page_id;
        Ok(())
    }

    pub(super) fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        self.bf.set_root_block_id(self.working_root)?;
        // Ensure the new checkpoint root is durable before reclaiming blocks.
        self.bf.sync_all()?;
        if !self.retired_blocks.is_empty() {
            self.release_retired_blocks()?;
            // Free-list updates are best-effort; sync so reuse after a successful checkpoint
            // is durable, but crashes before this point may still leak space.
            self.bf.sync_all()?;
        }
        Ok(())
    }

    pub(super) fn pin_page(&mut self, page_id: u64) -> Result<PinnedPage, WrongoDBError> {
        let payload = self.bf.read_block(page_id, true)?;
        Ok(PinnedPage { page_id, payload })
    }

    pub(super) fn pin_page_mut(&mut self, page_id: u64) -> Result<PinnedPageMut, WrongoDBError> {
        let payload = self.bf.read_block(page_id, true)?;
        Ok(PinnedPageMut { page_id, payload })
    }

    pub(super) fn unpin_page(&mut self, _page_id: u64) {
        // No-op until the page cache tracks pin counts.
    }

    pub(super) fn write_page(&mut self, page_id: u64, payload: &[u8]) -> Result<(), WrongoDBError> {
        self.bf.write_block(page_id, payload)
    }

    fn allocate_page(&mut self) -> Result<u64, WrongoDBError> {
        self.bf.allocate_block()
    }

    pub(super) fn write_new_page(&mut self, payload: &[u8]) -> Result<u64, WrongoDBError> {
        let page_id = self.allocate_page()?;
        self.write_page(page_id, payload)?;
        Ok(page_id)
    }

    pub(super) fn retire_page(&mut self, page_id: u64) {
        if page_id == NONE_BLOCK_ID {
            return;
        }
        self.retired_blocks.insert(page_id);
    }

    fn release_retired_blocks(&mut self) -> Result<(), WrongoDBError> {
        if self.retired_blocks.is_empty() {
            return Ok(());
        }
        let retired: Vec<u64> = self.retired_blocks.iter().copied().collect();
        for block_id in retired {
            self.bf.free_block(block_id)?;
            self.retired_blocks.remove(&block_id);
        }
        Ok(())
    }

    pub(super) fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.bf.sync_all()
    }

    #[allow(dead_code)]
    fn evict_cache_if_full(&mut self) -> Result<(), WrongoDBError> {
        if !self.cache.is_full() {
            return Ok(());
        }
        let entry = match self.cache.evict_lru()? {
            Some(entry) => entry,
            None => return Ok(()),
        };
        if entry.dirty {
            if let Err(err) = self.bf.write_block(entry.page_id, &entry.payload) {
                self.cache.entries.insert(entry.page_id, entry);
                return Err(err);
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn flush_cache(&mut self) -> Result<(), WrongoDBError> {
        let (bf, cache) = (&mut self.bf, &mut self.cache);
        for entry in cache.entries.values_mut() {
            if !entry.dirty {
                continue;
            }
            if entry.pin_count > 0 {
                return Err(
                    StorageError(format!("cannot flush dirty pinned page {}", entry.page_id)).into(),
                );
            }
            bf.write_block(entry.page_id, &entry.payload)?;
            entry.dirty = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn lru_skips_pinned_pages() {
        let mut cache = PageCache::new(PageCacheConfig {
            capacity_pages: 3,
            eviction_policy: EvictionPolicy::Lru,
        });
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
        let mut cache = PageCache::new(PageCacheConfig {
            capacity_pages: 2,
            eviction_policy: EvictionPolicy::Lru,
        });
        cache.insert(1, vec![1]).pin_count = 1;
        cache.insert(2, vec![2]).pin_count = 1;

        let err = cache.evict_lru().unwrap_err();
        assert!(matches!(err, WrongoDBError::Storage(_)));
    }

    #[test]
    fn eviction_writes_back_dirty_page() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("pager-cache.db");
        let mut pager = Pager::create(&path, 256).unwrap();
        let payload_len = pager.page_payload_len();
        let page_id = pager.write_new_page(&vec![0u8; payload_len]).unwrap();

        pager.cache = PageCache::new(PageCacheConfig {
            capacity_pages: 1,
            eviction_policy: EvictionPolicy::Lru,
        });

        let payload = vec![7u8; payload_len];
        let entry = pager.cache.insert(page_id, payload.clone());
        entry.dirty = true;

        pager.evict_cache_if_full().unwrap();
        assert!(!pager.cache.contains(page_id));

        let read = pager.bf.read_block(page_id, true).unwrap();
        assert_eq!(read, payload);
    }

    #[test]
    fn flush_rejects_dirty_pinned_page() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("pager-cache-flush.db");
        let mut pager = Pager::create(&path, 256).unwrap();
        let payload_len = pager.page_payload_len();
        let page_id = pager.write_new_page(&vec![0u8; payload_len]).unwrap();

        let entry = pager.cache.insert(page_id, vec![9u8; payload_len]);
        entry.dirty = true;
        entry.pin_count = 1;

        let err = pager.flush_cache().unwrap_err();
        assert!(err
            .to_string()
            .contains("cannot flush dirty pinned page"));
    }
}
