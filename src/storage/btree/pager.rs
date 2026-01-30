use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::wal::{wal_path_from_data_path, WalFile};
use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::block::file::{BlockFile, NONE_BLOCK_ID};

const DEFAULT_CACHE_CAPACITY_PAGES: usize = 256;

#[derive(Debug, Clone)]
struct PageCacheConfig {
    capacity_pages: usize,
}

impl Default for PageCacheConfig {
    fn default() -> Self {
        Self {
            capacity_pages: DEFAULT_CACHE_CAPACITY_PAGES,
        }
    }
}

#[derive(Debug)]
struct PageCacheEntry {
    page_id: u64,
    payload: Vec<u8>,
    dirty: bool,
    pin_count: u32,
    last_access: u64,
}

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

#[derive(Debug)]
struct PageCache {
    config: PageCacheConfig,
    entries: HashMap<u64, PageCacheEntry>,
    access_counter: u64,
}

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
    working_pages: HashSet<u64>,
    cache: PageCache,
    /// Number of updates since last checkpoint. Used for checkpoint scheduling.
    updates_since_checkpoint: usize,
    /// If set, checkpoint will be requested after this many updates.
    checkpoint_after_updates: Option<usize>,

    // WAL fields
    wal: Option<WalFile>,
    wal_enabled: bool,
    wal_sync_threshold: Option<usize>,
    wal_operations_since_sync: usize,

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
    original_page_id: Option<u64>,
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

pub(super) trait PageStore: std::fmt::Debug + Send + Sync {
    fn page_payload_len(&self) -> usize;
    fn root_page_id(&self) -> u64;
    fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError>;

    fn pin_page(&mut self, page_id: u64) -> Result<PinnedPage, WrongoDBError>;
    fn unpin_page(&mut self, page_id: u64);
    fn pin_page_mut(&mut self, page_id: u64) -> Result<PinnedPageMut, WrongoDBError>;
    fn unpin_page_mut_commit(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError>;
    fn unpin_page_mut_abort(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError>;
    fn write_new_page(&mut self, payload: &[u8]) -> Result<u64, WrongoDBError>;

    fn request_checkpoint_after_updates(&mut self, count: usize);
    fn checkpoint_requested(&self) -> bool;
    fn checkpoint_prepare(&self) -> u64;
    fn checkpoint_flush_data(&mut self) -> Result<(), WrongoDBError>;
    fn checkpoint_commit(&mut self, new_root: u64) -> Result<(), WrongoDBError>;
    fn sync_all(&mut self) -> Result<(), WrongoDBError>;

    fn wal(&mut self) -> Option<&mut WalFile>;
    fn wal_mut(&mut self) -> Result<&mut WalFile, WrongoDBError>;
    fn take_wal(&mut self) -> Option<WalFile>;
    fn restore_wal(&mut self, wal: Option<WalFile>);
    fn set_wal_sync_threshold(&mut self, threshold: usize);
    fn sync_wal(&mut self) -> Result<(), WrongoDBError>;
    fn log_wal_operation(&mut self) -> Result<bool, WrongoDBError>;

    fn data_path(&self) -> &Path;
}

impl Pager {
    pub(super) fn create<P: AsRef<Path>>(path: P, page_size: usize, wal_enabled: bool) -> Result<Self, WrongoDBError> {
        let bf = BlockFile::create(&path, page_size)?;
        let working_root = bf.root_block_id();

        // Create WAL file if enabled
        let wal = if wal_enabled {
            let wal_path = wal_path_from_data_path(path.as_ref());
            Some(WalFile::create(&wal_path, page_size as u32)?)
        } else {
            None
        };

        Ok(Self {
            bf,
            working_root,
            working_pages: HashSet::new(),
            cache: PageCache::new(PageCacheConfig::default()),
            updates_since_checkpoint: 0,
            checkpoint_after_updates: None,
            wal,
            wal_enabled,
            wal_sync_threshold: None,
            wal_operations_since_sync: 0,
        })
    }

    pub(super) fn open<P: AsRef<Path>>(path: P, wal_enabled: bool) -> Result<Self, WrongoDBError> {
        let bf = BlockFile::open(&path)?;
        let working_root = bf.root_block_id();

        // Open WAL file if enabled
        let wal = if wal_enabled {
            let wal_path = wal_path_from_data_path(path.as_ref());
            if wal_path.exists() {
                Some(WalFile::open(&wal_path)?)
            } else {
                Some(WalFile::create(&wal_path, bf.page_size as u32)?)
            }
        } else {
            None
        };

        Ok(Self {
            bf,
            working_root,
            working_pages: HashSet::new(),
            cache: PageCache::new(PageCacheConfig::default()),
            updates_since_checkpoint: 0,
            checkpoint_after_updates: None,
            wal,
            wal_enabled,
            wal_sync_threshold: None,
            wal_operations_since_sync: 0,
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

    /// Prepare for checkpoint by capturing the current working root.
    ///
    /// This freezes the root that will be persisted; subsequent mutations
    /// will update a new working root.
    pub(super) fn checkpoint_prepare(&self) -> u64 {
        self.working_root
    }

    /// Flush all dirty cached pages to disk.
    ///
    /// This is the data files stage of checkpoint. Dirty pages are written
    /// to their working block locations. Dirty pinned pages will cause an error.
    pub(super) fn checkpoint_flush_data(&mut self) -> Result<(), WrongoDBError> {
        self.flush_cache()
    }

    /// Commit the checkpoint by atomically swapping the root and reclaiming retired blocks.
    ///
    /// Steps:
    /// 1. Sync WAL to ensure checkpoint record is durable.
    /// 2. Write the new root to the checkpoint slot.
    /// 3. Sync to ensure the new root is durable.
    /// 4. Release retired blocks to the free list (after root is durable).
    /// 5. Sync again to make free list updates durable.
    /// 6. Clear working_pages (all pages are now stable).
    pub(super) fn checkpoint_commit(&mut self, new_root: u64) -> Result<(), WrongoDBError> {
        // Sync WAL before committing checkpoint (write-ahead logging)
        self.sync_wal()?;

        self.bf.set_root_block_id(new_root)?;
        // Ensure the new checkpoint root is durable before reclaiming blocks.
        self.bf.sync_all()?;
        // Reclaim discarded extents now that the checkpoint root is durable.
        self.bf.reclaim_discarded()?;
        // Free-list updates are best-effort; sync so reuse after a successful checkpoint
        // is durable, but crashes before this point may still leak space.
        self.bf.sync_all()?;
        self.working_pages.clear();
        Ok(())
    }

    pub(super) fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        let root = self.checkpoint_prepare();
        self.checkpoint_flush_data()?;
        self.checkpoint_commit(root)?;
        // Reset update counter after successful checkpoint
        self.updates_since_checkpoint = 0;
        Ok(())
    }

    /// Request a checkpoint after the specified number of updates.
    ///
    /// Once the configured number of updates is reached, `checkpoint_requested()` returns true.
    /// The caller is responsible for actually calling `checkpoint()`.
    pub(super) fn request_checkpoint_after_updates(&mut self, count: usize) {
        self.checkpoint_after_updates = Some(count);
    }

    /// Check if a checkpoint has been requested based on update count.
    ///
    /// Returns true if `checkpoint_after_updates` is set and the update threshold has been reached.
    pub(super) fn checkpoint_requested(&self) -> bool {
        if let Some(threshold) = self.checkpoint_after_updates {
            return self.updates_since_checkpoint >= threshold;
        }
        false
    }

    /// Increment the update counter (to be called after each mutation).
    fn track_update(&mut self) {
        self.updates_since_checkpoint = self.updates_since_checkpoint.saturating_add(1);
    }

    // WAL accessor methods

    pub(super) fn wal(&mut self) -> Option<&mut WalFile> {
        self.wal.as_mut()
    }

    pub(super) fn wal_mut(&mut self) -> Result<&mut WalFile, WrongoDBError> {
        self.wal.as_mut()
            .ok_or_else(|| StorageError("WAL not enabled".into()).into())
    }

    /// Temporarily remove the WAL handle (used to disable logging during recovery).
    pub(super) fn take_wal(&mut self) -> Option<WalFile> {
        self.wal.take()
    }

    /// Restore the WAL handle after recovery.
    pub(super) fn restore_wal(&mut self, wal: Option<WalFile>) {
        self.wal = wal;
    }

    /// Configure WAL batch sync threshold (sync every N operations)
    pub(super) fn set_wal_sync_threshold(&mut self, threshold: usize) {
        self.wal_sync_threshold = Some(threshold);
    }

    /// Sync WAL to disk (with batching logic)
    pub(super) fn sync_wal(&mut self) -> Result<(), WrongoDBError> {
        if let Some(wal) = self.wal.as_mut() {
            wal.sync()?;
            self.wal_operations_since_sync = 0;
        }
        Ok(())
    }

    /// Log a WAL operation and conditionally sync based on threshold
    pub(super) fn log_wal_operation(&mut self) -> Result<bool, WrongoDBError> {
        if !self.wal_enabled {
            return Ok(false);
        }

        self.wal_operations_since_sync += 1;

        // Check if we should sync
        if let Some(threshold) = self.wal_sync_threshold {
            if self.wal_operations_since_sync >= threshold {
                self.sync_wal()?;
                return Ok(true);  // Synced
            }
        }

        Ok(false)  // Not synced
    }

    pub(super) fn pin_page(&mut self, page_id: u64) -> Result<PinnedPage, WrongoDBError> {
        let payload = self.load_page_and_pin(page_id)?;
        Ok(PinnedPage { page_id, payload })
    }

    pub(super) fn pin_page_mut(&mut self, page_id: u64) -> Result<PinnedPageMut, WrongoDBError> {
        if self.working_pages.contains(&page_id) {
            let payload = self.load_page_and_pin(page_id)?;
            return Ok(PinnedPageMut {
                page_id,
                payload,
                original_page_id: None,
            });
        }

        let payload = self.load_cow_payload(page_id)?;
        self.evict_cache_if_full()?;
        let new_page_id = self.allocate_page()?;
        let entry = self.cache.insert(new_page_id, payload.clone());
        entry.pin_count = 1;
        self.working_pages.insert(new_page_id);
        Ok(PinnedPageMut {
            page_id: new_page_id,
            payload,
            original_page_id: Some(page_id),
        })
    }

    pub(super) fn unpin_page(&mut self, _page_id: u64) {
        if let Err(err) = self.cache.unpin(_page_id) {
            debug_assert!(false, "{err}");
        }
    }

    pub(super) fn unpin_page_mut_commit(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError> {
        let PinnedPageMut {
            page_id,
            payload,
            original_page_id,
        } = page;
        let entry = self
            .cache
            .get_mut(page_id)
            .ok_or_else(|| StorageError(format!("page cache miss for {page_id}")))?;
        if entry.pin_count == 0 {
            return Err(StorageError(format!("page cache pin underflow for {page_id}")).into());
        }
        entry.payload = payload;
        entry.dirty = true;
        entry.pin_count -= 1;
        if let Some(old_page_id) = original_page_id {
            self.retire_page(old_page_id)?;
        }
        // Track this mutation for checkpoint scheduling
        self.track_update();
        Ok(())
    }

    pub(super) fn unpin_page_mut_abort(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError> {
        let PinnedPageMut {
            page_id,
            original_page_id,
            ..
        } = page;
        let mut remove_entry = false;
        {
            let entry = self
                .cache
                .get_mut(page_id)
                .ok_or_else(|| StorageError(format!("page cache miss for {page_id}")))?;
            if entry.pin_count == 0 {
                return Err(StorageError(format!("page cache pin underflow for {page_id}")).into());
            }
            entry.pin_count -= 1;
            if original_page_id.is_some() && entry.pin_count == 0 {
                remove_entry = true;
            }
        }
        if original_page_id.is_some() {
            self.working_pages.remove(&page_id);
            if remove_entry {
                self.cache.remove(page_id);
            }
            self.retire_page(page_id)?;
        }
        Ok(())
    }

    pub(super) fn write_page(&mut self, page_id: u64, payload: &[u8]) -> Result<(), WrongoDBError> {
        self.bf.write_block(page_id, payload)
    }

    fn allocate_page(&mut self) -> Result<u64, WrongoDBError> {
        self.bf.allocate_block()
    }

    pub(super) fn write_new_page(&mut self, payload: &[u8]) -> Result<u64, WrongoDBError> {
        let page_id = self.allocate_page()?;
        self.working_pages.insert(page_id);
        self.write_page(page_id, payload)?;
        Ok(page_id)
    }

    pub(super) fn retire_page(&mut self, page_id: u64) -> Result<(), WrongoDBError> {
        if page_id == NONE_BLOCK_ID {
            return Ok(());
        }
        self.bf.free_block(page_id)?;
        Ok(())
    }

    pub(super) fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.bf.sync_all()
    }

    fn load_page_and_pin(&mut self, page_id: u64) -> Result<Vec<u8>, WrongoDBError> {
        if self.cache.contains(page_id) {
            self.cache.pin(page_id)?;
            let payload = self
                .cache
                .get(page_id)
                .expect("page cache entry just pinned")
                .payload
                .clone();
            return Ok(payload);
        }

        self.evict_cache_if_full()?;
        let payload = self.bf.read_block(page_id, true)?;
        let entry = self.cache.insert(page_id, payload.clone());
        entry.pin_count = 1;
        Ok(payload)
    }

    fn load_cow_payload(&mut self, page_id: u64) -> Result<Vec<u8>, WrongoDBError> {
        if let Some(entry) = self.cache.get_mut(page_id) {
            return Ok(entry.payload.clone());
        }
        self.bf.read_block(page_id, true)
    }

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

    fn flush_cache(&mut self) -> Result<(), WrongoDBError> {
        let (bf, cache) = (&mut self.bf, &mut self.cache);
        for entry in cache.entries.values_mut() {
            if !entry.dirty {
                continue;
            }
            // NOTE: Defensive programming - this check shouldn't be hit in the current
            // single-threaded design because:
            // 1. Pages are marked dirty only in unpin_page_mut_commit(), which also decrements pin_count
            // 2. No concurrent operations (no background threads yet)
            //
            // This check becomes important when adding concurrent eviction/checkpoint (Slice G2+).
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

impl PageStore for Pager {
    fn page_payload_len(&self) -> usize {
        Pager::page_payload_len(self)
    }

    fn root_page_id(&self) -> u64 {
        Pager::root_page_id(self)
    }

    fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError> {
        Pager::set_root_page_id(self, root_page_id)
    }

    fn pin_page(&mut self, page_id: u64) -> Result<PinnedPage, WrongoDBError> {
        Pager::pin_page(self, page_id)
    }

    fn unpin_page(&mut self, page_id: u64) {
        Pager::unpin_page(self, page_id);
    }

    fn pin_page_mut(&mut self, page_id: u64) -> Result<PinnedPageMut, WrongoDBError> {
        Pager::pin_page_mut(self, page_id)
    }

    fn unpin_page_mut_commit(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError> {
        Pager::unpin_page_mut_commit(self, page)
    }

    fn unpin_page_mut_abort(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError> {
        Pager::unpin_page_mut_abort(self, page)
    }

    fn write_new_page(&mut self, payload: &[u8]) -> Result<u64, WrongoDBError> {
        Pager::write_new_page(self, payload)
    }

    fn request_checkpoint_after_updates(&mut self, count: usize) {
        Pager::request_checkpoint_after_updates(self, count);
    }

    fn checkpoint_requested(&self) -> bool {
        Pager::checkpoint_requested(self)
    }

    fn checkpoint_prepare(&self) -> u64 {
        Pager::checkpoint_prepare(self)
    }

    fn checkpoint_flush_data(&mut self) -> Result<(), WrongoDBError> {
        Pager::checkpoint_flush_data(self)
    }

    fn checkpoint_commit(&mut self, new_root: u64) -> Result<(), WrongoDBError> {
        Pager::checkpoint_commit(self, new_root)
    }

    fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        Pager::sync_all(self)
    }

    fn wal(&mut self) -> Option<&mut WalFile> {
        Pager::wal(self)
    }

    fn wal_mut(&mut self) -> Result<&mut WalFile, WrongoDBError> {
        Pager::wal_mut(self)
    }

    fn take_wal(&mut self) -> Option<WalFile> {
        Pager::take_wal(self)
    }

    fn restore_wal(&mut self, wal: Option<WalFile>) {
        Pager::restore_wal(self, wal);
    }

    fn set_wal_sync_threshold(&mut self, threshold: usize) {
        Pager::set_wal_sync_threshold(self, threshold);
    }

    fn sync_wal(&mut self) -> Result<(), WrongoDBError> {
        Pager::sync_wal(self)
    }

    fn log_wal_operation(&mut self) -> Result<bool, WrongoDBError> {
        Pager::log_wal_operation(self)
    }

    fn data_path(&self) -> &Path {
        self.bf.path.as_path()
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
        let mut pager = Pager::create(&path, 256, false).unwrap();
        let payload_len = pager.page_payload_len();
        let page_id = pager.write_new_page(&vec![0u8; payload_len]).unwrap();

        pager.cache = PageCache::new(PageCacheConfig {
            capacity_pages: 1,
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
        let mut pager = Pager::create(&path, 256, false).unwrap();
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

    #[test]
    fn pin_blocks_eviction_until_unpinned() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("pager-cache-pin.db");
        let mut pager = Pager::create(&path, 256, false).unwrap();
        let payload_len = pager.page_payload_len();

        let page1 = pager.write_new_page(&vec![1u8; payload_len]).unwrap();
        let page2 = pager.write_new_page(&vec![2u8; payload_len]).unwrap();

        pager.cache = PageCache::new(PageCacheConfig {
            capacity_pages: 1,
        });

        let pinned = pager.pin_page(page1).unwrap();
        assert!(pager.cache.contains(page1));

        let err = pager.pin_page(page2).unwrap_err();
        assert!(matches!(err, WrongoDBError::Storage(_)));

        pager.unpin_page(pinned.page_id());

        let pinned2 = pager.pin_page(page2).unwrap();
        assert!(pager.cache.contains(page2));
        assert!(!pager.cache.contains(page1));

        pager.unpin_page(pinned2.page_id());

        let pinned2_again = pager.pin_page(page2).unwrap();
        assert!(pager.cache.contains(page2));
        pager.unpin_page(pinned2_again.page_id());
    }
}
