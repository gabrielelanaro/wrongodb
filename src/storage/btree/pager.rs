use std::collections::HashSet;
use std::path::Path;

use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::block::file::{BlockFile, NONE_BLOCK_ID};

use super::page_cache::{PageCache, PageCacheConfig};

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

pub(super) trait PageRead: std::fmt::Debug + Send + Sync {
    fn page_payload_len(&self) -> usize;
    fn pin_page(&mut self, page_id: u64) -> Result<PinnedPage, WrongoDBError>;
    fn unpin_page(&mut self, page_id: u64);
}

pub(super) trait PageWrite: std::fmt::Debug + Send + Sync {
    fn pin_page_mut(&mut self, page_id: u64) -> Result<PinnedPageMut, WrongoDBError>;
    fn unpin_page_mut_commit(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError>;
    fn unpin_page_mut_abort(&mut self, page: PinnedPageMut) -> Result<(), WrongoDBError>;
    fn write_new_page(&mut self, payload: &[u8]) -> Result<u64, WrongoDBError>;
}

pub(super) trait RootStore: std::fmt::Debug + Send + Sync {
    fn root_page_id(&self) -> u64;
    fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError>;
}

pub(super) trait CheckpointStore: std::fmt::Debug + Send + Sync {
    fn request_checkpoint_after_updates(&mut self, count: usize);
    fn checkpoint_requested(&self) -> bool;
    fn checkpoint_prepare(&self) -> u64;
    fn checkpoint_flush_data(&mut self) -> Result<(), WrongoDBError>;
    fn checkpoint_commit(&mut self, new_root: u64) -> Result<(), WrongoDBError>;
    fn sync_all(&mut self) -> Result<(), WrongoDBError>;
    fn data_path(&self) -> &Path;
}

pub(super) trait BTreeStore: PageRead + PageWrite + RootStore + CheckpointStore {}

impl<T> BTreeStore for T where
    T: PageRead + PageWrite + RootStore + CheckpointStore
{
}

impl Pager {
    pub(super) fn create<P: AsRef<Path>>(path: P, page_size: usize) -> Result<Self, WrongoDBError> {
        let bf = BlockFile::create(&path, page_size)?;
        let working_root = bf.root_block_id();

        Ok(Self {
            bf,
            working_root,
            working_pages: HashSet::new(),
            cache: PageCache::new(PageCacheConfig::default()),
            updates_since_checkpoint: 0,
            checkpoint_after_updates: None,
        })
    }

    pub(super) fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let bf = BlockFile::open(&path)?;
        let working_root = bf.root_block_id();

        Ok(Self {
            bf,
            working_root,
            working_pages: HashSet::new(),
            cache: PageCache::new(PageCacheConfig::default()),
            updates_since_checkpoint: 0,
            checkpoint_after_updates: None,
        })
    }

    pub(super) fn page_payload_len(&self) -> usize {
        self.bf.page_payload_len()
    }

    pub(super) fn page_size(&self) -> usize {
        self.bf.page_size
    }

    pub(super) fn root_page_id(&self) -> u64 {
        self.working_root
    }

    pub(super) fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError> {
        self.working_root = root_page_id;
        Ok(())
    }

    /// Prepare for checkpoint by capturing the current working root.
    fn checkpoint_prepare(&self) -> u64 {
        self.working_root
    }

    /// Flush all dirty cached pages to disk.
    fn checkpoint_flush_data(&mut self) -> Result<(), WrongoDBError> {
        self.flush_cache()
    }

    /// Commit the checkpoint by atomically swapping the root and reclaiming retired blocks.
    fn checkpoint_commit(&mut self, new_root: u64) -> Result<(), WrongoDBError> {
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

    /// Returns the path to the data file.
    pub(super) fn data_path(&self) -> &Path {
        self.bf.path.as_path()
    }

    pub(super) fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        let root = self.checkpoint_prepare();
        self.checkpoint_flush_data()?;
        self.checkpoint_commit(root)?;
        // Reset update counter after successful checkpoint
        self.updates_since_checkpoint = 0;
        Ok(())
    }


    /// Increment the update counter (to be called after each mutation).
    fn track_update(&mut self) {
        self.updates_since_checkpoint = self.updates_since_checkpoint.saturating_add(1);
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

    fn load_page_and_pin(&mut self, page_id: u64) -> Result<Vec<u8>, WrongoDBError> {
        if !self.cache.contains(page_id) {
            self.evict_cache_if_full()?;
        }
        self.cache
            .load_and_pin(page_id, |pid| self.bf.read_block(pid, true))
    }

    fn load_cow_payload(&mut self, page_id: u64) -> Result<Vec<u8>, WrongoDBError> {
        self.cache
            .load_cow_payload(page_id, |pid| self.bf.read_block(pid, true))
    }

    fn evict_cache_if_full(&mut self) -> Result<(), WrongoDBError> {
        self.cache
            .evict_if_full(|page_id, payload| self.bf.write_block(page_id, payload))
    }

    fn flush_cache(&mut self) -> Result<(), WrongoDBError> {
        self.cache
            .flush(|page_id, payload| self.bf.write_block(page_id, payload))
    }
}

impl PageRead for Pager {
    fn page_payload_len(&self) -> usize {
        Pager::page_payload_len(self)
    }

    fn pin_page(&mut self, page_id: u64) -> Result<PinnedPage, WrongoDBError> {
        Pager::pin_page(self, page_id)
    }

    fn unpin_page(&mut self, page_id: u64) {
        Pager::unpin_page(self, page_id);
    }
}

impl PageWrite for Pager {
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
}

impl RootStore for Pager {
    fn root_page_id(&self) -> u64 {
        Pager::root_page_id(self)
    }

    fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError> {
        Pager::set_root_page_id(self, root_page_id)
    }
}

impl CheckpointStore for Pager {
    fn request_checkpoint_after_updates(&mut self, count: usize) {
        self.checkpoint_after_updates = Some(count);
    }

    fn checkpoint_requested(&self) -> bool {
        if let Some(threshold) = self.checkpoint_after_updates {
            return self.updates_since_checkpoint >= threshold;
        }
        false
    }

    fn checkpoint_prepare(&self) -> u64 {
        self.working_root
    }

    fn checkpoint_flush_data(&mut self) -> Result<(), WrongoDBError> {
        self.flush_cache()
    }

    fn checkpoint_commit(&mut self, new_root: u64) -> Result<(), WrongoDBError> {
        self.bf.set_root_block_id(new_root)?;
        self.bf.sync_all()?;
        self.bf.reclaim_discarded()?;
        self.bf.sync_all()?;
        self.working_pages.clear();
        Ok(())
    }

    fn sync_all(&mut self) -> Result<(), WrongoDBError> {
        self.bf.sync_all()
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
        let mut pager = Pager::create(&path, 256).unwrap();
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

    #[test]
    fn pin_blocks_eviction_until_unpinned() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("pager-cache-pin.db");
        let mut pager = Pager::create(&path, 256).unwrap();
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
