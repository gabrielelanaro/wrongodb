use std::collections::HashSet;
use std::path::Path;

use crate::core::errors::{StorageError, WrongoDBError};
use crate::storage::block::file::{BlockFile, NONE_BLOCK_ID};

use super::page::Page;
use super::page_cache::{PageCache, PageCacheConfig};
use super::traits::{CheckpointStore, PageRead, PageWrite, RootStore};
use super::types::{PageEdit, ReadPin};

// ============================================================================
// BlockFilePageStore - File-backed page store implementation
// ============================================================================

/// File-backed page store with copy-on-write semantics and page caching.
///
/// `BlockFilePageStore` sits between the B+tree layer and the block file,
/// providing:
///
/// - **Page caching**: LRU cache with pin/unpin semantics to prevent eviction
///   of in-use pages
/// - **Copy-on-write (COW)**: Modifications create new pages, leaving originals
///   intact for crash recovery and checkpointing
/// - **Checkpoint coordination**: Tracks working pages and coordinates flush
///   operations with the block file's extent allocation
///
/// The store maintains a `working_root` separate from the stable checkpoint
/// root. During normal operation, all reads and writes go through the working
/// root. On checkpoint, the working root becomes the new stable root.
///
/// Pages that have been modified (COW copies) are tracked in `working_pages`.
/// On checkpoint commit, these are cleared and the old pages they replaced
/// become eligible for reclamation.
#[derive(Debug)]
pub struct BlockFilePageStore {
    bf: BlockFile,
    working_root: u64,
    working_pages: HashSet<u64>,
    cache: PageCache,
}

impl BlockFilePageStore {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------

    /// Create a new page store at the given path with the specified page size.
    ///
    /// This creates a new block file and initializes an empty page cache.
    /// If the `WRONGO_PREALLOC_PAGES` environment variable is set, the block
    /// file will preallocate that many pages to avoid filesystem allocation
    /// overhead in the hot path.
    ///
    /// The working root is initialized to the block file's root (typically 0
    /// for a new file).
    pub fn create<P: AsRef<Path>>(path: P, page_size: usize) -> Result<Self, WrongoDBError> {
        let mut bf = BlockFile::create(&path, page_size)?;
        if let Ok(raw) = std::env::var("WRONGO_PREALLOC_PAGES") {
            if !raw.is_empty() {
                let blocks: u64 = raw.parse().map_err(|_| {
                    StorageError(format!("invalid WRONGO_PREALLOC_PAGES value: {raw}"))
                })?;
                bf.preallocate_blocks(blocks)?;
            }
        }
        let working_root = bf.root_block_id();

        Ok(Self {
            bf,
            working_root,
            working_pages: HashSet::new(),
            cache: PageCache::new(PageCacheConfig::default()),
        })
    }

    /// Open an existing page store from the given path.
    ///
    /// This opens an existing block file and initializes the page cache.
    /// The working root is set to the block file's current root block ID,
    /// which represents the last stable checkpoint.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WrongoDBError> {
        let bf = BlockFile::open(&path)?;
        let working_root = bf.root_block_id();

        Ok(Self {
            bf,
            working_root,
            working_pages: HashSet::new(),
            cache: PageCache::new(PageCacheConfig::default()),
        })
    }

    // ------------------------------------------------------------------------
    // Lifecycle - Checkpoint
    // ------------------------------------------------------------------------

    /// Reconcile the working state to disk as a stable checkpoint.
    ///
    /// This flushes all dirty pages to disk, atomically updates the root
    /// pointer, and reclaims pages that were freed by COW operations.
    ///
    /// The checkpoint sequence is:
    /// 1. Flush all dirty cached pages to the block file
    /// 2. Update the block file's root block ID to the working root
    /// 3. Sync to ensure durability
    /// 4. Reclaim discarded blocks (pages freed by COW)
    /// 5. Sync again to ensure reclamation is durable
    /// 6. Clear the working pages set
    ///
    /// After checkpoint, the working root becomes the stable root, and
    /// all COW copies become the authoritative versions.
    pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
        let root = self.working_root;
        self.flush_cache()?;
        self.bf.set_root_block_id(root)?;
        self.bf.sync_all()?;
        self.bf.reclaim_discarded()?;
        self.bf.sync_all()?;
        self.working_pages.clear();
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Private Helpers - Page I/O
    // ------------------------------------------------------------------------

    fn allocate_page(&mut self) -> Result<u64, WrongoDBError> {
        self.bf.allocate_block()
    }

    #[cfg(test)]
    fn write_page(&mut self, page_id: u64, payload: &[u8]) -> Result<(), WrongoDBError> {
        self.bf.write_block(page_id, payload)
    }

    fn retire_page(&mut self, page_id: u64) -> Result<(), WrongoDBError> {
        if page_id == NONE_BLOCK_ID {
            return Ok(());
        }
        self.bf.free_block(page_id)?;
        Ok(())
    }

    // ------------------------------------------------------------------------
    // Private Helpers - Cache Operations
    // ------------------------------------------------------------------------

    fn load_page_and_pin(&mut self, page_id: u64) -> Result<ReadPin, WrongoDBError> {
        if !self.cache.contains(page_id) {
            self.evict_cache_if_full()?;
        }
        self.cache
            .load_and_pin(page_id, |pid| self.bf.read_block(pid, true))
    }

    fn load_cow_page(&mut self, page_id: u64) -> Result<Page, WrongoDBError> {
        self.cache
            .load_cow_page(page_id, |pid| self.bf.read_block(pid, true))
    }

    fn evict_cache_if_full(&mut self) -> Result<(), WrongoDBError> {
        self.cache
            .evict_if_full(|page_id, page| self.bf.write_block(page_id, page.data()))
    }

    fn flush_cache(&mut self) -> Result<(), WrongoDBError> {
        self.cache
            .flush(|page_id, page| self.bf.write_block(page_id, page.data()))
    }
}

// ============================================================================
// Trait Implementations
// ============================================================================

/// Read-only page access through the cache.
///
/// This implementation provides the core read operations:
/// - `pin_page`: Load and pin a page, preventing eviction
/// - `get_page`: Access a pinned page's contents
/// - `unpin_page`: Release a pin, allowing eviction
///
/// The pin/unpin pattern ensures pages remain in cache while being read,
/// even if the cache is full and eviction would otherwise occur.
impl PageRead for BlockFilePageStore {
    fn page_payload_len(&self) -> usize {
        self.bf.page_payload_len()
    }

    fn pin_page(&mut self, page_id: u64) -> Result<ReadPin, WrongoDBError> {
        self.load_page_and_pin(page_id)
    }

    fn get_page(&self, pin: &ReadPin) -> &Page {
        self.cache.get_page(pin.page_id())
    }

    fn unpin_page(&mut self, pin: ReadPin) {
        let _ = self.cache.unpin(pin.page_id());
    }
}

/// Write access with copy-on-write semantics.
///
/// This implementation provides modification operations that never overwrite
/// existing pages in place. Instead, modifications create new pages:
///
/// - `pin_page_mut`: Returns a `PageEdit` with either an existing working
///   page (if already modified) or a new COW copy
/// - `commit_page_edit`: Marks the edit as complete and retires the original
/// - `abort_page_edit`: Discards the COW copy, keeping the original
/// - `write_new_page`: Creates a fresh page (for splits, new trees)
///
/// The COW pattern enables crash recovery: uncommitted edits can be discarded
/// without corrupting the original data, and checkpoints can atomically switch
/// to a new consistent state.
impl PageWrite for BlockFilePageStore {
    fn pin_page_mut(&mut self, page_id: u64) -> Result<PageEdit, WrongoDBError> {
        if self.working_pages.contains(&page_id) {
            // Already a working page - return it directly (no new COW needed)
            let pin = self.load_page_and_pin(page_id)?;
            let page = self.cache.get_page(pin.page_id()).clone();
            return Ok(PageEdit {
                page_id,
                original_page_id: None,
                page,
            });
        }

        // Create a new COW copy
        let page = self.load_cow_page(page_id)?;
        self.evict_cache_if_full()?;
        let new_page_id = self.allocate_page()?;
        let entry = self.cache.insert(new_page_id, page.clone());
        entry.pin_count = 1;
        self.working_pages.insert(new_page_id);

        Ok(PageEdit {
            page_id: new_page_id,
            original_page_id: Some(page_id),
            page,
        })
    }

    fn commit_page_edit(&mut self, edit: PageEdit) -> Result<u64, WrongoDBError> {
        let PageEdit {
            page_id,
            original_page_id,
            page,
        } = edit;
        let entry = self
            .cache
            .get_mut(page_id)
            .ok_or_else(|| StorageError(format!("page cache miss for {page_id}")))?;
        if entry.pin_count == 0 {
            return Err(StorageError(format!("page cache pin underflow for {page_id}")).into());
        }

        entry.page = page;
        entry.dirty = true;
        entry.pin_count -= 1;

        if let Some(old_page_id) = original_page_id {
            self.retire_page(old_page_id)?;
        }

        Ok(page_id)
    }

    fn abort_page_edit(&mut self, edit: PageEdit) -> Result<(), WrongoDBError> {
        let PageEdit {
            page_id,
            original_page_id,
            ..
        } = edit;

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

    fn write_new_page(&mut self, page: Page) -> Result<u64, WrongoDBError> {
        self.evict_cache_if_full()?;
        let page_id = self.allocate_page()?;
        self.working_pages.insert(page_id);
        let entry = self.cache.insert(page_id, page);
        entry.dirty = true;
        Ok(page_id)
    }
}

/// Root page management for tree structures.
///
/// The root page ID identifies the entry point of a B+tree. This implementation
/// tracks the "working root"—the root of the tree as modified by uncommitted
/// operations. The stable root (on disk) is only updated during checkpoint.
impl RootStore for BlockFilePageStore {
    fn root_page_id(&self) -> u64 {
        self.working_root
    }

    fn set_root_page_id(&mut self, root_page_id: u64) -> Result<(), WrongoDBError> {
        self.working_root = root_page_id;
        Ok(())
    }
}

/// Checkpoint coordination for durable storage.
///
/// This implementation exposes the three-phase checkpoint protocol:
///
/// 1. **Prepare**: Capture the current working root
/// 2. **Flush**: Write all dirty pages to disk
/// 3. **Commit**: Atomically update the stable root and reclaim old pages
///
/// The protocol is split into phases so higher layers can coordinate
/// checkpoints across multiple stores (e.g., main table + indexes).
impl CheckpointStore for BlockFilePageStore {
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
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn eviction_writes_back_dirty_page() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("page-store-cache.db");
        let mut page_store = BlockFilePageStore::create(&path, 256).unwrap();
        let payload_len = page_store.page_payload_len();
        let page_id = page_store.allocate_page().unwrap();

        page_store.cache = PageCache::new(PageCacheConfig { capacity_pages: 1 });

        let mut page = Page::new_leaf(payload_len).unwrap();
        page.data_mut()[8..].fill(7);
        let expected = page.data().to_vec();
        let entry = page_store.cache.insert(page_id, page);
        entry.dirty = true;

        page_store.evict_cache_if_full().unwrap();
        assert!(!page_store.cache.contains(page_id));

        let read = page_store.bf.read_block(page_id, true).unwrap();
        assert_eq!(read, expected);
    }

    #[test]
    fn flush_rejects_dirty_pinned_page() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("page-store-cache-flush.db");
        let mut page_store = BlockFilePageStore::create(&path, 256).unwrap();
        let payload_len = page_store.page_payload_len();
        let page_id = page_store.allocate_page().unwrap();

        let mut page = Page::new_leaf(payload_len).unwrap();
        page.data_mut()[8..].fill(9);
        let entry = page_store.cache.insert(page_id, page);
        entry.dirty = true;
        entry.pin_count = 1;

        let err = page_store.flush_cache().unwrap_err();
        assert!(err.to_string().contains("cannot flush dirty pinned page"));
    }

    #[test]
    fn pin_blocks_eviction_until_unpinned() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("page-store-cache-pin.db");
        let mut page_store = BlockFilePageStore::create(&path, 256).unwrap();
        let payload_len = page_store.page_payload_len();

        let page1 = page_store.allocate_page().unwrap();
        let page2 = page_store.allocate_page().unwrap();
        page_store
            .write_page(page1, Page::new_leaf(payload_len).unwrap().data())
            .unwrap();
        page_store
            .write_page(page2, Page::new_leaf(payload_len).unwrap().data())
            .unwrap();

        page_store.cache = PageCache::new(PageCacheConfig { capacity_pages: 1 });

        let pinned = page_store.pin_page(page1).unwrap();
        assert!(page_store.cache.contains(page1));

        let err = page_store.pin_page(page2).unwrap_err();
        assert!(matches!(err, WrongoDBError::Storage(_)));

        page_store.unpin_page(pinned);

        let pinned2 = page_store.pin_page(page2).unwrap();
        assert!(page_store.cache.contains(page2));
        assert!(!page_store.cache.contains(page1));

        page_store.unpin_page(pinned2);

        let pinned2_again = page_store.pin_page(page2).unwrap();
        assert!(page_store.cache.contains(page2));
        page_store.unpin_page(pinned2_again);
    }

    #[test]
    fn repeated_pins_reference_same_cached_page() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("page-store-repin.db");
        let mut page_store = BlockFilePageStore::create(&path, 256).unwrap();
        let payload_len = page_store.page_payload_len();
        let page_id = page_store.allocate_page().unwrap();
        page_store
            .write_page(page_id, Page::new_leaf(payload_len).unwrap().data())
            .unwrap();

        let pin1 = page_store.pin_page(page_id).unwrap();
        let pin2 = page_store.pin_page(page_id).unwrap();

        let ptr1 = std::ptr::from_ref(page_store.get_page(&pin1));
        let ptr2 = std::ptr::from_ref(page_store.get_page(&pin2));
        assert_eq!(ptr1, ptr2);

        page_store.unpin_page(pin1);
        page_store.unpin_page(pin2);
    }

    #[test]
    fn abort_discards_new_cow_page() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("page-store-abort.db");
        let mut page_store = BlockFilePageStore::create(&path, 256).unwrap();
        let payload_len = page_store.page_payload_len();

        let base_id = page_store.allocate_page().unwrap();
        page_store
            .write_page(base_id, Page::new_leaf(payload_len).unwrap().data())
            .unwrap();

        let edit = page_store.pin_page_mut(base_id).unwrap();
        let new_page_id = edit.page_id();
        assert_ne!(new_page_id, base_id);

        page_store.abort_page_edit(edit).unwrap();
        assert!(!page_store.cache.contains(new_page_id));
        assert!(!page_store.working_pages.contains(&new_page_id));
    }
}
