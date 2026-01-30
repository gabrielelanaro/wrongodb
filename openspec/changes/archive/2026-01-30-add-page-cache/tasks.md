## 0. Refactor prep
- [x] 0.1 Refactor pager/BTree call sites to use pinned page handles (read + mutable) instead of raw `read_page` buffers.
- [x] 0.2 Introduce minimal pin/unpin plumbing for iterators/scans (no cache yet), so cache integration is localized later.

## 1. Design and API
- [x] 1.1 Define the cache entry shape (payload buffer, dirty flag, pin count, access metadata).
- [x] 1.2 Define the pager cache API (read, write-intent, pin/unpin, flush).
- [x] 1.3 Decide eviction policy and cache sizing configuration.

## 2. Cache core
- [x] 2.1 Implement the page cache container keyed by block id.
- [x] 2.2 Implement eviction (skip pinned pages; flush dirty pages; update state).
- [x] 2.3 Implement dirty tracking and write-back on eviction/checkpoint.

## 3. Copy-on-write integration
- [x] 3.1 Implement first-write COW for pages reachable from the stable root.
- [x] 3.2 Ensure parent links update to new block ids on first-write COW.
- [x] 3.3 Integrate cache updates with retired-block tracking and reuse rules.

## 4. Checkpoint integration
- [x] 4.1 Flush dirty cached pages before persisting the checkpoint root.
- [x] 4.2 Reset cache/retired state after a successful checkpoint.

## 5. Tests
- [x] 5.1 Cache hit/eviction tests (including pinned pages).
- [x] 5.2 Multiple updates coalesce to a single working block id between checkpoints.
- [x] 5.3 Crash-safety test: stable root remains readable after uncheckpointed writes.
