## Context
The storage engine currently writes a new block for every page mutation (copy-on-write). This keeps checkpoints safe, but it inflates I/O and retired-block churn when repeated inserts touch the same pages. A page cache between `Pager` and `BlockFile` can keep hot pages in memory, coalesce multiple mutations, and defer disk writes.

Current flow:
```
BTree -> Pager -> BlockFile
```
Proposed flow:
```
BTree -> Pager -> PageCache -> BlockFile
```

## Goals / Non-Goals
Goals:
- Reduce write amplification by coalescing repeated mutations to the same page between checkpoints.
- Preserve checkpoint safety (never overwrite pages reachable from the last durable root).
- Support pin/unpin so iterators/scans can hold pages in memory safely.

Non-Goals:
- WAL, crash recovery beyond the existing checkpoint model.
- Multi-threaded access or cross-process cache sharing.
- On-disk format changes for cache metadata.

## Decisions
- Introduce a bounded in-memory cache keyed by block id with dirty state, pin count, and access metadata.
- Use a write-back policy: dirty pages are flushed on eviction and before checkpoint commit.
- On first write to a page reachable from the last checkpoint, allocate a new block id and update the working tree to reference it; subsequent writes reuse that working block id until the next checkpoint.
- Pinned pages are not eligible for eviction.
- Use an LRU eviction policy based on a monotonic access counter; default cache capacity is 256 pages and is configurable via a cache config struct.

## Cache entry shape
Each cache entry tracks:
- `page_id: u64` (the current working block id for this page)
- `payload: Vec<u8>` (page payload bytes only, sized to `BlockFile::page_payload_len()`)
- `dirty: bool` (payload differs from on-disk contents)
- `pin_count: u32` (number of active pins)
- `last_access: u64` (monotonic access counter for LRU selection)

## Pager cache API
The pager exposes a minimal, explicit pin/unpin API around the cache:
- `pin_page(page_id) -> PinnedPage`: read-intent; loads from cache or disk, increments `pin_count`, updates `last_access`, returns a page handle with an owned payload buffer.
- `pin_page_mut(page_id) -> PinnedPageMut`: write-intent; performs first-write COW if the page is still stable, ensures a cache entry exists for the working block id, marks it dirty, increments `pin_count`, returns a page handle with an owned payload buffer.
- `unpin_page(PinnedPage)`: decrements `pin_count` for the cache entry.
- `unpin_page_mut(PinnedPageMut)`: replaces the cache entry payload with the handle’s buffer (to capture edits) and decrements `pin_count` (entry stays dirty).
- `flush_cache()`: writes all dirty cached pages to disk (via `BlockFile::write_block`) and marks them clean; used by eviction and before checkpoint commit.

This design keeps pinned handles as owned buffers (no shared borrowing), which keeps call sites simple and avoids borrow checker complexity in the single-threaded engine.

## Eviction + sizing
- Capacity is a fixed number of cached pages (default 256).
- Configuration is provided by `PageCacheConfig { capacity_pages, eviction_policy }`, with default values used by `Pager::create/open` and explicit override in `Pager::create_with_cache/open_with_cache` (and BTree wrappers if needed).
- Eviction scans for the least-recently-used entry with `pin_count == 0`; if none exist, the cache returns an error (“all pages pinned”) rather than evicting a pinned page.

## Alternatives Considered
- Write-through cache (simpler, but does not reduce write amplification).
- mmap-based page cache (larger redesign, more platform dependencies).
- WAL-first durability instead of caching (out of scope for this slice).

## Risks / Trade-offs
- Cache memory pressure: eviction may block if all pages are pinned.
- Dirty pages may remain only in memory until eviction or checkpoint; crashes discard uncheckpointed changes (consistent with the current model).
- More complex write path: first-write COW must update parent links and retired blocks correctly.

## Migration Plan
- No file format changes. Cache is in-memory only.
- Public APIs remain the same; internal pager/BTree APIs will change.

## Open Questions
- Instrumentation for cache hit/evict/flush metrics.
