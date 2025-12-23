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
- Eviction policy choice (LRU vs clock) and default cache size.
- Behavior when all pages are pinned (block, error, or grow cache temporarily).
- Instrumentation for cache hit/evict/flush metrics.
