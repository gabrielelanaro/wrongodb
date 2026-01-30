# page-cache Specification

## Purpose
TBD - created by archiving change add-page-cache. Update Purpose after archive.
## Requirements
### Requirement: Bounded in-memory page cache
The storage engine SHALL maintain an in-memory page cache between `Pager` and `BlockFile` with a configurable maximum size (in pages) and an eviction policy that favors recently used unpinned pages.

#### Scenario: Eviction selects an unpinned page
- **WHEN** the cache is full and a new page is requested
- **THEN** the engine SHALL evict an unpinned page according to the eviction policy

### Requirement: Cache pinning for iterators and scans
The storage engine SHALL support pinning cached pages so they are not evicted while in use, and SHALL release them on unpin.

#### Scenario: Pinned page is not evicted
- **WHEN** a page is pinned and the cache reaches capacity
- **THEN** the pinned page SHALL NOT be selected for eviction

### Requirement: Reclaiming cached pages requires unpin and (if dirty) flush
The storage engine SHALL evict cached pages only after their pin count reaches zero, and SHALL flush dirty pages before eviction. If all pages are pinned and the cache is at capacity, the cache SHALL fail the request to admit a new page.

#### Scenario: All pages pinned at capacity
- **WHEN** the cache is full and every cached page is pinned
- **THEN** the cache SHALL return an error instead of evicting a pinned page

### Requirement: Write-back of dirty pages on eviction or checkpoint
The storage engine SHALL mark modified cached pages as dirty and SHALL write their contents to disk on eviction or during checkpoint before the root swap is committed.

#### Scenario: Dirty pages are flushed before checkpoint commit
- **WHEN** a checkpoint is requested while dirty pages exist in the cache
- **THEN** those dirty pages SHALL be written to disk before the checkpoint root is persisted

### Requirement: Copy-on-write for the first dirtying of stable pages
The storage engine SHALL ensure that the first mutable access to a page reachable from the last durable checkpoint allocates a new block id and updates the working tree to reference it. Subsequent mutations before the next checkpoint SHALL update that same working block id and SHALL NOT allocate additional blocks for that page.

#### Scenario: Multiple updates coalesce between checkpoints
- **WHEN** the same page is modified multiple times between checkpoints
- **THEN** the working tree SHALL continue to reference a single new block id for that page

### Requirement: Retired blocks are reclaimed only after checkpoint
The storage engine SHALL NOT recycle retired blocks until after a successful checkpoint commits the new root, even if dirty pages are flushed due to eviction.

#### Scenario: Eviction does not reuse retired blocks
- **WHEN** a dirty page is evicted and written to disk before a checkpoint
- **THEN** any blocks retired by that update SHALL remain unreused until checkpoint completion

