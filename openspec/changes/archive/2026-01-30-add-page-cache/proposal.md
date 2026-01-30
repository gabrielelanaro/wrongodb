# Change: Add in-memory page cache for coalesced COW writes

## Why
The current copy-on-write path allocates and writes a new block for every page mutation. This amplifies I/O and churns the free list even when repeated inserts touch the same leaf or internal page. A page cache lets us keep hot pages in memory, coalesce multiple mutations, and write back only on eviction or checkpoint.

## What Changes
- Add an in-memory page cache between `Pager` and `BlockFile` with dirty/pin state and bounded eviction.
- Add a pin/unpin API for iterators/scans and a mutable-page API for write paths.
- Update write paths to reuse a single working block id between checkpoints (COW only on first mutation of a stable page).
- Flush dirty pages on eviction and before checkpoint root swap, while preserving existing COW and free-list rules.
- **BREAKING (internal)**: pager and BTree call patterns change (writes are no longer immediately persisted).

## Impact
- Affected specs: new `page-cache` capability (checkpoint-create semantics preserved).
- Affected code: `src/btree/pager.rs`, `src/btree.rs`, `src/blockfile.rs`, iterator code, tests.
