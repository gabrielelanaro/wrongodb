# WiredTiger‑like Storage Engine Roadmap (Option 2)

Goal: evolve `wrongodb` from a JSONL append‑only toy into a simplified, **WiredTiger‑inspired** storage engine. We’re cloning *structure and core invariants*, not full fidelity. Constraints: single‑process, no networking, no compression initially, Rust implementation.

## High‑level phases
1. **Paged K/V substrate**
   - Fixed‑size pages (blocks) in a single file per table.
   - Block manager: read/write blocks, allocate/free, checksums.
2. **Row‑store B+tree**
   - Leaf/internal pages, splits, ordered scans.
   - `_id` primary table as a B+tree.
3. **Catalog / metadata**
   - Metadata table mapping table/index names → root blocks + config.
   - Bootstrap “turtle” file pointing at metadata root.
4. **Journaling (WAL) + recovery**
   - Log page updates with LSNs; replay after crash.
5. **Checkpoints**
   - Periodic consistent snapshots; atomic root swap; WAL truncation.
6. **MVCC / transactions**
   - Version chains, snapshot reads, visibility rules, GC.
7. **Cache + eviction**
   - In‑memory page cache, dirty tracking, reconcile on eviction/checkpoint.
8. **Secondary indexes**
   - Each index is its own B+tree; atomic multi‑tree updates.
9. **Background maintenance**
   - Space reuse, compaction, optional compression, stats.

## Thin slices

### Slice A: File format + page I/O
- Pick fixed page size (default 4KB).
- Define file header (stored in block 0):
  - magic, version, page_size, root_block_id, free_list_head, reserved.
- Implement `BlockFile`:
  - `create/open`, `read_block(id)`, `write_block(id, payload)`.
  - Per‑block CRC32 checksum (first 4 bytes of each block).
- Tiny smoke check: write blocks, reopen, verify bytes.

### Slice B: Block allocation + free list
- `allocate_block()` appends or reuses freed blocks.
- `free_block(id)` pushes onto a persisted free list.
- Verify reuse survives reopen.

### Slice C: Leaf‑page KV store (single page, no splits)
- Leaf layout + slot directory.
- In‑page `put/get/delete` until full, then raise `PageFull`.

Status (2025-12-21)
- Slice A/B/C are implemented.
- Slice C lives in `src/leaf_page.rs` (slotted leaf page + `PageFull` + compaction-on-demand).
- Slice D is implemented (2-level B+tree root + leaf splits).
- Slice E is implemented (full B+tree height growth + range scan).
- Slice F is implemented (primary `_id` B+tree lookups).
- Slice G0 is implemented (basic page cache + checkpoint).

### Slice D: 2‑level B+tree
- Root internal + leaf pages.
- Leaf split + root update only (height ≤ 2).

### Slice E: Full B+tree (arbitrary height)
- Recursive insert + internal splits.
- Ordered range scan.
- Refactor: introduce a small `Pager` wrapper around `BlockFile` so `BTree` does not call `read_block/write_block/write_new_block/set_root_block_id` directly (no behavior change; keep `LeafPage`/`InternalPage` as pure “operate on bytes” types).

### Slice F: Plug B+tree into WrongoDB
- Primary `_id` storage uses B+tree.
- `find_one({_id: …})` hits primary; `find()` still scans.

### Slice G0: Page cache + basic checkpoint
- Add in-memory page cache between `Pager` and `BlockFile` with LRU eviction.
- Pin/unpin API for iterators and scans.
- Copy-on-write updates with working root vs stable root.
- Dirty page tracking and writeback on eviction/checkpoint.
- Basic checkpoint: flush dirty pages, atomic root swap (checkpoint slots), retired block reuse.
- Tests: pin blocks eviction, dirty pages written back, checkpoint commit selects new root on reopen, crash-before-checkpoint uses old root.

### Slice G1: Cache/checkpoint hardening (pre-WAL)
- **Pinning semantics**: Document pin lifetime for point ops vs range scans.
- **Cache as source of truth**: Audit BTree paths to ensure no direct BlockFile access; `write_new_page()` should go through cache.
- **Checkpoint stages**: Make prepare → flush data → commit explicit (currently monolithic).
- **Iterator safety**: Document pin strategy (current: leaf-only pinning); add eviction-during-iteration tests.
- **Checkpoint scheduling**: Helper to request checkpoint after N updates; optional interval-based trigger.
- **Hardening tests**: Dirty pinned pages, all-pinned-at-capacity, concurrent access patterns.

### Slice G2: WAL + recovery
- Append log records with LSNs.
- Startup replay to last durable state.

### Slice H: Checkpoint + WAL integration
- WAL truncation after checkpoint (log records before last checkpoint LSN can be discarded).
- Checkpoint scheduling integration with WAL (periodic checkpoints to bound WAL size).
- Note: checkpoints are the explicit durability boundary (eviction is opportunistic and only happens when the cache is under pressure).

### Slice I: MVCC / transactions
- Txn ids/timestamps, per‑record version chains.
- Snapshot reads + GC.

## WiredTiger alignment notes
- Eviction is opportunistic (memory pressure) vs checkpoint as explicit durability boundary.
- Checkpoint staging in WT: prepare → data files → history store → flush → metadata; we use a simpler stable-root vs working-root model without generations.
- Checkpoint scheduling: periodic or on-demand; added in Slice G1.

## Performance notes / TODOs
- B+tree splits currently choose a split point by rebuilding candidate left/right pages around the midpoint; OK for small pages but should be replaced with a single-pass “split-by-bytes” / one-build approach.
- Range scans currently avoid leaf sibling pointers (no on-disk format change) and instead advance using a parent stack; consider adding leaf links for O(1) leaf-to-leaf transitions.
