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

Status (2026-01-17)
- Slice A/B/C are implemented.
- Slice C lives in `src/leaf_page.rs` (slotted leaf page + `PageFull` + compaction-on-demand).
- Slice D is implemented (2-level B+tree root + leaf splits).
- Slice E is implemented (full B+tree height growth + range scan).
- Slice F is implemented (primary `_id` B+tree lookups).
- Slice G0 is implemented (basic page cache + checkpoint infrastructure).
- Slice G1a is implemented (cache/checkpoint hardening infrastructure).
- Slice G1b is implemented (wire checkpointing into database operations).

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

### Slice G1a: Cache/checkpoint hardening (infrastructure only)
- **Pinning semantics**: Document pin lifetime for point ops vs range scans.
- **Cache as source of truth**: Audit BTree paths to ensure no direct BlockFile access; `write_new_page()` should go through cache.
- **Checkpoint stages**: Make prepare → flush data → commit explicit (currently monolithic).
- **Iterator safety**: Document pin strategy (current: leaf-only pinning); add eviction-during-iteration tests.
- **Checkpoint scheduling**: Helper to request checkpoint after N updates; optional interval-based trigger.
- **Hardening tests**: Dirty pinned pages, all-pinned-at-capacity, concurrent access patterns.
- **Deliverable**: Infrastructure is solid, but checkpointing is NOT yet wired into database operations.

### Slice G1b: Wire checkpointing into database operations
- **Automatic checkpoint scheduling**: Configure `checkpoint_after_updates` in `Collection`; call `Collection::checkpoint()` when threshold reached.
- **Explicit checkpoint API**: Expose `Collection::checkpoint()` as the durability boundary (BTree remains explicit but lower-level).
- **Durability semantics**: Document that `sync_all()` = checkpoint = durability boundary; unflushed pages may be lost on crash.
- **Test coverage**:
  - Insert N records, crash before checkpoint → verify old root is recovered
  - Insert N records, checkpoint, crash → verify new root is recovered
  - Verify dirty pages are actually flushed to disk during checkpoint
  - Verify retired blocks are reclaimed after checkpoint
- **Deliverable**: Checkpointing actually happens during normal database operations; WAL can rely on this working.

### Slice G2: WAL + recovery
- Append log records with LSNs.
- Startup replay to last durable state.
- **Prerequisite**: Slice G1b must be complete (checkpointing is wired and tested).

**Design decision (2026-01-18)**: Using **change vector logging** (WiredTiger-style) instead of full-page logging.
- Logs operations (insert, split) not page images
- ~100x smaller WAL: ~30 bytes per operation vs 4KB full page
- More complex recovery (must replay operations) but matches production WiredTiger architecture
- Trade-off: implementation complexity vs space efficiency

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

### Architectural divergence from MongoDB/WiredTiger (2026-01-25)

**Current minimongo architecture:**
- `AppendOnlyStorage`: JSONL append-only log (actual document storage)
- `id_index` (renamed from `primary_index`): B+Tree mapping `_id` → offset
- `secondary_indexes` (renamed from `index`): In-memory HashMap mapping `field` → offsets

**MongoDB/WiredTiger architecture:**
- Table is a B+Tree with `_id` as key: `_id` → full_document
- All indexes (including `_id`) are separate B+Trees: `indexed_field` → `_id`
- Indexes store primary keys, not storage offsets

**Key differences:**
1. minimongo uses an append-only log + offset-based indexes
2. MongoDB/WiredTiger uses a primary B+Tree table + primary-key-based indexes
3. In minimongo, the log is the "source of truth"; in MongoDB, the B+Tree table is

**Re-alignment needed (future work):**
To align with MongoDB/WiredTiger, we would need to:
- Make the main collection a B+Tree keyed by `_id` (not append-only log)
- Store `_id` values in indexes, not log offsets
- Remove the append-only log architecture
- Handle in-place updates vs append-only model

**Why not yet:**
- The append-only log + offset model is simpler and works well for the current scope
- Would be a major rewrite of the storage layer
- Better to defer until after WAL, recovery, and transactions are solid

**Naming convention update (2026-01-25):**
- Renamed `primary_index` → `id_index` (clearer that it's specifically for `_id`)
- Renamed `index` → `secondary_indexes` (clearer that it's for non-`_id` fields)
- This makes the distinction clearer while keeping the current architecture

## Performance notes / TODOs
- B+tree splits currently choose a split point by rebuilding candidate left/right pages around the midpoint; OK for small pages but should be replaced with a single-pass “split-by-bytes” / one-build approach.
- Range scans currently avoid leaf sibling pointers (no on-disk format change) and instead advance using a parent stack; consider adding leaf links for O(1) leaf-to-leaf transitions.
