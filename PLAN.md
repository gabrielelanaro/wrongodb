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

Status (2025-12-14)
- Slice A/B/C are implemented.
- Slice C lives in `src/leaf_page.rs` (slotted leaf page + `PageFull` + compaction-on-demand).

### Slice D: 2‑level B+tree
- Root internal + leaf pages.
- Leaf split + root update only (height ≤ 2).

### Slice E: Full B+tree (arbitrary height)
- Recursive insert + internal splits.
- Ordered range scan.

### Slice F: Plug B+tree into WrongoDB
- Primary `_id` storage uses B+tree.
- `find_one({_id: …})` hits primary; `find()` still scans.

### Slice G: WAL + recovery
- Append log records with LSNs.
- Startup replay to last durable state.

### Slice H: Checkpoints
- Flush dirty pages to new blocks.
- Atomic metadata update + WAL truncation.

### Slice I: MVCC / transactions
- Txn ids/timestamps, per‑record version chains.
- Snapshot reads + GC.
