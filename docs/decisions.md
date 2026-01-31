# Decisions

## 2026-01-31: Configurable server listen address

**Decision**
- Allow the server listen address to be overridden via (in order): CLI arg, `WRONGO_ADDR`, `WRONGO_PORT`.
- Default remains `127.0.0.1:27017` when no overrides are provided.

**Why**
- Avoid port collisions during local development and benchmarks.
- Enable multiple local instances without code changes.

**Notes**
- `WRONGO_PORT` only changes the port and still binds on `127.0.0.1`.

## 2026-01-30: BTree pager abstraction via `PageStore`

**Decision**
- Introduce a `PageStore` trait in the B-tree layer to abstract pager operations.
- Store the pager in `BTree` as a `Box<dyn PageStore>` and route range iteration through the trait.
- Add a `data_path()` accessor on the trait to avoid direct `BlockFile` exposure in recovery.

**Why**
- Decouple B-tree algorithms/iteration from the concrete pager implementation (DIP).
- Enable alternative page-store implementations (tests, in-memory, future backends) without changing B-tree logic.
- Keep the public `BTree` API unchanged while still enforcing the abstraction internally.

**Notes**
- The trait uses the existing pinned page types (`PinnedPage`, `PinnedPageMut`) to keep the refactor small.
- Dynamic dispatch is limited to pager calls; behavior and persistence semantics are unchanged.

## 2026-01-30: Split pager abstraction into smaller traits

**Decision**
- Replace the monolithic `PageStore` with smaller traits (`PageRead`, `PageWrite`, `RootStore`,
  `CheckpointStore`, `WalStore`, `DataPath`) and compose them via `BTreeStore`.
- Keep `BTree` storing a boxed `dyn BTreeStore` while `BTreeRangeIter` only depends on `PageRead`.

**Why**
- Reduce interface surface per use site (ISP/SRP) while preserving the existing public `BTree` API.
- Allow future components (e.g., iterators/tests) to depend on smaller capability sets.

**Notes**
- This is a refactor-only change; storage semantics and behavior remain the same.

## 2026-01-30: Move WAL ownership out of Pager

**Decision**
- Introduce a `Wal` handle owned by `BTree` for logging/sync policy.
- Remove WAL state and methods from `Pager`; `Pager` is now page + checkpoint only.

**Why**
- Keep the pager focused on page IO/COW/checkpointing (SRP).
- Make WAL lifecycle/policy independent and easier to replace or disable.

**Notes**
- `Wal` is optional (`None` when disabled) and created in `BTree::create/open`.
- Recovery temporarily detaches the `Wal` handle to avoid logging during replay.

## 2026-01-28: Extent metadata in header payload + main table naming

**Decision**
- Replace the free-list head with persisted extent lists (alloc/avail/discard) stored in the header payload.
- Track discarded extents with a generation tag and reclaim them into avail after checkpoint commit.
- Store the main table as a dedicated B-tree file at `{collection_path}.main.wt`.
- Encode `_id` keys and document values as BSON; secondary index keys append a length-prefixed BSON `_id`.
- Use two skiplists per extent list (by-offset and by-size) instead of WiredTiger’s size-bucketed list.

**Why**
- Keep allocator metadata persistent without adding extra metadata files while aligning with COW + checkpoint semantics.
- Separate main-table data from index files using a predictable naming convention.
- BSON provides a deterministic binary representation for Mongo-like documents and ids.

**Notes**
- The alloc list reflects blocks reachable from the stable checkpoint; discard extents are reclaimed and coalesced into avail on checkpoint.
- WiredTiger uses a by-size skiplist of size buckets, each pointing to a by-offset skiplist of extents of that size; we use a flat by-size skiplist keyed by `(size, offset)` plus a separate by-offset skiplist.

**Skiplist Shapes (WT vs ours)**
```
WiredTiger (avail list):
  by-size skiplist
    size=4  -> size=8 -> size=16
       |        |        |
       v        v        v
    by-offset by-offset by-offset
    10->20    7->40     100

Ours (avail list):
  by-size skiplist (key = (size, offset))
    (4,10) -> (4,20) -> (8,7) -> (8,40) -> (16,100)

  by-offset skiplist (key = offset)
    7 -> 10 -> 20 -> 40 -> 100
```

**Implication**
- Allocation policy is the same (best-fit, then lowest offset within that size), but WT’s size buckets keep the top-level size list small when many extents share the same size. Our flat list is simpler to serialize but its size index scales with total extents.

## 2026-01-27: Logical WAL replay (WiredTiger-style)

**Decision**
- Replace page-level WAL records with logical put/delete records.
- Recovery replays logical ops through BTree writes with WAL disabled.
- Bump WAL format version; incompatible WAL is rejected during recovery (open continues with warning). No migration/backwards compatibility.
- Recovery does not auto-checkpoint; WAL is retained until an explicit checkpoint.

**Why**
- Avoid COW page-id drift and split ordering failures during recovery.
- Align with WiredTiger's logical WAL approach (key-based logging, normal B-tree replay).

**Notes**
- Replay is idempotent: put is upsert; delete missing keys is OK.

## 2026-01-11: Agentic image generation loop for blog diagrams

**Decision**
- Add an opt-in agentic loop to `blog/generate_image.py` that drafts prompts, critiques images, and iterates with a cap.
- Use `gemini-3-pro-image-preview` for draft, critique, and image generation steps.
- Emit a sidecar JSON file next to the output image in agentic mode with prompts and critique summaries.

**Why**
- Improve story effectiveness, visual consistency, and overall aesthetic quality for diagrams.
- Preserve iteration context for later tuning without manual logging.

**Notes**
- Default single-shot behavior remains unchanged when `--agentic` is not used.

## 2026-01-02: Commit/abort semantics for mutable pinned pages

**Decision**
- Introduce commit/abort semantics for mutable page pins:
  - `pin_page_mut` records the original (stable) page id when first-write COW happens.
  - `unpin_page_mut_commit` writes the updated payload into the cache and retires the original page.
  - `unpin_page_mut_abort` discards the working page and keeps the original page reachable.

**Why**
- Avoid retiring blocks that are still reachable from the stable root when a mutation fails.
- Make COW safe under error paths while preserving the “coalesce writes until checkpoint” behavior.

**Notes**
- Only first-write COW paths carry an `original_page_id`; subsequent writes to the same working page commit without retiring anything.
- The stable root pointer remains unchanged until checkpoint, so aborting a mutation must never invalidate stable pages.

## 2025-12-13: `BlockFile::write_block` does not auto-allocate

**Decision**
- `BlockFile::write_block(block_id, payload)` requires `block_id` to already exist in the file (i.e., `block_id < num_blocks()`).
- New blocks are obtained via `allocate_block()` (and optionally written via `write_new_block(payload)`).

**Why**
- Prevent accidental sparse/implicit file growth via caller-chosen ids, which can hide bugs and produce partially-initialized blocks.
- Keep a clear invariant boundary: allocation/freeing is explicit (needed for B+tree no-overwrite updates, checkpoints, and later recovery/WAL work).
- Closer to WiredTiger’s model: the block manager write path allocates space and returns an opaque “address cookie”, rather than allowing arbitrary overwrites by id.

**Notes**
- We still allow overwriting existing blocks (needed for the header page and for free-list metadata in this simplified engine).
- Future slices (WAL/checkpoint) will likely move callers to “write-new + root swap” patterns, and tighten in-place writes further.
- On-disk header uses `u64` block pointers with `0` meaning “none” (block 0 is reserved for the header page).
- Free space reuse is currently a persisted **singly-linked free list** of block IDs (simple Slice B mechanism).

**Current on-disk layout (checkpoint metadata v2)**
```
====================== file ======================+
| page 0 | page 1 | page 2 | page 3 | ... | page N|
+--------+--------+--------+--------+-----+-------+

page 0 (header page):
  +--------------------+-------------------------------+
  | CRC32 (4 bytes)    | header payload (padded)       |
  +--------------------+-------------------------------+
                       | magic[8]                      |
                       | version(u16)                  |
                       | page_size(u32)                |
                       | free_list_head(u64)           |
                       | slot0.root_block_id(u64)      |
                       | slot0.generation(u64)         |
                       | slot0.crc32(u32)              |
                       | slot1.root_block_id(u64)      |
                       | slot1.generation(u64)         |
                       | slot1.crc32(u32)              |
                       | ...zero padding...            |
                       +-------------------------------+

page k (k > 0):
  +--------------------+-------------------------------+
  | CRC32 (4 bytes)    | payload bytes (padded)        |
  +--------------------+-------------------------------+
```

**Free list encoding (Slice B)**
- `free_list_head == 0` means “no free blocks”.
- If `free_list_head == X`, page `X` is a free block; its payload begins with `next_free(u64)`:
```
header.free_list_head = 7

page 7 payload:
  +----------------------+
  | next_free = 12 (u64) |
  +----------------------+
  | unused/padding...    |
  +----------------------+

page 12 payload:
  +----------------------+
  | next_free = 3 (u64)  |
  +----------------------+

page 3 payload:
  +----------------------+
  | next_free = 0 (u64)  |
  +----------------------+
```

**Allocation / freeing behavior**
- `allocate_block()`:
  - If `free_list_head != 0`, pop the head free block and advance `free_list_head` to `next_free`.
  - Otherwise, append a new page by extending the file length.
- `free_block(id)`:
  - Write the current `free_list_head` into `id`’s payload as `next_free`, then set `free_list_head = id`.

**How this differs from WiredTiger**
- WiredTiger’s block manager is checkpoint-aware and uses extent lists (e.g., alloc/avail/discard) that are snapshotted and merged across checkpoints; it is not a single persisted linked list of block IDs.
- We’re intentionally starting with the simplest persisted allocator; later phases (checkpointing) can evolve toward extent-based free space management.

**References**
- WiredTiger Block Manager docs: https://source.wiredtiger.com/develop/arch-block.html
- WiredTiger file growth tuning: https://source.wiredtiger.com/develop/tune_file_alloc.html

## 2025-12-14: Slotted leaf-page format for in-page KV (Slice C)

**Decision**
- Implement the Slice C “single-page KV” as a **slotted leaf page** stored inside one `BlockFile` payload.
- The page uses a small fixed header + a slot directory at the beginning and variable-size records packed from the end of the page.
- Keys are ordered by the **slot directory order** (lexicographic byte ordering), enabling binary search.

**Why**
- Variable-sized K/V records need indirection; a slot directory allows inserts/deletes to mostly shift small fixed-size slot entries instead of moving record bytes on every change.
- This matches the common “slotted page” pattern used by B-tree implementations and sets us up for later slices (splits, scans).

**On-page layout**
- All integers are little-endian.
- Offsets/lengths are `u16`, so the page payload must be `<= 65535` bytes (true for the default 4KB pages).

Header (`HEADER_SIZE = 8` bytes):
```
byte 0: page_type (u8) = 1 (leaf)
byte 1: flags (u8)     = 0 (reserved)
byte 2: slot_count (u16)
byte 4: lower (u16)  = HEADER_SIZE + slot_count * SLOT_SIZE
byte 6: upper (u16)  = start of packed record bytes (grows downward)
```

Slot (`SLOT_SIZE = 4` bytes), stored in sorted key order:
```
offset (u16): record start within the page payload
len    (u16): record length in bytes
```

Record bytes, packed from the end of the page toward the front:
```
klen (u16)
vlen (u16)
key bytes (klen)
value bytes (vlen)
```

**Free space / fragmentation**
- Contiguous free space is `upper - lower`.
- `delete(key)` removes the slot entry but leaves record bytes as unreachable garbage.
- `put(key, value)` performs `compact()` automatically if `upper - lower` is insufficient; if still insufficient it returns `PageFull`.
- `compact()` rewrites the page into a tightly packed form (no garbage), preserving slot order and updating offsets.

**Notes**
- This is intentionally a simplified page format for Slice C; later slices can add page ids, overflow handling, and internal pages.

**References**
- PostgreSQL storage page layout: https://www.postgresql.org/docs/current/storage-page-layout.html
- SQLite database file format (B-tree pages): https://www.sqlite.org/fileformat2.html

## 2025-12-14: 2-level B+tree internal-root format + root persistence (Slice D)

**Decision**
- Introduce an **internal page** format (`page_type = 2`) for Slice D (root + leaf pages only).
- Store the tree’s current root block id in the file header’s `root_block_id`, and persist updates via a new API: `BlockFile::set_root_block_id(u64)`.
- Root can point at either a **leaf page** (`page_type = 1`) or an **internal page** (`page_type = 2`); on the first leaf split we *promote* the root to an internal page.

**Why**
- Slice D needs durable routing from root → leaf pages, so an on-disk internal node format is required.
- Persisting `root_block_id` in the header provides a stable “entry point” for recovery and later slices (WAL/checkpoint will evolve this to root-swap patterns).
- Keeping the initial tree as a single leaf until the first split keeps the bootstrapping simple.

**Internal page semantics (separator keys)**
- The internal page stores **separator keys** that represent the **minimum key** of each child *after the first*.
- Header stores `first_child` (child pointer for keys smaller than the first separator).
- Each slot stores `(sep_key_i, child_{i+1})` and applies for keys in `[sep_key_i, next_sep_key)` (or to the end for the last separator).
- When a leaf splits into left+right, we insert a new separator key equal to the **first key of the new right leaf**, with `child = right_leaf_block_id`.

**On-page layout**
- Like leaf pages, the internal page uses a slotted layout: slot directory grows forward, records pack from the end.
- Header (`HEADER_SIZE = 16` bytes):
```
byte 0: page_type (u8) = 2 (internal)
byte 1: flags (u8)     = 0 (reserved)
byte 2: slot_count (u16)
byte 4: lower (u16)  = HEADER_SIZE + slot_count * SLOT_SIZE
byte 6: upper (u16)  = start of packed record bytes (grows downward)
byte 8: first_child (u64) - child pointer for keys < key_at(0)
```
- Slot (`SLOT_SIZE = 4` bytes), stored in sorted separator-key order:
```
offset (u16): record start within the page payload
len    (u16): record length in bytes
```
- Record bytes:
```
klen (u16)
vlen (u16) = 8
key bytes (klen)     // separator key
child (u64 LE)       // block id of child_{i+1}
```

**Notes**
- Slice D intentionally does **not** split internal pages; if the root internal page runs out of space for more separators, inserts must fail with a “root full” error until Slice E adds height growth.

## 2025-12-14: Slice E height growth (internal splits) + range scan API

**Decision**
- Implement Slice E B+tree height growth via **recursive insert** that can split both leaf and internal pages, with **root growth** when the current root splits.
- Define internal split promotion using the existing Slice D separator semantics (“separator key is the minimum key of the child to its right”):
  - When splitting an internal node, we choose a **promoted separator** `(k_promote, child_promote)`.
  - The left internal page keeps the original `first_child` and all separator entries strictly **before** `k_promote`.
  - The right internal page uses `first_child = child_promote` and keeps all separator entries strictly **after** `k_promote`.
  - The parent inserts `k_promote` pointing to the **right internal page**.
- Add a public ordered scan API: `BTree::range(start, end)` with `start` **inclusive** and `end` **exclusive**, returning keys in ascending lexicographic byte order.

**Why**
- Recursive splits + root growth are the standard B+tree mechanism that guarantees the tree stays height-balanced: only root growth increases depth, and it increases depth for **all** leaves equally.
- Reusing the Slice D “min-of-right-child” separator invariant avoids redesigning routing semantics and keeps internal-page encoding stable.
- `range(start, end)` with inclusive/exclusive bounds matches common database iterator conventions and is easy to compose (adjacent ranges don’t overlap).

**Notes**
- Slice E range scanning is implemented without leaf sibling pointers (no on-disk format change); the iterator finds the “next leaf” using a parent stack and in-order traversal.
- A future slice may add leaf sibling pointers to make scans O(1) per leaf transition and more cache-friendly.

## 2025-12-14: Export `NONE_BLOCK_ID` sentinel constant

**Decision**
- Expose `NONE_BLOCK_ID` as a public constant so code can avoid using raw `0` when representing “no block / null pointer”.

**Why**
- Eliminates "magic number" checks (`== 0`) around root pointers and free-list links, making intent explicit and reducing the chance of accidentally treating block `0` (the header page) as a data block.

## 2025-12-14: Use `RefCell<BTree>` for primary `_id` lookups (Slice F)

**Decision**
- Keep `WrongoDB::find_one(&self, ...)` as-is and wrap the primary B+tree handle in `RefCell<BTree>` so primary lookups can call `BTree::get(&mut self, ...)` without making read APIs `&mut self`.

**Why**
- `BTree::get` requires `&mut self` today because the pager ultimately uses `std::fs::File::seek`, and `seek` is `&mut File` in Rust (moving the file cursor is a mutation).
- Slice F wants `_id` lookups to hit the primary index while keeping the existing engine API ergonomics (read operations should not require `mut db`).
- This is the smallest change that preserves the current public API while we wire in a persistent primary index.

**Tradeoffs**
- **Pros**
  - No public API break: callers keep using `find_one(&self, ...)`.
  - Minimal code churn: no need to redesign `BlockFile`/`Pager` I/O yet.
- **Cons**
  - Borrowing rules move from compile-time to runtime for the primary tree:
    - `RefCell` will panic on illegal re-entrant borrows (e.g., nested mutable borrows).
    - We should keep `borrow_mut()` scopes tight (borrow, do the `get`, drop).
  - Not thread-safe: if we ever want to share a `WrongoDB` across threads, we'll need a `Mutex`/`RwLock` (or a different I/O model).

**Alternatives considered**
- Change read APIs to `&mut self` (e.g., `find_one(&mut self, ...)`): simplest mechanically, but breaks ergonomics and public API expectations.
- Switch paging reads to positioned I/O (pread-style) or `mmap`: could allow `BTree::get(&self)`, but is a larger redesign and more platform-sensitive.
- Wrap the pager/tree in `Mutex`/`RwLock`: thread-safe, but heavier and unnecessary for the current single-threaded design.

**Mental model**
```
WrongoDB owns the BTree,
but find_one(&self) only has a shared reference to WrongoDB.

We need:  &mut BTree  (because File::seek mutates the cursor)
We have:  &WrongoDB

RefCell lets us do a runtime-checked temporary &mut to the BTree.
```

## 2025-12-14: Mongo-like `_id` defaults and uniqueness (Slice F)

**Decision**
- Default `_id` generation uses an **ObjectId-like** 24-hex string (12 bytes rendered as lower-case hex) instead of a UUID string.
- Enforce `_id` uniqueness:
  - `insert_one` returns an error if the `_id` already exists.
  - `open()` fails if the append-only log contains duplicate `_id` values.
- Primary-key encoding preserves embedded document key order (no key sorting) to better match MongoDB's "field order matters" semantics.

**Why**
- MongoDB uses `_id` as the primary key with a unique index; duplicate inserts should fail.
- MongoDB's default `_id` is `ObjectId`, not UUID.
- MongoDB's comparison and equality semantics treat embedded-document field order as significant; key-sorting would incorrectly merge distinct `_id` values.

**Tradeoffs**
- We store documents as JSON, not BSON, so this is still a best-effort approximation:
  - JSON number types don't preserve BSON numeric types (int32/int64/double/decimal), so our "1 vs 1.0" normalization is heuristic.
  - We represent ObjectId as a hex string (no dedicated ObjectId type in the document model yet).
- Failing `open()` on duplicate `_id` is strict; it treats such logs as corrupted/invalid (matching MongoDB invariants).

## 2025-12-14: Remove `MiniMongo` public alias

**Decision**
- Remove the public `MiniMongo` type alias and expose only `WrongoDB` as the engine entry point.

**Why**
- The crate and engine are named `wrongodb`; keeping a `MiniMongo` alias was confusing and no longer reflects the project naming.
- Reduces API surface area and avoids "two names for the same thing" in examples and docs.

## 2025-12-16: MongoDB wire protocol server with extensible command dispatch

**Decision**
- Implement MongoDB wire protocol (OP_MSG with document sequences, OP_QUERY/OP_REPLY for legacy) with a **Command trait + CommandRegistry** for O(1) dispatch.
- Support multi-collection CRUD: `insert` (auto `_id`), `find` (filter/skip/limit), `update` (`$set`/`$unset`/`$inc`/`$push`/`$pull`), `delete`, plus `count`, `distinct`, and basic aggregation (`$match`/`$limit`/`$skip`/`$project`).
- Command handlers in `src/commands/handlers/{connection,database,crud,index,cursor,aggregation}.rs`.

**Why**
- Enables mongosh and Rust driver connectivity with proper OP_MSG framing and `_id` materialization.
- Trait-based dispatch replaces a monolithic if-else chain, making new commands easy to add.

**Gaps** (future work): query operators (`$gt`/`$lt`/`$in`), sorting, projection, cursor batching, persistent indexes, transactions, auth.

## 2025-12-21: Proposed checkpoint-first durability via copy-on-write (Option B)

**Status**
- Proposed (not yet approved).

**Decision (proposed)**
- Use copy-on-write for page updates and keep a stable on-disk root until checkpoint commit.
- Persist checkpoints via an atomic root swap using dual header slots with generation + CRC.
- Recovery uses the last completed checkpoint; no WAL in this phase.
- Do not persist retired-block lists yet; space leaks on crash are acceptable for now.
- Checkpoints are explicit API calls in this phase (no implicit checkpoint per write).
- For metadata/turtle integration, keep the header root pointing at the primary B+tree for now; when metadata/turtle is added, repoint the header root to the metadata root and store the primary root in the metadata table.

**Why**
- Delivers crash-consistent snapshots without introducing WAL complexity at this stage.

**Notes**
- Requires a file format bump and new rules for reclaiming retired blocks.

## 2025-12-21: Header checkpoint slots (file format v2)

**Decision**
- Bump the blockfile header version to `2`.
- Replace the single `root_block_id` header field with **two checkpoint slots**.
- Each slot encodes `{root_block_id(u64), generation(u64), crc32(u32)}`; the slot CRC covers the little-endian bytes of `root_block_id` + `generation`.
- Initialize slot 0 at generation `1` with `root_block_id = 0`, and slot 1 at generation `0` with `root_block_id = 0`.

**Why**
- Per-slot CRCs + generation counters enable safe root selection and fallback during torn header writes.

## 2025-12-22: Copy-on-write B+tree updates allocate new blocks

**Decision**
- B+tree mutations never overwrite existing pages: every modified leaf/internal page is written to a new block.
- Each insert returns a new subtree root id; the tree’s header root pointer is updated on every write.
- When a node splits, both left and right siblings are new blocks; the parent rewrites its own page to point at the new child ids.
- Old blocks are left in place (no reuse) until checkpoint retirement is implemented.

**Why**
- Preserves the last durable checkpoint’s reachable pages while allowing in-progress mutations to proceed safely.
- Establishes the invariant needed for later “stable vs working root” and checkpoint commits.

## 2025-12-23: Stable root vs working root with checkpoint API

**Decision**
- Introduce two root pointers in `Pager`:
  - **stable root**: root stored in the active checkpoint slot on disk (last completed checkpoint).
  - **working root**: in-memory root used for ongoing mutations (advances with each write).
- `BTree::create()` now writes an initial checkpoint after creating the first leaf root.
- Add `BTree::checkpoint()` API that atomically swaps roots by calling `BlockFile::set_root_block_id()` and syncing.
- Mutations update the working root in memory but do not affect the stable root until `checkpoint()` is called.
- On `BTree::open()`, the working root is initialized from the stable root on disk.

**Why**
- Provides crash-consistent snapshots: the stable root always points to a valid tree state, while mutations proceed without risking corruption of the on-disk checkpoint.
- Atomic root swap ensures that a crash during a checkpoint either sees the old checkpoint or the new checkpoint, never a partially-written state.
- Separates durability boundaries (checkpoint) from write amplification (multiple mutations can be batched before a checkpoint).

## 2025-12-23: Retired block recycling only after checkpoint

**Decision**
- Track retired (replaced) page block IDs in memory during copy-on-write updates.
- Recycle retired blocks only after a successful checkpoint root swap.
- Retired block lists are not persisted yet; crashes may leak space.

**Why**
- Prevents reuse of blocks still reachable from the last durable checkpoint.
- Keeps the initial implementation simple while matching the "checkpoint-first" durability model.

**Ordering**
- Checkpoint root metadata is synced before any retired blocks are added to the free list, so the on-disk stable root never points at reclaimed blocks.

## 2025-12-23: Proposed in-memory page cache for COW coalescing

**Status**
- Proposed (not yet approved).

**Decision (proposed)**
- Add a bounded in-memory page cache between `Pager` and `BlockFile`.
- Dirty pages are written back on eviction and before checkpoint commit.
- The first mutation of a page reachable from the last checkpoint allocates a new block id; subsequent mutations reuse that working block id until the next checkpoint.
- Cache supports pin/unpin so iterators and scans can prevent eviction.

**Why**
- Reduce write amplification and block churn by coalescing repeated updates to the same pages.
- Provide a stable foundation for scan performance by keeping in-use pages resident.

**Notes**
- Eviction policy and cache sizing are finalized during implementation (LRU vs clock).

## 2025-12-23: Page cache entry shape, API, and eviction policy

**Decision**
- Define cache entries as `{ page_id, payload, dirty, pin_count, last_access }`, where `payload` is the page payload buffer and `last_access` is a monotonic counter for LRU.
- Pager cache API uses explicit pin/unpin calls:
  - `pin_page(page_id) -> PinnedPage` (read intent)
  - `pin_page_mut(page_id) -> PinnedPageMut` (write intent; marks dirty)
  - `unpin_page(PinnedPage)` and `unpin_page_mut(PinnedPageMut)` to release pins and (for mutable pins) write back the updated payload into the cache entry.
  - `flush_cache()` writes all dirty cached pages to disk and marks them clean.
- Eviction policy is LRU among unpinned pages; if all pages are pinned at capacity, cache admission returns an error.
- Cache sizing is fixed in pages with a default of 256; configuration is via `PageCacheConfig { capacity_pages, eviction_policy }`, with `Pager::create/open` using defaults and `Pager::create_with_cache/open_with_cache` enabling override.

**Why**
- Keeps cache bookkeeping straightforward and consistent with the single-threaded engine while meeting the spec’s pin/dirty/eviction requirements.
- LRU with a simple access counter is easy to implement and adequate for the expected cache sizes in this phase.

## 2025-12-23: Flush skips dirty pinned pages

**Decision**
- `Pager::flush_cache()` returns an error if it encounters a dirty cache entry with `pin_count > 0`.
- Dirty pinned pages are not flushed or evicted; callers must unpin them before checkpoint or explicit flush.

**Why**
- The mutable pinned handle owns the latest payload until `unpin_page_mut` writes it back to the cache entry.
- Flushing while a dirty page is pinned risks writing stale data and losing in-memory updates.

**Notes**
- Read-only pins remain flushable (they are not marked dirty).
- Eviction already skips pinned pages; flush uses the same rule for dirty pages.

## 2026-01-17: Pinning semantics for page cache (Slice G1)

**Decision**
- Define pin lifetime invariants for the page cache:
  - **Pinned pages cannot be evicted**: Pages with `pin_count > 0` are never selected for eviction by the LRU policy.
  - **Dirty pinned pages cannot be flushed**: `flush_cache()` returns an error if it encounters a dirty page with `pin_count > 0`.
  - **Pin/unpin must be balanced**: Every `pin_page()` or `pin_page_mut()` must have a corresponding `unpin_page()` or `unpin_page_mut_*()` call.
- Pin lifetimes:
  - **Point operations** (get/put): Pin is held only for the duration of the single page access. Unpin immediately after read/modify.
  - **Range scans/iterators**: Pin current leaf page during iteration, unpin when advancing to next leaf. Parent pages are unpinned after routing to child (safe to re-read with COW).
  - **Tree modifications**: `pin_page_mut()` holds pin until `unpin_page_mut_commit()` or `unpin_page_mut_abort()` is called.

**Why**
- Prevents use-after-free bugs where pages are evicted while still being accessed.
- Ensures flush/writeback doesn't race with in-memory modifications.
- Provides clear rules for iterator safety and concurrent access patterns.

**Iterator safety (current leaf-only pinning)**
- `BTreeRangeIter` pins only the current leaf page during iteration.
- Parent pages are unpinned after navigating down to the leaf (they may be re-read safely due to COW semantics).
- This is safe because:
  - Internal pages are immutable from the stable root perspective.
  - If a parent is evicted and re-read, the same stable page content is retrieved.
  - The iterator only needs stable access to the current leaf; mutations create new working pages that don't affect the in-memory pinned leaf.

**Tradeoffs**
- **Leaf-only pinning** minimizes pin count but may re-read parent pages during iteration if they're evicted.
- **Full-path pinning** would keep the entire path from root to leaf pinned, preventing any re-reads but consuming more cache capacity.
- Current implementation uses leaf-only pinning for simplicity; full-path pinning can be added later if needed for performance.
