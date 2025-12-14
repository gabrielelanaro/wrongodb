# Decisions

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

**Current on-disk layout (Slice A/B)**
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
                       | root_block_id(u64)            |
                       | free_list_head(u64)           |
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
