# Analysis: Table vs BTree Responsibility Split

## Executive Summary

After analyzing WiredTiger's architecture, our current `Table` is too thin and doesn't add sufficient value over `BTree`. WiredTiger uses a well-defined layered architecture where each component has clear responsibilities. This document analyzes the differences and proposes a refactoring to align our design with WiredTiger's proven approach.

## Current Architecture (WrongoDB)

```
Cursor (src/cursor.rs)
  └── Arc<RwLock<Table>>
        └── BTree (src/storage/btree/mod.rs)
              ├── Pager (page cache)
              ├── Page structs (leaf, internal)
              ├── WAL (logging)
              ├── MVCC (multi-version concurrency control)
              └── Statistics (minimal)
```

### Current Responsibilities

**Table (src/storage/table.rs - 153 lines)**
- Wraps BTree
- Adds duplicate key checking for insert (DocumentValidationError)
- Delegates to BTree methods
- Holds MVCC operations (insert_mvcc, update_mvcc, delete_mvcc, begin_txn, etc.)
- No metadata storage

**BTree (src/storage/btree/mod.rs - 804 lines)**
- BTree algorithms (insert, delete, search, split, etc.)
- Page cache via Pager
- WAL logging
- MVCC state
- Checkpointing
- Recovery
- Some statistics

**Cursor (src/cursor.rs - 77 lines)**
- Holds Arc<RwLock<Table>>
- Tracks current_key (simple Option<Vec<u8>>)
- Provides insert/update/delete/get/next API
- scan() implementation is inefficient (re-scans entire tree on each call)

## WiredTiger Architecture

```
WT_CURSOR_BTREE (cursor.h:116-287)
  └── WT_DATA_HANDLE (dhandle.h:115-204)
        ├── void *handle (points to WT_BTREE for btree type)
        ├── Metadata: name, URI, checkpoint info
        ├── Reference counting
        └── Locks
              └── WT_BTREE (btree.h:113-318)
                    ├── Page size, compression, checksum config
                    ├── Statistics (bytes in memory, dirty bytes, etc.)
                    ├── Root page reference (WT_REF)
                    ├── Block manager reference
                    ├── Checkpoint state
                    └── Eviction state
                          ├── WT_PAGE (in-memory page)
                          ├── WT_REF (page reference)
                          └── Update chains
```

### WiredTiger's Layered Responsibilities

**WT_DATA_HANDLE**
- Generic handle for data sources (btree, table, tiered, layered)
- Table metadata: name, URI, checkpoint name/order
- Reference counting and lifecycle management
- Locks for exclusive/shared access
- Statistics slots

**WT_BTREE**
- BTree-specific configuration:
  - Page sizes (maxintlpage, maxleafpage)
  - Key/value formats
  - Compression, checksum settings
- Tree state:
  - Root page reference (WT_REF root)
  - Maximum tree depth
  - Last record number (for column-store)
- Statistics:
  - Bytes in memory, dirty bytes, internal bytes
  - Updates bytes, delta updates bytes
  - Checkpoint generation
- Checkpoint and eviction state
- Block manager reference

**WT_CURSOR_BTREE**
- Position state:
  - WT_REF *ref (current page)
  - uint32_t slot (position within page)
- Search state for inserts:
  - WT_INSERT *ins (current insert node)
  - WT_INSERT **ins_stack (search stack for insert path)
- Comparison result from last search
- Temporary buffers for building keys/values
- Does NOT hold direct WT_BTREE reference (uses dhandle)

**WT_PAGE / WT_REF**
- WT_PAGE: In-memory page structure with disk image
- WT_REF: Reference to page with state machine (DISK, LOCKED, MEM, SPLIT)
- Page modification state (WT_PAGE_MODIFY)

## Key Differences

### 1. Metadata vs Algorithm Separation

| Aspect | WrongoDB | WiredTiger |
|--------|----------|------------|
| Where is table metadata stored? | Nowhere (in URI/path only) | WT_DATA_HANDLE |
| Where is BTree config stored? | Part of BTree | WT_BTREE |
| Where is table name/URI stored? | In higher-level code | WT_DATA_HANDLE |
| Separation of concerns? | Poor | Clear |

### 2. Cursor State Management

| Aspect | WrongoDB | WiredTiger |
|--------|----------|------------|
| Position tracking | Option<Vec<u8>> current_key | WT_REF *ref + uint32_t slot |
| Insert state | N/A | WT_INSERT *ins + insert stack |
| Comparison result | N/A | int compare (from last search) |
| Temporary buffers | N/A | WT_ITEM buffers for key/value building |

### 3. Page Reference Abstraction

| Aspect | WrongoDB | WiredTiger |
|--------|----------|------------|
| Page reference | Direct page_id in structures | WT_REF abstraction with state machine |
| State tracking | Implicit in pager | Explicit WT_REF_STATE (DISK, LOCKED, MEM, SPLIT) |
| Pinning support | In pager (pin_page/pin_page_mut) | Implicit in WT_REF state + hazard pointers |

### 4. Statistics and Monitoring

| Aspect | WrongoDB | WiredTiger |
|--------|----------|------------|
| Where are statistics stored? | Minimal (in BTree/Pager) | WT_BTREE + WT_DATA_HANDLE |
| What is tracked? | Cache size, page count | Bytes in memory, dirty bytes, updates, eviction state |
| Per-table statistics? | No | Yes (via WT_DATA_HANDLE) |

## Problems with Current Design

### 1. Table is Too Thin (150 lines, adds minimal value)

Current Table responsibilities:
- Wraps BTree (just holds btree: BTree)
- Adds duplicate key checking via DocumentValidationError
- Provides MVCC methods that should be at BTree or separate layer

Problems:
- Doesn't store any table metadata
- Doesn't track table-specific statistics
- MVCC methods should be at BTree level (MVCC is BTree feature, not table-level)
- Why have Table at all if it's just a wrapper?

### 2. BTree Does Too Much (800+ lines, multiple concerns)

Current BTree responsibilities:
- BTree algorithms (insert, delete, search, split, etc.)
- Page cache via Pager
- WAL logging
- MVCC state
- Checkpointing
- Recovery
- Statistics (minimal)

Problems:
- Violates Single Responsibility Principle
- Hard to test components in isolation
- WAL and MVCC could be separate layers
- No clear separation between storage algorithms and higher-level features

### 3. Cursor Has Insufficient State

Current Cursor state:
```rust
pub struct Cursor {
    table: Arc<RwLock<Table>>,
    current_key: Option<Vec<u8>>,  // Too simple
}
```

WiredTiger cursor state:
```c
struct __wt_cursor_btree {
    WT_CURSOR iface;
    WT_DATA_HANDLE *dhandle;

    // Position state
    WT_REF *ref;      // Current page reference
    uint32_t slot;     // Position within page

    // Insert state
    WT_INSERT_HEAD *ins_head;  // Insert chain head
    WT_INSERT *ins;            // Current insert node
    WT_INSERT **ins_stack[WT_SKIP_MAXDEPTH];  // Search stack

    // Search state
    int compare;  // Comparison result from last search

    // Temporary buffers
    WT_ITEM *tmp;  // For building keys/values
    // ... more fields
};
```

Problems:
- `current_key` is insufficient for efficient iteration
- No search stack for inserts
- No position within page tracking
- No temporary buffers for key/value construction
- `next()` re-scans entire tree (O(n) per call instead of O(log n) or O(1))

### 4. MVCC is at Wrong Layer

Current MVCC location:
- Table has: `begin_txn`, `commit_txn`, `abort_txn`, `insert_mvcc`, `update_mvcc`, `delete_mvcc`
- BTree has: `MvccState mvcc`

Problem:
- MVCC is a BTree feature (multi-version concurrency on BTree pages)
- Should be part of BTree, not Table
- Table should be unaware of MVCC implementation details

### 5. No Proper Abstraction for Page References

Current approach:
- Direct page_id used throughout
- No WT_REF-style abstraction
- State tracking implicit in Pager

WiredTiger approach:
- WT_REF abstraction with explicit state machine
- State can be: DISK, LOCKED, MEM, SPLIT
- Clear lifecycle management

## Proposed Refactoring

### Goal

Align with WiredTiger's layered architecture:
- Clear separation of concerns
- Each component has a single responsibility
- Proper state management in cursors
- Metadata stored at appropriate layer

### Phase 1: Clarify Table vs BTree Responsibilities

**Table should own:**
- Table metadata (name, URI, checkpoint info)
- Table-level statistics
- Table lifecycle (open/close)
- Reference counting (if needed for multiple cursors)

**BTree should own:**
- BTree algorithms (insert, delete, search, split)
- Page cache (currently via Pager)
- MVCC (move from Table)
- WAL (keep in BTree)
- Checkpointing (keep in BTree)
- BTree-level statistics

**Cursor should own:**
- Position state (WT_REF-style + slot)
- Insert search stack (WT_INSERT + ins_stack)
- Temporary buffers for key/value building
- Comparison result from last search

### Phase 2: Introduce Table Metadata

```rust
#[derive(Debug)]
pub struct Table {
    name: String,
    uri: String,
    checkpoint_name: Option<String>,
    btree: BTree,
    statistics: TableStatistics,
}

#[derive(Debug, Default)]
pub struct TableStatistics {
    pub bytes_in_memory: u64,
    pub bytes_dirty: u64,
    pub operations_count: u64,
    pub open_since: std::time::Instant,
}
```

### Phase 3: Move MVCC to BTree

```rust
// Remove from Table:
// - begin_txn
// - commit_txn
// - abort_txn
// - insert_mvcc
// - update_mvcc
// - delete_mvcc

// These are already in BTree (via MvccState)
// Table should not expose MVCC methods
```

### Phase 4: Improve Cursor State Management

```rust
pub struct Cursor {
    table: Arc<RwLock<Table>>,

    // Position state
    current_page_id: Option<u64>,  // WT_REF equivalent
    current_slot: Option<u32>,     // Position within page

    // Search state for inserts
    search_stack: Vec<(u64, u32)>,  // (page_id, slot) path

    // Comparison result from last search
    last_compare: Option<std::cmp::Ordering>,

    // Temporary buffers
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
}
```

### Phase 5: Fix scan() / next() Efficiency

Current problem:
```rust
pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)> {
    let mut table = self.table.write();
    let start_key = self.current_key.as_deref();
    let entries = table.scan(&NonTransactional)?;  // RE-SCANS ENTIRE TREE!

    let mut iter = entries.into_iter();
    if let Some(current_key) = start_key {
        while let Some((k, _)) = iter.next() {
            if k == current_key {
                break;
            }
        }
    }
    // ...
}
```

Proposed solution:
```rust
pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)> {
    let mut table = self.table.write();

    // Use position state instead of key
    match (&self.current_page_id, &self.current_slot) {
        (Some(page_id), Some(slot)) => {
            // Advance from current position
            let result = table.advance_from_position(page_id, *slot, &mut self.current_page_id, &mut self.current_slot)?;
            Ok(result)
        }
        _ => {
            // First iteration - start from beginning
            let result = table.first_position(&mut self.current_page_id, &mut self.current_slot)?;
            Ok(result)
        }
    }
}
```

Table/BTree would need:
- `first_position()` - Returns first entry and sets position
- `advance_from_position()` - Advances to next entry from given position

This would use BTreeRangeIter internally, making iteration O(1) or O(log n) instead of O(n).

## Implementation Plan

### Step 1: Add Table Metadata (non-breaking)
- Add `name`, `uri`, `checkpoint_name` fields to Table
- Add `TableStatistics` struct
- Update constructors to accept name/URI

### Step 2: Move MVCC Methods from Table
- Remove `begin_txn`, `commit_txn`, `abort_txn` from Table
- These are already in BTree (via MvccState)
- Update all callers to use BTree directly or via Collection

### Step 3: Improve Cursor State
- Replace `current_key: Option<Vec<u8>>` with proper position state
- Add search stack for inserts
- Add temporary buffers
- Update all Cursor methods to use new state

### Step 4: Fix scan() / next() Efficiency
- Add `first_position()` and `advance_from_position()` to Table
- Update `next()` to use position instead of re-scanning
- This is the biggest performance improvement

### Step 5: Extract Statistics
- Move statistics from BTree to Table
- Add proper tracking in Table
- Expose via Table API

### Step 6: Introduce Page Reference Abstraction (Optional, Future)
- Consider adding WT_REF-style abstraction
- Currently, direct page_id works but is less explicit
- This can be deferred if not immediately needed

## Benefits

### Improved Code Organization
- Clear separation of concerns
- Each component has a single responsibility
- Easier to understand and maintain

### Better Performance
- Efficient cursor iteration (O(1) instead of O(n) per call)
- Proper search stack reduces re-traversals
- Better memory locality with position-based iteration

### Easier Testing
- Can test Table metadata in isolation
- Can test BTree algorithms without Table
- Can test Cursor state management separately

### Alignment with Reference Implementation
- Follows WiredTiger's proven architecture
- Easier to compare implementations
- Benefits from WiredTiger's design decisions

## Trade-offs

### Complexity vs Simplicity
- More structs and layers = more initial complexity
- But better separation = easier long-term maintenance
- Trade-off: Worth it for a storage engine

### Breaking Changes
- Moving MVCC methods from Table to BTree is breaking
- Improving Cursor state is breaking
- Strategy: Do this in one large refactoring commit

### Implementation Effort
- Estimated effort: 2-3 days of focused work
- Breaking changes need to update all callers
- Testing effort: Need comprehensive tests for new Cursor behavior

## References

- WiredTiger BTree: ~/workspace/wiredtiger/src/include/btree.h
- WiredTiger Data Handle: ~/workspace/wiredtiger/src/include/dhandle.h
- WiredTiger Cursor: ~/workspace/wiredtiger/src/include/cursor.h
- WiredTiger Page Memory: ~/workspace/wiredtiger/src/include/btmem.h
- Our Table: src/storage/table.rs
- Our BTree: src/storage/btree/mod.rs
- Our Cursor: src/cursor.rs
