## 1. Extent-Based BlockManager
- [x] 1.1 Implement `Extent` struct with offset, size, generation
- [x] 1.2 Implement skiplist for by-offset and by-size searches
- [x] 1.3 Add extent lists to `BlockManager`: alloc, avail, discard
- [x] 1.4 Implement `allocate_extent()` with best-fit strategy
- [x] 1.5 Implement `free_extent()` (moves to discard list)
- [x] 1.6 Implement extent coalescing on checkpoint
- [x] 1.7 Write unit tests for extent allocation/freeing

## 2. Main Table B-Tree
- [x] 2.1 Create `MainTable` struct wrapping `BTree`
- [x] 2.2 Implement BSON key encoding for `_id` values
- [x] 2.3 Implement document serialization to B-tree values
- [x] 2.4 Implement `insert()`, `get()`, `update()`, `delete()` operations
- [x] 2.5 Integrate with extent-based `BlockManager`
- [x] 2.6 Write integration tests for document CRUD

## 3. Index Format Migration
- [x] 3.1 Change index key encoding from `(field, offset)` to `(field, _id)`
- [x] 3.2 Update `SecondaryIndexManager::add()` to use `_id`
- [x] 3.3 Update `SecondaryIndexManager::lookup()` to return `_id` values
- [x] 3.4 Update query path to do index → main_table lookup
- [x] 3.5 Write tests for index consistency after updates

## 4. Remove AppendOnlyStorage
- [x] 4.1 Remove `AppendOnlyStorage` from `Collection` struct
- [x] 4.2 Remove `doc_by_offset` HashMap
- [x] 4.3 Update `load_existing()` to use B-tree scan
- [x] 4.4 Delete `src/storage.rs`
- [x] 4.5 Update `src/lib.rs` to remove storage module

## 5. Testing
- [x] 5.1 Write stress tests for fragmentation/reclamation
- [x] 5.2 Benchmark vs old implementation
