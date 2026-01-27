# Change: Add persistent secondary index storage and basic usage

## Status: Completed

## Completion Date: 2026-01-27

## Summary

This change has been successfully implemented. The system now persists secondary indexes on disk using B+trees, loads them on database open, and maintains them during insert/update/delete operations.

### What Was Implemented

1. **Persistent B+tree Storage**: Secondary indexes are now stored in separate files per collection/field using the existing B+tree storage engine with copy-on-write and checkpoint semantics.

2. **Composite Key Encoding**: Implemented `index_key` module that encodes keys as (type tag + scalar value + record offset) to support duplicate values without posting lists. Includes proper f64 encoding for natural numeric sort order.

3. **Index Management**: Added `PersistentIndex` and `SecondaryIndexManager` structures to manage BTree indexes with file naming scheme: `{collection}.{field}.idx.wt`.

4. **Auto-Build on Open**: Indexes are automatically built when opening a database with new fields on existing data.

5. **Query Integration**: Updated `find`/`count`/`distinct` APIs to use secondary indexes for equality lookups.

### Files Added/Modified

- `src/index_key.rs` - New module for composite key encoding
- `src/index.rs` - Added PersistentIndex and SecondaryIndexManager
- `src/engine.rs` - Integrated persistent indexes into database operations
- `src/commands/handlers/index.rs` - Updated index creation
- `tests/persistent_secondary_index.rs` - Integration tests for persistence

### Test Coverage

- Create index on existing data and reopen to confirm persistence
- Verify equality lookups use index and return correct results
- Verify update/delete maintain index entries
- Verify index persistence across database restarts

## Why
Secondary indexes are currently in-memory only, so every restart rebuilds them and queries fall back to collection scans when the index is absent. Persisting index storage enables Mongo-like durability and faster startup while keeping usage semantics predictable.

## What Changes
- Persist secondary indexes on disk using the existing B+tree storage engine
- Load secondary indexes on open instead of rebuilding purely from the data log
- Maintain secondary indexes on insert/update/delete for basic equality queries
- Define minimal index metadata and files per collection
- Add a minimal "index file" checklist (layout, key encoding, metadata, maintenance, recovery, query use)

## Impact
- Affected specs: indexing (new)
- Affected code: src/index.rs, src/engine.rs, src/btree.rs, src/storage/*, src/commands/handlers/index.rs
