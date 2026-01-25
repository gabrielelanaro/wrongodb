# Change: Add persistent secondary index storage and basic usage

## Why
Secondary indexes are currently in-memory only, so every restart rebuilds them and queries fall back to collection scans when the index is absent. Persisting index storage enables Mongo-like durability and faster startup while keeping usage semantics predictable.

## What Changes
- Persist secondary indexes on disk using the existing B+tree storage engine
- Load secondary indexes on open instead of rebuilding purely from the data log
- Maintain secondary indexes on insert/update/delete for basic equality queries
- Define minimal index metadata and files per collection
- Add a minimal “index file” checklist (layout, key encoding, metadata, maintenance, recovery, query use)

## Impact
- Affected specs: indexing (new)
- Affected code: src/index.rs, src/engine.rs, src/btree.rs, src/storage/*, src/commands/handlers/index.rs
