# Change: B-Tree Storage Architecture (Replacing Append-Only Log)

## Why

The current `AppendOnlyStorage` accumulates garbage on every update (new JSON line appended, old version abandoned) with no space reclamation. Secondary indexes store file offsets, coupling index format to storage layout and preventing defragmentation. This limits the database to toy workloads where disk space is infinite.

## What Changes

- Replace `AppendOnlyStorage` with a main table B-tree mapping `_id` → document
- Change secondary indexes to store `_id` references instead of file offsets
- Add extent-based free space management to `BlockManager` using skiplists (alloc/avail/discard lists)
- Enable true updates/deletes with COW semantics and space reclamation after checkpoint
- **BREAKING**: Storage format changes; no migration from old append-only format (POC/learning stage)

## Impact

- Affected specs: new `btree-storage` capability, changes to `indexing` spec (offset → _id)
- Affected code: `src/storage.rs` (removed), `src/engine.rs`, `src/index.rs`, `src/index_key.rs`, `src/blockfile.rs`
- New files: `src/main_table.rs`, extent management in block manager
