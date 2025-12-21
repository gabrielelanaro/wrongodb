# Change: Add checkpoint-first durability via copy-on-write

## Why
The engine currently overwrites pages in place and persists the root pointer immediately, so a crash during writes can corrupt the reachable tree. Option B is to make checkpoints the durability boundary and deliver crash-consistent snapshots without WAL.

## What Changes
- Introduce copy-on-write page updates for B+tree mutations; do not overwrite blocks reachable from the last durable checkpoint.
- Add checkpoint metadata with dual root slots (generation + CRC) and an atomic root swap.
- Add a checkpoint API that persists the new root and (optionally) releases retired blocks.
- Extend open/recovery to select the latest valid root slot.
- Defer WAL; recovery is based solely on the last completed checkpoint (updates since the last checkpoint may be lost).

## Impact
- Affected specs: `checkpoint-create` (new)
- Affected code: `src/blockfile.rs`, `src/btree/`, `src/btree/pager.rs`, free-list handling
