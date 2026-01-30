# Change: Refactor WAL to logical recovery (WiredTiger-style)

## Why
The current WAL logs physical page operations (leaf/internal inserts and splits) and recovery applies page-level edits.
This creates COW page-id drift, split logging gaps, and replay ordering failures. WiredTiger avoids these issues by
logging logical key/value operations and replaying them through the normal B-tree API.

## What Changes
- Replace page-level WAL records with logical operations (put/delete).
- Replay WAL using BTree writes (no page-id mapping, no in-place page edits).
- Remove split record logging and recovery page-allocation mapping.
- Define WAL versioning and compatibility behavior for the new format.
- Update recovery tests to validate logical replay and idempotence.

## Impact
- Affected specs: checkpoint-create (context), new wal-recovery spec.
- Affected code: src/btree.rs, src/btree/wal.rs, src/btree/recovery.rs, src/btree/pager.rs, tests/btree_recovery.rs,
  docs/wal_recovery_implementation.md.
