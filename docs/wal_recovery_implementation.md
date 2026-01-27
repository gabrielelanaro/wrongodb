# WAL Recovery Implementation Summary

## Overview

Minimongo now uses **logical WAL recovery** (WiredTiger-style). The WAL records logical B+tree operations
(put/delete/checkpoint), and recovery replays those operations through the normal `BTree` API with WAL
logging disabled. This eliminates page-id drift and split logging complexity.

## Completed Components

### 1. WalReader (`src/btree/wal.rs`)

Sequential WAL reader with validation:
- Opens WAL file and validates header (magic number, CRC32, version)
- Reads records from checkpoint LSN to end of file
- Validates CRC32 checksums for each record
- Validates LSN chain (detects gaps in sequence)
- Handles partial records at EOF gracefully
- Returns `RecoveryError` with detailed error information

Key features:
- `open()`: Open WAL file for reading
- `read_record()`: Read next WAL record
- `checkpoint_lsn()`: Get checkpoint LSN from header
- `is_eof()`: Check if end of file reached

### 2. Logical WAL Records (`src/btree/wal.rs`)

WAL now logs logical operations:
- **Put**: Insert/update a key/value pair
- **Delete**: Remove a key (optional, idempotent)
- **Checkpoint**: Consistency marker with root id + generation

### 3. Recovery Integration (`src/btree.rs`)

Recovery is integrated into `BTree::open()`:
- Opens WAL reader if WAL is enabled
- Temporarily disables WAL logging during replay
- Replays logical records via `BTree::put` / `BTree::delete`
- Logs recovery stats and continues on errors (corrupted WAL does not prevent open)

### 4. WAL Logging (`src/btree.rs` + `src/btree/wal.rs`)

WAL logging now happens at the **logical operation** level:
- `BTree::put` logs `WalRecord::Put`
- `BTree::delete` logs `WalRecord::Delete`
- Checkpoint logs `WalRecord::Checkpoint`

No split records or page-id tracking are needed.

## Design Decisions

### Logical Replay from Checkpoint LSN
- Recovery reads records sequentially starting at the checkpoint LSN
- Only post-checkpoint records are applied

### WAL Disabled During Replay
- Recovery temporarily removes the WAL handle to avoid re-logging
- Replay uses normal B+tree code paths (splits/root updates included)

### Graceful Degradation
- Recovery errors do not prevent database open
- Corrupted WAL results in warnings and partial recovery

## Known Issues / Limitations

- **No automatic checkpoint after recovery**: recovered data remains in the working tree until the user checkpoints.
- **Delete is non-balancing**: deletes do not merge/rebalance pages (acceptable for now; performance may degrade).

## Files Modified

- `src/btree/wal.rs`: logical WAL record types, version bump, reader validation
- `src/btree.rs`: logical logging + replay during open
- `src/btree/pager.rs`: WAL handle suspension helpers
- `tests/btree_recovery.rs`: recovery tests updated for logical WAL
- `docs/wal_recovery_comparison.md`: updated to logical WAL alignment

## Verification

Run recovery tests:
```bash
cargo test --test btree_recovery
```

Run WAL tests:
```bash
cargo test --test btree_wal
```

## Conclusion

Recovery now follows WiredTiger’s logical replay strategy. This removes the COW page-id drift problem,
eliminates split logging dependencies, and simplifies the recovery pipeline while preserving idempotence.
