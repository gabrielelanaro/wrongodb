# WAL Checkpoint LSN Bug Analysis

## Date
2025-01-27

## Problem Statement

After implementing the COW bypass fix and adding `set_checkpoint_lsn()` call, 5 recovery tests are still failing with "0 operations replayed".

## Root Cause Analysis

### The Bug: Checkpoint LSN Points to Checkpoint Record, Not After It

Current incorrect behavior:
```
1. Insert 0-4, write WAL records (LSN 512, 600, 650, ...)
2. Checkpoint - write checkpoint RECORD at LSN 802
3. set_checkpoint_lsn(802) ← Points TO checkpoint record
4. Insert 5-9, write WAL records (LSN 850, 900, ...)
5. Crash
6. Recovery: start reading from LSN 802
7. Problem: Records 5-9 are at LSN 850+, but we read from 802!
```

**Recovery reads from checkpoint LSN, but it should start AFTER the checkpoint record.**

### Correct Behavior

```
1. Insert 0-4, write WAL records (LSN 512, 600, 650, ...)
2. Checkpoint - write checkpoint RECORD at LSN 802
3. set_checkpoint_lsn(802 + sizeof(checkpoint_record)) ← Should point AFTER checkpoint
4. Insert 5-9, write WAL records (LSN 850, 900, ...)
5. Crash
6. Recovery: start reading from LSN 802 + size
7. Correct: Records 5-9 are read and replayed ✓
```

## The Fix

### In `WalFile::log_checkpoint()`

Currently returns the LSN of the checkpoint record. Should return the LSN **after** the checkpoint record:

```rust
pub fn log_checkpoint(&mut self, root_block_id: u64, generation: u64) -> Result<Lsn, WrongoDBError> {
    let record = WalRecord::Checkpoint {
        root_block_id,
        generation,
    };
    let record_lsn = self.append_record(record)?;

    // Checkpoint LSN should point to AFTER the checkpoint record
    // so recovery knows to start reading from there
    let next_lsn = Lsn::new(
        self.last_lsn.file_id,
        self.last_lsn.offset  // This is already advanced by append_record
    );

    Ok(next_lsn)  // Return the LSN AFTER checkpoint, not OF checkpoint
}
```

### In `BTree::checkpoint()`

Currently:
```rust
let checkpoint_lsn = wal.log_checkpoint(root, generation)?;
wal.set_checkpoint_lsn(checkpoint_lsn)?;  // Points to checkpoint record
```

Should be:
```rust
let checkpoint_lsn = wal.log_checkpoint(root, generation)?;
wal.set_checkpoint_lsn(checkpoint_lsn)?;  // Points to after checkpoint record
```

## Implementation Steps

1. Modify `WalFile::log_checkpoint()` to return the LSN after the checkpoint record
2. Update recovery to start reading from `checkpoint_lsn` (which now points after checkpoint)
3. All recovery tests should pass

## Why This Happened

The confusion was between:
- **Checkpoint record LSN**: Where the checkpoint record is written
- **Checkpoint LSN (in header)**: Where recovery should START reading

These are different! The checkpoint LSN in the header should point to the **first LSN after the checkpoint**, not the checkpoint record itself.

## Impact

This bug affects any scenario where:
1. A checkpoint is performed
2. More WAL operations occur after the checkpoint
3. Recovery happens

The data after the checkpoint is lost because recovery starts from the wrong position.

## Test Cases Affected

- `recovery_after_checkpoint`: ❌ Checkpoint then more inserts
- `recovery_idempotent`: ❌ Multiple recovery cycles
- `recovery_large_keys_and_values`: ❌ Checkpoint then more inserts
- `recovery_with_multiple_splits`: ❌ Checkpoint then more inserts

## Test Cases That Pass

Tests that DON'T involve checkpoints pass because:
- They start recovery from LSN 0 (beginning of WAL)
- All records are replayed in order
- No checkpoint LSN to worry about

## Status

**FIXED (2026-01-27)** - `WalFile::log_checkpoint()` now returns the LSN *after* the checkpoint record, and recovery
starts from that LSN.

## Resolution

- `WalFile::log_checkpoint()` returns `self.last_lsn` (post-append) so the header points past the checkpoint record.
- `BTree::checkpoint()` persists that LSN into the WAL header, so recovery skips the checkpoint record itself.

## Related Files

- `src/btree/wal.rs`: Need to fix `log_checkpoint()` return value
- `src/btree.rs`: Need to use correct checkpoint LSN
- `tests/btree_recovery.rs`: Tests now properly sync WAL before crash

## Next Steps

None. The issue is resolved by logical WAL replay and the corrected checkpoint LSN handling.
