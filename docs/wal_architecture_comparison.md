# WiredTiger vs WrongoDB: WAL Architecture Comparison

## Overview

This document compares the fundamental architectural differences in how Write-Ahead Logging (WAL) is organized in WiredTiger versus WrongoDB.

## High-Level Architecture

### WiredTiger: Connection-Level (Global) WAL

```
WT_CONNECTION_IMPL
├── WT_LOG_MANAGER log_mgr              ← Single global log manager
│   ├── WTI_LOG log                     → Log state (LSNs, file handles)
│   ├── WTI_LOGSLOT slot_pool[128]     → Write consolidation slots
│   ├── WT_FH log_fh                   → Single log file handle
│   └── WT_FH log_dir_fh              → Log directory handle
│
├── WT_TXN_GLOBAL txn_global            ← Global transaction state
├── WT_DATA_HANDLE *dhhash             → Data handles (tables/collections)
└── ...
```

**Key characteristics:**
- **Single WAL system per database**: One log manager for the entire connection
- **Log rotation**: Automatic rotation of log files (`WiredTigerLog.000001`, `WiredTigerLog.000002`, etc.)
- **Consolidation system**: 128-slot pool for batched writes from multiple threads
- **Shared across all tables**: All B-trees write to the same global WAL
- **Complex slot management**: Slot states (FREE, OPEN, CLOSED, WRITTEN) with thread coordination

### WrongoDB: B-Tree-Level (Per-Table) WAL

```
WrongoDB (Connection-level)
├── HashMap<String, Collection>        → Collections
│   └── Collection
│       └── Table
│           └── BTree
│               └── Option<Wal>       ← WAL per B-tree
│                   ├── WalFile       → .wal file (e.g., test.db.main.wt.wal)
│                   └── buffer        → Write buffer
│
└── GlobalTxnState                    ← Global transaction state (no log manager)
```

**Key characteristics:**
- **Per-table WAL files**: Each B-tree has its own WAL (e.g., `table.wt.wal`)
- **No log rotation**: Single WAL file per table, truncated after checkpoint
- **Simple write path**: Direct writes to table-specific WAL
- **No consolidation**: No slot-based batching system
- **No global log manager**: WAL management is decentralized

## Detailed Comparison

### 1. WAL Location and Scope

| Aspect | WiredTiger | WrongoDB |
|--------|-----------|----------|
| **Scope** | Connection-wide (all tables share one log) | Per-B-tree (each table has its own log) |
| **Owner** | `WT_CONNECTION_IMPL` → `WT_LOG_MANAGER` | `BTree` → `Option<Wal>` |
| **File structure** | `WiredTigerLog.000001`, `WiredTigerLog.000002`, ... (rotation) | `collection_name.main.wt.wal`, `collection_name.name.idx.wt.wal` (no rotation) |
| **File count** | N log files (N = rotation count) | 1 per table/collection × M tables/collections |

**Code references:**

WiredTiger (`src/include/connection.h:838`):
```c
struct __wt_connection_impl {
    ...
    WT_LOG_MANAGER log_mgr;  // Single global log manager
    ...
};
```

WrongoDB (`src/storage/btree/mod.rs:50-54`):
```rust
pub struct BTree {
    pager: Box<dyn BTreeStore>,
    wal: Option<Wal>,  // WAL per B-tree
    mvcc: MvccState,
}
```

### 2. Write Path and Batching

**WiredTiger: Slot-based consolidation**

```c
// From log_private.h
struct __wti_logslot {
    wt_shared volatile int64_t slot_state;  // FREE, OPEN, CLOSED, WRITTEN
    wt_shared int64_t slot_unbuffered;
    wt_shared int slot_error;
    wt_shared wt_off_t slot_start_offset;
    WT_FH *slot_fh;
    WT_ITEM slot_buf;  // Consolidation buffer (256KB)
    ...
};

#define WTI_SLOT_POOL 128  // 128 consolidation slots
```

**Write flow:**
1. Thread joins an open slot (`__wti_log_slot_join`)
2. Copies record into slot buffer (or writes directly if large)
3. When slot is full or closed (`WTI_LOG_SLOT_CLOSE`), buffer is written
4. Worker thread processes written slots (`__wti_log_force_write`)
5. Sync is coordinated across slots

**WrongoDB: Direct writes with simple batching**

```rust
// From src/storage/btree/wal.rs
pub struct Wal {
    file: WalFile,
    buffer: Vec<u8>,  // Simple buffer (64KB)
    buffer_sync_threshold: usize,
    ...
}
```

**Write flow:**
1. `Wal::append_record` adds record to buffer
2. If buffer exceeds threshold, flush to disk
3. No multi-threaded slot coordination
4. Each B-tree flushes independently

### 3. LSN (Log Sequence Number) Management

**WiredTiger: Global LSN across all tables**

```c
// From log.h
union __wt_lsn {
    struct {
        uint32_t file;    // Log file number
        uint32_t offset;  // Offset within file
    } l;
    uint64_t file_offset;
};

// In WTI_LOG structure:
WT_LSN alloc_lsn;       // Next LSN for allocation
WT_LSN ckpt_lsn;       // Last checkpoint LSN (global)
WT_LSN dirty_lsn;      // LSN of last non-synced write (global)
WT_LSN sync_lsn;       // LSN of last sync (global)
WT_LSN write_lsn;      // End of last LSN written (global)
```

**WrongoDB: Per-B-tree LSN**

```rust
// From src/storage/btree/wal.rs
pub struct Lsn {
    pub file_id: u32,
    pub offset: u64,
}

// In WalFile structure:
struct WalFile {
    last_lsn: Lsn,  // Per-WAL LSN
    checkpoint_lsn: Lsn,  // Per-WAL checkpoint
    ...
}
```

**Key difference:** WiredTiger's LSN uniquely identifies a position in the global log, while WrongoDB's LSN is per-table.

### 4. Log Record Types

**WiredTiger: Table-specific operations with file IDs**

```c
// From log.h - log operation packers
__wt_logop_row_put_pack(session, record, fileid, key, value);
__wt_logop_col_put_pack(session, record, fileid, recno, value);
__wt_logop_txn_timestamp_pack(session, record, time_sec, time_nsec,
                            commit_ts, durable_ts, ...);
```

Each record includes:
- `fileid`: Identifies which table/collection the operation affects
- Operation-specific data (key, value, recno, etc.)
- Transaction ID and timestamps

**WrongoDB: Simple per-table records**

```rust
// From src/storage/btree/wal.rs
pub enum WalRecord {
    Put { key: Vec<u8>, value: Vec<u8>, txn_id: TxnId },
    Delete { key: Vec<u8>, txn_id: TxnId },
    Checkpoint { root_block_id: u64, generation: u64 },
    TxnCommit { txn_id: TxnId, commit_ts: Timestamp },
    TxnAbort { txn_id: TxnId },
}
```

Records don't include file IDs because WAL is already per-table.

### 5. Checkpoint and WAL Truncation

**WiredTiger: Global checkpoint, coordinated truncation**

1. Checkpoint writes a record in global log with global LSN
2. All tables updated to checkpoint state
3. Log truncation based on `ckpt_lsn` (global)
4. Old log files can be removed safely

```c
// Connection-level checkpoint
__wt_checkpoint()
    └── __wt_txn_checkpoint()
        └── __wt_log_ckpt()  // Updates ckpt_lsn
            └── __wt_log_truncate_files()  // Removes old files
```

**WrongoDB: Per-table checkpoint, independent truncation**

1. Each table checkpoints independently
2. Checkpoint record written to that table's WAL
3. WAL truncated to checkpoint LSN for that table only
4. No coordination between tables

```rust
// From src/storage/btree/mod.rs
pub fn checkpoint(&mut self) -> Result<(), WrongoDBError> {
    if let Some(ref mut wal) = self.wal {
        let root = self.pager.root_page_id();
        let generation = self.pager.generation();
        let checkpoint_lsn = wal.log_checkpoint(root, generation)?;
        wal.update_checkpoint_lsn(checkpoint_lsn);
        wal.sync()?;
        // Truncate WAL after checkpoint
        wal.truncate_to_checkpoint()?;
    }
    self.pager.checkpoint()?;
    Ok(())
}
```

### 6. Recovery

**WiredTiger: Single-pass global recovery**

```c
// Recovery flow:
__wt_recover()
    └── __wt_log_scan()  // Scan global log
        └── For each record:
            - Parse fileid to determine target table
            - Apply operation to that table's B-tree
            - Track transaction state
    └── __wt_txn_recover()  // Transaction recovery
```

**WrongoDB: Per-table recovery**

```rust
// From src/storage/btree/mod.rs
fn recover_from_wal(&mut self) -> Result<(), WrongoDBError> {
    let mut wal = self.wal.take().ok_or("No WAL to recover")?;
    let checkpoint_lsn = wal.checkpoint_lsn();

    // Replay records from checkpoint LSN
    while let Some(record) = wal.read_record()? {
        match record.record_type() {
            WalRecordType::Put => { /* apply to B-tree */ }
            WalRecordType::Delete => { /* apply to B-tree */ }
            WalRecordType::Checkpoint => { /* skip */ }
            WalRecordType::TxnCommit => { /* track */ }
            WalRecordType::TxnAbort => { /* skip */ }
        }
    }
}
```

Each table recovers independently when opened.

## Architectural Implications

### Advantages of WiredTiger's Global WAL

1. **Single recovery point**: One log to scan during recovery, not multiple
2. **Atomic multi-table transactions**: Easier to ensure all-or-nothing semantics
3. **Efficient batching**: Consolidation slots allow efficient multi-threaded writes
4. **Consistent ordering**: Global LSN provides total order of all operations
5. **Space efficiency**: Log rotation allows reclaiming old log space
6. **Backup coordination**: Incremental backups can reference global LSN ranges

### Advantages of WrongoDB's Per-Table WAL

1. **Simpler implementation**: No complex slot management or coordination
2. **Independent checkpoints**: Tables can checkpoint without coordination
3. **Isolation**: Issues with one table's WAL don't affect others
4. **No rotation complexity**: Simple truncate-after-checkpoint model
5. **Parallelism**: Multiple tables can write to WALs concurrently without contention

### Disadvantages

**WiredTiger's Global WAL:**
- Complex slot management logic
- Single point of contention (though mitigated by slots)
- More code to maintain
- Recovery must handle cross-table dependencies

**WrongoDB's Per-Table WAL:**
- Multiple recovery passes needed (one per table)
- No global ordering guarantees
- Multi-table transactions require coordination at higher level
- Less efficient batching for concurrent writes to same table

## Multi-Collection Transaction Implications

### WiredTiger

Since WAL is global, multi-collection transactions naturally work:

```c
WT_SESSION *session = conn->open_session(...);

session->begin_transaction();
cursor1 = session->open_cursor(..., "table:collection_a", ...);
cursor2 = session->open_cursor(..., "table:collection_b", ...);

cursor1->put(...);  // Writes to global log
cursor2->put(...);  // Writes to global log (same transaction ID)

session->commit_transaction();  // Single commit in global log
```

Recovery replays the global log, ensuring atomicity.

### WrongoDB

Multi-collection transactions require explicit coordination:

```rust
// From src/engine/transaction.rs
pub fn commit(mut self) -> Result<(), WrongoDBError> {
    // Commit once on first touched collection
    let first_coll = self.touched_collections.iter().next();
    if let Some(first_coll_name) = first_coll {
        let coll = self.db.get_collection_mut(&first_coll_name)?;
        coll.main_table().commit_txn(&mut txn)?;
    }

    // Sync WAL for others (don't commit, just sync)
    for coll_name in self.touched_collections.iter().skip(1) {
        let coll = self.db.get_collection_mut(coll_name)?;
        coll.main_table_btree().sync_wal()?;
    }
}
```

The first collection gets the transaction commit record; others just sync WAL.

**Issue:** If crash occurs after first collection commits but before others sync, recovery is complex.

## Recommendations

For WrongoDB, consider:

1. **Global WAL for multi-collection support**: Move WAL to connection level to simplify multi-collection transactions
2. **Per-table LSN within global WAL**: Keep file IDs in records but manage WAL globally
3. **Simpler consolidation**: Adopt a simplified slot model without complex state machine
4. **Single recovery pass**: Scan one global log instead of multiple per-table logs

However, if simplicity is a priority:
1. **Keep per-table WAL**: Simpler to understand and debug
2. **Better transaction coordination**: Explicitly handle multi-collection atomicity at higher level
3. **Document recovery semantics**: Clearly explain crash behavior for multi-collection transactions

## Summary

| Aspect | WiredTiger | WrongoDB |
|--------|-----------|----------|
| WAL scope | Global (connection) | Per-table |
| Log files | Rotating sequence | One per table |
| LSN | Global offset | Per-table offset |
| Write batching | 128-slot consolidation | Simple buffer |
| Recovery | Single global pass | Per-table passes |
| Multi-table txns | Natural (same log) | Requires coordination |
| Complexity | High (slots, rotation) | Low (direct writes) |
| Code size | ~2000 lines in log subsystem | ~850 lines in wal.rs |

The fundamental difference is architectural choice: **global coordination vs. per-table isolation**. WiredTiger prioritizes global ordering and efficient multi-table operations at the cost of complexity. WrongoDB prioritizes simplicity and isolation at the cost of more complex multi-collection transaction handling.
