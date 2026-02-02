# WiredTiger vs WrongoDB: Session Structure and Multi-Collection Transaction Design

## Architecture Overview

### WiredTiger (C Implementation)

**Three-Tier Architecture:**

1. **Connection Level** (`WT_CONNECTION_IMPL`)
   - Single connection per database instance
   - Global transaction state (`WT_TXN_GLOBAL`)
   - Per-process singleton structure
   - Manages shared resources (cache, locks, metadata)

2. **Session Level** (`WT_SESSION_IMPL`)
   - Thread-affine session objects
   - Owned by application, used by threads
   - Contains per-session transaction context (`WT_TXN *txn`)
   - Persistent across multiple operations

3. **Transaction Level** (`WT_TXN`)
   - Per-session transaction state
   - Snapshot data (snapshot IDs)
   - Modifications array
   - Transaction flags and metadata

### WrongoDB (Rust Implementation)

**Two-Tier Architecture:**

1. **Database Level** (`WrongoDB`)
   - Single database instance
   - Global transaction state (`GlobalTxnState`)
   - Collection management
   - No explicit session object

2. **Transaction Level** (`MultiCollectionTxn`)
   - Transaction object temporarily owned by API user
   - No persistence across operations
   - Scoped to single transaction lifecycle

## Key Structural Differences

### 1. Session Abstraction

**WiredTiger:**
```c
// Session is a first-class object
WT_SESSION_IMPL {
    WT_SESSION iface;
    WT_TXN *txn;                    // Per-session transaction
    WT_CURSOR_LIST cursors;         // Owned cursors
    WT_DATA_HANDLE *dhandle;        // Current handle
    // ... many more fields
};
```

**WrongoDB:**
```rust
// No session abstraction - operations are API-bound
WrongoDB {
    GlobalTxnState;                 // Global transaction state
    HashMap<String, Collection>;    // Collections
    // ... simpler structure
}

MultiCollectionTxn<'a> {
    db: &'a mut WrongoDB;
    txn: Option<Transaction>;       // Transient
    touched_collections: HashSet;   // Per-transaction state
    pending_index_ops: HashMap;     // Rollback tracking
}
```

**Impact:** WiredTiger's session model enables cursor caching, handle reuse, and better resource management across multiple operations.

### 2. Multi-Collection Transaction Design

**WiredTiger:**

```c
// Session owns transaction, can access multiple collections
WT_SESSION *session;
WT_TXN *txn = session->txn;

// Multi-collection operations use same session context
session->txn->id;  // Same ID across all collections
```

**WrongoDB:**

```rust
// Transaction owns access to database
let mut txn = db.begin_txn();

// Must explicitly track collection involvement
txn.touched_collections.insert("coll_a".to_string());
txn.touched_collections.insert("coll_b".to_string());

// Index rollback tracking per collection
txn.pending_index_ops.insert("coll_a".to_string(), vec![]);
```

**Key Difference:** WiredTiger automatically tracks which collections are affected by a transaction. WrongoDB requires explicit tracking.

### 3. Transaction Visibility & Isolation

**WiredTiger's Session-Transaction Relationship:**

```c
// Session provides transaction visibility context
WT_SESSION_TXN_SHARED *txn_shared = WT_SESSION_TXN_SHARED(session);

// Per-session shared state handles visibility
typedef struct __wt_txn_shared {
    uint64_t id;                    // Transaction ID
    uint64_t pinned_id;             // Pinned read point
    uint64_t metadata_pinned;       // Metadata read point
    wt_timestamp_t read_timestamp;  // Timestamp isolation
    // ...
} WT_TXN_SHARED;

// Connection maintains per-session transaction state
typedef struct __wt_txn_global {
    WT_TXN_SHARED *txn_shared_list;  // Per-session shared state
    uint64_t current;                // Current transaction ID
    uint64_t oldest_id;              // Oldest visible transaction
    uint64_t last_running;           // Last running transaction
    // ...
} WT_TXN_GLOBAL;
```

**WrongoDB's Global Transaction State:**

```rust
// Simple global state
GlobalTxnState {
    current_txn_id: AtomicU64,      // Current ID counter
    active_txns: RwLock<Vec<TxnId>>, // Active transactions
    aborted_txns: RwLock<HashSet<TxnId>>, // Aborted transactions
    oldest_active_txn_id: AtomicU64, // GC threshold
}

// Snapshot based on current state
fn take_snapshot(&self, my_txn_id: TxnId) -> Snapshot {
    // Captures current, active, and aborted transactions
    // Determines visibility rules
}
```

### 4. Cursor Management

**WiredTiger:**

```c
// Cursors are owned by session, can be cached
WT_SESSION *session;
WT_CURSOR *cursor;

session->cursor_cache[hash] = cursor;
session->ncursors++;

// Session sweeps and reuses cursors
__wt_session_cursor_cache_sweep(session, false);
```

**WrongoDB:**

```rust
// Cursors are ephemeral, not cached
let mut txn = db.begin_txn();
let docs = txn.find("collection", None)?;
// Cursor goes away when txn ends
```

### 5. Locking and Resource Management

**WiredTiger:**

```c
// Session maintains many locks
WT_SESSION_IMPL {
    uint32_t lock_flags;  // Table read/write, schema, metadata
    WT_RWLOCK *current_rwlock;  // Per-operation lock
    // ...
};

// Connection-level locks
WT_CONNECTION_IMPL {
    WT_SPINLOCK checkpoint_lock;
    WT_SPINLOCK table_lock;
    WT_SPINLOCK metadata_lock;
    // ...
};
```

**WrongoDB:**

```rust
// Simplified locking through collection access
pub fn collection(&mut self, name: &str) -> Result<&mut Collection, WrongoDBError> {
    // Simple HashMap access
}

// Collection-level locking (implicit)
// No explicit session-level locking
```

### 6. Multi-Collection Commit Coordination

**WiredTiger:**

```c
// Transaction ID is the coordination mechanism
WT_TXN *txn = session->txn;

// Commit is a single session API call
session->commit_transaction();

// Global state ensures atomicity
S2C(session)->txn_global.current++;
```

**WrongoDB:**

```rust
// Explicit collection tracking
pub fn commit(mut self) -> Result<(), WrongoDBError> {
    // Commit once on first touched collection
    let first_coll = self.touched_collections.iter().next();
    if let Some(first_coll_name) = first_coll {
        let coll = self.db.get_collection_mut(&first_coll_name)?;
        coll.main_table().commit_txn(&mut txn)?;
    }

    // Sync WAL for others
    for coll_name in self.touched_collections.iter().skip(1) {
        let coll = self.db.get_collection_mut(coll_name)?;
        coll.main_table().sync_wal()?;
    }
}
```

## Implications for Multi-Collection Transactions

### WiredTiger Advantages:

1. **Automatic Collection Tracking**: Session's dhandle list naturally tracks affected collections
2. **Better Resource Management**: Cursors and handles are persistent and optimized
3. **Implicit Coordination**: Transaction ID ensures atomic visibility across collections
4. **Advanced Features**: Checkpoint support, backup support, prepared transactions

### WrongoDB Advantages:

1. **Simpler API**: No need to manage session objects explicitly
2. **Explicit Tracking**: Clear understanding of which collections are touched
3. **Easier to Debug**: Simple snapshot and state management
4. **Type Safety**: Rust's type system catches errors

### WrongoDB Challenges:

1. **Borrow Checker Issues**: Must use `Option<Transaction>` pattern to work around Rust's ownership rules
2. **No Cursor Caching**: Each operation creates new cursor state
3. **Manual Rollback**: Must track pending index operations explicitly
4. **Reduced Feature Set**: Missing advanced transaction features

## Architectural Recommendations

For improving WrongoDB's multi-collection transaction handling:

1. **Introduce Session Abstraction**: Create a `Session` object that owns the transaction
2. **Implement Cursor Caching**: Allow reusable cursor state within sessions
3. **Simplify Rollback Tracking**: Automatic tracking of modified collections
4. **Add Transaction Metadata**: Track transaction scope and dependencies
5. **Consider Hybrid Approach**: Keep WrongoDB's simplicity while adding session-like features

## Performance Implications

**WiredTiger:**
- Better performance for multi-statement transactions due to session reuse
- More efficient cursor management
- Better resource utilization through caching

**WrongoDB:**
- Simpler implementation with lower overhead per operation
- Easier to understand and maintain
- Better for read-mostly workloads or simple transactions

## Conclusion

WiredTiger's session-abstraction-first design provides a robust foundation for complex transaction scenarios, especially multi-collection operations. WrongoDB's simpler, API-bound transaction model is appropriate for its use case but would benefit from session-level features to match WiredTiger's capabilities.
