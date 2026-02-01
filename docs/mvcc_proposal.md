# MVCC/Transactions Proposal for WrongoDB

Based on exploration of WrongoDB, WiredTiger, and MongoDB, this is a comprehensive proposal for implementing MVCC transactions that stays close to the WiredTiger/MongoDB design patterns.

---

## Executive Summary

**WrongoDB Current State:**
- Single-threaded (all operations require `&mut self`)
- Copy-on-Write (COW) at the page level
- Logical WAL for recovery
- No concurrent access = no isolation needed

**Target Architecture (WiredTiger-inspired):**
- Update chains per key with transaction IDs
- Snapshot isolation for reads
- Optimistic concurrency control for writes
- History store for older committed versions
- Garbage collection of obsolete versions

---

## Phase 1: Core MVCC Infrastructure

### 1.1 Transaction IDs and Global State

Following WiredTiger's pattern (`src/include/txn.h`):

```rust
// src/storage/mvcc/global_txn.rs
pub struct GlobalTxnState {
    /// Monotonically increasing counter for transaction IDs
    current_txn_id: AtomicU64,

    /// Oldest transaction ID still running
    /// Used for garbage collection decisions
    oldest_txn_id: AtomicU64,

    /// List of currently active transaction IDs
    /// Snapshots are derived from this
    active_txns: RwLock<Vec<TxnId>>,
}

pub type TxnId = u64;
pub const TXN_NONE: TxnId = 0;
pub const TXN_ABORTED: TxnId = u64::MAX;
```

### 1.2 Per-Transaction State

```rust
// src/storage/mvcc/transaction.rs
pub struct Transaction {
    id: TxnId,
    isolation: IsolationLevel,
    snapshot: Option<Snapshot>,
    state: TxnState,
    read_ts: Option<Timestamp>,
    commit_ts: Option<Timestamp>,
    durable_ts: Option<Timestamp>,
}

pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    Snapshot,  // Default, like WT_ISO_SNAPSHOT
}

pub enum TxnState {
    Started,
    Committed { commit_ts: Timestamp },
    Aborted,
}

/// A snapshot captures active transactions at a point in time
pub struct Snapshot {
    /// Transactions >= snap_max are invisible (started after snapshot)
    snap_max: TxnId,
    /// Transactions < snap_min are visible (committed before snapshot)
    snap_min: TxnId,
    /// Active transaction IDs at snapshot time
    active: Vec<TxnId>,
    /// Transaction id of the reader (reads see own writes)
    my_txn_id: TxnId,
}
```

**Snapshot visibility check** (from WiredTiger `src/include/txn_inline.h:1212`):
```rust
impl Snapshot {
    pub fn is_visible(&self, txn_id: TxnId) -> bool {
        // Our own writes are always visible
        if txn_id == self.my_txn_id {
            return true;
        }

        // IDs >= snap_max started after snapshot - invisible
        if txn_id >= self.snap_max {
            return false;
        }

        // IDs < snap_min committed before snapshot - visible
        if txn_id < self.snap_min {
            return true;
        }

        // Otherwise, visible only if NOT in active list
        !self.active.contains(&txn_id)
    }
}
```

### 1.3 Timestamps and Time Windows (WT/Mongo Alignment)

WiredTiger visibility checks are based on **transaction id + timestamps**. MongoDB exposes this
through `RecoveryUnit` read sources (timestamped or not). To stay close to WT, we model the same
time window structure even if Phase 1 starts with `commit_ts == durable_ts`.

```rust
pub type Timestamp = u64;
pub const TS_NONE: Timestamp = 0;
pub const TS_MAX: Timestamp = u64::MAX;

pub struct TimeWindow {
    start_ts: Timestamp,
    durable_start_ts: Timestamp,
    start_txn: TxnId,
    stop_ts: Timestamp,
    durable_stop_ts: Timestamp,
    stop_txn: TxnId,
    prepared: bool,
}

pub struct GlobalTimestampState {
    oldest_ts: AtomicU64,
    stable_ts: AtomicU64,
    // Pinned = min(oldest_ts, min(active read_ts))
    pinned_ts: AtomicU64,
}
```

**Visibility rule (WT-like):**
```
visible(update, txn) =
  txn_id_visible(update.start_txn, txn.snapshot) &&
  (txn.read_ts == TS_NONE || update.start_ts <= txn.read_ts < update.stop_ts)
```

Prepared updates: WT either returns a prepare conflict or ignores prepared updates and reads older
versions. Phase 1 can return a conflict and require retry.

### 1.4 Update Chains (The Heart of MVCC)

This is the key data structure from WiredTiger (`src/include/btmem.h:1481`):

```rust
// src/storage/mvcc/update.rs

/// An update in the version chain for a key
pub struct Update {
    /// Transaction ID that created this version
    txn_id: TxnId,
    /// Time window for visibility (WT-style)
    time_window: TimeWindow,

    /// Link to older version (newest -> oldest chain)
    next: Option<Box<Update>>,

    /// The update type
    type_: UpdateType,

    /// The value (if not a tombstone)
    data: Vec<u8>,
}

pub enum UpdateType {
    Standard,    // Regular value
    Tombstone,   // Deleted
    Reserve,     // Placeholder for in-progress insert
}
```

**How update chains work:**
- Each key in the BTree can have an optional update chain
- Newest update is at the head of the chain
- Chain is singly-linked: `new_update -> older_update -> oldest_update -> None`
- On read, traverse chain to find first visible version

---

## Phase 2: BTree Modifications for MVCC

### 2.1 Leaf Page Extension

Current WrongoDB leaf pages store `(key, value)` pairs directly. For MVCC, we add update chains:

```rust
// src/storage/btree/page.rs (modification)

pub struct LeafPage {
    data: &mut [u8],

    // NEW: Update chain storage per key
    // This could be:
    // Option A: In-page overflow area (like WT)
    // Option B: Separate in-memory structure
}

// NEW: Per-key update chain management
pub struct UpdateChain {
    head: Option<Box<Update>>,
}
```

**Key insight from WiredTiger:** Updates are stored **separate from the page** initially, then reconciled to the page during checkpoint.

### 2.2 History Store (HS) for Older Versions

WiredTiger keeps older committed versions in a dedicated history store so long-running readers can see
older snapshots even after pages are reconciled or evicted.

**Minimal HS design (WT-like):**
- Internal BTree table, not visible to users.
- Key = `(btree_id, user_key, start_ts, counter)` to keep versions unique and ordered.
- Value = `{stop_ts, durable_ts, update_type, value}` (WT keeps stop/durable timestamps).

**Write path to HS:**
- During reconciliation/eviction/checkpoint, choose the newest committed update visible to the
  checkpoint snapshot for the base page.
- Move older committed updates to the history store.

**Read path with HS:**
```
update chain (cache) -> on-disk base value -> history store
```

This allows eviction/checkpointing without breaking snapshot isolation.

### 2.3 The Read Path (MVCC Visibility)

```rust
// src/storage/btree/mvcc_read.rs

impl BTree {
    pub fn get_mvcc(
        &self,
        key: &[u8],
        txn: &Transaction,
    ) -> Result<Option<Vec<u8>>, WrongoDBError> {
        // 1. Get the leaf page
        let leaf = self.find_leaf(key)?;

        // 2. Check update chain first (newest versions)
        if let Some(update) = self.get_update_chain(leaf.page_id(), key) {
            // Traverse chain to find visible version
            for u in update.chain_iter() {
                if u.time_window.prepared && !txn.can_ignore_prepare() {
                    return Err(WrongoDBError::PrepareConflict);
                }
                if txn.can_see(u.txn_id) && txn.can_see_ts(&u.time_window) {
                    return match u.type_ {
                        UpdateType::Standard => Ok(Some(u.data.clone())),
                        UpdateType::Tombstone => Ok(None),
                        UpdateType::Reserve => continue, // skip placeholders
                    };
                }
            }
        }

        // 3. No visible update - read from base page
        // But only if no uncommitted updates exist
        if let Some(value) = self.get_from_base_page(&leaf, key, txn)? {
            return Ok(Some(value));
        }

        // 4. Still nothing visible - consult history store
        self.get_from_history_store(key, txn)
    }
}
```

### 2.4 The Write Path

```rust
// src/storage/btree/mvcc_write.rs

impl BTree {
    pub fn put_mvcc(
        &mut self,
        key: &[u8],
        value: &[u8],
        txn: &mut Transaction,
    ) -> Result<(), WrongoDBError> {
        // 1. Ensure transaction has an ID
        if txn.id == TXN_NONE {
            txn.id = self.global_txn.allocate_txn_id();
        }

        // 2. Get or create update chain for this key
        let update_chain = self.get_or_create_update_chain(key)?;

        // 3. Create new update
        let new_update = Update {
            txn_id: txn.id,
            time_window: txn.write_time_window(),
            type_: UpdateType::Standard,
            data: value.to_vec(),
            next: update_chain.take_head(),
        };

        // 4. Prepend to chain
        update_chain.set_head(Box::new(new_update));

        // 5. Track this modification for commit/rollback
        txn.track_modification(key, ModificationType::Update);

        Ok(())
    }

    pub fn delete_mvcc(
        &mut self,
        key: &[u8],
        txn: &mut Transaction,
    ) -> Result<bool, WrongoDBError> {
        // Similar to put, but create Tombstone update
        let tombstone = Update {
            txn_id: txn.id,
            type_: UpdateType::Tombstone,
            data: vec![],
            next: update_chain.take_head(),
        };
        update_chain.set_head(Box::new(tombstone));
        Ok(true)
    }
}
```

---

## Phase 3: Transaction Lifecycle

Following MongoDB's `RecoveryUnit` pattern and WiredTiger's transaction state machine:

### 3.1 Transaction Begin

```rust
// src/storage/mvcc/transaction.rs

impl Transaction {
    pub fn begin(
        &mut self,
        isolation: IsolationLevel,
        global: &GlobalTxnState,
    ) -> Result<(), WrongoDBError> {
        self.isolation = isolation;
        self.state = TxnState::Started;

        // For snapshot isolation, capture snapshot immediately
        if isolation == IsolationLevel::Snapshot {
            self.snapshot = Some(global.take_snapshot());
        }

        // Register as active transaction
        global.register_active(self.id);

        Ok(())
    }
}
```

**MongoDB alignment:** In MongoDB, a `RecoveryUnit` snapshot is opened lazily on first read/write.
We can preserve WT/Mongo semantics by creating the snapshot at the first data access when
`isolation == Snapshot` and reusing it for the life of the transaction.

**Taking a snapshot** (from WiredTiger `src/txn/txn.c:178-293`):
```rust
impl GlobalTxnState {
    pub fn take_snapshot(&self, my_txn_id: TxnId) -> Snapshot {
        let current = self.current_txn_id.load(Ordering::Acquire);

        // Collect all active transaction IDs
        let active = self.active_txns.read()
            .iter()
            .filter(|&&id| id != my_txn_id && id != TXN_NONE)
            .copied()
            .collect::<Vec<_>>();

        // snap_min = oldest active transaction
        let snap_min = active.iter().copied().min().unwrap_or(current);

        Snapshot {
            snap_max: current,
            snap_min,
            active,
            my_txn_id,
        }
    }
}
```

### 3.2 Transaction Commit

```rust
impl Transaction {
    pub fn commit(&mut self, global: &GlobalTxnState) -> Result<(), WrongoDBError> {
        match self.state {
            TxnState::Started => {
                // For snapshot isolation, set commit timestamp
                // (In simple version, set commit_ts == durable_ts)
                self.state = TxnState::Committed {
                    commit_ts: global.next_timestamp()
                };

                // Unregister from active list
                global.unregister_active(self.id);

                // Update global oldest_txn_id if we were the oldest
                global.recalculate_oldest();

                Ok(())
            }
            _ => Err(WrongoDBError::InvalidTransactionState),
        }
    }
}
```

### 3.3 Transaction Abort/Rollback

```rust
impl Transaction {
    pub fn abort(&mut self, global: &GlobalTxnState) -> Result<(), WrongoDBError> {
        // Mark all our updates as aborted
        for (key, _) in &self.modifications {
            if let Some(chain) = self.get_update_chain(key) {
                chain.mark_aborted(self.id);
            }
        }

        self.state = TxnState::Aborted;
        global.unregister_active(self.id);
        global.recalculate_oldest();

        Ok(())
    }
}
```

---

## Phase 4: Garbage Collection (GC)

WiredTiger's approach (`src/txn/txn.c:378-460`): advance `oldest_id` and remove obsolete updates.

```rust
// src/storage/mvcc/gc.rs

pub struct GarbageCollector {
    global: Arc<GlobalTxnState>,
}

impl GarbageCollector {
    /// Check if an update is obsolete and can be freed
    fn is_obsolete(&self, update: &Update, pinned_ts: Timestamp) -> bool {
        // Update is obsolete if its stop_ts is before the pinned timestamp
        update.time_window.stop_ts < pinned_ts
    }

    /// Clean update chains during page reconciliation
    pub fn clean_page_updates(&self, page_id: PageId) {
        let pinned_ts = self.global.pinned_ts.load(Ordering::Relaxed);

        for key in self.keys_in_page(page_id) {
            if let Some(chain) = self.get_update_chain(key) {
                chain.retain(|update| !self.is_obsolete(update, pinned_ts));
            }
        }
    }
}
```

**HS GC rule (timestamp aware):**
- History store entries can be removed when `stop_ts < pinned_ts`
  (pinned is min of active read timestamps and configured oldest).

---

## Phase 5: Integration with Existing Architecture

### 5.1 Leveraging Existing COW

WrongoDB already has COW in the pager. MVCC extends this to the key level:

**Current COW:**
```
Page modification -> copy entire page -> modify copy
```

**MVCC + COW:**
```
Key modification -> create Update -> prepend to chain
Page split/reconcile -> apply visible updates to new page
```

### 5.2 WAL Enhancement

Current WAL logs operations (put/delete). For MVCC, we need to ensure atomicity:

```rust
// src/storage/btree/wal.rs (extension)

pub enum WalRecord {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },

    // NEW: Transaction markers
    TxnBegin { txn_id: TxnId },
    TxnCommit { txn_id: TxnId },
    TxnAbort { txn_id: TxnId },
}
```

**Recovery rules (WT-like, logical replay):**
```
1) Read WAL from checkpoint LSN forward.
2) Build txn table:
   - TxnCommit => committed set
   - TxnAbort  => aborted set
3) Replay logical ops in order:
   - apply Put/Delete only if txn_id is committed
   - skip if txn_id is aborted or missing commit
4) Any txn without a commit at end of log is treated as aborted.
```

Notes:
- This mirrors WT’s logical replay: redo operations are applied via normal BTree paths.
- If we add prepared transactions later, recovery should roll back to stable (treat
  prepared without durable/stable visibility as aborted during replay).

### 5.3 Server-Level API (MongoDB-style)

Following MongoDB's `TransactionParticipant` pattern:

```rust
// src/engine/transaction.rs

/// High-level transaction API for the server layer
pub struct DocumentTransaction {
    collection: Arc<Collection>,
    txn: Transaction,
}

impl DocumentTransaction {
    pub fn start(&mut self) -> Result<(), WrongoDBError> {
        self.txn.begin(IsolationLevel::Snapshot, &self.collection.global_txn())
    }

    pub fn find(&self, filter: Document) -> Result<Vec<Document>, WrongoDBError> {
        // Use MVCC snapshot read
        self.collection.find_with_txn(filter, &self.txn)
    }

    pub fn insert(&mut self, doc: Document) -> Result<(), WrongoDBError> {
        self.collection.insert_with_txn(doc, &mut self.txn)
    }

    pub fn commit(&mut self) -> Result<(), WrongoDBError> {
        // 1. Validate no conflicts (write-write)
        // 2. Write WAL commit record
        // 3. Update transaction state
        self.txn.commit(&self.collection.global_txn())
    }

    pub fn abort(&mut self) -> Result<(), WrongoDBError> {
        self.txn.abort(&self.collection.global_txn())
    }
}
```

---

## Phase 6: Concurrency Control

Since WrongoDB is currently single-threaded, we need to add synchronization:

### 6.1 Granular Locking Strategy

```rust
// src/storage/mvcc/locks.rs

/// Page-level lock for tree structure modifications
pub struct PageLock {
    inner: RwLock<PageState>,
}

/// Update chain lock for key-level modifications
pub struct UpdateLock {
    inner: Mutex<UpdateChain>,
}

/// Global transaction state lock
pub struct TxnStateLock {
    active: RwLock<Vec<TxnId>>,
}
```

**Read path (lock-free for snapshot reads):**
```rust
// Readers don't acquire locks - they use their snapshot
pub fn get(&self, key: &[u8], txn: &Transaction) -> Option<Value> {
    // No locks! Snapshot ensures consistency
    let chain = self.update_chains.get(key)?;
    chain.find_visible(txn.snapshot.as_ref()?)
}
```

**Write path (optimistic concurrency):**
```rust
// Writers prepend to update chains
pub fn put(&self, key: &[u8], value: Value, txn: &mut Transaction) {
    let chain = self.update_chains.get_or_create(key);
    let mut guard = chain.lock();
    guard.prepend(Update::new(txn.id, value));
}
```

---

## Key Design Decisions

| Decision | WiredTiger Approach | WrongoDB Approach |
|----------|-------------------|-------------------|
| **Version storage** | Update chains per key | Same - in-memory chains |
| **Isolation** | Snapshot (default) + Read Committed | Start with Snapshot only |
| **Timestamps** | commit_ts, durable_ts, read_ts | Model WT time windows; Phase 1 sets commit_ts == durable_ts |
| **Prepared transactions** | Full 2PC support | Skip for Phase 1 |
| **History store** | Separate HS for old versions | Dedicated HS table, GC by pinned/oldest |
| **Concurrency** | Lock-free reads, atomic writes | RwLock + Mutex |

---

## Implementation Roadmap

### Phase 1: Foundation (2-3 weeks)
1. Add `GlobalTxnState`, `Transaction`, `Snapshot`
2. Add `Update` and `UpdateChain` structures
3. Modify leaf pages to reference update chains
4. Single-threaded MVCC (no real concurrency yet)

### Phase 2: Transaction API (1-2 weeks)
1. Begin/Commit/Abort lifecycle
2. Snapshot management
3. Integration with existing WAL

### Phase 3: Concurrency (2 weeks)
1. Add synchronization primitives
2. Lock-free snapshot reads
3. Write-write conflict detection

### Phase 4: GC and Optimization (1-2 weeks)
1. Oldest ID tracking
2. Update chain truncation
3. Integration with checkpoint

### Phase 5: MongoDB Compatibility

#### 5.1 Multi-document transactions (WT/Mongo semantics)

Goal: allow a single transaction to update multiple documents across multiple collections with
**atomic visibility** and **snapshot isolation**.

```
Txn 42
  ├─ collA.main  put K1
  ├─ collA.index put (K1,_id)
  └─ collB.main  del K9

Visibility:
  before commit: none visible
  after  commit: all visible
```

Mechanics:
- Every update carries the same `txn_id` (and time window).
- Readers only see updates from committed transactions.
- Write conflicts are detected per key/update-chain; Mongo retries with backoff.
- Commit flips visibility for **all** updates in the txn (across collections).

#### 5.2 Collection-level coordination (document + indexes)

Mongo requires document updates and index updates to be atomic together.
We model this by treating *all record stores and indexes touched by the txn* as part
of the same storage transaction.

```
Collection A
  main table  <- txn 42 update chain
  idx table   <- txn 42 update chain

Collection B
  main table  <- txn 42 update chain
```

Commit/abort is driven by a single transaction context shared across collections:

```
TransactionContext
  - txn_id
  - snapshot (lazy, first read)
  - write set (keys/pages touched)
  - state (active/committed/aborted)

Collections/Indexes all write into the same context.
```

#### 5.3 Commit all-at-once + crash safety

Atomic visibility across **multiple files** is achieved via global transaction visibility + WAL
commit markers (WT-style logical replay).

```
WAL:
  TxnBegin(42)
  Put(collA.main, K1, V1)
  Put(collA.idx,  K1,_id)
  Del(collB.main, K9)
  TxnCommit(42)   <-- durability boundary (fsync)
```

Rules:
- Reads only see committed transactions.
- Crash before `TxnCommit` => no commit record => replay skips all ops.
- Crash after `TxnCommit`  => replay applies all ops => atomic across files.

Checkpoint interaction:
- Checkpoints may flush some files earlier than others (COW roots remain consistent).
- Recovery replays committed WAL ops to bring all files to the same committed state.

#### 5.4 Wire-protocol commands (Mongo-like)

Mongo uses `lsid` + `txnNumber` on commands. We can map these to a storage `TransactionContext`:

```
startTransaction:
  - allocate txn_id
  - (optional) capture snapshot immediately

commitTransaction:
  - write TxnCommit record
  - fsync WAL
  - mark txn committed (visibility flip)

abortTransaction:
  - write TxnAbort record
  - mark txn aborted (discard updates)
```

#### 5.5 Minimal state machine

```
ACTIVE --commit--> COMMITTED (visible)
ACTIVE --abort-->  ABORTED  (invisible)
```

All collections/indexes rely on the same state transition.

#### 5.6 Data structures (transaction + coordination)

Core transaction context shared across collections (Mongo `RecoveryUnit` / `WriteUnitOfWork` shape):

```rust
// src/engine/transaction.rs (conceptual)
pub struct TransactionContext {
    pub txn_id: TxnId,
    pub state: TxnState,
    pub isolation: IsolationLevel,
    pub snapshot: Option<Snapshot>, // lazy, first read/write
    pub read_ts: Option<Timestamp>,
    pub commit_ts: Option<Timestamp>,
    pub durable_ts: Option<Timestamp>,

    // Write set for conflict checks + abort bookkeeping
    pub write_set: Vec<WriteRef>,

    // Optional: participants touched by this txn
    pub participants: Vec<ParticipantId>,
}

pub struct WriteRef {
    pub file_id: FileId,  // main table or index btree id
    pub key: Vec<u8>,
    pub op: WriteOp,
}

pub enum WriteOp {
    Put,
    Delete,
}
```

Update chains (per key, newest first), stored in cache alongside pages:

```
UpdateChain (key=K):
  head -> U3(txn=42, ts=120)
        -> U2(txn=37, ts=100)
        -> U1(txn=12, ts=80)
```

History Store (HS) table schema (WT-like):

```rust
pub struct HistoryStoreKey {
    pub btree_id: u64,
    pub user_key: Vec<u8>,
    pub start_ts: Timestamp,
    pub counter: u32,
}

pub struct HistoryStoreValue {
    pub stop_ts: Timestamp,
    pub durable_ts: Timestamp,
    pub update_type: UpdateType,
    pub value: Vec<u8>,
}
```

Recovery transaction table (built from WAL during replay):

```rust
pub struct RecoveryTxnTable {
    pub committed: HashSet<TxnId>,
    pub aborted: HashSet<TxnId>,
}
```

Participants (collections + indexes) coordinate through the shared `TransactionContext`:

```
TransactionContext
  ├─ collA.main
  ├─ collA.index
  └─ collB.main
```

Notes:
- `write_set` is used for conflict checks, abort cleanup, and WAL grouping.
- `participants` is optional now but useful if we later add per-collection hooks at commit.

---

## Files to Create/Modify

| File | Purpose |
|------|---------|
| `src/storage/mvcc/mod.rs` | MVCC module entry |
| `src/storage/mvcc/global_txn.rs` | `GlobalTxnState` |
| `src/storage/mvcc/transaction.rs` | `Transaction`, `TxnState` |
| `src/storage/mvcc/snapshot.rs` | `Snapshot`, visibility checks |
| `src/storage/mvcc/update.rs` | `Update`, `UpdateChain` |
| `src/storage/mvcc/gc.rs` | Garbage collector |
| `src/storage/btree/leaf_mvcc.rs` | MVCC leaf operations |
| `src/engine/transaction.rs` | High-level transaction API |

---

## Summary

This design stays close to WiredTiger's proven architecture:

1. **Update chains** provide version history per key
2. **Transaction IDs + Snapshots** provide snapshot isolation
3. **History store** keeps older committed versions for long-running readers
4. **Optimistic concurrency** minimizes locking overhead
5. **GC based on pinned/oldest** reclaims obsolete versions safely

The key simplification for WrongoDB is starting with transaction-ID-based visibility only (no timestamps), which captures the essence of MVCC without the full complexity of timestamp management.
