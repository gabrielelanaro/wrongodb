# Session Refactor Plan: Connection → Session → Cursor

**Goal**: Replace `WrongoDB`/`Collection` API with WiredTiger-style `Connection` → `Session` → `Cursor` interface.

**Reference**: WiredTiger architecture - see `~/workspace/wiredtiger/src/docs/arch-*.dox`:
- `Connection` creates sessions (`WT_CONNECTION::open_session`)
- `Session` is the context for all operations (cursors, transactions, schema)
- `Cursor` is created from session (`WT_SESSION::open_cursor`) and performs data I/O
- URI prefixes distinguish object types: `table:<name>`, `index:<table>:<field>`, `file:<path>`
- Data handles (`dhandles`) are cached per session and point at underlying B-trees

## 1. Target Layout

```
src/
├── connection.rs          # public: Connection, ConnectionConfig, DataHandleCache
├── session.rs             # public: Session, transaction state, open_cursor/create/drop
├── cursor.rs              # public: Cursor, CursorKind (Table/Index)
├── catalog/               # new: schema/metadata (table/index definitions)
│   ├── mod.rs
│   └── metadata.rs
├── data/                  # new: internal table/index logic (renamed from engine/collection)
│   ├── mod.rs
│   ├── table.rs             # renamed from Collection
│   ├── index.rs             # wraps PersistentIndex/SecondaryIndexManager
│   └── update.rs            # from collection/update.rs
├── engine/                 # internal glue only (or remove)
├── index/                 # unchanged
├── storage/               # unchanged
└── txn/                   # unchanged
```

## 2. Public API Surface

### `src/connection.rs`
```rust
pub struct ConnectionConfig {
    pub wal_enabled: bool,
    pub checkpoint_after_updates: Option<usize>,
}

pub struct Connection {
    base_path: PathBuf,
    dhandle_cache: DataHandleCache,
    wal_enabled: bool,
    checkpoint_after_updates: Option<usize>,
    global_txn: Arc<GlobalTxnState>,
}

impl Connection {
    pub fn open<P>(path: P) -> Result<Self, WrongoDBError>;
    pub fn open_with_config<P>(path: P, config: ConnectionConfig) -> Result<Self, WrongoDBError>;
    pub fn open_session(&self) -> Session;
}
```

### `src/session.rs`
```rust
pub struct Session {
    connection: Arc<Connection>,       // shares dhandle cache
    txn: Option<Transaction>,             // single active transaction
    touched_handles: Vec<String>,         // URIs touched by current txn
}

impl Session {
    pub fn begin_transaction(&mut self) -> Result<(), WrongoDBError>;
    pub fn commit_transaction(&mut self) -> Result<(), WrongoDBError>;
    pub fn rollback_transaction(&mut self) -> Result<(), WrongoDBError>;

    pub fn with_transaction<F, T>(&mut self, f: F) -> Result<T, WrongoDBError>
    where F: FnOnce(&mut Session) -> Result<T, WrongoDBError>;

    // WT-style schema operations (URI prefix distinguishes type)
    pub fn create(&mut self, uri: &str, config: &str) -> Result<(), WrongoDBError>;
    pub fn drop(&mut self, uri: &str, config: &str) -> Result<(), WrongoDBError>;

    // Primary access method
    pub fn open_cursor(&mut self, uri: &str, config: &str) -> Result<Cursor, WrongoDBError>;

    // Convenience helpers
    pub fn list(&self) -> Result<Vec<String>, WrongoDBError>;
}
```

### `src/cursor.rs`
```rust
pub enum CursorKind {
    Table { collection_name: String },
    Index { collection_name: String, field: String },
}

pub struct Cursor<'s> {
    session: &'s mut Session,
    kind: CursorKind,
    // Internal: Table or PersistentIndex handle
}

impl Cursor<'_> {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), WrongoDBError>;
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, WrongoDBError>;
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, WrongoDBError>;
    pub fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, WrongoDBError>;
    pub fn reset(&mut self) -> Result<(), WrongoDBError>;
}
```

### `src/catalog/metadata.rs`
```rust
// Stores table/index definitions in a metadata table (WT-like)
// Maps URIs to:
//   - file paths for table/index B-trees
//   - configuration (key/value formats)
//   - index lists (which indexes exist for a table)

pub struct Metadata {
    // Internal: BTree for metadata records
}

impl Metadata {
    pub fn load_or_create(base_path: &Path) -> Result<Self, WrongoDBError>;
    pub fn get_table_config(&self, name: &str) -> Option<TableConfig>;
    pub fn list_tables(&self) -> Vec<String>;
    pub fn register_table(&mut self, name: &str, config: TableConfig) -> Result<(), WrongoDBError>;
    pub fn register_index(&mut self, table: &str, field: &str) -> Result<(), WrongoDBError>;
    // ... more schema operations
}
```

## 3. Internal Structure

### `DataHandle` Layer
```rust
// Internal representation (in `src/connection.rs` or `src/data/handle.rs`)
pub struct DataHandle {
    uri: String,
    table: Option<Table>,              // main table + indexes
    index: Option<PersistentIndex>, // for index cursors
    refcount: u32,
}

pub struct DataHandleCache {
    handles: HashMap<String, DataHandle>,
}

impl DataHandleCache {
    pub fn get_or_open(&mut self, uri: &str, conn: &Connection) -> Result<&mut DataHandle, WrongoDBError>;
    pub fn release(&mut self, uri: &str);
}
```

### `Table` (renamed from `Collection`)
- Location: `src/data/table.rs` (renamed from `src/engine/collection/mod.rs`)
- Internal logic unchanged, but now owned by `DataHandle` instead of `WrongoDB`
- `MainTable` and `SecondaryIndexManager` remain as is

### `Index` Wrapper
- Location: `src/data/index.rs`
- Thin wrapper over `PersistentIndex`/`SecondaryIndexManager`
- Provides table-like API for cursor ops

## 4. URI Scheme

- `table:<name>` → main table for collection (file: `<base>.<name>.main.wt`)
- `index:<table>:<field>` → secondary index (file: `<base>.<name>.<field>.idx.wt`)
- File naming preserved to avoid migration; catalog maps URIs to paths

## 5. Transaction Model

- **Moved to Session**: `Session` owns a single `Option<Transaction>` and a list of touched URIs
- **Removed**: `CollectionTxn` and `MultiCollectionTxn`
- **Commit/Abort**:
  - `Session::commit_transaction()` commits transaction across all touched handles
  - Switches index updates to MVCC (`BTree::put_mvcc/delete_mvcc`) for uniformity
  - On abort, rolls back all touched handles

## 6. Server Integration

- Replace `Arc<Mutex<WrongoDB>>` with `Arc<Connection>`
- One `Session` per request (or per socket)
- Command handlers use:
  ```rust
  let mut session = connection.open_session();
  let mut cursor = session.open_cursor("table:test", "")?;
  cursor.insert(key, value)?;
  ```

## 7. Catalog/Metadata

Two options (choose in implementation):

**Option A: Metadata Table**
- Single B-tree `metadata:` table storing:
  - `table:<name>` → `{ config, indexes: [<field>] }`
  - `index:<table>:<field>` → `{ config }`
- Pros: Consistent with WT; extensible to future catalog features

**Option B: File + Scanning**
- Scan for `<base>.<name>.main.wt` and `<base>.<name>.<field>.idx.wt` files
- No metadata table
- Pros: Simpler initially; easier to debug
- Cons: No transactional schema updates; scans may get messy

**Recommendation**: Start with Option B, migrate to Option A if schema operations become complex

## 8. Migration Steps

1. **Create new files** (`connection.rs`, `session.rs`, `cursor.rs`, `catalog/`)
2. **Rename/move internal logic**:
   - `src/engine/collection/mod.rs` → `src/data/table.rs`
   - Keep `src/storage/main_table.rs` and `src/index/` unchanged
   - Update internal imports across codebase
3. **Update `src/lib.rs` exports**:
   - Remove `WrongoDB`, `Collection`, `CollectionTxn`, `MultiCollectionTxn`
   - Add `Connection`, `Session`, `Cursor`, `ConnectionConfig`
4. **Update server**:
   - Replace `WrongoDB` → `Connection`
   - Update command handlers to use `Session` + `Cursor`
5. **Update tests**:
   - Rewrite `tests/engine`, `tests/server` to use new API
   - Keep `tests/storage` and `tests/perf` unchanged
6. **Add decision entry** to `docs/decisions.md`

## 9. Decisions to Record

- **Session over Collection**: Public API is Connection → Session → Cursor; collections exist only as internal `Table` objects
- **URI Scheme**: `table:<name>` and `index:<table>:<field>` for schema ops; matches WT style
- **Transaction Ownership**: Sessions own transaction state and track touched data handles; no separate txn types
- **Catalog Choice**: File-based scanning initially; metadata table deferred until schema complexity requires it
- **File Naming**: Keep existing `.main.wt` and `.idx.wt` extensions; map in catalog rather than rename

## 10. Post-Migration State

- Clean API surface: 3 public types (`Connection`, `Session`, `Cursor`) + `ConnectionConfig`
- Internal separation: `data/` owns table/index logic; `connection/session/cursor` own API glue
- Test coverage: All engine tests use new session/cursor API
- Documentation updated: `README.md`, `docs/server.md` reflect new patterns

---

## Implementation Log

### Slice 1: Basic Connection → Session → Cursor (Table-only, no transactions)

**Goal**: Get a minimal working Connection API that can:
1. Open a database connection
2. Create a session
3. Open a cursor on a table
4. Insert and get documents

**Files to Create/Modify:**

```
src/
├── connection.rs          # NEW: Connection, ConnectionConfig, DataHandleCache
├── session.rs             # NEW: Session (minimal - no txn yet)
├── cursor.rs              # NEW: Cursor (Table-only)
├── data/
│   ├── mod.rs             # NEW: module exports
│   └── table.rs           # NEW: move from engine/collection/mod.rs
└── lib.rs                 # MODIFY: export new types
```

**API for this slice:**

```rust
// connection.rs
pub struct ConnectionConfig { pub wal_enabled: bool; }
pub struct Connection { ... }
impl Connection {
    pub fn open<P>(path: P) -> Result<Self>;
    pub fn open_session(&self) -> Session;
}

// session.rs
pub struct Session { ... }
impl Session {
    pub fn create(&mut self, uri: &str) -> Result<()>;  // "table:<name>"
    pub fn open_cursor(&mut self, uri: &str) -> Result<Cursor<'_>>;
}

// cursor.rs
pub struct Cursor<'s> { ... }
impl Cursor<'_> {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;
}
```

**Test to verify the slice:**

```rust
#[test]
fn test_connection_basic() {
    let tmp = tempdir().unwrap();
    let conn = Connection::open(tmp.path()).unwrap();

    let mut session = conn.open_session();
    session.create("table:test").unwrap();

    let mut cursor = session.open_cursor("table:test").unwrap();
    cursor.insert(b"key1", b"value1").unwrap();

    let value = cursor.get(b"key1").unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
}
```

**Key decisions for this slice:**

1. **Catalog**: Use Option B (file scanning) - simplest for now
2. **DataHandle**: Cache opened tables in Connection
3. **Table**: Move existing Collection logic, strip down to just main table operations
4. **No transactions yet**: Add in next slice
5. **No indexes yet**: Add in next slice
6. **Raw bytes API**: Work with raw bytes first, add BSON/Document layer later

**Status**: Completed (2025-02-02)

**Implementation Notes**:
- Used `parking_lot::RwLock` for concurrent access to DataHandleCache
- Connection is shared across sessions via `Arc<Connection>`
- DataHandle refcount is simple `u32` (no additional locking needed)
- All e2e tests pass (test_connection_basic, test_connection_with_config, test_cursor_delete)
- All existing tests still pass (86 total)
