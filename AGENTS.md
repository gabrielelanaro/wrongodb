# Repo agent guidelines

## Decisions log
- When making a non-trivial design/behavior decision (API semantics, file formats, invariants), record it in `docs/decisions.md`.
- Add an entry even if the change is “small” but affects persistence, crash-safety, or public APIs.

## Notes
- Use `NOTES.md` for investigation notes and research summaries.

## Communication style
- Use normal paragraph style for explanations and reviews unless asked otherwise.
- If asked for explanations or reviews with structure, use short, meaningful section titles (not "Chunk N") and small sections; include ASCII diagrams where helpful.

## Architecture guidelines

### Avoid circular indirection and unnecessary abstraction

**Problem**: Modules holding references to "parent" or "context" objects when they only need specific data leads to:
- Circular dependencies (A depends on B, B depends on A)
- Unclear ownership and semantics (what does it mean to "clone" a connection?)
- Indirection through wrapper structs when values would suffice

**Examples to avoid**:

```rust
// BAD: Session holds entire Connection, creating circular dependency
struct Session {
    connection: Arc<Connection>,
}

// BAD: DataHandleCache depends on Connection wrapper
impl DataHandleCache {
    fn get_or_open(&self, uri: &str, connection: &Connection) { ... }
}

// BAD: Unnecessary wrapper struct that just groups fields
struct SessionConfig {
    base_path: PathBuf,
    wal_enabled: bool,
    // ...
}
```

**Preferred approach**: Pass only what's needed explicitly

```rust
// GOOD: Session holds exactly what it needs
struct Session {
    cache: Arc<DataHandleCache>,
    base_path: PathBuf,
    wal_enabled: bool,
    // ...
}

// GOOD: DataHandleCache takes explicit parameters, no dependency on Connection
impl DataHandleCache {
    fn get_or_open(
        &self,
        uri: &str,
        base_path: &Path,
        wal_enabled: bool,
        checkpoint_after_updates: Option<usize>,
        global_txn: Arc<GlobalTxnState>,
    ) { ... }
}
```

**Guidelines**:
1. **Explicit parameters over context objects**: If a function needs data, pass it directly instead of requiring access to a parent/context object
2. **Minimal ownership**: Structs should hold only the data they actually use
3. **Avoid wrapper structs**: Don't create structs just to group parameters unless they're used in multiple places
4. **Watch for circular dependencies**: If module A imports module B and B imports A (even indirectly), reconsider the design
5. **Remove unused access**: If fields are marked `pub(crate)` or `pub` but never accessed from other modules, make them private

# Reference implementation

You can find the repositories to compare the implementation of wrongodb with the reference implementation of WiredTiger and MongoDB:

- WiredTiger: ~/workspace/wiredtiger
- MongoDB: ~/workspace/mongo
