# Repo agent guidelines

## Build/lint/test commands

### Core commands
- `cargo build` - build the project
- `cargo test` - run all unit and integration tests
- `cargo test <testname>` - run tests matching a specific name/pattern
- `cargo test -- <filter>` - filter test names at runtime
- `cargo check` - compile check (faster than build, no code generation)
- `cargo clippy -- -D warnings` - run clippy linter (warnings treated as errors)
- `cargo fmt` - format code with rustfmt

### Justfile shortcuts
- `just build` / `just check` / `just test` / `just clippy` / `just fmt`
- `just all` - run all checks (check, test, clippy, fmt)
- `just dev-server` - run server with auto-reload (cargo watch)

### Running single tests
Use the test function name as a filter:
- `cargo test test_insert_one` - runs tests containing "test_insert_one"
- `cargo test transaction::tests::test_commit` - runs a specific module test
- `cargo test -- --exact test_name` - exact match on test name

Integration tests are in `tests/` directory and can be run similarly.

## Code style guidelines

### Imports
Order: std imports first, then third-party crates, then local crate imports (grouped with empty lines).

### Naming conventions
- Functions, variables, modules: `snake_case`
- Types, structs, enums: `PascalCase`
- Constants: `SCREAMING_SNAKE_CASE`
- Private fields: avoid prefixes (e.g., use `id` not `_id` unless needed)
- Use type aliases for clarity: `type Key = Vec<u8>`, `type Timestamp = u64`

### Error handling
- Use `thiserror` derive for error enums
- Return `Result<T, WrongoDBError>` for fallible operations
- Use `#[from]` attribute for automatic error conversion when appropriate
- Custom error types for domain-specific validation: `StorageError`, `DocumentValidationError`

### Documentation
- Use `///` for public API documentation
- Keep docs concise; let code be self-documenting where possible

### Types and structs
- Prefer explicit field types over generics when clarity is needed
- Use `#[derive(Debug, Clone, PartialEq, Eq)]` for data-carrying types
- Mark fields `pub(crate)` only when needed by sibling modules
- Use `pub` only for true public API

### Concurrency
- Use `parking_lot::RwLock` instead of `std::sync::RwLock` (lower overhead)
- Wrap shared state in `Arc<RwLock<T>>` for read-write access
- Use `parking_lot::Mutex` for exclusive access when RwLock not needed

### Re-exports
- Re-export public types at `lib.rs` and module levels for clean API
- Use `#[allow(unused_imports)]` on re-exports when they're for consumers only

### Tests
- Unit tests: use `#[cfg(test)]` modules within source files
- Integration tests: place in `tests/` with modular subdirectories
- Use `tempfile` for test fixtures
- Test names follow pattern: `test_<feature>`, `test_<feature>_<scenario>`

### General Rust patterns
- Use `Result<T, WrongoDBError>` over `Option<T>` for errors that need context
- Prefer `iter().collect::<Vec<_>>()` over manual loops when appropriate
- Use `?` operator for error propagation
- Avoid `unwrap()` and `expect()` in production code (ok in tests)
- Use `#[allow(dead_code)]` temporarily for work-in-progress fields

### File organization
- `src/core/`: shared types/utilities (BSON, document helpers, errors)
- `src/storage/`: on-disk storage (block file, B-tree, WAL, table)
- `src/engine/`: database API and collection logic
- `src/server/`: MongoDB wire-protocol server and command handlers
- `src/txn/`: transaction management, recovery, MVCC
- `src/index/`: secondary index implementation

## Decisions log
- When making a non-trivial design/behavior decision (API semantics, file formats, invariants), record it in `docs/decisions.md`.
- Add an entry even if the change is "small" but affects persistence, crash-safety, or public APIs.

## Notes
- Use `NOTES.md` for investigation notes and research summaries.

## Communication style
- Use normal paragraph style for explanations and reviews unless asked otherwise.
- If asked for explanations or reviews with structure, use short, meaningful section titles (not "Chunk N") and small sections; include ASCII diagrams where helpful.

## Architecture guidelines

### Avoid circular indirection and unnecessary abstraction

Modules holding references to "parent" or "context" objects when they only need specific data leads to circular dependencies and unclear ownership. Pass only what's needed explicitly.

**BAD**:
```rust
struct Session { connection: Arc<Connection> }
impl DataHandleCache { fn get_or_open(&self, uri: &str, connection: &Connection) { ... } }
```

**GOOD**:
```rust
struct Session { cache: Arc<DataHandleCache>, base_path: PathBuf, wal_enabled: bool }
impl DataHandleCache { fn get_or_open(&self, uri: &str, base_path: &Path, wal_enabled: bool) { ... } }
```

**Guidelines**:
1. Explicit parameters over context objects
2. Minimal ownership - structs hold only what they use
3. Avoid wrapper structs unless used in multiple places
4. Watch for circular dependencies between modules
5. Make fields private if never accessed from other modules

# Reference implementation

You can find the repositories to compare the implementation of wrongodb with the reference implementation of WiredTiger and MongoDB:

- WiredTiger: ~/workspace/wiredtiger
- MongoDB: ~/workspace/mongo
