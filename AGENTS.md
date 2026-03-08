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

#### What Gets Documented

**Always document:**
- Public structs, enums, and type aliases - explain what they represent and why
- Public functions/methods - especially constructors and key API methods
- Trait definitions and their purpose
- Error types and their variants

**Do NOT document:**
- Private helper functions - let code be self-documenting
- Simple getters/setters - `fn page_id(&self) -> u64` is obvious
- Obvious implementation details
- Module-level (`//!`) comments - not used in this codebase

#### Docstring Format

Use `///` comments with this structure:

```rust
/// [Single-line summary - what the thing is]
///
/// [Explanation of what it does, architectural context, design decisions]
///
/// [Optional: Bulleted list of key behaviors]
/// - **Feature 1**: Description
/// - **Feature 2**: Description
```

**Example:**
```rust
/// File-backed page store with copy-on-write semantics and page caching.
///
/// `BlockFilePageStore` sits between the B+tree layer and the block file,
/// providing:
///
/// - **Page caching**: LRU cache with pin/unpin semantics
/// - **Copy-on-write (COW)**: Modifications create new pages
/// - **Checkpoint coordination**: Tracks working pages for flush operations
pub struct BlockFilePageStore { ... }
```

#### Documentation Principles

1. **Explain "Why", not just "What"** - Design decisions and rationale matter
2. **Use intra-doc links** - Reference related types with [`TypeName`]
3. **No code examples in docs** - Put examples in `tests/` directory
4. **No boilerplate** - Don't document obvious things

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
- `src/storage/`: on-disk storage (block file, page store, B-tree, WAL, table)
- `src/engine/`: database API and collection logic
- `src/server/`: MongoDB wire-protocol server and command handlers
- `src/txn/`: transaction management, recovery, MVCC
- `src/index/`: secondary index implementation

### Clean Code: Step-Down Rule

**CRITICAL**: All code must follow the **Step-Down Rule** from Robert C. Martin's *Clean Code*. Code should read like a top-down narrative:

1. **High-level abstractions at the top** - Public APIs, "what" the code does
2. **Implementation details below** - Private methods, "how" it works
3. **Lowest-level utilities at the bottom** - Helper functions, primitives

#### File Structure Pattern

```rust
// Imports (std, then external crates, then local)

// ============================================================================
// Constants
// ============================================================================

const MAGIC: [u8; 8] = *b"MMWT0001";
const VERSION: u16 = 3;

// ============================================================================
// High-level types (public abstractions)
// ============================================================================

pub struct MyType { ... }

impl MyType {
    // ------------------------------------------------------------------------
    // Constructors (highest level)
    // ------------------------------------------------------------------------
    pub fn new(...) -> Result<Self> { ... }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------
    pub fn process(&mut self) -> Result<()> { ... }

    // ------------------------------------------------------------------------
    // Private helpers (lowest level)
    // ------------------------------------------------------------------------
    fn validate(&self) -> bool { ... }
}

// ============================================================================
// Helper functions (utilities)
// ============================================================================

fn utility(...) -> Result<()> { ... }
```

#### impl Block Organization

Within each `impl` block, group methods by purpose and abstraction level:

1. **Constructors** - `new()`, `create()`, `open()`
2. **Lifecycle methods** - `close()`, `sync()`, `flush()`
3. **Public API methods** - grouped by functionality (e.g., all allocation methods together)
4. **Private helpers** - internal implementation details

**Example**:
```rust
impl BlockFile {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------
    pub fn create<P: AsRef<Path>>(path: P, page_size: usize) -> Result<Self> { ... }
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> { ... }

    // ------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------
    pub fn close(self) -> Result<(), WrongoDBError> { ... }
    pub fn sync_all(&mut self) -> Result<(), WrongoDBError> { ... }

    // ------------------------------------------------------------------------
    // Block allocation
    // ------------------------------------------------------------------------
    pub fn allocate_block(&mut self) -> Result<u64, WrongoDBError> { ... }
    pub fn free_block(&mut self, block_id: u64) -> Result<(), WrongoDBError> { ... }

    // ------------------------------------------------------------------------
    // Block I/O
    // ------------------------------------------------------------------------
    pub fn read_block(&mut self, block_id: u64, verify: bool) -> Result<Vec<u8>> { ... }
    pub fn write_block(&mut self, block_id: u64, payload: &[u8]) -> Result<()> { ... }

    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------
    fn write_header(&mut self) -> Result<(), WrongoDBError> { ... }
    fn num_blocks(&mut self) -> Result<u64, WrongoDBError> { ... }
}
```

#### Section Comment Style

Use consistent comment formatting:
- Major sections: `// ============================================================================`
- Subsections within impl: `// --------`
- Purpose labels: `// Constructors`, `// Public API`, `// Private helpers`

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
