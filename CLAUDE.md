# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WrongoDB is a learning-oriented MongoDB-like database written in Rust. It implements a WiredTiger-inspired storage engine with B+trees, WAL, checkpoints, and secondary indexes. The architecture is intentionally educational—starting simple and evolving toward production patterns.

## Common Commands

### Build and Check
```bash
# Build the project
cargo build

# Fast check (no codegen)
cargo check

# Check with just
just check
```

### Test
```bash
# Run all tests
cargo test
just test

# Run a specific test
cargo test insert_and_find_roundtrip
cargo test storage::block_file

# Run tests in a specific file
cargo test --test storage_suite
cargo test --test engine_suite
```

### Lint and Format
```bash
# Run clippy (treats warnings as errors)
cargo clippy -- -D warnings
just clippy

# Format code
cargo fmt
just fmt

# Run all checks (check + test + clippy + fmt)
just all
```

### Development Server
```bash
# Run the MongoDB wire-protocol server
cargo run --bin server

# Run with custom address
cargo run --bin server -- 127.0.0.1:27019

# With auto-reload (requires cargo-watch)
just dev-server

# Connect with mongosh
mongosh mongodb://localhost:27017
```

### Benchmarks
```bash
# Run insert latency benchmarks
cargo bench
```

### Bacon (file watcher)
```bash
# Watch and check (default)
bacon

# Watch and test
bacon test

# Watch and run clippy
bacon clippy
```

## High-Level Architecture

### Source Layout

```
src/
├── api/         # Connection/session/cursor APIs and handle cache
├── core/        # Shared types: BSON codec, document helpers, errors
├── storage/     # On-disk storage engine (WiredTiger-inspired)
│   ├── block/      # Block file I/O, allocation, free lists
│   ├── page_store/ # Page cache, COW, checkpoint coordination
│   ├── btree/      # B+tree implementation, splits, range scans
│   ├── table.rs    # Document storage via BTree
│   └── wal.rs      # Connection-level global WAL
├── index/       # Secondary indexes, key encoding
├── engine/      # Database API and collection logic
│   ├── database.rs    # WrongoDB handle, config
│   └── collection/  # Collection operations, checkpointing
├── server/      # MongoDB wire-protocol server
│   └── commands/    # Command handlers (find, insert, update, delete)
└── bin/
    └── server.rs    # Server binary entry point
```

### Storage Engine Architecture

The storage layer follows WiredTiger's design patterns:

**BlockFile** (`storage/block/file.rs`): Fixed-size page I/O with checksums. Manages extent allocation (alloc/avail/discard lists) and checkpoint slots.

**BlockFilePageStore** (`storage/page_store/store.rs`): Page cache layer between BTree and BlockFile. Implements copy-on-write (COW), dirty tracking, and checkpoint coordination.

**BTree** (`storage/btree/mod.rs`): B+tree with leaf/internal pages, splits, and range scans. Owns the WAL handle and orchestrates recovery.

**WAL** (`storage/wal.rs`): Connection-level logical logging (put/delete operations, not pages). Recovery replays through normal BTree writes.

**MainTable** (`storage/table.rs`): Document storage layer. Encodes documents as BSON, uses `_id` as BTree key.

### Engine Layer

**WrongoDB** (`engine/database.rs`): Database handle. Manages multiple collections. Configuration via `WrongoDBConfig`.

**Collection** (`engine/collection/mod.rs`): Primary API for CRUD operations. Owns:
- One `MainTable` for document storage (BTree-based)
- Multiple `SecondaryIndex` instances for indexed fields

Checkpoint scheduling happens at the Collection layer to ensure main table and secondary indexes checkpoint together.

### Key Data Flows

**Insert**: `Collection::insert_one` → `MainTable::insert` (BTree put) → WAL append → dirty page in cache

**Query by _id**: `Collection::find_one` with `_id` filter → `MainTable::get` → BTree get → page cache → BlockFile

**Query by indexed field**: `Collection::find` → secondary index lookup → main table fetches by `_id`

**Checkpoint**: `Collection::checkpoint` → main table checkpoint + each secondary index checkpoint → atomically swap stable roots

## Environment Variables

```bash
# Server address override (in priority order):
# 1. CLI arg: cargo run --bin server -- 127.0.0.1:27019
# 2. WRONGO_ADDR=127.0.0.1:27019
# 3. WRONGO_PORT=27019  (binds 127.0.0.1)

# Preallocate pages for new BTree files (avoids ftruncate in hot path)
WRONGO_PREALLOC_PAGES=20000
```

## Test Organization

```
tests/
├── storage_suite.rs
├── engine_suite.rs
├── server_suite.rs
├── smoke_suite.rs
├── connection_suite.rs
├── storage/
│   ├── mod.rs
│   ├── block_file.rs
│   ├── iterator_safety.rs
│   └── btree/      # BTree-specific tests
├── engine/
│   ├── mod.rs
│   ├── crud.rs
│   ├── checkpoint.rs
│   ├── indexes.rs
│   └── transactions.rs
├── server/
│   └── protocol.rs
└── connection/
    └── basic.rs
```

Tests use `tempfile::tempdir()` for isolation. See `tests/engine/crud.rs` for roundtrip examples.

## Design Decisions

Important architectural decisions are recorded in `docs/decisions.md`. Recent key decisions:

- **Logical WAL**: Logs operations (put/delete) not pages. Recovery replays through BTree writes.
- **Collection-owned checkpointing**: Automatic checkpoint scheduling at Collection layer ensures main table + indexes stay consistent.
- **Composite index keys**: Secondary indexes encode as `(field_value, _id)` to handle duplicates without posting lists.
- **Extent-based allocation**: Three lists (alloc = reachable from stable checkpoint, avail = free, discard = free after checkpoint).

## Configuration

```rust
use wrongodb::{WrongoDB, WrongoDBConfig};

let config = WrongoDBConfig::new()
    .wal_enabled(true)
    .checkpoint_after_updates(100);  // auto-checkpoint every 100 updates

let db = WrongoDB::open_with_config("data.db", config)?;
let coll = db.collection("test")?;
```

## File Formats

Collection files use the naming convention `{base}.{collection}.main.wt` for the main table and `{base}.{collection}.{field}.idx.wt` for secondary indexes.

The append-only log legacy format (`{base}.{collection}.log`) is deprecated in favor of BTree-based storage.

## Code Organization Standards

### Clean Code: Step-Down Rule

This project follows the **Step-Down Rule** from Robert C. Martin's *Clean Code*. Code should read like a top-down narrative, where:

1. **High-level abstractions come first** - Public APIs and "what" the code does
2. **Implementation details follow** - Private methods and "how" it works
3. **Lowest-level utilities at the bottom** - Helper functions and primitives

#### File Structure Template

```rust
// Imports
use std::...;

// ============================================================================
// Constants (highest-level, most stable)
// ============================================================================

const MAGIC: [u8; 8] = *b"MMWT0001";
const VERSION: u16 = 3;

// ============================================================================
// High-level types (public abstractions)
// ============================================================================

pub struct MyType { ... }

impl MyType {
    // ------------------------------------------------------------------------
    // Public API (highest level of abstraction)
    // ------------------------------------------------------------------------
    pub fn new(...) -> Result<Self> { ... }
    pub fn do_something(&mut self) -> Result<()> { ... }

    // ------------------------------------------------------------------------
    // Implementation helpers
    // ------------------------------------------------------------------------
    fn internal_helper(&self) -> usize { ... }
}

// ============================================================================
// Helper functions (lowest-level utilities)
// ============================================================================

fn utility_function(...) -> Result<()> { ... }
```

#### impl Block Organization

Within `impl` blocks, group methods by abstraction level:

1. **Constructors** - `new()`, `create()`, `open()`
2. **Public API methods** - What users can do with the type
3. **Internal helpers** - Private methods used by public API
4. **Getters/Setters** - Simple property access

Each section should be marked with a comment header:
```rust
impl MyType {
    // ------------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------------
    pub fn new(...) -> Self { ... }

    // ------------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------------
    pub fn process(&mut self) -> Result<()> { ... }

    // ------------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------------
    fn validate(&self) -> bool { ... }
}
```

#### Related Grouping

Keep related functionality together:
- All allocation methods in one section
- All I/O methods in another section
- All lifecycle methods (open/close/sync) together

This organization makes the file **skimmable** - readers can quickly find what they need by looking at the section headers.

### Documentation Style

#### What Gets Documented

**Always document:**
- Public structs, enums, and type aliases - explain what they represent and why they exist
- Public functions/methods - especially constructors and key API methods
- Trait definitions and their purpose
- Error types and their variants

**Do NOT document:**
- Private helper functions - let code be self-documenting
- Simple getters/setters - `fn page_id(&self) -> u64` is obvious
- Obvious implementation details - trust the code
- Module-level (`//!`) comments - not used in this codebase

#### Docstring Format

Use `///` comments with this structure:

```rust
/// [Single-line summary - what the thing is]
///
/// [One or more paragraphs explaining what it does, how it fits
/// into the architecture, and important design decisions]
///
/// [Optional: Bulleted list of key behaviors or features]
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
/// - **Page caching**: LRU cache with pin/unpin semantics to prevent eviction
///   of in-use pages
/// - **Copy-on-write (COW)**: Modifications create new pages, leaving originals
///   intact for crash recovery and checkpointing
/// - **Checkpoint coordination**: Tracks working pages and coordinates flush
///   operations with the block file's extent allocation
pub struct BlockFilePageStore { ... }
```

#### Documentation Philosophy

1. **Explain "Why", not just "What"** - Design decisions and rationale matter more than mechanics
2. **Tell a story** - Docs should read like a narrative explaining how pieces fit together
3. **Educational focus** - This is a learning-oriented codebase; explain storage engine concepts
4. **Use intra-doc links** - Reference related types with [`TypeName`] for navigation
5. **No code examples in docs** - Put examples in `tests/` directory instead
6. **No boilerplate** - Don't document obvious things; respect the reader's time


# Reference implementation

You can find the repositories to compare the implementation of wrongodb with the reference implementation of WiredTiger and MongoDB:

- WiredTiger: ~/workspace/wiredtiger
- MongoDB: ~/workspace/mongodb
