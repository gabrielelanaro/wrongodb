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
cargo test storage_tests::blockfile

# Run tests in a specific file
cargo test --test storage
cargo test --test engine
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
├── core/        # Shared types: BSON codec, document helpers, errors
├── storage/     # On-disk storage engine (WiredTiger-inspired)
│   ├── block/   # Block file I/O, allocation, free lists
│   ├── btree/   # B+tree implementation, page cache, WAL
│   └── main_table.rs  # Document storage via BTree
├── index/       # Secondary indexes, key encoding
├── engine/      # Database API and collection logic
│   ├── db.rs    # WrongoDB handle, config
│   └── collection/  # Collection operations, checkpointing
├── server/      # MongoDB wire-protocol server
│   └── commands/    # Command handlers (find, insert, update, delete)
└── bin/
    └── server.rs    # Server binary entry point
```

### Storage Engine Architecture

The storage layer follows WiredTiger's design patterns:

**BlockFile** (`storage/block/file.rs`): Fixed-size page I/O with checksums. Manages extent allocation (alloc/avail/discard lists) and checkpoint slots.

**Pager** (`storage/btree/pager.rs`): Page cache between BTree and BlockFile. Implements copy-on-write (COW), dirty tracking, and checkpoint coordination.

**BTree** (`storage/btree/mod.rs`): B+tree with leaf/internal pages, splits, and range scans. Owns the WAL handle and orchestrates recovery.

**WAL** (`storage/btree/wal.rs`): Logical logging (put/delete operations, not pages). Recovery replays through normal BTree writes.

**MainTable** (`storage/main_table.rs`): Document storage layer. Encodes documents as BSON, uses `_id` as BTree key.

### Engine Layer

**WrongoDB** (`engine/db.rs`): Database handle. Manages multiple collections. Configuration via `WrongoDBConfig`.

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
├── storage.rs      # Entry point for storage tests
├── storage/
│   ├── mod.rs      # Re-exports as `storage_tests`
│   ├── blockfile.rs
│   ├── iterator_safety.rs
│   └── btree/      # BTree-specific tests
├── engine.rs       # Entry point for engine tests
└── engine/
    ├── mod.rs      # Integration tests for Collection/WrongoDB
    ├── collection_checkpoint.rs
    └── persistent_secondary_index.rs
```

Tests use `tempfile::tempdir()` for isolation. See `tests/engine/mod.rs` for examples of roundtrip tests.

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


# Reference implementation

You can find the repositories to compare the implementation of wrongodb with the reference implementation of WiredTiger and MongoDB:

- WiredTiger: ~/workspace/wiredtiger
- MongoDB: ~/workspace/mongo
