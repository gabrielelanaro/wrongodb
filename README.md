# WrongoDB

The database that teaches you how databases work by doing everything wrong first, then fixing it.

<p align="center">
  <img src="logo.png" alt="WrongoDB logo" width="280" />
</p>

## Philosophy: Learning by Breaking Things

WrongoDB is a journey into the internals of database systems. Instead of trying to build a production-ready database from scratch, we start with the simplest, most naive implementations possible—approaches that are often "wrong" for a real-world system—and evolve them.

By starting with "wrong" (but working) solutions, we can visualize the problems they cause (performance, data integrity, concurrency) and then implement the "right" solutions (WAL, B-trees, MVCC) to solve those specific problems.

It is a learning resource, a playground, and a documentation of the path from "naive JSON file" to "robust storage engine".

## Current State

A tiny, learning-oriented MongoDB-like store written in Rust.

### Features
- Append-only log on disk (newline-delimited JSON).
- In-memory index rebuilt at startup.
- API: `collection("name")` with `insert_one`, `find_one`, `find` and top-level equality filters.
- Single-process, no concurrency controls (yet!).

This repo evolves step by step. Check [PLAN.md](PLAN.md) to see where we are going (WiredTiger-inspired storage engine).

## Source Layout

The codebase is organized by domain to keep storage, engine, and server concerns separate:

- `src/core/`: shared types/utilities (BSON codec, document helpers, errors).
- `src/storage/`: on-disk storage (block file, B-tree, WAL, main table).
- `src/index/`: secondary index implementation and key encoding.
- `src/engine/`: database API and collection logic.
- `src/server/`: MongoDB wire-protocol server and command handlers.

Integration tests are grouped under `tests/` with entry points (e.g. `tests/storage.rs`) that include submodules in `tests/storage/`, `tests/engine/`, etc.

### Quickstart

#### As a Library
```rust
use serde_json::json;
use wrongodb::WrongoDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database (WAL enabled by default)
    let mut db = WrongoDB::open("data/db.log")?;

    // Get collection
    let mut coll = db.collection("test")?;

    // Create indexes for faster queries (optional)
    coll.create_index("name")?;

    // Insert documents
    coll.insert_one(json!({"name": "alice", "age": 30}))?;
    coll.insert_one(json!({"name": "bob", "age": 25}))?;

    // Query (uses index if available, otherwise scans)
    println!("{:?}", coll.find(None)?); // all docs
    println!("{:?}", coll.find(Some(json!({"name": "bob"})))?);
    Ok(())
}
```

#### As a Server
For interactive usage with mongosh, see [Server Documentation](docs/server.md).

Run tests with `cargo test`.

## License

MIT
