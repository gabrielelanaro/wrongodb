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
- API: `insert_one`, `find_one`, `find` with top-level equality filters.
- Single-process, no concurrency controls (yet!).

This repo evolves step by step. Check [PLAN.md](PLAN.md) to see where we are going (WiredTiger-inspired storage engine).

### Quickstart

#### As a Library
```rust
use serde_json::json;
use wrongodb::WrongoDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = WrongoDB::open("data/db.log", ["name"], false)?;
    db.insert_one(json!({"name": "alice", "age": 30}))?;
    db.insert_one(json!({"name": "bob", "age": 25}))?;

    println!("{:?}", db.find(None)?); // all docs
    println!("{:?}", db.find(Some(json!({"name": "bob"})))?); // equality on indexed field
    Ok(())
}
```

#### As a Server
For interactive usage with mongosh, see [Server Documentation](docs/server.md).

Run tests with `cargo test`.

## License

MIT
