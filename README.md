# WrongoDB

A tiny, learning-oriented MongoDB-like store written in Rust.

### Thin-slice scope
- Append-only log on disk (newline-delimited JSON).
- In-memory index rebuilt at startup.
- API: `insert_one`, `find_one`, `find` with top-level equality filters.
- Single-process, no concurrency controls in the first iteration.

This repo evolves step by step with small, focused commits to make the design easy to follow.

### Quickstart (current state)
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

Run tests with `cargo test`.
