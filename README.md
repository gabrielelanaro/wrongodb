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

- `src/api/`: connection/session/cursor APIs and handle cache.
- `src/core/`: shared types/utilities (BSON codec, document helpers, errors).
- `src/storage/`: on-disk storage (block file, B-tree, WAL, main table).
- `src/index/`: secondary index implementation and key encoding.
- `src/engine/`: database API and collection logic.
- `src/server/`: MongoDB wire-protocol server and command handlers.

Integration tests are grouped under `tests/` with explicit suite entrypoints:
`tests/engine_suite.rs`, `tests/storage_suite.rs`, `tests/server_suite.rs`,
`tests/smoke_suite.rs`, and `tests/connection_suite.rs`.
Each suite loads domain folders such as `tests/engine/` and `tests/storage/`.

## Install

Download and install the prebuilt server binary from GitHub Releases (Linux/macOS):

```bash
curl -sSL https://github.com/gabrielelanaro/wrongodb/releases/latest/download/wrongodb-installer.sh | sh
```

This installer installs the `wrongodb-server` binary.

### Quickstart

#### As a Library
```rust
use serde_json::json;
use wrongodb::WrongoDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database (WAL enabled by default)
    let db = WrongoDB::open("data/db")?;

    // Get collection
    let coll = db.collection("test");
    let mut session = db.open_session();

    // Create indexes for faster queries (optional)
    coll.create_index(&mut session, "name")?;

    // Insert documents
    coll.insert_one(&mut session, json!({"name": "alice", "age": 30}))?;
    coll.insert_one(&mut session, json!({"name": "bob", "age": 25}))?;

    // Query (uses index if available, otherwise scans)
    println!("{:?}", coll.find(&mut session, None)?); // all docs
    println!("{:?}", coll.find(&mut session, Some(json!({"name": "bob"})))?);
    Ok(())
}
```

#### As a Server
For interactive usage with mongosh, see [Server Documentation](docs/server.md).

Run the server directly:

```bash
wrongodb-server
```

You can also set the listen address with `--addr`, `--port`, `WRONGO_ADDR`, or `WRONGO_PORT`.
For benchmark isolation, you can set data path with `--db-path` or `WRONGO_DB_PATH`.

Run tests with `cargo test`.

## Benchmarking

Run wire-protocol A/B benchmarks (WrongoDB vs MongoDB via Docker):

```bash
just bench-wire-ab
```

This runs `bench_wire_ab` with defaults:

- warmup: `15s`
- measure: `60s`
- concurrency: `1,4,8,16,32,64`
- scenario: `all` (`insert_unique` and `update_hotspot`)
- repetitions: `3`
- mongo image: `mongo:7.0`

Artifacts are written to:

- `target/benchmarks/wire_ab/results.csv`
- `target/benchmarks/wire_ab/gate.json`
- `target/benchmarks/wire_ab/summary.md`

## Release

Create a version tag and push it to trigger the GitHub Actions release workflow:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The workflow builds artifacts and publishes them to GitHub Releases.

## License

MIT
