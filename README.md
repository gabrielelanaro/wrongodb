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

A learning-oriented MongoDB-compatible database in Rust, inspired by WiredTiger's architecture.

### Storage Engine
- **B+tree storage**: Fixed-size paged files with slotted leaf/internal pages, arbitrary height, splits, range scans
- **Page cache**: LRU eviction, pin/unpin API, dirty tracking, copy-on-write updates
- **Checkpoints**: Atomic root swap via checkpoint slots, dirty page flush, retired block reuse
- **WAL (Write-Ahead Logging)**: Global WAL with LSNs, CRC32 checksums, change-vector logging, recovery replay
- **MVCC/Transactions**: Global transaction state, snapshot isolation, version chains, commit/abort

### API
- `Connection`/`Session`/`Cursor` low-level storage API
- Transaction-scoped reads/writes via `Session::transaction()`
- MongoDB wire protocol server (works with `mongosh`)

### Future Work
- Background maintenance: space reuse, compaction, compression

## Source Layout

The codebase is organized by domain to keep storage and server concerns separate:

- `src/api/`: connection/session/cursor APIs and handle cache.
- `src/core/`: shared types/utilities (BSON codec, document helpers, errors).
- `src/storage/`: on-disk storage (block file, B-tree, WAL, main table).
- `src/index/`: secondary index implementation and key encoding.
- `src/document_ops/`: internal document CRUD/index orchestration used by server handlers.
- `src/server/`: MongoDB wire-protocol server and command handlers.

Integration tests are grouped under `tests/` with explicit suite entrypoints:
`tests/storage_suite.rs`, `tests/server_suite.rs`, and `tests/connection_suite.rs`.
Each suite loads domain folders such as `tests/storage/`.

## Install

Download and install the prebuilt server binary from GitHub Releases (Linux/macOS):

```bash
curl -sSL https://github.com/gabrielelanaro/wrongodb/releases/latest/download/wrongodb-installer.sh | sh
```

This installer installs the `wrongodb-server` binary.

### Quickstart

#### As a Library
```rust
use wrongodb::{Connection, ConnectionConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open connection (WAL enabled by default)
    let conn = Connection::open("data/db", ConnectionConfig::default())?;
    let mut session = conn.open_session();

    // Execute one transactional write unit
    let mut txn = session.transaction()?;
    let txn_id = txn.as_ref().id();
    let mut cursor = txn.session_mut().open_cursor("table:test")?;
    cursor.insert(b"alice", b"age=30", txn_id)?;
    cursor.insert(b"bob", b"age=25", txn_id)?;
    txn.commit()?;

    // Read back values
    let mut cursor = session.open_cursor("table:test")?;
    println!("{:?}", cursor.get(b"alice", 0)?);
    println!("{:?}", cursor.get(b"bob", 0)?);
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
