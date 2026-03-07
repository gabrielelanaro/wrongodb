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

### Replication
- **RAFT**: Multi-node distributed consensus for replicated writes

### API
- `Connection`/`Session`/`Cursor` low-level storage API
- Transaction-scoped reads/writes via `Session::transaction()`
- MongoDB wire protocol server (works with `mongosh`)

`Cursor` is intentionally a local/store-level API. In clustered RAFT mode, replicated writes go
through the server/internal collection write path, not through low-level cursor writes.

### Future Work
- Background maintenance: space reuse, compaction, compression
- Additional query operators and aggregation

## Source Layout

The codebase is organized by domain to keep storage, server, and replication concerns separate:

- `src/api/`: connection/session/cursor APIs and handle cache
- `src/core/`: shared types/utilities (BSON codec, document helpers, errors)
- `src/storage/`: on-disk storage (block file, B-tree, WAL, main table)
- `src/index/`: secondary index implementation and key encoding
- `src/durability/`: WAL and durability backend
- `src/txn/`: global transaction state, MVCC, recovery units
- `src/raft/`: RAFT replication implementation
- `src/recovery/`: recovery manager for crash recovery
- `src/schema/`: schema catalog and metadata
- `src/document_ops/`: document-level operations
- `src/collection_write_path.rs`: internal Mongo-style document/index write orchestration
- `src/document_query.rs`: internal document/query helpers used by the server path
- `src/store_write_path.rs`: store-level write path
- `src/server/`: MongoDB wire-protocol server and command handlers
- `src/bin/`: server binary entry point

Integration tests are grouped under `tests/`:
- `tests/storage_suite.rs`: storage layer tests (`tests/storage/`)
- `tests/connection_suite.rs`: connection API tests (`tests/connection/`)
- `tests/public_api_*.rs`: public API surface tests and trybuild UI tests (`tests/ui/`)
- `tests/bench_wire_ab_smoke.rs`: wire protocol benchmark smoke tests

See the `examples/` directory for additional standalone programs demonstrating recovery, checkpointing, and WAL behavior.

## Documentation

See the [docs/](docs/) directory for detailed architecture documentation:
- [Server Usage](docs/server.md): running the MongoDB-compatible server
- [Design Decisions](docs/decisions.md): recorded architectural decisions
- [MVCC Proposal](docs/mvcc_proposal.md): multi-version concurrency design
- [Global WAL Architecture](docs/global_wal_architecture.md): write-ahead logging design

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
    session.create("table:test")?;

    // Execute one transactional write unit
    let mut write_unit = session.transaction()?;
    let mut cursor = write_unit.open_cursor("table:test")?;
    cursor.insert(b"alice", b"age=30")?;
    cursor.insert(b"bob", b"age=25")?;
    write_unit.commit()?;

    // Read back values
    let mut cursor = session.open_cursor("table:test")?;
    println!("{:?}", cursor.get(b"alice")?);
    println!("{:?}", cursor.get(b"bob")?);
    Ok(())
}
```

Schema objects are URI-based at the low-level API:
- `session.create("table:users")?`

Index creation is handled by the Mongo-style internal write/schema path rather than the
WT-style public `Session` API.

#### As a Server
For interactive usage with mongosh, see [Server Documentation](docs/server.md).

Run the server directly:

```bash
wrongodb-server
```

You can also set the listen address with `--addr`, `--port`, `WRONGO_ADDR`, or `WRONGO_PORT`.
For benchmark isolation, you can set data path with `--db-path` or `WRONGO_DB_PATH`.

Run tests with `cargo test`.

## Development

```bash
# Build
cargo build
just build

# Run tests
cargo test
just test

# Check compilation (faster than full build)
cargo check
just check

# Lint (clippy with warnings as errors)
cargo clippy -- -D warnings
just clippy

# Format code
cargo fmt
just fmt

# Run all checks (check, test, clippy, fmt)
just all
```

## Benchmarking

Run wire-protocol A/B benchmarks (WrongoDB vs MongoDB via Docker):

```bash
just bench-wire-ab
```

This runs `bench_wire_ab` with defaults:

- warmup: `15s`
- measure: `60s`
- concurrency: `1,4,8,16,32,64`
- mongo image: `mongo:7.0`

Artifacts are written to:

- `target/benchmarks/wire_ab/results.csv`
- `target/benchmarks/wire_ab/gate.json`
- `target/benchmarks/wire_ab/summary.md`

## Release

Create a version tag and push it to trigger the GitHub Actions release workflow (uses `cargo-dist`):

```bash
git tag v0.1.0
git push origin v0.1.0
```

The workflow builds artifacts (archives, installers) and publishes them to GitHub Releases.

## License

MIT
