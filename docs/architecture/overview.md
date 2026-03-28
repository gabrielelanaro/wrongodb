# Architecture Overview

WrongoDB currently has two externally visible personalities:

- a WT-like local storage API exposed as `Connection`, `Session`, and `TableCursor`
- a MongoDB wire-protocol server layered on top of that storage API

The repository does not currently contain an active RAFT or replication subsystem. Older references to RAFT in the README were historical and have been removed from the maintained architecture entry points.

## Layer map

```text
MongoDB wire protocol
  -> src/server/*
  -> src/api/database_context.rs
  -> src/catalog/*
  -> src/document_query.rs
  -> src/collection_write_path.rs
  -> src/storage/api/*
  -> src/storage/* and src/txn/*
```

## Current subsystem boundaries

### Public storage API

Files:
- `src/lib.rs`
- `src/storage/api/connection.rs`
- `src/storage/api/session.rs`
- `src/storage/api/cursor/*`

Responsibilities:
- open a database rooted at a filesystem path
- create short-lived sessions
- open file, table, and index cursors
- coordinate checkpoints through `Session`

This layer is intentionally local and storage-shaped. It does not own BSON document semantics or MongoDB collection behavior.

### Server and document layer

Files:
- `src/server/*`
- `src/api/database_context.rs`
- `src/document_query.rs`
- `src/collection_write_path.rs`

Responsibilities:
- accept MongoDB wire-protocol requests
- translate commands into storage sessions
- implement collection CRUD, `createCollection`, `createIndexes`, and query planning over the storage API

### Durable catalog layer

Files:
- `src/catalog/*`
- `src/storage/metadata_store.rs`

Responsibilities:
- `MetadataStore` persists storage-engine metadata in `metadata.wt`
- `CollectionCatalog` persists Mongo-visible collection and index state in `file:_catalog.wt`

The split is deliberate:
- storage metadata answers "which logical URI maps to which physical store and schema?"
- collection catalog answers "which collections and indexes exist from the server's point of view?"

### Storage engine internals

Files:
- `src/storage/table.rs`
- `src/storage/row.rs`
- `src/storage/btree/*`
- `src/storage/page_store/*`
- `src/storage/block/*`

Responsibilities:
- row encoding and table metadata
- B+tree reads, writes, and range scans
- page cache and copy-on-write page management
- block allocation, checkpoint roots, and on-disk extent reuse

### Transactions and recovery

Files:
- `src/txn/*`
- `src/storage/log_manager.rs`
- `src/storage/wal.rs`
- `src/storage/checkpoint/*`
- `src/storage/recovery/*`
- `src/server/recovery/*`

Responsibilities:
- global transaction state and snapshot visibility
- transaction-local write tracking and WAL buffering
- multi-store checkpoint coordination
- WAL replay and startup reconciliation

## What was outdated

The repo had architecture information, but it was scattered and partially stale:

- `README.md` referenced missing files: `docs/server.md`, `docs/mvcc_proposal.md`, and `docs/global_wal_architecture.md`
- `README.md` listed modules that no longer exist at those paths: `src/durability/`, `src/raft/`, `src/recovery/`, and `src/schema/`
- `NOTES.md`, `notebooks/`, and `blog/` contained useful design context, but they mixed current behavior with investigations and historical narrative

Those materials are still useful, but they are now explicitly secondary to the docs in this directory.
