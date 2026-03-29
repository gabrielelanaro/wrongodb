# Server Stack

This document describes the current architecture above the storage API.

## Startup flow

Server startup currently looks like this:

1. open a `Connection`
2. run `audit_catalog`
3. build a `DatabaseContext`
4. accept MongoDB wire-protocol requests
5. dispatch each command through `CommandRegistry`

The startup reconciliation step matters because the server depends on both storage metadata and the durable collection catalog being internally consistent.

## Main server services

### `DatabaseContext`

File:
- `src/api/database_context.rs`

Role:
- groups the server-only services layered on top of one storage `Connection`

It currently owns:

- `CollectionCatalog`
- `DocumentQuery`
- `DdlPath`
- `WriteOps`
- `ReplicationCoordinator`

This keeps the MongoDB command handlers thin and prevents server-only policy from leaking into the storage API.

### `CollectionCatalog`

Files:
- `src/catalog/catalog_store.rs`
- `src/catalog/collection_catalog.rs`

Role:
- durable Mongo-visible catalog stored in `file:_catalog.wt`

It persists collection definitions that include:

- collection name
- table URI
- UUID
- options document
- declared `storageColumns`
- durable secondary index definitions and readiness

This is intentionally separate from `MetadataStore`. The server catalog answers collection-level questions; it does not replace storage metadata.

### `DocumentQuery`

File:
- `src/document_query.rs`

Role:
- read path for collection queries

Current behavior:

- resolves the committed collection definition from the catalog
- opens a storage `TableCursor`
- performs direct `_id` lookup when possible
- performs a simple single-index equality plan when a ready secondary index matches the filter
- falls back to a full table scan otherwise

This is still a deliberately small query planner.

### `DdlPath`

File:
- `src/api/ddl_path.rs`

Role:
- top-level DDL path above the storage API

Current behavior:

- gates collection and index creation on writable-primary state
- creates collections with explicit `storageColumns`
- registers secondary indexes in the durable collection catalog
- repairs missing storage metadata for already-ready indexes
- delegates physical index build/backfill to `CollectionCatalog::build_and_mark_index_ready`

### `WriteOps`

File:
- `src/write_ops/mod.rs`

Role:
- top-level CRUD executor modeled after MongoDB's `write_ops_exec`

Current behavior:

- gates writes on writable-primary state
- opens one `Session`
- runs one `Session::with_transaction(...)` per logical write operation from the command layer
- calls `CollectionWritePath` only through its in-transaction mutation methods

### `CollectionWritePath`

File:
- `src/collection_write_path.rs`

Role:
- low-level in-transaction collection mutator

Current behavior:

- validates and normalizes documents
- encodes storage rows
- performs insert, update, and delete through the storage API
- calls the replication observer after each logical document mutation

### `ReplicationObserver` and oplog

Files:
- `src/replication/mod.rs`

Role:
- append one logical oplog row per replicated document mutation

Current behavior:

- stores oplog entries in the internal table `table:__oplog`
- reserves `OpTime { term, index }` through `ReplicationCoordinator`
- writes oplog rows inside the same storage transaction as the user-data mutation
- supports `GenerateOplog` and `SuppressOplog` modes so a future follower-apply path can reuse `CollectionWritePath` without recursively re-oplogging

## Current write layering

The primary write path deliberately crosses five layers:

1. command handler dispatch in `src/server/*`
2. top-level write orchestration in `WriteOps`
3. document-local mutation in `CollectionWritePath`
4. logical oplog capture in `ReplicationObserver`
5. row/table/index mutation in `Session` and `TableCursor`

That split is important:

- the command/write layer owns primary admission and transaction scope
- the collection write layer owns BSON/document mutation semantics
- the replication layer owns oplog shape and position allocation
- the catalog owns committed collection/index definitions
- the storage API owns transactional row mutation and checkpoint/recovery behavior

## Current read layering

The read path is simpler:

1. command handler asks `DocumentQuery`
2. `DocumentQuery` resolves catalog state
3. `DocumentQuery` executes against `Session` cursors
4. storage rows are decoded back into documents

The server layer never reads raw files directly.

## Important invariants

- MongoDB command handlers should stay thin and delegate to `DatabaseContext` services
- mutating commands should call `WriteOps` or `DdlPath`, not `CollectionWritePath` directly
- document semantics should not move down into the storage engine
- storage metadata and collection catalog must remain distinct persistence planes
- index readiness is a server-level concern even though index stores are built through `Session`
- collection creation is explicit; writes do not auto-create collections
- the oplog is logical replication state in `table:__oplog`, not part of `storage/wal`
- `CollectionWritePath` should not open its own top-level transactions

## Extension guidelines

When adding server features:

- extend `CollectionCatalog` if the change affects durable collection metadata
- extend `DocumentQuery` if the change affects read planning or result materialization
- extend `WriteOps` if the change affects top-level CRUD orchestration
- extend `DdlPath` if the change affects top-level DDL semantics
- extend `CollectionWritePath` if the change affects low-level document mutation
- extend `src/replication/*` if the change affects oplog shape, write admission, or future follower apply
- avoid adding BSON-specific rules to `Session` or lower storage modules unless the storage abstraction itself is changing

For the current user-visible command surface and query limitations, see [`command-query-capabilities.md`](command-query-capabilities.md).
